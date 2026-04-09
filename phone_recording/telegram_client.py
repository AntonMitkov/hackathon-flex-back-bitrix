from __future__ import annotations

import asyncio
import json
import logging
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import httpx
from pyrogram import Client
from pyrogram.errors import BadRequest, FloodWait, Forbidden, PeerIdInvalid
from pyrogram.raw.functions.channels import GetFullChannel
from pyrogram.raw.functions.messages import GetFullChat
from pyrogram.raw.functions.phone import GetGroupCall, GetGroupParticipants, ToggleGroupCallRecord
from pyrogram.raw.types import InputPeerChannel, InputPeerChat
from pyrogram.types import Message

from phone_recording.models import ProcessedCall
from phone_recording.storage import CallPaths, Storage
from phone_recording.transcription import Transcriber, render_transcript
from phone_recording.openrouter_client import OpenRouterSummarizer

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ActiveRecording:
    call_id: str
    chat_id: int
    chat_title: str
    started_at: datetime
    report_chat_id: int | str
    recording_expected: bool
    recording_source: Literal["started_by_service", "already_active", "unavailable"]
    saved_messages_anchor_id: int
    last_seen_at: datetime
    missing_since: datetime | None = None
    recording_started_at: datetime | None = None
    unavailable_reason: str | None = None
    participant_ids: list[int] | None = None


@dataclass(slots=True)
class GroupCallContext:
    input_call: object
    full_call: object


class TelegramCallService:
    def __init__(
        self,
        client: Client,
        storage: Storage,
        transcriber: Transcriber,
        summarizer: OpenRouterSummarizer,
        target_chats: list[str],
        report_chat_id: str | None,
        poll_interval_seconds: int,
        bridge_callback_url: str | None = None,
    ) -> None:
        self.client = client
        self.storage = storage
        self.transcriber = transcriber
        self.summarizer = summarizer
        self.target_chats = target_chats
        self.report_chat_id = self._normalize_report_chat_id(report_chat_id)
        self.poll_interval_seconds = poll_interval_seconds
        self.bridge_callback_url = bridge_callback_url.rstrip("/") if bridge_callback_url else None
        self.active_recordings: dict[int, ActiveRecording] = {}
        self._dialog_cache: dict[str, int] = {}
        self._call_disappearance_grace_seconds = 0
        self._consumed_recording_message_ids: set[int] = set()
        self._unsupported_basic_group_call_chats: set[int] = set()

    async def run(self) -> None:
        await self.client.start()
        await self._refresh_dialog_cache()
        logger.info("Telegram client started. Watching chats: %s", ", ".join(self.target_chats))
        try:
            while True:
                await self._poll_once()
                await asyncio.sleep(self.poll_interval_seconds)
        finally:
            await self.client.stop()

    async def _poll_once(self) -> None:
        for target in self.target_chats:
            try:
                chat = await self._resolve_target_chat(target)
                call_ctx = await self._get_group_call(chat.id)
            except FloodWait as exc:
                await asyncio.sleep(exc.value)
                continue
            except Exception:
                logger.exception("Failed to poll target chat %s", target)
                continue

            if call_ctx and chat.id not in self.active_recordings:
                logger.info("Detected active group call in chat %s (%s)", chat.title, chat.id)
                try:
                    self.active_recordings[chat.id] = await self._start_recording(
                        chat.id,
                        chat.title or str(chat.id),
                        call_ctx,
                    )
                except Exception as exc:
                    logger.exception("Failed to start recording for chat %s", chat.id)
                    await self._notify_error(chat.id, f"Failed to start recording: {exc}")
            elif call_ctx and chat.id in self.active_recordings:
                try:
                    await self._refresh_active_recording(
                        self.active_recordings[chat.id],
                        call_ctx,
                    )
                except Exception:
                    logger.exception("Failed to refresh call state for chat %s", chat.id)
            elif not call_ctx and chat.id in self.active_recordings:
                recording = self.active_recordings[chat.id]
                if not self._should_finalize_missing_call(recording):
                    continue
                self.active_recordings.pop(chat.id, None)
                logger.info("Group call finished in chat %s (%s)", recording.chat_title, recording.chat_id)
                try:
                    await self._finalize_recording(recording)
                except Exception as exc:
                    logger.exception("Failed to finalize recording for chat %s", recording.chat_id)
                    await self._notify_error(recording.report_chat_id, f"Failed to process call {recording.call_id}: {exc}")

    async def _get_group_call(self, chat_id: int) -> GroupCallContext | None:
        input_peer = await self.client.resolve_peer(chat_id)

        if isinstance(input_peer, InputPeerChannel):
            full_chat = await self.client.invoke(GetFullChannel(channel=input_peer))
            input_call = getattr(full_chat.full_chat, "call", None)
        elif isinstance(input_peer, InputPeerChat):
            full_chat = await self.client.invoke(GetFullChat(chat_id=input_peer.chat_id))
            input_call = getattr(full_chat.full_chat, "call", None)
        else:
            return None

        if not input_call:
            return None
        try:
            full_call = await self.client.invoke(GetGroupCall(call=input_call, limit=1))
        except BadRequest as exc:
            combined_error = self._extract_rpc_error(exc)
            if "GROUPCALL_INVALID" in combined_error:
                if isinstance(input_peer, InputPeerChat):
                    if chat_id not in self._unsupported_basic_group_call_chats:
                        self._unsupported_basic_group_call_chats.add(chat_id)
                        logger.warning(
                            "Chat %s is a basic group with a call reference, but Telegram rejects phone.GetGroupCall. "
                            "Server-side recording appears unsupported for this basic group; migrate it to a supergroup "
                            "and use the new -100... chat id.",
                            chat_id,
                        )
                else:
                    self._unsupported_basic_group_call_chats.discard(chat_id)
                logger.info(
                    "Telegram returned a stale group call reference for chat %s: %s",
                    chat_id,
                    combined_error,
                )
                return None
            raise
        if not self._is_live_group_call(full_call):
            return None
        return GroupCallContext(input_call=input_call, full_call=full_call)

    async def _refresh_dialog_cache(self) -> None:
        cache: dict[str, int] = {}
        async for dialog in self.client.get_dialogs():
            chat = dialog.chat
            chat_id = chat.id
            cache[str(chat_id)] = chat_id
            cache[self._compact_chat_id(chat_id)] = chat_id
            if chat.username:
                cache[chat.username.lower()] = chat_id
                cache[f"@{chat.username.lower()}"] = chat_id
        self._dialog_cache = cache
        logger.info("Dialog cache loaded with %s entries", len(cache))

    async def _resolve_target_chat(self, target: str):
        normalized = target.strip()
        try:
            return await self.client.get_chat(normalized)
        except Exception:
            pass

        resolved_id = self._dialog_cache.get(normalized)
        if resolved_id is None:
            resolved_id = self._dialog_cache.get(normalized.lower())
        if resolved_id is None and normalized.lstrip("-").isdigit():
            resolved_id = self._dialog_cache.get(self._compact_chat_id(int(normalized)))

        if resolved_id is not None:
            return await self.client.get_chat(resolved_id)

        await self._refresh_dialog_cache()
        resolved_id = self._dialog_cache.get(normalized) or self._dialog_cache.get(normalized.lower())
        if resolved_id is None and normalized.lstrip("-").isdigit():
            resolved_id = self._dialog_cache.get(self._compact_chat_id(int(normalized)))
        if resolved_id is not None:
            return await self.client.get_chat(resolved_id)

        raise RuntimeError(
            "Unknown target chat. Open this chat with the same Telegram account first, "
            "or use its @username / invite link / full supergroup id with -100 prefix."
        )

    def _compact_chat_id(self, chat_id: int) -> str:
        value = str(chat_id)
        if value.startswith("-100"):
            return f"-{value[4:]}"
        return value

    def _normalize_report_chat_id(self, report_chat_id: str | None) -> int | str | None:
        if not report_chat_id:
            return None
        normalized = report_chat_id.strip()
        if normalized.lower() == "me":
            return "me"
        if normalized.lstrip("-").isdigit():
            return int(normalized)
        return normalized

    async def _start_recording(self, chat_id: int, title: str, group_call: GroupCallContext) -> ActiveRecording:
        now = datetime.now(UTC)
        report_chat_id = self.report_chat_id or chat_id
        call_id = str(group_call.input_call.id)
        recording_started_at = self._get_recording_started_at(group_call)
        saved_messages_anchor_id = await self._get_saved_messages_anchor_id()
        participant_ids = await self._get_call_participant_ids(chat_id)

        active = ActiveRecording(
            call_id=call_id,
            chat_id=chat_id,
            chat_title=title,
            started_at=now,
            report_chat_id=report_chat_id,
            recording_expected=False,
            recording_source="unavailable",
            saved_messages_anchor_id=saved_messages_anchor_id,
            last_seen_at=now,
            recording_started_at=recording_started_at,
            participant_ids=participant_ids or None,
        )

        if recording_started_at is not None:
            active.recording_expected = True
            active.recording_source = "already_active"
            logger.info("Server-side recording is already active for chat %s", chat_id)
            await self._notify_status(
                report_chat_id,
                (
                    f"Detected active call in **{title}**.\n\n"
                    "Telegram already has server-side recording enabled, so I will wait for the final file.\n\n"
                    f"`call_id`: `{call_id}`"
                ),
            )
            return active

        logger.info("Attempting to start Telegram server-side recording for chat %s", chat_id)
        try:
            await self.client.invoke(
                ToggleGroupCallRecord(
                    call=group_call.input_call,
                    start=True,
                    title=f"recording_{group_call.input_call.id}",
                    video=False,
                )
            )
            active.recording_expected = True
            active.recording_source = "started_by_service"
            logger.info("Recording started for chat %s, call id %s", chat_id, group_call.input_call.id)
            await self._notify_status(
                report_chat_id,
                f"Detected active call in **{title}** and requested Telegram recording.\n\n`call_id`: `{call_id}`",
            )
            return active
        except BadRequest as exc:
            combined_error = self._extract_rpc_error(exc)
            if "GROUPCALL_NOT_MODIFIED" in combined_error:
                active.recording_expected = True
                active.recording_source = "already_active"
                logger.info(
                    "Recording was already active or unchanged for chat %s, continuing with active call tracking",
                    chat_id,
                )
                await self._notify_status(
                    report_chat_id,
                    (
                        f"Detected active call in **{title}**.\n\n"
                        "Telegram reports that recording is already running, so I will wait for the final file.\n\n"
                        f"`call_id`: `{call_id}`"
                    ),
                )
                return active
            if "GROUPCALL_INVALID" in combined_error:
                active.unavailable_reason = combined_error
                logger.warning(
                    "Telegram rejected recording for chat %s: %s",
                    chat_id,
                    combined_error,
                )
                await self._notify_status(
                    report_chat_id,
                    (
                        f"Detected active call in **{title}**, but Telegram rejected server-side recording.\n\n"
                        f"`call_id`: `{call_id}`\n"
                        f"`error`: `{combined_error}`\n\n"
                        "I will stop retrying for this call and wait until it ends."
                    ),
                )
                return active
            raise
        except Forbidden as exc:
            active.unavailable_reason = self._extract_rpc_error(exc)
            logger.warning("Recording is forbidden for chat %s: %s", chat_id, active.unavailable_reason)
            await self._notify_status(
                report_chat_id,
                (
                    f"Detected active call in **{title}**, but this account has no permission to start Telegram recording.\n\n"
                    f"`call_id`: `{call_id}`\n"
                    f"`error`: `{active.unavailable_reason}`\n\n"
                    "I will stop retrying for this call and wait until it ends."
                ),
            )
            return active

    async def _finalize_recording(self, recording: ActiveRecording) -> None:
        if not recording.recording_expected:
            logger.info(
                "Skipping finalization for call %s because no recording file is expected",
                recording.call_id,
            )
            return

        message = await self._wait_for_saved_recording(recording)
        if message is None:
            raise RuntimeError(
                "Telegram finished the call but no recording file was found in Saved Messages"
            )
        extension = self._infer_media_extension(message)
        call_paths = self.storage.prepare_call(recording.call_id, extension=extension)
        logger.info("Recording file found in Saved Messages for call %s", recording.call_id)
        await self._download_recording(message, call_paths)
        processed = await self._process_call(recording, call_paths)
        await self._send_report(processed)

    async def _wait_for_saved_recording(
        self,
        recording: ActiveRecording,
        timeout_seconds: int = 300,
    ) -> Message | None:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while asyncio.get_running_loop().time() < deadline:
            fallback_candidate: Message | None = None
            async for message in self.client.get_chat_history("me", limit=100):
                if message.id in self._consumed_recording_message_ids:
                    continue
                if message.id <= recording.saved_messages_anchor_id:
                    break
                if self._matches_recording_message(message, recording.call_id):
                    self._consumed_recording_message_ids.add(message.id)
                    return message
                if fallback_candidate is None and self._is_fallback_recording_candidate(message, recording):
                    fallback_candidate = message
            if fallback_candidate is not None:
                self._consumed_recording_message_ids.add(fallback_candidate.id)
                return fallback_candidate
            await asyncio.sleep(5)
        return None

    def _matches_recording_message(self, message: Message, call_id: str) -> bool:
        if not self._is_recording_media(message):
            return False
        name = None
        if message.document:
            name = message.document.file_name
        elif message.audio:
            name = message.audio.file_name
        elif message.video:
            name = message.video.file_name
        caption = message.caption or ""
        joined = f"{name or ''} {caption}".lower()
        return "recording_" in joined or call_id in joined

    async def _download_recording(self, message: Message, call_paths: CallPaths) -> None:
        temp_path = Path(
            await self.client.download_media(
                message,
                file_name=str(self.storage.recordings_dir / f"{call_paths.call_id}{call_paths.audio_path.suffix}"),
            )
        )
        shutil.copy2(temp_path, call_paths.audio_path)
        logger.info("Downloaded recording to %s", call_paths.audio_path)

    async def _refresh_active_recording(
        self,
        recording: ActiveRecording,
        group_call: GroupCallContext,
    ) -> None:
        current_call_id = str(group_call.input_call.id)
        if current_call_id != recording.call_id:
            logger.info(
                "Call id changed in chat %s from %s to %s, resetting tracking",
                recording.chat_id,
                recording.call_id,
                current_call_id,
            )
            self.active_recordings.pop(recording.chat_id, None)
            replacement = await self._start_recording(
                recording.chat_id,
                recording.chat_title,
                group_call,
            )
            self.active_recordings[recording.chat_id] = replacement
            return

        recording.last_seen_at = datetime.now(UTC)
        recording.missing_since = None

        # Обновляем список участников звонка пока он активен
        participant_ids = await self._get_call_participant_ids(recording.chat_id)
        if participant_ids:
            recording.participant_ids = participant_ids

        if not recording.recording_expected:
            recording_started_at = self._get_recording_started_at(group_call)
            if recording_started_at is not None:
                recording.recording_expected = True
                recording.recording_source = "already_active"
                recording.recording_started_at = recording_started_at
                logger.info(
                    "Recording became active later for chat %s, call %s",
                    recording.chat_id,
                    recording.call_id,
                )
                await self._notify_status(
                    recording.report_chat_id,
                    (
                        f"Telegram recording became active for **{recording.chat_title}** while the call was still running.\n\n"
                        f"`call_id`: `{recording.call_id}`"
                    ),
                )

    def _should_finalize_missing_call(self, recording: ActiveRecording) -> bool:
        now = datetime.now(UTC)
        if recording.missing_since is None:
            recording.missing_since = now
            logger.info(
                "Call %s is temporarily missing in chat %s, waiting %s seconds before finalizing",
                recording.call_id,
                recording.chat_id,
                self._call_disappearance_grace_seconds,
            )
            return False

        missing_for = (now - recording.missing_since).total_seconds()
        if missing_for < self._call_disappearance_grace_seconds:
            return False
        return True

    def _extract_rpc_error(self, exc: Exception) -> str:
        error_markers = {
            str(exc),
            getattr(exc, "ID", ""),
            getattr(exc, "MESSAGE", ""),
            exc.__class__.__name__,
        }
        return " | ".join(marker for marker in error_markers if marker)

    def _get_recording_started_at(self, group_call: GroupCallContext) -> datetime | None:
        full_call = getattr(group_call.full_call, "call", None)
        timestamp = getattr(full_call, "record_start_date", None)
        if timestamp is None:
            return None
        return datetime.fromtimestamp(timestamp, tz=UTC)

    def _is_live_group_call(self, full_call: object) -> bool:
        call = getattr(full_call, "call", None)
        if call is None:
            return False
        if getattr(call, "schedule_date", None) is not None:
            return False
        participants_count = getattr(call, "participants_count", None)
        if participants_count is None:
            return True
        return participants_count > 0

    async def _get_saved_messages_anchor_id(self) -> int:
        async for message in self.client.get_chat_history("me", limit=1):
            return message.id
        return 0

    def _is_recording_media(self, message: Message) -> bool:
        if message.audio or message.voice or message.video:
            return True
        if not message.document:
            return False
        mime_type = (message.document.mime_type or "").lower()
        if mime_type.startswith("audio/") or mime_type.startswith("video/"):
            return True
        extension = Path(message.document.file_name or "").suffix.lower()
        return extension in {".ogg", ".oga", ".opus", ".mp3", ".m4a", ".wav", ".mp4", ".mov", ".mkv", ".webm"}

    def _is_fallback_recording_candidate(self, message: Message, recording: ActiveRecording) -> bool:
        if not self._is_recording_media(message):
            return False
        if message.id <= recording.saved_messages_anchor_id:
            return False
        message_date = message.date
        if message_date is None:
            return False
        baseline = recording.recording_started_at or recording.started_at
        return self._coerce_utc(message_date) >= self._coerce_utc(baseline)

    def _infer_media_extension(self, message: Message) -> str:
        media = message.document or message.audio or message.voice or message.video
        file_name = getattr(media, "file_name", None)
        if file_name:
            suffix = Path(file_name).suffix.lower()
            if suffix:
                return suffix

        mime_type = (getattr(media, "mime_type", None) or "").lower()
        if mime_type == "audio/ogg":
            return ".ogg"
        if mime_type in {"audio/mp4", "video/mp4"}:
            return ".mp4"
        if mime_type == "audio/mpeg":
            return ".mp3"
        return ".ogg"

    def _coerce_utc(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    async def _get_call_participant_ids(self, chat_id: int) -> list[int]:
        """Получает Telegram user IDs участников группового звонка."""
        try:
            input_peer = await self.client.resolve_peer(chat_id)
            if isinstance(input_peer, InputPeerChannel):
                full_chat = await self.client.invoke(GetFullChannel(channel=input_peer))
            elif isinstance(input_peer, InputPeerChat):
                full_chat = await self.client.invoke(GetFullChat(chat_id=input_peer.chat_id))
            else:
                print(f"[PARTICIPANTS] Chat {chat_id}: unsupported peer type {type(input_peer)}")
                return []

            input_call = getattr(full_chat.full_chat, "call", None)
            if not input_call:
                print(f"[PARTICIPANTS] Chat {chat_id}: no active call found")
                return []

            result = await self.client.invoke(
                GetGroupParticipants(
                    call=input_call,
                    ids=[],
                    sources=[],
                    offset="",
                    limit=100,
                )
            )
            user_ids = []
            for participant in getattr(result, "participants", []):
                peer = getattr(participant, "peer", None)
                user_id = getattr(peer, "user_id", None)
                if user_id:
                    user_ids.append(user_id)
            print(f"[PARTICIPANTS] Chat {chat_id}: found {len(user_ids)} participants → Telegram IDs: {user_ids}")
            logger.info("Got %d participant IDs for chat %s: %s", len(user_ids), chat_id, user_ids)
            return user_ids
        except Exception as exc:
            print(f"[PARTICIPANTS] Chat {chat_id}: ERROR getting participants → {exc}")
            logger.exception("Failed to get call participants for chat %s", chat_id)
            return []

    async def _process_call(self, recording: ActiveRecording, call_paths: CallPaths) -> ProcessedCall:
        transcript = await self.transcriber.transcribe(call_paths.audio_path)
        transcript_rendered = render_transcript(transcript)
        call_paths.transcript_path.write_text(transcript_rendered, encoding="utf-8")

        summary_markdown = await self.summarizer.summarize(transcript_rendered, recording.chat_title)
        call_paths.summary_path.write_text(summary_markdown + "\n", encoding="utf-8")

        metadata = {
            "call_id": recording.call_id,
            "chat_id": recording.chat_id,
            "chat_title": recording.chat_title,
            "started_at": recording.started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "recording_source": recording.recording_source,
            "recording_started_at": recording.recording_started_at.isoformat() if recording.recording_started_at else None,
            "unavailable_reason": recording.unavailable_reason,
            "audio_path": str(call_paths.audio_path),
            "transcript_path": str(call_paths.transcript_path),
            "summary_path": str(call_paths.summary_path),
        }
        call_paths.metadata_path.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return ProcessedCall(
            call_id=recording.call_id,
            chat_id=recording.chat_id,
            title=recording.chat_title,
            audio_path=call_paths.audio_path,
            transcript_path=call_paths.transcript_path,
            summary_path=call_paths.summary_path,
            summary_markdown=summary_markdown,
            transcript_text=transcript_rendered,
            metadata=metadata,
            participant_telegram_ids=recording.participant_ids,
        )

    async def _send_report(self, processed: ProcessedCall) -> None:
        # ── Отправляем в bridge (Битрикс24) ──
        print(f"[REPORT] bridge_callback_url = {self.bridge_callback_url!r}")
        print(f"[REPORT] participant_telegram_ids = {processed.participant_telegram_ids}")
        if self.bridge_callback_url:
            await self._send_to_bridge(processed)
        else:
            print("[REPORT] SKIPPING bridge — BRIDGE_CALLBACK_URL is not set!")

        # ── Отправляем в Telegram (как раньше) ──
        text = (
            f"Резюме звонка: {processed.title}\n\n"
            f"{processed.summary_markdown}"
        )
        target_chat = processed.chat_id if self.report_chat_id is None else self.report_chat_id
        try:
            await self.client.send_message(target_chat, text)
            await self.client.send_document(
                target_chat,
                document=str(processed.transcript_path),
                caption=f"Транскрипция звонка: {processed.title}",
            )
        except Exception:
            logger.exception("Failed to send report to chat %s, falling back to Saved Messages", target_chat)
            await self.client.send_message(
                "me",
                f"Не удалось отправить в чат {target_chat}.\n\nЗвонок: {processed.title}\n\n{text}",
            )
            await self.client.send_document(
                "me",
                document=str(processed.transcript_path),
                caption=f"Транскрипция звонка: {processed.title}",
            )
        logger.info("Sent summary and transcript for call %s", processed.call_id)

    async def _send_to_bridge(self, processed: ProcessedCall) -> None:
        """Отправляет summary звонка в bridge для маршрутизации в Битрикс24."""
        payload = {
            "call_id": processed.call_id,
            "chat_id": processed.chat_id,
            "chat_title": processed.title,
            "summary_markdown": processed.summary_markdown,
            "transcript_text": processed.transcript_text,
            "started_at": processed.metadata.get("started_at"),
            "finished_at": processed.metadata.get("finished_at"),
        }

        # Telegram IDs участников звонка
        if processed.participant_telegram_ids:
            payload["participant_telegram_ids"] = processed.participant_telegram_ids
            print(f"[BRIDGE] Sending call summary with participant Telegram IDs: {processed.participant_telegram_ids}")
        else:
            print("[BRIDGE] WARNING: No participant Telegram IDs available!")

        # Имена участников из транскрипции (для обратной совместимости)
        participants = set()
        for line in (processed.transcript_text or "").split("\n"):
            if "] " in line:
                after_bracket = line.split("] ", 1)[-1]
                if ": " in after_bracket:
                    speaker = after_bracket.split(": ", 1)[0].strip()
                    if speaker and speaker != "Speaker":
                        participants.add(speaker)
        if participants:
            payload["participants"] = list(participants)

        url = f"{self.bridge_callback_url}/call-summary"
        print(f"[BRIDGE] POST {url}")
        print(f"[BRIDGE] Payload keys: {list(payload.keys())}")

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(url, json=payload)
            if resp.is_success:
                result = resp.json()
                print(f"[BRIDGE] SUCCESS: {result}")
                logger.info("Bridge accepted call summary: %s", result)
            else:
                print(f"[BRIDGE] REJECTED: {resp.status_code} {resp.text[:300]}")
                logger.warning("Bridge rejected call summary: %s %s", resp.status_code, resp.text[:200])
        except Exception as exc:
            print(f"[BRIDGE] ERROR: {exc}")
            logger.exception("Failed to send call summary to bridge at %s", self.bridge_callback_url)

    async def _notify_error(self, chat_id: int, text: str) -> None:
        try:
            await self.client.send_message(chat_id, f"Recording service error:\n\n`{text}`")
        except PeerIdInvalid:
            logger.warning("Cannot send error to chat %s, falling back to Saved Messages", chat_id)
            try:
                await self.client.send_message("me", f"Recording service error for chat `{chat_id}`:\n\n`{text}`")
            except Exception:
                logger.exception("Failed to send error fallback to Saved Messages")
        except Exception:
            logger.exception("Failed to send error to chat %s, falling back to Saved Messages", chat_id)
            try:
                await self.client.send_message("me", f"Recording service error for chat `{chat_id}`:\n\n`{text}`")
            except Exception:
                logger.exception("Failed to send error fallback to Saved Messages")

    async def _notify_status(self, chat_id: int, text: str) -> None:
        try:
            await self.client.send_message(chat_id, text)
        except PeerIdInvalid:
            logger.warning("Cannot send status message to chat %s, falling back to Saved Messages", chat_id)
            try:
                await self.client.send_message("me", f"Status for chat `{chat_id}`:\n\n{text}")
            except Exception:
                logger.exception("Failed to send status fallback to Saved Messages")
        except Exception:
            logger.exception("Failed to send status message to chat %s, falling back to Saved Messages", chat_id)
            try:
                await self.client.send_message("me", f"Status for chat `{chat_id}`:\n\n{text}")
            except Exception:
                logger.exception("Failed to send status fallback to Saved Messages")
