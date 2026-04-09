from __future__ import annotations

import asyncio
import json
import logging
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pyrogram import Client
from pyrogram.errors import BadRequest
from pyrogram.raw.functions.channels import GetFullChannel
from pyrogram.raw.functions.phone import GetGroupCall, ToggleGroupCallRecord
from pyrogram.raw.types import InputPeerChannel, InputPeerChat
from pyrogram.types import Message

from phone_recording.models import ProcessedCall
from phone_recording.openrouter_client import OpenRouterSummarizer
from phone_recording.storage import CallPaths, Storage
from phone_recording.transcription import Transcriber, render_transcript


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ActiveCall:
    call_id: str
    chat_id: int
    title: str
    report_chat_id: int | str
    started_at: datetime
    saved_messages_anchor_id: int
    recording_enabled: bool = False
    recording_source: Literal["started_by_service", "already_active", "not_available"] = "not_available"
    recording_started_at: datetime | None = None
    missing_since: datetime | None = None


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
    ) -> None:
        self.client = client
        self.storage = storage
        self.transcriber = transcriber
        self.summarizer = summarizer
        self.target_chats = target_chats
        self.report_chat_id = self._normalize_report_chat_id(report_chat_id)
        self.poll_interval_seconds = poll_interval_seconds
        self.active_calls: dict[int, ActiveCall] = {}
        self._consumed_recording_message_ids: set[int] = set()
        self._warned_basic_groups: set[int] = set()
        self._missing_call_grace_seconds = max(self.poll_interval_seconds * 3, 45)

    async def run(self) -> None:
        await self.client.start()
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
                chat = await self.client.get_chat(target.strip())
                call_ctx = await self._get_group_call(chat.id)
                active = self.active_calls.get(chat.id)

                if call_ctx is None:
                    if active and self._should_finalize_missing_call(active):
                        self.active_calls.pop(chat.id, None)
                        await self._finalize_call(active)
                    continue

                if active is None:
                    self.active_calls[chat.id] = await self._start_recording(
                        chat.id,
                        chat.title or str(chat.id),
                        call_ctx,
                    )
                    continue

                active.missing_since = None

                current_call_id = str(call_ctx.input_call.id)
                if current_call_id != active.call_id:
                    self.active_calls.pop(chat.id, None)
                    await self._finalize_call(active)
                    self.active_calls[chat.id] = await self._start_recording(
                        chat.id,
                        chat.title or str(chat.id),
                        call_ctx,
                    )
                    continue

                if not active.recording_enabled:
                    recording_started_at = self._get_recording_started_at(call_ctx)
                    if recording_started_at is not None:
                        active.recording_enabled = True
                        active.recording_source = "already_active"
                        active.recording_started_at = recording_started_at
            except Exception:
                logger.exception("Failed to poll target chat %s", target)

    async def _get_group_call(self, chat_id: int) -> GroupCallContext | None:
        input_peer = await self.client.resolve_peer(chat_id)

        if isinstance(input_peer, InputPeerChat):
            if chat_id not in self._warned_basic_groups:
                self._warned_basic_groups.add(chat_id)
                logger.warning(
                    "Chat %s is a basic group. This simplified recorder only supports supergroups/channels "
                    "because Telegram often rejects phone.GetGroupCall for basic groups.",
                    chat_id,
                )
            return None

        if not isinstance(input_peer, InputPeerChannel):
            return None

        full_chat = await self.client.invoke(GetFullChannel(channel=input_peer))
        input_call = getattr(full_chat.full_chat, "call", None)
        if not input_call:
            return None

        try:
            full_call = await self.client.invoke(GetGroupCall(call=input_call, limit=1))
        except BadRequest as exc:
            if "GROUPCALL_INVALID" in self._extract_rpc_error(exc):
                return None
            raise

        if not self._is_live_group_call(full_call):
            return None

        return GroupCallContext(input_call=input_call, full_call=full_call)

    async def _start_recording(
        self,
        chat_id: int,
        title: str,
        group_call: GroupCallContext,
    ) -> ActiveCall:
        now = datetime.now(UTC)
        report_chat_id = self.report_chat_id or chat_id
        call_id = str(group_call.input_call.id)
        recording_started_at = self._get_recording_started_at(group_call)

        active = ActiveCall(
            call_id=call_id,
            chat_id=chat_id,
            title=title,
            report_chat_id=report_chat_id,
            started_at=now,
            saved_messages_anchor_id=await self._get_saved_messages_anchor_id(),
            recording_started_at=recording_started_at,
        )

        if recording_started_at is not None:
            active.recording_enabled = True
            active.recording_source = "already_active"
            logger.info("Recording is already active for chat %s", chat_id)
            return active

        try:
            await self.client.invoke(
                ToggleGroupCallRecord(
                    call=group_call.input_call,
                    start=True,
                    title=f"recording_{call_id}",
                    video=False,
                )
            )
            active.recording_enabled = True
            active.recording_source = "started_by_service"
            active.recording_started_at = now
            logger.info("Started recording for chat %s, call %s", chat_id, call_id)
        except Exception as exc:
            error = self._extract_rpc_error(exc)
            if "GROUPCALL_NOT_MODIFIED" in error:
                active.recording_enabled = True
                active.recording_source = "already_active"
                active.recording_started_at = now
                logger.info("Recording was already active for chat %s", chat_id)
            else:
                logger.warning("Could not start recording for chat %s: %s", chat_id, error)

        return active

    def _should_finalize_missing_call(self, active: ActiveCall) -> bool:
        now = datetime.now(UTC)
        if active.missing_since is None:
            active.missing_since = now
            return False
        return (now - active.missing_since).total_seconds() >= self._missing_call_grace_seconds

    async def _finalize_call(self, active: ActiveCall) -> None:
        if not active.recording_enabled:
            logger.info("Skipping call %s because recording was never enabled", active.call_id)
            return

        message = await self._wait_for_saved_recording(active)
        if message is None:
            raise RuntimeError(f"Recording file not found for call {active.call_id}")

        extension = self._infer_media_extension(message)
        call_paths = self.storage.prepare_call(active.call_id, extension=extension)
        await self._download_recording(message, call_paths)

        processed = await self._process_call(active, call_paths)
        await self._send_report(processed)

    async def _wait_for_saved_recording(
        self,
        active: ActiveCall,
        timeout_seconds: int = 300,
    ) -> Message | None:
        deadline = asyncio.get_running_loop().time() + timeout_seconds

        while asyncio.get_running_loop().time() < deadline:
            fallback_candidate: Message | None = None

            async for message in self.client.get_chat_history("me", limit=100):
                if message.id in self._consumed_recording_message_ids:
                    continue
                if message.id <= active.saved_messages_anchor_id:
                    break
                if self._matches_recording_message(message, active.call_id):
                    self._consumed_recording_message_ids.add(message.id)
                    return message
                if fallback_candidate is None and self._is_fallback_recording_candidate(message, active):
                    fallback_candidate = message

            if fallback_candidate is not None:
                self._consumed_recording_message_ids.add(fallback_candidate.id)
                return fallback_candidate

            await asyncio.sleep(5)

        return None

    def _matches_recording_message(self, message: Message, call_id: str) -> bool:
        if not self._is_recording_media(message):
            return False

        media = message.document or message.audio or message.voice or message.video
        file_name = getattr(media, "file_name", "") or ""
        caption = message.caption or ""
        haystack = f"{file_name} {caption}".lower()
        return "recording_" in haystack or call_id in haystack

    def _is_fallback_recording_candidate(self, message: Message, active: ActiveCall) -> bool:
        if not self._is_recording_media(message):
            return False
        if message.id <= active.saved_messages_anchor_id:
            return False
        if message.date is None:
            return False

        baseline = active.recording_started_at or active.started_at
        return message.date >= baseline

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

    async def _download_recording(self, message: Message, call_paths: CallPaths) -> None:
        downloaded_path = await self.client.download_media(
            message,
            file_name=str(self.storage.recordings_dir / f"{call_paths.call_id}{call_paths.audio_path.suffix}"),
        )
        if downloaded_path is None:
            raise RuntimeError("Telegram returned no file path while downloading recording")

        temp_path = Path(downloaded_path)
        shutil.copy2(temp_path, call_paths.audio_path)
        logger.info("Downloaded recording to %s", call_paths.audio_path)

    async def _process_call(self, active: ActiveCall, call_paths: CallPaths) -> ProcessedCall:
        transcript = await self.transcriber.transcribe(call_paths.audio_path)
        transcript_text = render_transcript(transcript)
        call_paths.transcript_path.write_text(transcript_text, encoding="utf-8")

        summary_markdown = await self.summarizer.summarize(transcript_text, active.title)
        call_paths.summary_path.write_text(summary_markdown + "\n", encoding="utf-8")

        metadata = {
            "call_id": active.call_id,
            "chat_id": active.chat_id,
            "chat_title": active.title,
            "started_at": active.started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "recording_source": active.recording_source,
            "recording_started_at": active.recording_started_at.isoformat() if active.recording_started_at else None,
            "audio_path": str(call_paths.audio_path),
            "transcript_path": str(call_paths.transcript_path),
            "summary_path": str(call_paths.summary_path),
        }
        call_paths.metadata_path.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        return ProcessedCall(
            call_id=active.call_id,
            chat_id=active.chat_id,
            title=active.title,
            audio_path=call_paths.audio_path,
            transcript_path=call_paths.transcript_path,
            summary_path=call_paths.summary_path,
            summary_markdown=summary_markdown,
            transcript_text=transcript_text,
            metadata=metadata,
        )

    async def _send_report(self, processed: ProcessedCall) -> None:
        target_chat = self.report_chat_id or processed.chat_id
        text = (
            f"## Call Summary: {processed.title}\n\n"
            f"{processed.summary_markdown}\n\n"
            f"`call_id`: `{processed.call_id}`"
        )

        try:
            await self.client.send_message(target_chat, text)
            await self.client.send_document(
                target_chat,
                document=str(processed.transcript_path),
                caption=f"Transcript for call {processed.call_id}",
            )
        except Exception:
            logger.exception("Failed to send report to %s, sending to Saved Messages", target_chat)
            await self.client.send_message("me", text)
            await self.client.send_document(
                "me",
                document=str(processed.transcript_path),
                caption=f"Transcript for call {processed.call_id}",
            )

    async def _get_saved_messages_anchor_id(self) -> int:
        async for message in self.client.get_chat_history("me", limit=1):
            return message.id
        return 0

    def _get_recording_started_at(self, group_call: GroupCallContext) -> datetime | None:
        call = getattr(group_call.full_call, "call", None)
        timestamp = getattr(call, "record_start_date", None)
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
        return participants_count is None or participants_count > 0

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

    def _normalize_report_chat_id(self, report_chat_id: str | None) -> int | str | None:
        if not report_chat_id:
            return None
        value = report_chat_id.strip()
        if value.lower() == "me":
            return "me"
        if value.lstrip("-").isdigit():
            return int(value)
        return value

    def _extract_rpc_error(self, exc: Exception) -> str:
        parts = [
            str(exc),
            getattr(exc, "ID", ""),
            getattr(exc, "MESSAGE", ""),
            exc.__class__.__name__,
        ]
        return " | ".join(part for part in parts if part)
