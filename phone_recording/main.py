from __future__ import annotations

import asyncio
import logging
import shutil
import struct

from pyrogram import Client
from pyrogram.errors import AuthKeyUnregistered

from phone_recording.config import Settings
from phone_recording.openrouter_client import OpenRouterSummarizer
from phone_recording.storage import Storage
from phone_recording.telegram_client import TelegramCallService
from phone_recording.transcription import Transcriber


def ensure_ffmpeg() -> None:
    if shutil.which("ffmpeg") is None:
        raise RuntimeError("ffmpeg is required and must be available in PATH")


async def async_main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = Settings.from_env()
    settings.ensure_directories()
    ensure_ffmpeg()

    client = Client(
        name="phone_recording",
        api_id=settings.telegram_api_id,
        api_hash=settings.telegram_api_hash,
        session_string=settings.telegram_string_session,
        in_memory=True,
    )
    storage = Storage(data_dir=settings.data_dir, recordings_dir=settings.recordings_dir)
    transcriber = Transcriber(
        provider=settings.transcribe_provider,
        model_name=settings.transcribe_model,
        language=settings.transcribe_language,
        api_base_url=settings.transcribe_api_base_url,
        api_key=settings.transcribe_api_key,
        api_model=settings.transcribe_api_model,
    )
    summarizer = OpenRouterSummarizer(
        api_key=settings.openrouter_api_key,
        default_model=settings.default_model,
        fallback_model=settings.fallback_model,
        http_referer=settings.openrouter_http_referer,
        x_title=settings.openrouter_x_title,
    )
    service = TelegramCallService(
        client=client,
        storage=storage,
        transcriber=transcriber,
        summarizer=summarizer,
        target_chats=settings.telegram_target_chats,
        report_chat_id=settings.telegram_report_chat_id,
        poll_interval_seconds=settings.poll_interval_seconds,
        bridge_callback_url=settings.bridge_callback_url,
    )
    try:
        await service.run()
    except (struct.error, AuthKeyUnregistered) as exc:
        raise RuntimeError(
            "Invalid TELEGRAM_STRING_SESSION. Generate a fresh Pyrogram StringSession "
            "with `uv run generate-telegram-session` and paste it into .env without truncation."
        ) from exc


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
