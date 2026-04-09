from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1].strip()
    return value


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass(slots=True)
class Settings:
    telegram_api_id: int
    telegram_api_hash: str
    telegram_string_session: str
    telegram_target_chats: list[str]
    telegram_report_chat_id: str | None
    openrouter_api_key: str
    openrouter_http_referer: str
    openrouter_x_title: str
    default_model: str
    fallback_model: str
    transcribe_provider: str
    transcribe_model: str
    transcribe_api_base_url: str | None
    transcribe_api_key: str | None
    transcribe_api_model: str | None
    transcribe_language: str | None
    poll_interval_seconds: int
    recordings_dir: Path
    data_dir: Path
    bridge_callback_url: str | None

    @classmethod
    def from_env(cls) -> "Settings":
        telegram_target_chats = _split_csv(os.getenv("TELEGRAM_TARGET_CHATS"))
        if not telegram_target_chats:
            raise ValueError("TELEGRAM_TARGET_CHATS must contain at least one chat id or username")

        return cls(
            telegram_api_id=int(_require_env("TELEGRAM_API_ID")),
            telegram_api_hash=_require_env("TELEGRAM_API_HASH"),
            telegram_string_session=_require_env("TELEGRAM_STRING_SESSION"),
            telegram_target_chats=telegram_target_chats,
            telegram_report_chat_id=os.getenv("TELEGRAM_REPORT_CHAT_ID"),
            openrouter_api_key=_require_env("OPENROUTER_API_KEY"),
            openrouter_http_referer=os.getenv("OPENROUTER_HTTP_REFERER", "http://localhost"),
            openrouter_x_title=os.getenv("OPENROUTER_X_TITLE", "Telegram Call Secretary MVP"),
            default_model=os.getenv("DEFAULT_MODEL", "google/gemini-flash-1.5"),
            fallback_model=os.getenv("FALLBACK_MODEL", "meta-llama/llama-3-70b-instruct"),
            transcribe_provider=os.getenv("TRANSCRIBE_PROVIDER", "faster_whisper"),
            transcribe_model=os.getenv("TRANSCRIBE_MODEL", "small"),
            transcribe_api_base_url=os.getenv("TRANSCRIBE_API_BASE_URL") or None,
            transcribe_api_key=os.getenv("TRANSCRIBE_API_KEY") or None,
            transcribe_api_model=os.getenv("TRANSCRIBE_API_MODEL") or None,
            transcribe_language=os.getenv("TRANSCRIBE_LANGUAGE") or None,
            poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "15")),
            recordings_dir=Path(os.getenv("RECORDINGS_DIR", "recordings")).resolve(),
            data_dir=Path(os.getenv("DATA_DIR", "data")).resolve(),
            bridge_callback_url=os.getenv("BRIDGE_CALLBACK_URL") or None,
        )

    def ensure_directories(self) -> None:
        self.recordings_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
