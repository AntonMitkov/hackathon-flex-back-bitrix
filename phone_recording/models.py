from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class TranscriptSegment:
    start: float
    end: float
    text: str
    speaker: str | None = None


@dataclass(slots=True)
class TranscriptResult:
    text: str
    segments: list[TranscriptSegment] = field(default_factory=list)


@dataclass(slots=True)
class ProcessedCall:
    call_id: str
    chat_id: int | str
    title: str
    audio_path: Path
    transcript_path: Path
    summary_path: Path
    summary_markdown: str
    transcript_text: str
    metadata: dict[str, Any]
    participant_telegram_ids: list[int] | None = None
