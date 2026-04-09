from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class CallPaths:
    call_id: str
    root: Path
    audio_path: Path
    transcript_path: Path
    summary_path: Path
    metadata_path: Path


class Storage:
    def __init__(self, data_dir: Path, recordings_dir: Path) -> None:
        self.data_dir = data_dir
        self.recordings_dir = recordings_dir

    def prepare_call(self, call_id: str, extension: str = ".ogg") -> CallPaths:
        root = self.data_dir / call_id
        root.mkdir(parents=True, exist_ok=True)
        audio_path = root / f"audio{extension}"
        transcript_path = root / f"transcript_{call_id}.txt"
        summary_path = root / "summary.md"
        metadata_path = root / "metadata.json"
        return CallPaths(
            call_id=call_id,
            root=root,
            audio_path=audio_path,
            transcript_path=transcript_path,
            summary_path=summary_path,
            metadata_path=metadata_path,
        )
