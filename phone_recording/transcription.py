from __future__ import annotations

import asyncio
from pathlib import Path

from faster_whisper import WhisperModel
from openai import AsyncOpenAI

from phone_recording.models import TranscriptResult, TranscriptSegment


def _format_ts(seconds: float) -> str:
    total_ms = max(int(seconds * 1000), 0)
    hours, remainder = divmod(total_ms, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    secs, milliseconds = divmod(remainder, 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}.{milliseconds:03d}"


def render_transcript(result: TranscriptResult) -> str:
    lines: list[str] = []
    for segment in result.segments:
        ts = f"[{_format_ts(segment.start)} - {_format_ts(segment.end)}]"
        if segment.speaker:
            lines.append(f"{ts} {segment.speaker}: {segment.text.strip()}")
        else:
            lines.append(f"{ts} {segment.text.strip()}")
    if not lines:
        lines.append(result.text.strip())
    return "\n".join(lines).strip() + "\n"


class Transcriber:
    def __init__(
        self,
        provider: str,
        model_name: str,
        language: str | None = None,
        api_base_url: str | None = None,
        api_key: str | None = None,
        api_model: str | None = None,
    ) -> None:
        self.provider = provider
        self.model_name = model_name
        self.language = language
        self.api_model = api_model
        self._whisper_model: WhisperModel | None = None
        self._client = (
            AsyncOpenAI(api_key=api_key, base_url=api_base_url.rstrip("/"))
            if api_base_url and api_key
            else None
        )

    async def transcribe(self, audio_path: Path) -> TranscriptResult:
        if self.provider == "api":
            return await self._transcribe_via_api(audio_path)
        return await self._transcribe_via_faster_whisper(audio_path)

    async def _transcribe_via_faster_whisper(self, audio_path: Path) -> TranscriptResult:
        if self._whisper_model is None:
            self._whisper_model = WhisperModel(self.model_name, device="auto", compute_type="auto")

        segments, info = await asyncio.to_thread(
            self._whisper_model.transcribe,
            str(audio_path),
            vad_filter=True,
            language=self.language,
            beam_size=5,
        )
        parsed_segments: list[TranscriptSegment] = []
        full_text: list[str] = []
        for segment in segments:
            text = segment.text.strip()
            parsed_segments.append(
                TranscriptSegment(start=segment.start, end=segment.end, text=text, speaker=None)
            )
            if text:
                full_text.append(text)
        transcript_text = " ".join(full_text).strip()
        if info.language and not self.language:
            self.language = info.language
        return TranscriptResult(text=transcript_text, segments=parsed_segments)

    async def _transcribe_via_api(self, audio_path: Path) -> TranscriptResult:
        if self._client is None or not self.api_model:
            raise RuntimeError("API transcription requested, but TRANSCRIBE_API_* variables are incomplete")

        with audio_path.open("rb") as audio_file:
            response = await self._client.audio.transcriptions.create(
                model=self.api_model,
                file=audio_file,
                language=self.language,
                response_format="verbose_json",
                timestamp_granularities=["segment"],
            )

        text = getattr(response, "text", "") or ""
        segments_raw = getattr(response, "segments", None) or []
        segments = [
            TranscriptSegment(
                start=float(segment["start"]),
                end=float(segment["end"]),
                text=segment["text"].strip(),
            )
            for segment in segments_raw
        ]
        return TranscriptResult(text=text.strip(), segments=segments)
