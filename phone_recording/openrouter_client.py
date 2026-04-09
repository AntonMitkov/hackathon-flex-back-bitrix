from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from openai import APIStatusError, AsyncOpenAI

# Импортируем anonymizer из корня проекта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from anonymizer import TextAnonymizer


SYSTEM_PROMPT = (
    "Ты — профессиональный секретарь. Проанализируй текст звонка и выдели:\n"
    "1) Основные темы обсуждения.\n"
    "2) Принятые решения.\n"
    "3) Список поручений (Action Items) с указанием ответственных.\n\n"
    "Ответ верни в виде простого текста БЕЗ Markdown-разметки (без #, *, `, --- и т.д.). "
    "Используй нумерацию и тире для списков."
)


class OpenRouterSummarizer:
    def __init__(
        self,
        api_key: str,
        default_model: str,
        fallback_model: str,
        http_referer: str,
        x_title: str,
    ) -> None:
        self.default_model = default_model
        self.fallback_model = fallback_model
        self.client = AsyncOpenAI(
            api_key=api_key,
            base_url="https://openrouter.ai/api/v1",
            default_headers={
                "HTTP-Referer": http_referer,
                "X-Title": x_title,
            },
        )

    async def summarize(self, transcript_text: str, title: str) -> str:
        anon = TextAnonymizer()
        anon_title = anon.anonymize(title)
        anon_transcript = anon.anonymize(transcript_text)

        if len(anon_transcript) > 120_000:
            raw_summary = await self._summarize_large_transcript(anon_transcript, anon_title)
        else:
            raw_summary = await self._request_summary(
                transcript_text=anon_transcript,
                title=anon_title,
                prompt_prefix=f"Название звонка: {anon_title}\n\nТранскрипт:\n",
            )

        return anon.deanonymize(raw_summary)

    async def _summarize_large_transcript(self, transcript_text: str, title: str) -> str:
        chunk_size = 60_000
        chunks = [
            transcript_text[i : i + chunk_size]
            for i in range(0, len(transcript_text), chunk_size)
        ]
        chunk_summaries: list[str] = []
        total = len(chunks)
        for index, chunk in enumerate(chunks, start=1):
            chunk_summary = await self._request_summary(
                transcript_text=chunk,
                title=title,
                prompt_prefix=(
                    f"Название звонка: {title}\n"
                    f"Это часть {index} из {total} большого транскрипта.\n\n"
                    "Транскрипт:\n"
                ),
            )
            chunk_summaries.append(f"Часть {index}/{total}\n{chunk_summary}")

        combined = "\n\n".join(chunk_summaries)
        return await self._request_summary(
            transcript_text=combined,
            title=title,
            prompt_prefix=(
                f"Название звонка: {title}\n\n"
                "Ниже частичные summary большого звонка. "
                "Собери из них единый итоговый отчёт без повторов:\n"
            ),
        )

    async def _request_summary(self, transcript_text: str, title: str, prompt_prefix: str) -> str:
        models = [self.default_model, self.fallback_model]
        last_error: Exception | None = None
        for attempt, model in enumerate(models, start=1):
            retries = 3 if attempt == 1 else 1
            for retry in range(retries):
                try:
                    response = await self.client.chat.completions.create(
                        model=model,
                        messages=[
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {
                                "role": "user",
                                "content": f"{prompt_prefix}{transcript_text}",
                            },
                        ],
                        temperature=0.2,
                    )
                    content = response.choices[0].message.content
                    if isinstance(content, list):
                        return "\n".join(part.text for part in content if getattr(part, "text", None)).strip()
                    return (content or "").strip()
                except APIStatusError as exc:
                    last_error = exc
                    if exc.status_code == 429 and retry < retries - 1:
                        await asyncio.sleep(2 * (retry + 1))
                        continue
                    break
        raise RuntimeError(f"Failed to summarize transcript via OpenRouter: {last_error}")
