"""
Модуль обезличивания данных перед отправкой в LLM.

Принцип работы:
- Детерминированное псевдонимирование на основе MD5-хэша:
  один и тот же реальный контакт → всегда один и тот же токен.
- Явная регистрация структурированных данных (имя, email, telegram).
- Regex-замена PII-паттернов в свободном тексте (телефоны, email, @username).
- Метод deanonymize() восстанавливает оригиналы в ответе LLM.

Использование:
    anon = TextAnonymizer()
    anon_name = anon.add("Иван Петров", kind="name")
    anon_text = anon.anonymize(transcript)
    llm_response = await call_llm(anon_name, anon_text)
    real_response = anon.deanonymize(llm_response)
"""

from __future__ import annotations

import hashlib
import re


# ─── Regex-паттерны для PII ──────────────────────────────────────────────────

_EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"
)
_PHONE_RE = re.compile(
    r"(?:\+7|8)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}"
)
_TG_RE = re.compile(
    r"(?<!\w)@([A-Za-z0-9_]{3,})(?!\w)"
)


def _h(value: str) -> str:
    """Возвращает 4-символьный HEX-хэш строки."""
    return hashlib.md5(value.lower().encode()).hexdigest()[:4].upper()


# ─── Основной класс ──────────────────────────────────────────────────────────

class TextAnonymizer:
    """
    Обезличивает данные для одного LLM-запроса и восстанавливает ответ.

    Все замены детерминированы: одинаковые реальные значения
    всегда заменяются на одинаковые токены.
    """

    def __init__(self) -> None:
        self._fwd: dict[str, str] = {}   # real → token
        self._rev: dict[str, str] = {}   # token → real

    # ── Регистрация сущностей ────────────────────────────────────────────────

    def add(self, real: str, kind: str = "entity") -> str:
        """
        Регистрирует реальное значение и возвращает его токен.

        kind: 'name' | 'email' | 'telegram' | 'entity'
        """
        if not real:
            return real
        if real in self._fwd:
            return self._fwd[real]

        h = _h(real)
        if kind == "name":
            token = f"Клиент_{h}"
        elif kind == "email":
            token = f"[email_{h}]"
        elif kind == "telegram":
            clean = real.lstrip("@")
            token = f"@user_{h}"
            # Регистрируем оба варианта: с @ и без
            self._fwd[clean] = f"user_{h}"
            self._rev[f"user_{h}"] = clean
        else:
            token = f"[X_{h}]"

        self._fwd[real] = token
        self._rev[token] = real
        return token

    # ── Обезличивание ────────────────────────────────────────────────────────

    def anonymize(self, text: str) -> str:
        """
        Заменяет в тексте:
        1. Все зарегистрированные сущности (по словарю).
        2. Email-адреса, телефоны, Telegram-@username (по regex).
        """
        if not text:
            return text

        result = text

        # Применяем зарегистрированные замены (длинные первыми, чтобы не сломать частичные совпадения)
        for real in sorted(self._fwd, key=len, reverse=True):
            if real and real in result:
                result = result.replace(real, self._fwd[real])

        # Email
        def _replace_email(m: re.Match) -> str:
            orig = m.group()
            if orig in self._rev:  # уже является токеном — не трогаем
                return orig
            token = f"[email_{_h(orig)}]"
            self._fwd.setdefault(orig, token)
            self._rev.setdefault(token, orig)
            return token

        result = _EMAIL_RE.sub(_replace_email, result)

        # Телефоны
        def _replace_phone(m: re.Match) -> str:
            orig = m.group()
            token = f"[тел_{_h(orig)}]"
            self._fwd.setdefault(orig, token)
            self._rev.setdefault(token, orig)
            return token

        result = _PHONE_RE.sub(_replace_phone, result)

        # Telegram @username (оставшиеся)
        def _replace_tg(m: re.Match) -> str:
            orig = m.group()          # включая @
            username = m.group(1)     # без @
            token = f"@user_{_h(username)}"
            self._fwd.setdefault(orig, token)
            self._rev.setdefault(token, orig)
            return token

        result = _TG_RE.sub(_replace_tg, result)

        return result

    # ── Восстановление ───────────────────────────────────────────────────────

    def deanonymize(self, text: str) -> str:
        """
        Заменяет токены обратно на реальные значения в ответе LLM.
        Восстанавливает только то, что было зарегистрировано в этом экземпляре.
        """
        if not text:
            return text

        result = text
        # Длинные токены первыми, чтобы не сломать вложенные совпадения
        for token in sorted(self._rev, key=len, reverse=True):
            if token and token in result:
                result = result.replace(token, self._rev[token])
        return result
