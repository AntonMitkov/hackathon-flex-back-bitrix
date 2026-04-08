"""
Интеграция Telegram user bot + Email → Битрикс24 через Bot Platform.

Архитектура:
  1. Регистрируем бота через imbot.v2.Bot.register (входящий вебхук, скоуп imbot)
  2. При входящем сообщении из Telegram/Email:
     - создаём групповой чат через imbot.chat.add (старый стабильный метод)
     - отправляем сообщение через imbot.message.add
  3. Источник (Telegram/Email) виден в названии чата и в каждом сообщении

Зависимости:
    pip install httpx telethon aioimaplib python-dotenv
"""

import asyncio
import email as emaillib
import email.utils
import hashlib
import logging
import os
from email.header import decode_header
from enum import Enum
from pathlib import Path

import httpx
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# Конфиг (.env)
# ─────────────────────────────────────────────────────────────────
BITRIX_WEBHOOK   = os.getenv("BITRIX_WEBHOOK", "").rstrip("/")
# Входящий вебхук со скоупом imbot:
# https://your.bitrix24.ru/rest/USER_ID/TOKEN

BOT_TOKEN        = os.getenv("BOT_TOKEN", "my_secret_bot_token")
# Любая строка — секрет бота (задаётся вами при регистрации)

OPERATOR_USER_ID = int(os.getenv("OPERATOR_USER_ID", "1"))
# ID сотрудника Битрикс24, которому будут приходить сообщения

TG_API_ID        = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH      = os.getenv("TG_API_HASH", "")
TG_SESSION       = os.getenv("TG_SESSION", "")   # Telethon StringSession

EMAIL_HOST       = os.getenv("EMAIL_HOST", "imap.gmail.com")
EMAIL_PORT       = int(os.getenv("EMAIL_PORT", "993"))
EMAIL_USER       = os.getenv("EMAIL_USER", "")
EMAIL_PASS       = os.getenv("EMAIL_PASS", "")
EMAIL_FOLDER     = os.getenv("EMAIL_FOLDER", "INBOX")
EMAIL_POLL_SEC   = int(os.getenv("EMAIL_POLL_SEC", "30"))

BOT_ID_FILE = Path(".bot_id")

# ─────────────────────────────────────────────────────────────────
# Источники
# ─────────────────────────────────────────────────────────────────
class Source(str, Enum):
    TELEGRAM = "Telegram"
    EMAIL    = "Email"

SOURCE_EMOJI = {Source.TELEGRAM: "✈️", Source.EMAIL: "📧"}

# ─────────────────────────────────────────────────────────────────
# Хранилище чатов: contact_key → chat_id в Битрикс24
# ─────────────────────────────────────────────────────────────────
_chats: dict[str, str] = {}

def _contact_key(source: Source, contact_id: str) -> str:
    return hashlib.md5(f"{source}:{contact_id}".encode()).hexdigest()

# ─────────────────────────────────────────────────────────────────
# Битрикс24 REST (вебхук)
# ─────────────────────────────────────────────────────────────────
# 1. Обновленная функция отправки запросов
async def b24(method: str, params: dict) -> dict:
    url = f"{BITRIX_WEBHOOK}/{method}.json"
    
    # КРИТИЧНО: Для методов imbot.* через вебхук ОБЯЗАТЕЛЬНО 
    # передавать CLIENT_ID, который равен вашему BOT_TOKEN
    if "imbot." in method and "CLIENT_ID" not in params:
        params["CLIENT_ID"] = BOT_TOKEN

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, json=params)

    data = resp.json()
    if not resp.is_success or "error" in data:
        error_msg = data.get("error_description") or data.get("error") or resp.text
        log.error("Ошибка B24 [%s]: %s", method, error_msg)
        # Если получаем "Method not found", значит не хватает прав 'imbot' в вебхуке
        raise RuntimeError(f"[{method}] {error_msg}")
    
    return data.get("result", data)

# 2. Исправленная регистрация
async def get_or_register_bot() -> int:
    # Обязательно удалите файл .bot_id перед запуском этого кода!
    if BOT_ID_FILE.exists():
        bot_id = int(BOT_ID_FILE.read_text().strip())
        try:
            await b24("imbot.bot.get", {"BOT_ID": bot_id})
            log.info("Бот подтвержден, ID=%d", bot_id)
            return bot_id
        except Exception:
            log.warning("Бот не найден, создаем нового...")
            BOT_ID_FILE.unlink(missing_ok=True)

    log.info("Регистрируем бота...")
    
    params = {
        "CODE": "tg_email_bridge_v5",     # Каждый раз новый, если была ошибка
        "TYPE": "B", 
        "CLIENT_ID": BOT_TOKEN,           # ДУБЛИРУЕМ ЗДЕСЬ ДЛЯ НАДЕЖНОСТИ
        "EVENT_HANDLER": "https://example.com/bot", 
        "PROPERTIES": {
            "NAME": "TG+Email Bridge",
            "WORK_POSITION": "Support Bot",
            "COLOR": "AQUA",
        }
    }

    # Используем imbot.register (базовый метод)
    result = await b24("imbot.register", params)
    
    bot_id = int(result)
    BOT_ID_FILE.write_text(str(bot_id))
    log.info("Бот успешно зарегистрирован! ID=%d", bot_id)
    return bot_id

# Исправленный метод создания чата
async def create_chat(bot_id: int, title: str) -> str:
    # Важно: для imbot.chat.add права должны быть у создателя вебхука
    result = await b24("imbot.chat.add", {
        "BOT_ID": bot_id,
        "TITLE": title,
        "TYPE": "CHAT", # Изменил OPEN на CHAT для приватности (по желанию)
        "USERS": [OPERATOR_USER_ID],
    })
    
    # Метод возвращает ID чата напрямую
    return f"chat{result}"

# ─────────────────────────────────────────────────────────────────
# Отправить сообщение от имени бота в чат
# ─────────────────────────────────────────────────────────────────
async def bot_send(bot_id: int, dialog_id: str, text: str) -> None:
    await b24("imbot.message.add", {
        "BOT_ID":    bot_id,
        "CLIENT_ID": BOT_TOKEN,
        "DIALOG_ID": dialog_id,
        "MESSAGE":   text,
    })

# ─────────────────────────────────────────────────────────────────
# Центральный обработчик входящих сообщений
# ─────────────────────────────────────────────────────────────────
async def handle_incoming(
    bot_id: int,
    source: Source,
    contact_id: str,
    sender_name: str,
    sender_meta: str,   # @username или email
    text: str,
) -> None:
    key   = _contact_key(source, contact_id)
    emoji = SOURCE_EMOJI[source]

    if key not in _chats:
        # Первое сообщение — создаём чат
        title     = f"{emoji} {source.value}: {sender_name}"
        dialog_id = await create_chat(bot_id, title)
        _chats[key] = dialog_id

        # Шапка с источником
        header = (
            f"{emoji} *Новый диалог из {source.value}*\n"
            f"👤 {sender_name}\n"
            f"🔗 {sender_meta}"
        )
        await bot_send(bot_id, dialog_id, header)

    dialog_id = _chats[key]
    await bot_send(bot_id, dialog_id, f"{emoji} [{sender_name}]: {text}")
    log.info("Сообщение → %s", dialog_id)

# ─────────────────────────────────────────────────────────────────
# Telegram userbot (Telethon + StringSession)
# ─────────────────────────────────────────────────────────────────
async def start_telegram(bot_id: int) -> None:
    try:
        from telethon import TelegramClient, events
        from telethon.sessions import StringSession
    except ImportError:
        log.warning("telethon не установлен, Telegram пропущен.")
        return
    if not TG_SESSION:
        log.warning("TG_SESSION не задан, Telegram пропущен.")
        return

    client = TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)

    @client.on(events.NewMessage(incoming=True))
    async def on_message(event):
        try:
            sender = await event.get_sender()
            if not sender or getattr(sender, "bot", False):
                return
            contact_id  = str(sender.id)
            first       = getattr(sender, "first_name", "") or ""
            last        = getattr(sender, "last_name",  "") or ""
            sender_name = f"{first} {last}".strip() or f"id{sender.id}"
            username    = (
                f"@{sender.username}"
                if getattr(sender, "username", None)
                else f"tg_id:{sender.id}"
            )
            text = event.message.message or "[медиа]"
            log.info("TG | %s (%s): %s", sender_name, username, text[:80])
            await handle_incoming(bot_id, Source.TELEGRAM,
                                  contact_id, sender_name, username, text)
        except Exception as e:
            log.error("Ошибка TG: %s", e, exc_info=True)

    await client.start()
    log.info("Telegram userbot запущен")
    await client.run_until_disconnected()

# ─────────────────────────────────────────────────────────────────
# Email polling (IMAP SSL)
# ─────────────────────────────────────────────────────────────────
async def start_email(bot_id: int) -> None:
    try:
        import aioimaplib
    except ImportError:
        log.warning("aioimaplib не установлен, Email пропущен.")
        return

    def _dec(v, charset="utf-8") -> str:
        return v.decode(charset or "utf-8", errors="replace") if isinstance(v, bytes) else (v or "")

    def _subj(raw: str) -> str:
        return "".join(_dec(p, c) for p, c in decode_header(raw))

    seen: set[str] = set()

    while True:
        try:
            imap = aioimaplib.IMAP4_SSL(EMAIL_HOST, EMAIL_PORT)
            await imap.wait_hello_from_server()
            await imap.login(EMAIL_USER, EMAIL_PASS)
            await imap.select(EMAIL_FOLDER)

            _, data = await imap.search("UNSEEN")
            uids = data[0].split() if data and data[0] else []

            for uid in uids:
                uid_str = uid.decode() if isinstance(uid, bytes) else uid
                if uid_str in seen:
                    continue
                seen.add(uid_str)

                _, msg_data = await imap.fetch(uid_str, "(RFC822)")
                raw = msg_data[1] if len(msg_data) > 1 else b""
                msg = emaillib.message_from_bytes(raw)

                subject     = _subj(msg.get("Subject", "(без темы)"))
                name_raw, addr = email.utils.parseaddr(msg.get("From", ""))
                sender_name = name_raw or addr

                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            body = part.get_payload(decode=True).decode(
                                part.get_content_charset() or "utf-8", errors="replace"
                            )
                            break
                else:
                    body = msg.get_payload(decode=True).decode(
                        msg.get_content_charset() or "utf-8", errors="replace"
                    )

                text = f"Тема: {subject}\n\n{body.strip()}"
                log.info("Email | %s <%s>: %s", sender_name, addr, subject)
                await handle_incoming(bot_id, Source.EMAIL,
                                      addr.lower(), sender_name, addr, text)

            await imap.logout()
        except Exception as e:
            log.error("Ошибка Email: %s", e, exc_info=True)

        await asyncio.sleep(EMAIL_POLL_SEC)

# ─────────────────────────────────────────────────────────────────
# Точка входа
# ─────────────────────────────────────────────────────────────────
async def main() -> None:
    if not BITRIX_WEBHOOK:
        log.error("Задайте BITRIX_WEBHOOK в .env")
        return

    bot_id = await get_or_register_bot()
    await asyncio.gather(
        start_telegram(bot_id),
        start_email(bot_id),
    )

if __name__ == "__main__":
    asyncio.run(main())
