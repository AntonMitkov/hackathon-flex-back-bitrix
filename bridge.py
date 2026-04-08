"""
B2B маршрутизация: Telegram + Email → Битрикс24 → ответ в Telegram DM

Схема работы:
  1. Контакты регистрируются в SQLite (привязка telegram_id ↔ email).
  2. Входящие сообщения из Telegram или Email попадают в ОДИН чат
     Битрикс24, закреплённый за этим контактом.
  3. Ответ оператора в чате Битрикс24 → Битрикс24 шлёт событие ONIMBOTMESSAGEADD
     на /event → бот пересылает текст в Telegram DM контакту.

API:
  POST /contacts          — зарегистрировать / обновить контакт
  GET  /contacts          — список всех контактов
  POST /event             — вебхук событий Битрикс24 (ответы оператора)
  GET  /health            — статус сервиса

Переменные окружения (.env):
  BITRIX_WEBHOOK   — https://your.bitrix24.ru/rest/USER_ID/TOKEN  (scope: imbot)
  BOT_TOKEN        — произвольная строка (секрет бота)
  OPERATOR_USER_ID — ID сотрудника Битрикс24, которому идут сообщения
  WEBHOOK_URL      — публичный URL этого сервера (ngrok / production)
  SERVER_PORT      — порт FastAPI (по умолчанию 8000)
  DB_PATH          — путь к SQLite файлу (по умолчанию contacts.db)

  TG_API_ID        — Telegram API id
  TG_API_HASH      — Telegram API hash
  TG_SESSION       — Telethon StringSession (userbot)

  EMAIL_HOST       — IMAP хост (по умолчанию imap.gmail.com)
  EMAIL_PORT       — IMAP порт (по умолчанию 993)
  EMAIL_USER       — email аккаунт
  EMAIL_PASS       — пароль / app-password
  EMAIL_FOLDER     — папка IMAP (по умолчанию INBOX)
  EMAIL_POLL_SEC   — интервал проверки почты в секундах (по умолчанию 30)

Запуск:
  uv run bridge.py
"""

import asyncio
import email as _emaillib
import email.utils
import logging
import os
import re
from contextlib import asynccontextmanager
from email.header import decode_header
from pathlib import Path
from typing import Any, Optional

import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import db

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# Конфигурация
# ─────────────────────────────────────────────────────────────────
BITRIX_WEBHOOK   = os.getenv("BITRIX_WEBHOOK", "").rstrip("/")
BOT_TOKEN        = os.getenv("BOT_TOKEN", "my_secret_bot_token")
BITRIX_APP_TOKEN = os.getenv("BITRIX_APP_TOKEN", "")  # токен исходящего вебхука
OPERATOR_USER_ID = int(os.getenv("OPERATOR_USER_ID", "1"))
WEBHOOK_URL      = os.getenv("WEBHOOK_URL", "https://example.com").rstrip("/")
SERVER_PORT      = int(os.getenv("SERVER_PORT", "8000"))

TG_API_ID        = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH      = os.getenv("TG_API_HASH", "")
TG_SESSION       = os.getenv("TG_SESSION", "")

EMAIL_HOST       = os.getenv("EMAIL_HOST", "imap.gmail.com")
EMAIL_IMAP_PORT  = int(os.getenv("EMAIL_PORT", "993"))
EMAIL_USER       = os.getenv("EMAIL_USER", "")
EMAIL_PASS       = os.getenv("EMAIL_PASS", "")
EMAIL_FOLDER     = os.getenv("EMAIL_FOLDER", "INBOX")
EMAIL_POLL_SEC   = int(os.getenv("EMAIL_POLL_SEC", "30"))

OPENROUTER_API   = os.getenv("OPENROUTER_API", "")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")

BOT_ID_FILE = Path(".bot_id")

# ─────────────────────────────────────────────────────────────────
# Глобальное состояние
# ─────────────────────────────────────────────────────────────────
_bot_id: int = 0
_tg_client: Any = None          # Telethon TelegramClient
_employee_ids: set[int] = set() # ID сотрудников Битрикс24 (кешируем при старте)

# ─────────────────────────────────────────────────────────────────
# Битрикс24 REST
# ─────────────────────────────────────────────────────────────────
async def b24(method: str, params: dict | None = None) -> Any:
    if params is None:
        params = {}
    url = f"{BITRIX_WEBHOOK}/{method}.json"
    if "imbot." in method and "CLIENT_ID" not in params:
        params["CLIENT_ID"] = BOT_TOKEN
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, json=params)
    data = resp.json()
    if not resp.is_success or "error" in data:
        err = data.get("error_description") or data.get("error") or resp.text
        log.error("B24 ошибка [%s]: %s", method, err)
        raise RuntimeError(f"[{method}] {err}")
    return data.get("result", data)


async def get_or_register_bot() -> int:
    # imbot.bot.get недоступен через пользовательский вебхук (METHOD_NOT_FOUND),
    # поэтому верификацию не делаем — imbot.register идемпотентен и вернёт
    # существующий bot_id если бот с таким CODE уже зарегистрирован.
    bot_params = {
        "CODE": "b2b_routing_bridge_v1",
        "TYPE": "B",
        "CLIENT_ID": BOT_TOKEN,
        "EVENT_HANDLER":     f"{WEBHOOK_URL}/event",
        "EVENT_MESSAGE_ADD": f"{WEBHOOK_URL}/event",
        "PROPERTIES": {
            "NAME": "B2B Bridge",
            "WORK_POSITION": "Routing Bot",
            "COLOR": "AQUA",
        },
    }

    if BOT_ID_FILE.exists():
        bot_id = int(BOT_ID_FILE.read_text().strip())
        log.info("Используем сохранённый бот ID=%d, обновляем EVENT_HANDLER → %s/event", bot_id, WEBHOOK_URL)
        # Обновляем URL обработчика на случай если ngrok изменился
        try:
            await b24("imbot.update", {
                "BOT_ID": bot_id,
                "CLIENT_ID": BOT_TOKEN,
                "EVENT_HANDLER":     f"{WEBHOOK_URL}/event",
                "EVENT_MESSAGE_ADD": f"{WEBHOOK_URL}/event",
                "FIELDS": {"NAME": "B2B Bridge"},
            })
            log.info("EVENT_HANDLER обновлён")
        except Exception as e:
            log.warning("Не удалось обновить бота: %s", e)
        return bot_id

    log.info("Регистрируем бота...")
    result = await b24("imbot.register", bot_params)
    bot_id = int(result)
    BOT_ID_FILE.write_text(str(bot_id))
    log.info("Бот зарегистрирован, ID=%d", bot_id)
    return bot_id


async def get_all_employee_ids() -> list[int]:
    """Возвращает список ID всех активных сотрудников из Битрикс24."""
    result = await b24("user.get", {"ACTIVE": True})
    return [int(u["ID"]) for u in result]


async def _create_bitrix_chat(title: str) -> str:
    """Создаёт групповой чат со всеми сотрудниками и возвращает dialog_id вида 'chatN'."""
    employee_ids = await get_all_employee_ids()
    result = await b24("imbot.chat.add", {
        "BOT_ID": _bot_id,
        "TITLE":  title,
        "TYPE":   "CHAT",
        "USERS":  employee_ids,
    })
    return f"chat{result}"


async def _bot_send(dialog_id: str, text: str) -> None:
    await b24("imbot.message.add", {
        "BOT_ID":    _bot_id,
        "CLIENT_ID": BOT_TOKEN,
        "DIALOG_ID": dialog_id,
        "MESSAGE":   text,
    })

# ─────────────────────────────────────────────────────────────────
# Центральный маршрутизатор входящих сообщений
# ─────────────────────────────────────────────────────────────────
_SOURCE_EMOJI = {"telegram": "✈️", "email": "📧"}


async def handle_incoming(
    source: str,        # "telegram" | "email"
    contact_id: str,    # telegram_id (str) или email адрес
    sender_name: str,
    sender_meta: str,   # @username или email — отображается в шапке
    text: str,
) -> None:
    emoji = _SOURCE_EMOJI.get(source, "💬")

    # ── 1. Найти контакт в БД ──────────────────────────────────
    if source == "telegram":
        contact = await db.get_by_telegram(contact_id)
    else:
        contact = await db.get_by_email(contact_id)

    if contact is None:
        # Неизвестный отправитель — авторегистрируем
        kwargs: dict[str, Any] = {"name": sender_name}
        if source == "telegram":
            kwargs["telegram_id"] = contact_id
            kwargs["telegram_username"] = sender_meta
        else:
            kwargs["email"] = contact_id
        contact = await db.upsert_contact(**kwargs)
        log.info("Авторегистрация: %s (ID=%d)", sender_name, contact.id)

    # ── 2. Получить или создать чат Битрикс24 для контакта ─────
    dialog_id = contact.bitrix_chat_id

    if not dialog_id:
        title = f"👤 {contact.name}"
        dialog_id = await _create_bitrix_chat(title)
        await db.set_chat(contact.id, dialog_id)

        # Шапка с данными контакта
        lines = [f"👤 *Контакт: {contact.name}*"]
        if contact.telegram_id:
            tg_link = contact.telegram_username or f"tg_id:{contact.telegram_id}"
            lines.append(f"✈️ Telegram: {tg_link}")
        if contact.email:
            lines.append(f"📧 Email: {contact.email}")
        await _bot_send(dialog_id, "\n".join(lines))
        log.info("Создан чат %s для контакта %s", dialog_id, contact.name)

    await _bot_send(dialog_id, f"{emoji} [{sender_name}]: {text}")
    log.info("Сообщение → %s (%s)", dialog_id, contact.name)

# ─────────────────────────────────────────────────────────────────
# Отправка ответа оператора в Telegram DM
# ─────────────────────────────────────────────────────────────────
async def send_telegram_reply(telegram_id: str, text: str) -> bool:
    if _tg_client is None:
        log.warning("Telegram клиент не инициализирован, ответ не отправлен")
        return False
    try:
        await _tg_client.send_message(int(telegram_id), text)
        log.info("Ответ отправлен в Telegram user_id=%s", telegram_id)
        return True
    except Exception as exc:
        log.error("Ошибка отправки в Telegram: %s", exc)
        return False

# ─────────────────────────────────────────────────────────────────
# Telegram userbot (Telethon + StringSession)
# ─────────────────────────────────────────────────────────────────
async def start_telegram() -> Optional[Any]:
    global _tg_client
    try:
        from telethon import TelegramClient, events
        from telethon.sessions import StringSession
    except ImportError:
        log.warning("telethon не установлен, Telegram пропущен.")
        return None
    if not TG_SESSION:
        log.warning("TG_SESSION не задан, Telegram пропущен.")
        return None

    client = TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)

    @client.on(events.NewMessage(incoming=True))
    async def on_message(event):
        try:
            sender = await event.get_sender()
            if not sender or getattr(sender, "bot", False):
                return
            first = getattr(sender, "first_name", "") or ""
            last  = getattr(sender, "last_name",  "") or ""
            sender_name = f"{first} {last}".strip() or f"id{sender.id}"
            username = (
                f"@{sender.username}"
                if getattr(sender, "username", None)
                else f"tg_id:{sender.id}"
            )
            text = event.message.message or "[медиа]"
            log.info("TG | %s (%s): %s", sender_name, username, text[:80])
            await handle_incoming(
                "telegram", str(sender.id), sender_name, username, text
            )
        except Exception as exc:
            log.error("Ошибка TG: %s", exc, exc_info=True)

    await client.start()
    _tg_client = client
    log.info("Telegram userbot запущен")
    return client

# ─────────────────────────────────────────────────────────────────
# Email polling (IMAP SSL)
# ─────────────────────────────────────────────────────────────────
async def run_email_poller() -> None:
    try:
        import aioimaplib
    except ImportError:
        log.warning("aioimaplib не установлен, Email пропущен.")
        return

    def _decode_bytes(value: Any, charset: str = "utf-8") -> str:
        if isinstance(value, bytes):
            return value.decode(charset or "utf-8", errors="replace")
        return value or ""

    def _decode_subject(raw: str) -> str:
        return "".join(
            _decode_bytes(part, charset)
            for part, charset in decode_header(raw)
        )

    seen: set[str] = set()

    while True:
        try:
            imap = aioimaplib.IMAP4_SSL(EMAIL_HOST, EMAIL_IMAP_PORT)
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
                raw_bytes = msg_data[1] if len(msg_data) > 1 else b""
                msg = _emaillib.message_from_bytes(raw_bytes)

                subject     = _decode_subject(msg.get("Subject", "(без темы)"))
                name_raw, addr = email.utils.parseaddr(msg.get("From", ""))
                sender_name = name_raw or addr

                if msg.is_multipart():
                    body = ""
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            body = part.get_payload(decode=True).decode(
                                part.get_content_charset() or "utf-8",
                                errors="replace",
                            )
                            break
                else:
                    body = msg.get_payload(decode=True).decode(
                        msg.get_content_charset() or "utf-8", errors="replace"
                    )

                text = f"Тема: {subject}\n\n{body.strip()}"
                log.info("Email | %s <%s>: %s", sender_name, addr, subject)
                await handle_incoming(
                    "email", addr.lower(), sender_name, addr, text
                )

            await imap.logout()
        except Exception as exc:
            log.error("Ошибка Email: %s", exc, exc_info=True)

        await asyncio.sleep(EMAIL_POLL_SEC)

# ─────────────────────────────────────────────────────────────────
# FastAPI приложение
# ─────────────────────────────────────────────────────────────────
def _parse_bitrix_event(raw: dict) -> dict:
    """
    Разворачивает PHP-стиль вложенных ключей формы в обычный dict.
    Например: "data[PARAMS][MESSAGE]" → {"data": {"PARAMS": {"MESSAGE": ...}}}
    """
    result: dict = {}
    for key, value in raw.items():
        parts = re.findall(r"[^\[\]]+", str(key))
        node = result
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value
    return result


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _bot_id, _employee_ids

    if not BITRIX_WEBHOOK:
        log.error("BITRIX_WEBHOOK не задан! Укажите его в .env")
        yield
        return

    await db.init_db()
    log.info("База данных инициализирована")

    _bot_id = await get_or_register_bot()

    _employee_ids = set(await get_all_employee_ids())
    log.info("Сотрудники Битрикс24: %s", _employee_ids)

    tg_client = await start_telegram()

    email_task = None
    if EMAIL_USER and EMAIL_PASS:
        email_task = asyncio.create_task(run_email_poller())
        log.info("Email poller запущен")
    else:
        log.warning("EMAIL_USER/EMAIL_PASS не заданы, Email пропущен.")

    yield

    if email_task:
        email_task.cancel()
    if tg_client:
        await tg_client.disconnect()
        log.info("Telegram клиент отключён")


app = FastAPI(
    title="B2B Routing Bridge",
    description=(
        "Маршрутизирует сообщения из Telegram и Email в Битрикс24, "
        "а ответы оператора отправляет обратно в Telegram DM."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ── Получение сообщения по ID (для исходящего вебхука с минимальными данными)
async def _fetch_message_text(message_id: int) -> str:
    """Получает текст сообщения по ID через REST API."""
    try:
        result = await b24("im.dialog.messages.get", {
            "DIALOG_ID": 0,  # будет переопределено message_id
            "MESSAGE_ID": message_id,
            "LIMIT": 1,
        })
        messages = result.get("messages", [])
        if messages:
            return messages[0].get("text", "")
    except Exception:
        pass

    # Альтернативный метод
    try:
        result = await b24("im.message.get", {"MESSAGE_ID": message_id})
        if isinstance(result, dict):
            return result.get("text", "") or result.get("TEXT", "")
    except Exception as exc:
        log.warning("Не удалось получить сообщение %d: %s", message_id, exc)
    return ""


# Множество обрабатываемых событий
_HANDLED_EVENTS = {"ONIMBOTMESSAGEADD", "ONIMMESSAGEADD", "ONIMBOTV2MESSAGEADD"}


# ── Вебхук событий Битрикс24 ─────────────────────────────────────
@app.post("/event")
async def handle_bitrix_event(request: Request):
    """
    Принимает события от Битрикс24 (Bot Platform + исходящий вебхук).
    Обрабатывает ONIMBOTMESSAGEADD и ONIMMESSAGEADD:
    сообщения от сотрудников пересылает в Telegram DM контакту.
    """
    try:
        content_type = request.headers.get("content-type", "")
        if "json" in content_type:
            raw = await request.json()
        else:
            form = await request.form()
            raw = dict(form)

        # Логируем сырые данные для диагностики
        log.info("<<< BITRIX RAW: %s", raw)

        parsed = _parse_bitrix_event(raw)
        event_type = (parsed.get("event") or "").upper()
        log.info("<<< BITRIX EVENT: %s | parsed=%s", event_type, parsed)

        # ── Проверка токена ──────────────────────────────────────
        # Bot Platform шлёт auth[application_token] — это токен, сгенерированный Битрикс24
        # Исходящий вебхук шлёт auth[application_token] = BITRIX_APP_TOKEN
        auth = parsed.get("auth", {})
        incoming_app_token = auth.get("application_token", "")
        log.info("  application_token received: %r", incoming_app_token)

        token_valid = False
        if not incoming_app_token:
            # Нет токена — пропускаем проверку (некоторые версии Битрикс не шлют)
            token_valid = True
        elif incoming_app_token == BOT_TOKEN:
            token_valid = True  # Bot Platform (CLIENT_ID совпадает)
        elif BITRIX_APP_TOKEN and incoming_app_token == BITRIX_APP_TOKEN:
            token_valid = True  # Исходящий вебхук
        else:
            # Bot Platform может слать свой application_token, который не равен CLIENT_ID
            # Принимаем если событие от Bot Platform (есть data.BOT)
            if parsed.get("data", {}).get("BOT"):
                token_valid = True
                log.info("  Принято по наличию data.BOT (Bot Platform event)")

        if not token_valid:
            log.warning("Неверный application_token: %r (ожидались: BOT_TOKEN=%r, APP_TOKEN=%r)",
                        incoming_app_token, BOT_TOKEN, BITRIX_APP_TOKEN)
            return JSONResponse({"status": "forbidden"}, status_code=403)

        # ── Фильтр событий ───────────────────────────────────────
        if event_type not in _HANDLED_EVENTS:
            log.info("  Игнорируем событие типа %s", event_type)
            return JSONResponse({"status": "ok"})

        data   = parsed.get("data", {})
        params = data.get("PARAMS") or data.get("FIELDS") or data

        # ── Извлекаем поля (разные форматы для Bot Platform / исходящего вебхука)
        raw_dialog_id = str(
            params.get("DIALOG_ID")
            or params.get("TO_CHAT_ID")
            or params.get("CHAT_ID")
            or params.get("chatId")
            or ""
        )
        message = str(
            params.get("MESSAGE")
            or params.get("text")
            or params.get("message")
            or ""
        ).strip()
        from_user_id_raw = (
            params.get("FROM_USER_ID")
            or params.get("AUTHOR_ID")
            or params.get("authorId")
            or 0
        )
        from_user_id = int(from_user_id_raw) if from_user_id_raw else 0
        message_id_raw = params.get("MESSAGE_ID") or params.get("ID") or 0
        message_id = int(message_id_raw) if message_id_raw else 0

        # Если сообщения нет в payload — пытаемся получить по ID (исходящий вебхук)
        if not message and message_id:
            log.info("  Сообщение отсутствует в payload, получаем по ID=%d", message_id)
            message = await _fetch_message_text(message_id)

        # Нормализуем dialog_id → формат "chatN"
        if raw_dialog_id and not raw_dialog_id.startswith("chat") and raw_dialog_id.isdigit():
            dialog_id = f"chat{raw_dialog_id}"
        else:
            dialog_id = raw_dialog_id

        log.info(
            "  dialog=%s (raw=%s) from_user=%s msg=%r",
            dialog_id, raw_dialog_id, from_user_id, message[:80] if message else "",
        )

        if not dialog_id or not message:
            log.info("  Пропущено: пустой dialog_id или message")
            return JSONResponse({"status": "ok"})

        # ── Пропускаем сообщения от бота и не-сотрудников ─────────
        if from_user_id == 0:
            log.info("  Пропущено: from_user_id=0 (системное сообщение)")
            return JSONResponse({"status": "ok"})

        # Пропускаем сообщения от самого бота (бот имеет user_id в Битрикс24)
        if from_user_id == _bot_id:
            log.info("  Пропущено: сообщение от самого бота (bot_id=%d)", _bot_id)
            return JSONResponse({"status": "ok"})

        if _employee_ids and from_user_id not in _employee_ids:
            log.info(
                "  Пропущено: from_user_id=%d не найден в сотрудниках %s",
                from_user_id, _employee_ids,
            )
            return JSONResponse({"status": "ok"})

        # ── Ищем контакт по dialog_id ─────────────────────────────
        contact = await db.get_by_chat(dialog_id)

        # Если не нашли по dialog_id — пробуем по TO_CHAT_ID/CHAT_ID
        if not contact and raw_dialog_id != dialog_id:
            contact = await db.get_by_chat(raw_dialog_id)

        if not contact:
            log.info("  Чат %s не привязан ни к одному контакту", dialog_id)
            return JSONResponse({"status": "ok"})

        if not contact.telegram_id:
            log.info("  Контакт %s без Telegram — ответ не переслан", contact.name)
            return JSONResponse({"status": "ok"})

        # Убираем BB-код ботовых упоминаний из текста: [USER=123]BotName[/USER]
        clean_message = re.sub(r"\[USER=\d+\][^[]*\[/USER\]\s*,?\s*", "", message).strip()
        if not clean_message:
            log.info("  Пропущено: после очистки BB-кода сообщение пустое")
            return JSONResponse({"status": "ok"})

        log.info(
            "  Пересылаем → %s (TG %s): %s",
            contact.name, contact.telegram_id, clean_message[:80],
        )
        await send_telegram_reply(contact.telegram_id, clean_message)

    except Exception as exc:
        log.error("Ошибка обработки события: %s", exc, exc_info=True)

    return JSONResponse({"status": "ok"})


@app.post("/debug/event")
async def debug_event(request: Request):
    """Диагностический эндпоинт: логирует всё что пришло от Битрикс24."""
    content_type = request.headers.get("content-type", "")
    body = await request.body()
    log.info("DEBUG /event | content-type=%s | body=%s", content_type, body.decode(errors="replace"))
    if "json" in content_type:
        raw = await request.json()
    else:
        form = await request.form()
        raw = dict(form)
    parsed = _parse_bitrix_event(raw)
    return {"raw": raw, "parsed": parsed}


# ── REST API управления контактами ───────────────────────────────
class ContactCreate(BaseModel):
    name: str
    telegram_id: Optional[str] = None
    telegram_username: Optional[str] = None
    email: Optional[str] = None


@app.post("/contacts", status_code=201)
async def register_contact(data: ContactCreate):
    """
    Зарегистрировать или обновить контакт.
    Привязывает telegram_id и email к одному контакту.
    При повторном вызове с тем же telegram_id или email — обновляет запись.
    """
    if not data.telegram_id and not data.email:
        return JSONResponse(
            {"error": "Необходимо указать хотя бы telegram_id или email"},
            status_code=400,
        )
    contact = await db.upsert_contact(
        name=data.name,
        telegram_id=data.telegram_id,
        telegram_username=data.telegram_username,
        email=data.email,
    )
    return {
        "id":                contact.id,
        "name":              contact.name,
        "telegram_id":       contact.telegram_id,
        "telegram_username": contact.telegram_username,
        "email":             contact.email,
        "bitrix_chat_id":    contact.bitrix_chat_id,
    }


@app.get("/contacts")
async def list_contacts():
    """Список всех зарегистрированных контактов."""
    contacts = await db.list_all()
    return [
        {
            "id":                c.id,
            "name":              c.name,
            "telegram_id":       c.telegram_id,
            "telegram_username": c.telegram_username,
            "email":             c.email,
            "bitrix_chat_id":    c.bitrix_chat_id,
        }
        for c in contacts
    ]


@app.delete("/contacts")
async def clear_contacts():
    """Удалить все контакты из базы данных."""
    import aiosqlite
    async with aiosqlite.connect(db.DB_PATH) as conn:
        await conn.execute("DELETE FROM contacts")
        await conn.commit()
    return {"status": "ok", "message": "Все контакты удалены"}


@app.get("/employees")
async def list_employees():
    """Получить всех активных сотрудников из Битрикс24."""
    result = await b24("user.get", {
        "ACTIVE": True,
        "select": ["ID", "NAME", "LAST_NAME", "EMAIL", "WORK_POSITION"],
    })
    return [
        {
            "id":            int(u["ID"]),
            "name":          f"{u.get('NAME', '')} {u.get('LAST_NAME', '')}".strip(),
            "email":         u.get("EMAIL", ""),
            "work_position": u.get("WORK_POSITION", ""),
        }
        for u in result
    ]


# ─────────────────────────────────────────────────────────────────
# OpenRouter AI
# ─────────────────────────────────────────────────────────────────
async def _openrouter_chat(messages: list[dict], json_mode: bool = False) -> str:
    """Отправляет запрос в OpenRouter и возвращает текст ответа."""
    if not OPENROUTER_API:
        raise RuntimeError("OPENROUTER_API не задан в .env")

    payload: dict[str, Any] = {
        "model": OPENROUTER_MODEL,
        "messages": messages,
    }
    if json_mode:
        payload["response_format"] = {"type": "json_object"}

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API}",
        "Content-Type": "application/json",
        "HTTP-Referer": WEBHOOK_URL,
        "X-Title": "B2B Bridge",
    }
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://openrouter.ai/api/v1/chat/completions",
            json=payload,
            headers=headers,
        )
    if not resp.is_success:
        raise RuntimeError(f"OpenRouter error {resp.status_code}: {resp.text[:300]}")

    data = resp.json()
    return data["choices"][0]["message"]["content"]


async def _fetch_dialog_messages(dialog_id: str, limit: int = 50) -> list[dict]:
    """
    Загружает последние `limit` сообщений из чата Битрикс24.
    Возвращает список {"author_id": int, "text": str, "date": str}.
    """
    try:
        result = await b24("im.dialog.messages.get", {
            "DIALOG_ID": dialog_id,
            "LIMIT": limit,
        })
        items = result.get("messages") if isinstance(result, dict) else result
        if not isinstance(items, list):
            items = []
        return [
            {
                "author_id": int(m.get("author_id") or m.get("AUTHOR_ID") or 0),
                "text": str(m.get("text") or m.get("MESSAGE") or "").strip(),
                "date": str(m.get("date") or m.get("DATE_CREATE") or ""),
            }
            for m in items
            if (m.get("text") or m.get("MESSAGE"))
        ]
    except Exception as exc:
        log.warning("Не удалось загрузить сообщения чата %s: %s", dialog_id, exc)
        return []


def _messages_to_transcript(messages: list[dict], contact_name: str) -> str:
    """Форматирует список сообщений в текстовый транскрипт."""
    lines = []
    for m in messages:
        author = contact_name if m["author_id"] != OPERATOR_USER_ID else "Оператор"
        date = f" [{m['date']}]" if m["date"] else ""
        lines.append(f"{author}{date}: {m['text']}")
    return "\n".join(lines) or "(сообщения не найдены)"


_SUMMARY_SYSTEM = """\
Ты — ассистент менеджера по B2B-продажам печатной продукции и упаковки.
Проанализируй переписку и верни ТОЛЬКО валидный JSON (без markdown, без пояснений):

{
  "company":        "название компании клиента (если не найдено — имя контакта)",
  "contact":        "имя и должность представителя клиента (если есть)",
  "segment":        "отрасль / сегмент (например: напитки, продукты, фармацевтика)",
  "product_type":   "тип продукции / материал (например: самоклейка, гофрокороб, PET)",
  "current_request":"суть текущего запроса клиента — что хочет заказать, параметры",
  "client_readiness":"оценка готовности к сделке: Горячий | Тёплый | Холодный | Неясно",
  "last_actions":   "последние действия со стороны менеджера (КП, образцы, переговоры и т.д.)",
  "last_contact":   "дата или описание последнего сообщения в переписке",
  "next_step":      "рекомендуемый следующий шаг менеджера",
  "deal_risk":      "риск срыва сделки в % (число 0–100, без знака %)",
  "priority":       "VIP | Высокий | Средний | Низкий",
  "client_mood":    "настроение/тон клиента в переписке: позитивный | нейтральный | скептичный | негативный | неясно",
  "note":           "важная особенность или нюанс, на который стоит обратить внимание"
}

Если какое-то поле невозможно определить из переписки — ставь null.\
"""


def _format_summary(data: dict, contact_name: str) -> str:
    """Форматирует JSON от AI в текстовый CLIENT SUMMARY."""
    def val(key: str, suffix: str = "") -> str:
        v = data.get(key)
        if v is None or v == "":
            return "—"
        return f"{v}{suffix}"

    risk_raw = data.get("deal_risk")
    risk = f"{risk_raw}%" if risk_raw is not None else "—"

    return (
        "CLIENT SUMMARY\n"
        "\n"
        f"Компания:            {val('company')}\n"
        f"Контакт:             {val('contact') if val('contact') != '—' else contact_name}\n"
        f"Сегмент:             {val('segment')}\n"
        f"Тип продукции:       {val('product_type')}\n"
        "\n"
        f"Текущий запрос:      {val('current_request')}\n"
        "\n"
        f"Готовность клиента:  {val('client_readiness')}\n"
        "\n"
        f"Последние действия:  {val('last_actions')}\n"
        "\n"
        f"Последний контакт:   {val('last_contact')}\n"
        "\n"
        f"Следующий шаг:       {val('next_step')}\n"
        "\n"
        f"Риск сделки:         {risk}\n"
        "\n"
        f"Приоритет:           {val('priority')}\n"
        "\n"
        f"Настроение клиента:  {val('client_mood')}\n"
        "\n"
        f"Особенность:         {val('note')}\n"
    )


# ── Эндпоинт: Summary переписки ──────────────────────────────────
@app.post("/contacts/{contact_id}/summary")
async def generate_summary(contact_id: int):
    """
    Генерирует CLIENT SUMMARY по переписке с контактом.
    Возвращает: {"summary": "текст", "data": {поля как объект}}
    """
    contact = await db.get_by_id(contact_id)
    if not contact:
        return JSONResponse({"error": "Контакт не найден"}, status_code=404)
    if not contact.bitrix_chat_id:
        return JSONResponse({"error": "У контакта нет привязанного чата"}, status_code=400)

    messages = await _fetch_dialog_messages(contact.bitrix_chat_id)
    transcript = _messages_to_transcript(messages, contact.name)

    prompt = [
        {"role": "system", "content": _SUMMARY_SYSTEM},
        {
            "role": "user",
            "content": (
                f"Контакт: {contact.name}"
                + (f"\nEmail: {contact.email}" if contact.email else "")
                + f"\n\nПереписка:\n{transcript}"
            ),
        },
    ]

    import json as _json
    raw = await _openrouter_chat(prompt, json_mode=True)
    log.info("Summary сгенерирован для контакта %d (%s)", contact_id, contact.name)

    try:
        parsed = _json.loads(raw)
    except Exception:
        log.warning("Summary: не удалось распарсить JSON")
        return {"contact_id": contact_id, "contact_name": contact.name, "summary": raw, "data": {}}

    return {
        "contact_id":   contact_id,
        "contact_name": contact.name,
        "summary":      _format_summary(parsed, contact.name),
        "data":         parsed,
    }


# ── Эндпоинт: Pre-call Brief ─────────────────────────────────────
@app.post("/contacts/{contact_id}/brief")
async def generate_brief(contact_id: int):
    """
    Генерирует AI-бриф для подготовки к звонку с клиентом.

    Возвращает JSON:
    {
      "company":      "Название компании",
      "segment":      "отрасль / сегмент",
      "circulation":  "тираж",
      "material":     "тип материала",
      "last_stage":   "последний этап переговоров",
      "churn_risk":   "XX%",
      "priority":     "VIP | Высокий | Средний | Низкий",
      "call_tips":    ["совет 1", "совет 2", "совет 3"]
    }
    """
    contact = await db.get_by_id(contact_id)
    if not contact:
        return JSONResponse({"error": "Контакт не найден"}, status_code=404)
    if not contact.bitrix_chat_id:
        return JSONResponse({"error": "У контакта нет привязанного чата"}, status_code=400)

    messages = await _fetch_dialog_messages(contact.bitrix_chat_id, limit=100)
    transcript = _messages_to_transcript(messages, contact.name)

    system_prompt = """Ты — эксперт по B2B-продажам печатной продукции и упаковки.
Проанализируй переписку с клиентом и верни ТОЛЬКО валидный JSON (без markdown, без пояснений) со следующими полями:

{
  "company":      "название компании клиента (если не найдено — имя контакта)",
  "segment":      "отрасль или сегмент клиента (например: напитки, продукты, фармацевтика)",
  "circulation":  "тираж заказа (например: 10 000, если не упомянут — null)",
  "material":     "материал заказа (например: самоклейка, картон, PET, если не упомянут — null)",
  "last_stage":   "последний этап переговоров (например: КП отправлено, ждём оплату, на согласовании)",
  "churn_risk":   "оценка риска ухода клиента в % (число от 0 до 100, без знака %)",
  "priority":     "приоритет клиента: VIP | Высокий | Средний | Низкий",
  "call_tips":    ["3-5 конкретных советов что сказать/спросить на звонке"]
}

Для оценки churn_risk учитывай: длину паузы в переписке, наличие возражений, упоминание конкурентов, неопределённость клиента.
Для priority учитывай: объём заказа, частоту коммуникации, стратегическую важность."""

    prompt = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": (
                f"Контакт: {contact.name}"
                + (f"\nEmail: {contact.email}" if contact.email else "")
                + (f"\nTelegram: {contact.telegram_username or contact.telegram_id}" if contact.telegram_id else "")
                + f"\n\nПереписка:\n{transcript}"
            ),
        },
    ]

    raw = await _openrouter_chat(prompt, json_mode=True)
    log.info("Brief сгенерирован для контакта %d (%s)", contact_id, contact.name)

    try:
        import json as _json
        brief = _json.loads(raw)
        # Нормализуем churn_risk → строка с %
        if "churn_risk" in brief and brief["churn_risk"] is not None:
            brief["churn_risk"] = f"{brief['churn_risk']}%"
    except Exception:
        # Если модель вернула не чистый JSON — возвращаем как есть
        log.warning("Brief: не удалось распарсить JSON, возвращаем raw")
        return {"contact_id": contact_id, "contact_name": contact.name, "raw": raw}

    return {"contact_id": contact_id, "contact_name": contact.name, "brief": brief}


@app.get("/health")
async def health():
    return {"status": "ok", "bot_id": _bot_id}


# ─────────────────────────────────────────────────────────────────
# Точка входа
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        "bridge:app",
        host="0.0.0.0",
        port=SERVER_PORT,
        reload=False,
    )
