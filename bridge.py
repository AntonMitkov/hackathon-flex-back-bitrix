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
_tg_client: Any = None    # Telethon TelegramClient
# ID сотрудников Битрикс24 → {"name": str, "position": str}
_employees: dict[int, dict] = {}

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


async def fetch_all_employees() -> dict[int, dict]:
    """Загружает всех активных сотрудников и возвращает {id: {name, position}}."""
    result = await b24("user.get", {
        "ACTIVE": True,
        "select": ["ID", "NAME", "LAST_NAME", "WORK_POSITION"],
    })
    return {
        int(u["ID"]): {
            "name":     f"{u.get('NAME', '')} {u.get('LAST_NAME', '')}".strip(),
            "position": (u.get("WORK_POSITION") or "").strip(),
        }
        for u in result
    }


async def _create_bitrix_chat(title: str, user_ids: list[int] | None = None) -> str:
    """Создаёт групповой чат с указанными (или всеми) сотрудниками и возвращает dialog_id вида 'chatN'."""
    if user_ids is None:
        user_ids = list(_employees.keys()) if _employees else list((await fetch_all_employees()).keys())
    result = await b24("imbot.chat.add", {
        "BOT_ID": _bot_id,
        "TITLE":  title,
        "TYPE":   "CHAT",
        "USERS":  user_ids,
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
        # Анализируем клиента через AI и подбираем команду
        card, team = await _create_card_and_assign_team(contact, text)

        # Определяем кого добавить в чат
        if team:
            chat_user_ids = [e.bitrix_user_id for e in team]
            priority_label = card.priority
        else:
            chat_user_ids = None  # fallback: все сотрудники
            priority_label = "Средний"

        priority_emoji = {"VIP": "🔥", "Высокий": "🟠", "Средний": "🟡", "Низкий": "🟢"}.get(priority_label, "⚪")
        title = f"{priority_emoji} {contact.name} [{priority_label}]"
        dialog_id = await _create_bitrix_chat(title, chat_user_ids)
        await db.set_chat(contact.id, dialog_id)

        # Шапка с данными контакта и карточкой
        lines = [f"👤 *Контакт: {contact.name}*"]
        if contact.telegram_id:
            tg_link = contact.telegram_username or f"tg_id:{contact.telegram_id}"
            lines.append(f"✈️ Telegram: {tg_link}")
        if contact.email:
            lines.append(f"📧 Email: {contact.email}")
        lines.append("")
        lines.append(f"📋 *Карточка клиента #{card.id}*")
        lines.append(f"Приоритет: {priority_emoji} {card.priority}")
        if card.company:
            lines.append(f"Компания: {card.company}")
        if card.segment:
            lines.append(f"Сегмент: {card.segment}")
        if card.product_type:
            lines.append(f"Продукция: {card.product_type}")
        if card.volume:
            lines.append(f"Объём: {card.volume}")
        if card.notes:
            lines.append(f"Заметка: {card.notes}")
        if team:
            lines.append("")
            lines.append("👥 *Назначенная команда:*")
            for emp in team:
                lines.append(f"  • {emp.name} — {emp.role} (рейтинг: {emp.rating}/10)")

        await _bot_send(dialog_id, "\n".join(lines))

        # ── Устанавливаем описание чата (видно всегда в шапке) ──
        chat_id_num = dialog_id.replace("chat", "")
        desc_lines = [f"Приоритет: {priority_emoji} {card.priority}"]
        if card.company:
            desc_lines.append(f"Компания: {card.company}")
        if card.segment:
            desc_lines.append(f"Сегмент: {card.segment}")
        if card.product_type:
            desc_lines.append(f"Продукция: {card.product_type}")
        if card.volume:
            desc_lines.append(f"Объём: {card.volume}")
        if team:
            desc_lines.append("Команда: " + ", ".join(f"{e.name} ({e.role})" for e in team))
        try:
            await b24("im.chat.update", {
                "CHAT_ID": int(chat_id_num),
                "DESCRIPTION": "\n".join(desc_lines),
            })
        except Exception as exc:
            log.warning("Не удалось обновить описание чата: %s", exc)

        # ── Закрепляем сообщение с карточкой ──
        try:
            # Получаем последнее сообщение бота (карточка) и пиним его
            msgs = await b24("im.dialog.messages.get", {
                "DIALOG_ID": dialog_id,
                "LIMIT": 1,
            })
            msg_list = msgs.get("messages", []) if isinstance(msgs, dict) else []
            if msg_list:
                pin_msg_id = msg_list[0].get("id") or msg_list[0].get("ID")
                if pin_msg_id:
                    await b24("im.chat.pin", {
                        "CHAT_ID": int(chat_id_num),
                        "MESSAGE_ID": int(pin_msg_id),
                    })
        except Exception as exc:
            log.warning("Не удалось закрепить сообщение: %s", exc)

        log.info("Создан чат %s для контакта %s (приоритет: %s)", dialog_id, contact.name, priority_label)

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
    global _bot_id, _employees

    if not BITRIX_WEBHOOK:
        log.error("BITRIX_WEBHOOK не задан! Укажите его в .env")
        yield
        return

    await db.init_db()
    log.info("База данных инициализирована")

    _bot_id = await get_or_register_bot()

    _employees = await fetch_all_employees()
    log.info("Сотрудники Битрикс24: %s", {uid: e["name"] for uid, e in _employees.items()})

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
    openapi_tags=[
        {"name": "Звонки", "description": "Приём, хранение и анализ телефонных звонков"},
        {"name": "Контакты", "description": "Управление контактами клиентов"},
        {"name": "Сотрудники", "description": "Управление сотрудниками и командами"},
        {"name": "Карточки клиентов", "description": "Карточки клиентов с приоритетами и назначениями"},
        {"name": "Вебхуки", "description": "Приём событий от Битрикс24"},
        {"name": "Система", "description": "Статус сервиса"},
    ],
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
@app.post("/event", tags=["Вебхуки"])
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

        if _employees and from_user_id not in _employees:
            log.info(
                "  Пропущено: from_user_id=%d не найден в сотрудниках %s",
                from_user_id, list(_employees.keys()),
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

        # Добавляем подпись сотрудника
        employee = _employees.get(from_user_id, {})
        emp_name = employee.get("name", "")
        emp_pos  = employee.get("position", "")
        if emp_name:
            signature = f"\n\nС уважением,\n{emp_name}"
            if emp_pos:
                signature += f", {emp_pos}"
            clean_message = clean_message + signature

        log.info(
            "  Пересылаем → %s (TG %s): %s",
            contact.name, contact.telegram_id, clean_message[:80],
        )
        await send_telegram_reply(contact.telegram_id, clean_message)

    except Exception as exc:
        log.error("Ошибка обработки события: %s", exc, exc_info=True)

    return JSONResponse({"status": "ok"})


@app.post("/debug/event", tags=["Вебхуки"])
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


class LinkChannel(BaseModel):
    email: Optional[str] = None
    telegram_id: Optional[str] = None
    telegram_username: Optional[str] = None


@app.post("/contacts", status_code=201, tags=["Контакты"])
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


@app.post("/contacts/{contact_id}/link", tags=["Контакты"])
async def link_channel_to_contact(contact_id: int, data: LinkChannel):
    """
    Привязать email и/или telegram к существующему контакту.
    Например, чтобы письма с определённого email попадали в чат этого клиента.
    """
    if not data.email and not data.telegram_id:
        return JSONResponse(
            {"error": "Укажите хотя бы email или telegram_id"},
            status_code=400,
        )
    try:
        contact = await db.link_channel(
            contact_id=contact_id,
            email=data.email,
            telegram_id=data.telegram_id,
            telegram_username=data.telegram_username,
        )
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)

    if not contact:
        return JSONResponse({"error": "Контакт не найден"}, status_code=404)

    return {
        "id":                contact.id,
        "name":              contact.name,
        "telegram_id":       contact.telegram_id,
        "telegram_username": contact.telegram_username,
        "email":             contact.email,
        "bitrix_chat_id":    contact.bitrix_chat_id,
    }


@app.get("/contacts", tags=["Контакты"])
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


@app.delete("/contacts", tags=["Контакты"])
async def clear_contacts():
    """Удалить все контакты из базы данных."""
    import aiosqlite
    async with aiosqlite.connect(db.DB_PATH) as conn:
        await conn.execute("DELETE FROM contacts")
        await conn.commit()
    return {"status": "ok", "message": "Все контакты удалены"}


@app.get("/employees", tags=["Сотрудники"])
async def list_employees():
    """Получить всех активных сотрудников из Битрикс24 (с обновлением кэша)."""
    global _employees
    _employees = await fetch_all_employees()
    return [
        {"id": uid, "name": e["name"], "position": e["position"]}
        for uid, e in _employees.items()
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
@app.post("/contacts/{contact_id}/summary", tags=["Контакты"])
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
@app.post("/contacts/{contact_id}/brief", tags=["Контакты"])
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


# ─────────────────────────────────────────────────────────────────
# AI-анализ клиента и подбор группы сотрудников
# ─────────────────────────────────────────────────────────────────
_CLIENT_ANALYSIS_SYSTEM = """\
Ты — ассистент менеджера по B2B-продажам печатной продукции и упаковки.
Проанализируй первое сообщение (или переписку) нового клиента и верни ТОЛЬКО валидный JSON:

{
  "company":      "название компании клиента (если не найдено — null)",
  "segment":      "отрасль / сегмент (напитки, продукты, фармацевтика и т.д., если неясно — null)",
  "product_type": "тип продукции / материал (самоклейка, гофрокороб, PET и т.д., если неясно — null)",
  "volume":       "предполагаемый объём / тираж (если упомянут, иначе null)",
  "priority":     "VIP | Высокий | Средний | Низкий",
  "reasoning":    "краткое обоснование выбранного приоритета (1-2 предложения)"
}

Критерии приоритета:
- VIP: крупная компания, большие объёмы, стратегически важный сегмент, срочность, упоминание тендера или долгосрочного контракта
- Высокий: средний бизнес, конкретный крупный заказ, понятные требования
- Средний: стандартный запрос, малый/средний объём
- Низкий: разовый мелкий запрос, неясные потребности, просто вопрос без намерения заказать\
"""


async def _analyze_client(contact_name: str, message_text: str) -> dict:
    """Анализирует сообщение клиента через AI и возвращает структурированные данные."""
    import json as _json
    try:
        prompt = [
            {"role": "system", "content": _CLIENT_ANALYSIS_SYSTEM},
            {"role": "user", "content": f"Клиент: {contact_name}\n\nСообщение:\n{message_text}"},
        ]
        raw = await _openrouter_chat(prompt, json_mode=True)
        return _json.loads(raw)
    except Exception as exc:
        log.warning("AI-анализ клиента не удался: %s", exc)
        return {"priority": "Средний"}


async def _select_team_for_client(priority: str) -> list[db.Employee]:
    """
    Подбирает группу из 5 ролей для клиента.
    VIP → самые опытные (наивысший rating), остальные → не обязательно лучшие.
    """
    all_employees = await db.list_employees()
    if not all_employees:
        return []

    by_role: dict[str, list[db.Employee]] = {}
    for emp in all_employees:
        by_role.setdefault(emp.role, []).append(emp)

    # Сортируем по рейтингу: для VIP — лучших первыми
    is_vip = priority in ("VIP", "Высокий")

    team: list[db.Employee] = []
    for role in db.VALID_ROLES:
        candidates = by_role.get(role, [])
        if not candidates:
            continue
        candidates.sort(key=lambda e: e.rating, reverse=True)
        if is_vip:
            # Берём самого опытного
            team.append(candidates[0])
        else:
            # Берём наименее загруженного / среднего (последний по рейтингу, но не худший)
            idx = len(candidates) // 2 if len(candidates) > 2 else 0
            team.append(candidates[idx])
    return team


async def _create_card_and_assign_team(
    contact: db.Contact, first_message: str
) -> tuple[db.ClientCard, list[db.Employee]]:
    """
    Создаёт карточку клиента, анализирует через AI, подбирает команду.
    Возвращает (карточку, список сотрудников).
    """
    import json as _json

    analysis = await _analyze_client(contact.name, first_message)
    priority = analysis.get("priority", "Средний")
    team = await _select_team_for_client(priority)

    assigned_ids = [e.bitrix_user_id for e in team]
    card = await db.create_client_card(
        contact_id=contact.id,
        company=analysis.get("company"),
        segment=analysis.get("segment"),
        product_type=analysis.get("product_type"),
        volume=analysis.get("volume"),
        priority=priority,
        notes=analysis.get("reasoning"),
        assigned_employees=_json.dumps(assigned_ids),
    )
    log.info(
        "Карточка клиента #%d создана: priority=%s, team=%s",
        card.id, priority, [(e.name, e.role) for e in team],
    )
    return card, team


# ─────────────────────────────────────────────────────────────────
# REST: Управление сотрудниками
# ─────────────────────────────────────────────────────────────────
class EmployeeCreate(BaseModel):
    bitrix_user_id: int
    name: str
    role: str
    experience_text: str = ""
    rating: int = 5


@app.post("/employees/register", status_code=201, tags=["Сотрудники"])
async def register_employee(data: EmployeeCreate):
    """Зарегистрировать или обновить сотрудника с описанием опыта."""
    if data.role not in db.VALID_ROLES:
        return JSONResponse(
            {"error": f"Неверная роль. Допустимые: {db.VALID_ROLES}"},
            status_code=400,
        )
    if not 1 <= data.rating <= 10:
        return JSONResponse({"error": "rating должен быть от 1 до 10"}, status_code=400)
    emp = await db.upsert_employee(
        bitrix_user_id=data.bitrix_user_id,
        name=data.name,
        role=data.role,
        experience_text=data.experience_text,
        rating=data.rating,
    )
    return {
        "id": emp.id,
        "bitrix_user_id": emp.bitrix_user_id,
        "name": emp.name,
        "role": emp.role,
        "experience_text": emp.experience_text,
        "rating": emp.rating,
    }


@app.get("/employees/team", tags=["Сотрудники"])
async def list_team():
    """Список всех зарегистрированных сотрудников отдела продаж."""
    employees = await db.list_employees()
    return [
        {
            "id": e.id,
            "bitrix_user_id": e.bitrix_user_id,
            "name": e.name,
            "role": e.role,
            "experience_text": e.experience_text,
            "rating": e.rating,
        }
        for e in employees
    ]


# ─────────────────────────────────────────────────────────────────
# REST: Карточки клиентов
# ─────────────────────────────────────────────────────────────────
@app.get("/client-cards", tags=["Карточки клиентов"])
async def list_cards():
    """Список всех карточек клиентов."""
    cards = await db.list_client_cards()
    return [
        {
            "id": c.id,
            "contact_id": c.contact_id,
            "company": c.company,
            "segment": c.segment,
            "product_type": c.product_type,
            "volume": c.volume,
            "priority": c.priority,
            "notes": c.notes,
            "assigned_employees": c.assigned_employees,
            "created_at": c.created_at,
        }
        for c in cards
    ]


@app.get("/client-cards/{card_id}", tags=["Карточки клиентов"])
async def get_card(card_id: int):
    """Получить карточку клиента по ID."""
    card = await db.get_client_card(card_id)
    if not card:
        return JSONResponse({"error": "Карточка не найдена"}, status_code=404)
    return {
        "id": card.id,
        "contact_id": card.contact_id,
        "company": card.company,
        "segment": card.segment,
        "product_type": card.product_type,
        "volume": card.volume,
        "priority": card.priority,
        "notes": card.notes,
        "assigned_employees": card.assigned_employees,
        "created_at": card.created_at,
    }


@app.get("/contacts/{contact_id}/card", tags=["Контакты"])
async def get_contact_card(contact_id: int):
    """Получить карточку клиента по ID контакта."""
    card = await db.get_card_by_contact(contact_id)
    if not card:
        return JSONResponse({"error": "Карточка не найдена для этого контакта"}, status_code=404)
    return {
        "id": card.id,
        "contact_id": card.contact_id,
        "company": card.company,
        "segment": card.segment,
        "product_type": card.product_type,
        "volume": card.volume,
        "priority": card.priority,
        "notes": card.notes,
        "assigned_employees": card.assigned_employees,
        "created_at": card.created_at,
    }


# ─────────────────────────────────────────────────────────────────
# Интеграция с телефонией (phone_recording)
# ─────────────────────────────────────────────────────────────────
class CallSummaryRequest(BaseModel):
    call_id: str
    chat_id: int | str
    chat_title: str
    summary_markdown: str
    transcript_text: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    participants: list[str] | None = None  # имена участников звонка
    participant_telegram_ids: list[int] | None = None  # Telegram user IDs участников


def _format_call_time(iso_string: str) -> str:
    """Конвертирует ISO datetime в читаемый формат UTC+3 (Москва/Минск)."""
    from datetime import datetime, timezone, timedelta
    try:
        dt = datetime.fromisoformat(iso_string)
        utc3 = timezone(timedelta(hours=3))
        dt_local = dt.astimezone(utc3)
        return dt_local.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return iso_string


async def _find_contact_for_call(
    chat_title: str,
    participants: list[str] | None,
) -> Optional[db.Contact]:
    """
    Ищет контакт в БД по участникам звонка или названию чата.
    Сначала проверяет имена участников, потом title чата.
    """
    all_contacts = await db.list_all()
    if not all_contacts:
        return None

    # 1. Ищем по именам участников (точное совпадение имени контакта)
    if participants:
        for p_name in participants:
            p_lower = p_name.lower().strip()
            for c in all_contacts:
                if c.name.lower().strip() == p_lower:
                    return c

    # 2. Ищем по вхождению имени контакта в title чата
    title_lower = chat_title.lower()
    for c in all_contacts:
        if c.name.lower().strip() in title_lower:
            return c

    # 3. Частичное совпадение участников
    if participants:
        for p_name in participants:
            p_parts = set(p_name.lower().split())
            for c in all_contacts:
                c_parts = set(c.name.lower().split())
                if p_parts and c_parts and p_parts & c_parts:
                    return c

    return None


@app.post("/call-summary", tags=["Звонки"])
async def receive_call_summary(data: CallSummaryRequest):
    """
    Принимает summary звонка от сервиса phone_recording.
    По Telegram ID участников находит их контакты и отправляет summary
    в Битрикс-чат каждого участника. Если контакта нет — создаёт.
    """
    print(f"\n{'='*60}")
    print(f"[CALL-SUMMARY] Получен summary звонка")
    print(f"[CALL-SUMMARY] call_id={data.call_id}")
    print(f"[CALL-SUMMARY] chat_title={data.chat_title}")
    print(f"[CALL-SUMMARY] participants={data.participants}")
    print(f"[CALL-SUMMARY] participant_telegram_ids={data.participant_telegram_ids}")
    print(f"{'='*60}\n")

    log.info(
        "Получен summary звонка: call_id=%s, chat=%s, participants=%s, tg_ids=%s",
        data.call_id, data.chat_title, data.participants, data.participant_telegram_ids,
    )

    # ── 1. Собираем контакты участников по Telegram ID ──
    contacts: list[db.Contact] = []

    if data.participant_telegram_ids:
        for tg_id in data.participant_telegram_ids:
            print(f"[CALL-SUMMARY] Ищем контакт по Telegram ID: {tg_id}")
            contact = await db.get_by_telegram(str(tg_id))
            if contact:
                contacts.append(contact)
                print(f"[CALL-SUMMARY] ✓ Найден: {contact.name} (ID={contact.id}, bitrix_chat={contact.bitrix_chat_id})")
                log.info("Найден контакт по Telegram ID %s: %s (ID=%d)", tg_id, contact.name, contact.id)
            else:
                print(f"[CALL-SUMMARY] ✗ Контакт с Telegram ID {tg_id} НЕ найден в БД")

    # Если по Telegram ID никого не нашли — пробуем старый путь (имена/title)
    if not contacts:
        contact = await _find_contact_for_call(data.chat_title, data.participants)
        if contact:
            contacts.append(contact)

    # Если вообще ничего не нашли — создаём нового контакта
    if not contacts:
        client_name = data.chat_title.strip()
        if data.participants:
            employee_names = {e["name"].lower() for e in _employees.values()} if _employees else set()
            for p in data.participants:
                if p.lower().strip() not in employee_names:
                    client_name = p.strip()
                    break

        # Если есть telegram_id, привязываем его к контакту
        tg_id_str = str(data.participant_telegram_ids[0]) if data.participant_telegram_ids else None
        contact = await db.upsert_contact(name=client_name, telegram_id=tg_id_str)
        contacts.append(contact)
        log.info("Создан контакт из звонка: %s (ID=%d)", client_name, contact.id)

    # ── 2. Сохраняем звонок в БД ──
    import json as _json
    tg_ids_json = _json.dumps(data.participant_telegram_ids) if data.participant_telegram_ids else None
    call_record = await db.create_call(
        call_id=data.call_id,
        chat_title=data.chat_title,
        started_at=data.started_at,
        finished_at=data.finished_at,
        transcript_text=data.transcript_text,
        summary_text=data.summary_markdown,
        participant_telegram_ids=tg_ids_json,
    )
    # Привязываем участников
    for contact in contacts:
        await db.add_call_participant(call_record.id, contact.id)
    print(f"[CALL-SUMMARY] Звонок сохранён в БД: id={call_record.id}, call_id={call_record.call_id}")

    # ── 3. Формируем сообщение ──
    time_info = ""
    if data.started_at:
        time_info = f"\nНачало: {_format_call_time(data.started_at)}"
    if data.finished_at:
        time_info += f"\nОкончание: {_format_call_time(data.finished_at)}"

    summary_message = (
        f"Резюме звонка{time_info}\n\n"
        f"{data.summary_markdown}"
    )

    transcript_message = None
    if data.transcript_text:
        transcript_preview = data.transcript_text[:3000]
        if len(data.transcript_text) > 3000:
            transcript_preview += "\n\n... (транскрипция обрезана)"
        transcript_message = f"Транскрипция звонка:\n\n{transcript_preview}"

    # ── 4. Отправляем summary в чат каждого участника ──
    results = []
    for contact in contacts:
        dialog_id = contact.bitrix_chat_id

        if not dialog_id:
            # Создаём карточку и чат для этого контакта
            card, team = await _create_card_and_assign_team(contact, data.summary_markdown)

            if team:
                chat_user_ids = [e.bitrix_user_id for e in team]
                priority_label = card.priority
            else:
                chat_user_ids = None
                priority_label = "Средний"

            priority_emoji = {"VIP": "🔥", "Высокий": "🟠", "Средний": "🟡", "Низкий": "🟢"}.get(priority_label, "⚪")
            title = f"{priority_emoji} {contact.name} [{priority_label}]"
            dialog_id = await _create_bitrix_chat(title, chat_user_ids)
            await db.set_chat(contact.id, dialog_id)

            # Шапка чата
            lines = [f"👤 *Контакт: {contact.name}*"]
            if contact.telegram_id:
                lines.append(f"✈️ Telegram: {contact.telegram_username or contact.telegram_id}")
            if contact.email:
                lines.append(f"📧 Email: {contact.email}")
            lines.append("")
            lines.append(f"📋 *Карточка клиента #{card.id}*")
            lines.append(f"Приоритет: {priority_emoji} {card.priority}")
            if card.company:
                lines.append(f"Компания: {card.company}")
            if card.segment:
                lines.append(f"Сегмент: {card.segment}")
            if team:
                lines.append("")
                lines.append("👥 *Назначенная команда:*")
                for emp in team:
                    lines.append(f"  • {emp.name} — {emp.role} (рейтинг: {emp.rating}/10)")

            await _bot_send(dialog_id, "\n".join(lines))

            # Описание чата
            chat_id_num = dialog_id.replace("chat", "")
            desc_lines = [f"Приоритет: {priority_emoji} {card.priority}"]
            if card.company:
                desc_lines.append(f"Компания: {card.company}")
            if team:
                desc_lines.append("Команда: " + ", ".join(f"{e.name} ({e.role})" for e in team))
            try:
                await b24("im.chat.update", {
                    "CHAT_ID": int(chat_id_num),
                    "DESCRIPTION": "\n".join(desc_lines),
                })
            except Exception:
                pass

            log.info("Создан чат %s для контакта %s из звонка", dialog_id, contact.name)

        # Отправляем summary
        print(f"[CALL-SUMMARY] → Отправляем summary в Битрикс чат {dialog_id} контакта {contact.name}")
        await _bot_send(dialog_id, summary_message)
        if transcript_message:
            await _bot_send(dialog_id, transcript_message)
        print(f"[CALL-SUMMARY] ✓ Summary отправлен в {dialog_id}")

        log.info(
            "Summary звонка %s отправлен в чат %s контакта %s",
            data.call_id, dialog_id, contact.name,
        )
        results.append({
            "contact_id": contact.id,
            "contact_name": contact.name,
            "bitrix_chat_id": dialog_id,
        })

    return {
        "status": "ok",
        "delivered_to": results,
    }


# ─────────────────────────────────────────────────────────────────
# Рекомендации по улучшению звонка (AI)
# ─────────────────────────────────────────────────────────────────
_CALL_RECOMMENDATIONS_SYSTEM = """\
Ты — опытный тренер по продажам и деловым переговорам.
Проанализируй транскрипт звонка и верни JSON строго в следующем формате:
{
  "rating": число от 1 до 10 (общая оценка качества звонка),
  "explanation": "общее пояснение оценки — что было хорошо и что плохо (2-4 предложения)",
  "errors": [
    {
      "type": "тип ошибки (напр. 'Перебивание', 'Нет резюмирования', 'Слабое закрытие')",
      "description": "подробное описание ошибки",
      "recommendation": "конкретная рекомендация как исправить"
    }
  ]
}

Критерии оценки:
- Чёткость и структура разговора
- Активное слушание (не перебивали ли собеседника)
- Выявление потребностей клиента
- Презентация решения / ответы на вопросы
- Работа с возражениями
- Резюмирование договорённостей и следующие шаги
- Вежливость и профессионализм
- Управление временем звонка\
"""


class CallRecommendationsRequest(BaseModel):
    call_id: str | None = None
    transcript_text: str | None = None
    summary_markdown: str | None = None


@app.post("/call-recommendations", tags=["Звонки"])
async def get_call_recommendations(data: CallRecommendationsRequest):
    """
    Анализирует звонок и возвращает рекомендации:
    оценку, пояснение и список ошибок с рекомендациями.
    Если передан call_id без transcript_text — берёт данные из БД.
    Результат сохраняется в БД.
    """
    import json as _json

    transcript = data.transcript_text
    summary = data.summary_markdown

    # Если передан call_id — пробуем взять данные из БД
    if data.call_id and not transcript:
        call_record = await db.get_call_by_call_id(data.call_id)
        if call_record:
            transcript = call_record.transcript_text
            summary = summary or call_record.summary_text
        if not transcript:
            return JSONResponse(
                {"error": "Транскрипция не найдена. Передайте transcript_text или корректный call_id."},
                status_code=400,
            )

    if not transcript:
        return JSONResponse(
            {"error": "Необходимо передать transcript_text или call_id."},
            status_code=400,
        )

    content = f"Транскрипт звонка:\n{transcript}"
    if summary:
        content = f"Резюме звонка:\n{summary}\n\n{content}"

    prompt = [
        {"role": "system", "content": _CALL_RECOMMENDATIONS_SYSTEM},
        {"role": "user", "content": content},
    ]

    try:
        raw = await _openrouter_chat(prompt, json_mode=True)
        result = _json.loads(raw)
    except Exception as exc:
        log.error("Ошибка AI-анализа звонка: %s", exc)
        return JSONResponse(
            {"error": "Не удалось проанализировать звонок", "details": str(exc)},
            status_code=500,
        )

    # Нормализуем rating в диапазон 1-10
    rating = result.get("rating")
    if isinstance(rating, (int, float)):
        result["rating"] = max(1, min(10, int(rating)))

    # Сохраняем AI review в БД
    if data.call_id:
        await db.update_call(data.call_id, ai_review=_json.dumps(result, ensure_ascii=False))
        log.info("AI review сохранён для звонка %s", data.call_id)

    log.info(
        "Рекомендации по звонку %s: rating=%s, errors=%d",
        data.call_id or "?", result.get("rating"), len(result.get("errors", [])),
    )

    return result


# ─────────────────────────────────────────────────────────────────
# REST: Звонки
# ─────────────────────────────────────────────────────────────────
def _call_to_dict(call: db.Call) -> dict:
    import json as _json
    result = {
        "id": call.id,
        "call_id": call.call_id,
        "chat_title": call.chat_title,
        "started_at": call.started_at,
        "finished_at": call.finished_at,
        "has_transcript": bool(call.transcript_text),
        "has_summary": bool(call.summary_text),
        "has_ai_review": bool(call.ai_review),
        "created_at": call.created_at,
    }
    if call.started_at:
        result["started_at_formatted"] = _format_call_time(call.started_at)
    if call.finished_at:
        result["finished_at_formatted"] = _format_call_time(call.finished_at)
    if call.participant_telegram_ids:
        try:
            result["participant_telegram_ids"] = _json.loads(call.participant_telegram_ids)
        except Exception:
            result["participant_telegram_ids"] = []
    return result


def _call_to_detail_dict(call: db.Call) -> dict:
    import json as _json
    result = _call_to_dict(call)
    result["transcript_text"] = call.transcript_text
    result["summary_text"] = call.summary_text
    if call.ai_review:
        try:
            result["ai_review"] = _json.loads(call.ai_review)
        except Exception:
            result["ai_review"] = call.ai_review
    else:
        result["ai_review"] = None
    return result


@app.get("/calls", tags=["Звонки"])
async def list_calls():
    """Список всех звонков."""
    calls = await db.list_calls()
    return [_call_to_dict(c) for c in calls]


@app.get("/calls/contact/{contact_id}", tags=["Звонки"])
async def list_calls_by_contact(contact_id: int):
    """Все звонки с определённым контактом."""
    contact = await db.get_by_id(contact_id)
    if not contact:
        return JSONResponse({"error": "Контакт не найден"}, status_code=404)
    calls = await db.get_calls_by_contact(contact_id)
    return {
        "contact": {
            "id": contact.id,
            "name": contact.name,
            "telegram_id": contact.telegram_id,
        },
        "calls": [_call_to_dict(c) for c in calls],
    }


@app.get("/calls/{call_id}", tags=["Звонки"])
async def get_call_detail(call_id: str):
    """Подробная информация по конкретному звонку (включая транскрипцию, резюме, AI review)."""
    # Пробуем найти по внешнему call_id
    call = await db.get_call_by_call_id(call_id)
    # Если не нашли — пробуем как числовой id из БД
    if not call and call_id.isdigit():
        call = await db.get_call_by_id(int(call_id))
    if not call:
        return JSONResponse({"error": "Звонок не найден"}, status_code=404)

    result = _call_to_detail_dict(call)

    # Добавляем участников
    participants = await db.get_call_participants(call.id)
    result["participants"] = [
        {
            "id": c.id,
            "name": c.name,
            "telegram_id": c.telegram_id,
            "telegram_username": c.telegram_username,
        }
        for c in participants
    ]

    return result


@app.get("/health", tags=["Система"])
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
