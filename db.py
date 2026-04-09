"""
Реестр контактов — SQLite через aiosqlite.

Каждый контакт может иметь:
  - telegram_id + telegram_username
  - email
  - bitrix_chat_id  (один чат в Битрикс24 для всех сообщений этого контакта)
"""

import os
from dataclasses import dataclass
from typing import Optional

import aiosqlite

DB_PATH = os.getenv("DB_PATH", "contacts.db")


@dataclass
class Contact:
    id: int
    name: str
    telegram_id: Optional[str]
    telegram_username: Optional[str]
    email: Optional[str]
    bitrix_chat_id: Optional[str]


@dataclass
class Employee:
    id: int
    bitrix_user_id: int
    name: str
    role: str  # Руководитель | Активный продавец | Технолог | Экономист | Диспетчер
    experience_text: str  # Свободное описание: материалы, стаж, показатели
    rating: int  # 1-10, уровень опытности


@dataclass
class Call:
    id: int
    call_id: str  # внешний ID звонка (из phone_recording)
    chat_title: str
    started_at: Optional[str]
    finished_at: Optional[str]
    transcript_text: Optional[str]
    summary_text: Optional[str]
    ai_review: Optional[str]  # JSON: {rating, explanation, errors}
    participant_telegram_ids: Optional[str]  # JSON list
    created_at: Optional[str]


@dataclass
class CallParticipant:
    id: int
    call_db_id: int  # FK → calls.id
    contact_id: int  # FK → contacts.id


@dataclass
class ClientCard:
    id: int
    contact_id: int
    company: Optional[str]
    segment: Optional[str]
    product_type: Optional[str]
    volume: Optional[str]
    priority: str  # VIP | Высокий | Средний | Низкий
    notes: Optional[str]
    assigned_employees: Optional[str]  # JSON список ID сотрудников
    created_at: Optional[str]


def _row_to_contact(row: aiosqlite.Row) -> Contact:
    return Contact(
        id=row["id"],
        name=row["name"],
        telegram_id=row["telegram_id"],
        telegram_username=row["telegram_username"],
        email=row["email"],
        bitrix_chat_id=row["bitrix_chat_id"],
    )


def _row_to_employee(row: aiosqlite.Row) -> Employee:
    return Employee(
        id=row["id"],
        bitrix_user_id=row["bitrix_user_id"],
        name=row["name"],
        role=row["role"],
        experience_text=row["experience_text"],
        rating=row["rating"],
    )


def _row_to_call(row: aiosqlite.Row) -> Call:
    return Call(
        id=row["id"],
        call_id=row["call_id"],
        chat_title=row["chat_title"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        transcript_text=row["transcript_text"],
        summary_text=row["summary_text"],
        ai_review=row["ai_review"],
        participant_telegram_ids=row["participant_telegram_ids"],
        created_at=row["created_at"],
    )


def _row_to_client_card(row: aiosqlite.Row) -> ClientCard:
    return ClientCard(
        id=row["id"],
        contact_id=row["contact_id"],
        company=row["company"],
        segment=row["segment"],
        product_type=row["product_type"],
        volume=row["volume"],
        priority=row["priority"],
        notes=row["notes"],
        assigned_employees=row["assigned_employees"],
        created_at=row["created_at"],
    )


VALID_ROLES = [
    "Руководитель",
    "Активный продавец",
    "Технолог",
    "Экономист",
    "Диспетчер",
]


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS contacts (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                name              TEXT NOT NULL,
                telegram_id       TEXT UNIQUE,
                telegram_username TEXT,
                email             TEXT UNIQUE,
                bitrix_chat_id    TEXT UNIQUE,
                created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                bitrix_user_id  INTEGER UNIQUE NOT NULL,
                name            TEXT NOT NULL,
                role            TEXT NOT NULL,
                experience_text TEXT NOT NULL DEFAULT '',
                rating          INTEGER NOT NULL DEFAULT 5
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS client_cards (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                contact_id          INTEGER NOT NULL REFERENCES contacts(id),
                company             TEXT,
                segment             TEXT,
                product_type        TEXT,
                volume              TEXT,
                priority            TEXT NOT NULL DEFAULT 'Средний',
                notes               TEXT,
                assigned_employees  TEXT,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS calls (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                call_id                 TEXT UNIQUE NOT NULL,
                chat_title              TEXT NOT NULL,
                started_at              TEXT,
                finished_at             TEXT,
                transcript_text         TEXT,
                summary_text            TEXT,
                ai_review               TEXT,
                participant_telegram_ids TEXT,
                created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS call_participants (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                call_db_id  INTEGER NOT NULL REFERENCES calls(id),
                contact_id  INTEGER NOT NULL REFERENCES contacts(id),
                UNIQUE(call_db_id, contact_id)
            )
        """)
        await db.commit()


async def upsert_contact(
    name: str,
    telegram_id: Optional[str] = None,
    telegram_username: Optional[str] = None,
    email: Optional[str] = None,
) -> Contact:
    """
    Создаёт нового контакта или обновляет существующего.
    Поиск ведётся по telegram_id, затем по email.
    """
    norm_email = email.lower() if email else None

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        existing_id: Optional[int] = None

        if telegram_id:
            async with db.execute(
                "SELECT id FROM contacts WHERE telegram_id = ?", (telegram_id,)
            ) as cur:
                row = await cur.fetchone()
                if row:
                    existing_id = row["id"]

        if existing_id is None and norm_email:
            async with db.execute(
                "SELECT id FROM contacts WHERE email = ?", (norm_email,)
            ) as cur:
                row = await cur.fetchone()
                if row:
                    existing_id = row["id"]

        if existing_id is not None:
            await db.execute(
                """
                UPDATE contacts SET
                    name              = ?,
                    telegram_id       = COALESCE(?, telegram_id),
                    telegram_username = COALESCE(?, telegram_username),
                    email             = COALESCE(?, email)
                WHERE id = ?
                """,
                (name, telegram_id, telegram_username, norm_email, existing_id),
            )
            await db.commit()
            async with db.execute(
                "SELECT * FROM contacts WHERE id = ?", (existing_id,)
            ) as cur:
                return _row_to_contact(await cur.fetchone())
        else:
            await db.execute(
                "INSERT INTO contacts (name, telegram_id, telegram_username, email) VALUES (?, ?, ?, ?)",
                (name, telegram_id, telegram_username, norm_email),
            )
            await db.commit()
            async with db.execute(
                "SELECT * FROM contacts WHERE id = last_insert_rowid()"
            ) as cur:
                return _row_to_contact(await cur.fetchone())


async def link_channel(
    contact_id: int,
    email: Optional[str] = None,
    telegram_id: Optional[str] = None,
    telegram_username: Optional[str] = None,
) -> Optional[Contact]:
    """
    Привязывает email и/или telegram к существующему контакту.
    Возвращает обновлённый контакт или None если контакт не найден.
    Бросает ValueError если email/telegram уже заняты другим контактом.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Проверяем что контакт существует
        async with db.execute("SELECT * FROM contacts WHERE id = ?", (contact_id,)) as cur:
            row = await cur.fetchone()
        if not row:
            return None

        norm_email = email.lower().strip() if email else None

        # Проверяем что email не занят другим контактом
        if norm_email:
            async with db.execute(
                "SELECT id FROM contacts WHERE email = ? AND id != ?", (norm_email, contact_id)
            ) as cur:
                conflict = await cur.fetchone()
            if conflict:
                raise ValueError(f"Email {norm_email} уже привязан к контакту #{conflict['id']}")

        # Проверяем что telegram_id не занят другим контактом
        if telegram_id:
            async with db.execute(
                "SELECT id FROM contacts WHERE telegram_id = ? AND id != ?", (telegram_id, contact_id)
            ) as cur:
                conflict = await cur.fetchone()
            if conflict:
                raise ValueError(f"Telegram ID {telegram_id} уже привязан к контакту #{conflict['id']}")

        # Обновляем только переданные поля
        updates = []
        values = []
        if norm_email:
            updates.append("email = ?")
            values.append(norm_email)
        if telegram_id:
            updates.append("telegram_id = ?")
            values.append(telegram_id)
        if telegram_username:
            updates.append("telegram_username = ?")
            values.append(telegram_username)

        if not updates:
            return _row_to_contact(row)

        values.append(contact_id)
        await db.execute(
            f"UPDATE contacts SET {', '.join(updates)} WHERE id = ?", values
        )
        await db.commit()

        async with db.execute("SELECT * FROM contacts WHERE id = ?", (contact_id,)) as cur:
            return _row_to_contact(await cur.fetchone())


async def get_by_id(contact_id: int) -> Optional[Contact]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM contacts WHERE id = ?", (contact_id,)
        ) as cur:
            row = await cur.fetchone()
    return _row_to_contact(row) if row else None


async def get_by_telegram(telegram_id: str) -> Optional[Contact]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM contacts WHERE telegram_id = ?", (telegram_id,)
        ) as cur:
            row = await cur.fetchone()
    return _row_to_contact(row) if row else None


async def get_by_email(email: str) -> Optional[Contact]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM contacts WHERE email = ?", (email.lower(),)
        ) as cur:
            row = await cur.fetchone()
    return _row_to_contact(row) if row else None


async def get_by_chat(bitrix_chat_id: str) -> Optional[Contact]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM contacts WHERE bitrix_chat_id = ?", (bitrix_chat_id,)
        ) as cur:
            row = await cur.fetchone()
    return _row_to_contact(row) if row else None


async def set_chat(contact_id: int, bitrix_chat_id: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE contacts SET bitrix_chat_id = ? WHERE id = ?",
            (bitrix_chat_id, contact_id),
        )
        await db.commit()


async def list_all() -> list[Contact]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM contacts ORDER BY created_at DESC"
        ) as cur:
            rows = await cur.fetchall()
    return [_row_to_contact(r) for r in rows]


# ─────────────────────────────────────────────────────────────────
# Сотрудники
# ─────────────────────────────────────────────────────────────────
async def upsert_employee(
    bitrix_user_id: int,
    name: str,
    role: str,
    experience_text: str = "",
    rating: int = 5,
) -> Employee:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id FROM employees WHERE bitrix_user_id = ?", (bitrix_user_id,)
        ) as cur:
            row = await cur.fetchone()

        if row:
            await db.execute(
                """UPDATE employees SET name=?, role=?, experience_text=?, rating=?
                   WHERE bitrix_user_id=?""",
                (name, role, experience_text, rating, bitrix_user_id),
            )
            await db.commit()
            async with db.execute(
                "SELECT * FROM employees WHERE bitrix_user_id=?", (bitrix_user_id,)
            ) as cur:
                return _row_to_employee(await cur.fetchone())
        else:
            await db.execute(
                "INSERT INTO employees (bitrix_user_id, name, role, experience_text, rating) VALUES (?,?,?,?,?)",
                (bitrix_user_id, name, role, experience_text, rating),
            )
            await db.commit()
            async with db.execute(
                "SELECT * FROM employees WHERE id = last_insert_rowid()"
            ) as cur:
                return _row_to_employee(await cur.fetchone())


async def list_employees() -> list[Employee]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM employees ORDER BY role, rating DESC") as cur:
            rows = await cur.fetchall()
    return [_row_to_employee(r) for r in rows]


async def get_employees_by_role(role: str) -> list[Employee]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM employees WHERE role=? ORDER BY rating DESC", (role,)
        ) as cur:
            rows = await cur.fetchall()
    return [_row_to_employee(r) for r in rows]


async def get_employee_by_bitrix_id(bitrix_user_id: int) -> Optional[Employee]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM employees WHERE bitrix_user_id=?", (bitrix_user_id,)
        ) as cur:
            row = await cur.fetchone()
    return _row_to_employee(row) if row else None


# ─────────────────────────────────────────────────────────────────
# Карточки клиентов
# ─────────────────────────────────────────────────────────────────
async def create_client_card(
    contact_id: int,
    company: Optional[str] = None,
    segment: Optional[str] = None,
    product_type: Optional[str] = None,
    volume: Optional[str] = None,
    priority: str = "Средний",
    notes: Optional[str] = None,
    assigned_employees: Optional[str] = None,
) -> ClientCard:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute(
            """INSERT INTO client_cards
               (contact_id, company, segment, product_type, volume, priority, notes, assigned_employees)
               VALUES (?,?,?,?,?,?,?,?)""",
            (contact_id, company, segment, product_type, volume, priority, notes, assigned_employees),
        )
        await db.commit()
        async with db.execute("SELECT * FROM client_cards WHERE id = last_insert_rowid()") as cur:
            return _row_to_client_card(await cur.fetchone())


async def update_client_card(card_id: int, **fields) -> Optional[ClientCard]:
    allowed = {"company", "segment", "product_type", "volume", "priority", "notes", "assigned_employees"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return await get_client_card(card_id)
    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [card_id]
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute(f"UPDATE client_cards SET {set_clause} WHERE id=?", values)
        await db.commit()
        async with db.execute("SELECT * FROM client_cards WHERE id=?", (card_id,)) as cur:
            row = await cur.fetchone()
    return _row_to_client_card(row) if row else None


async def get_client_card(card_id: int) -> Optional[ClientCard]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM client_cards WHERE id=?", (card_id,)) as cur:
            row = await cur.fetchone()
    return _row_to_client_card(row) if row else None


async def get_card_by_contact(contact_id: int) -> Optional[ClientCard]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM client_cards WHERE contact_id=? ORDER BY created_at DESC LIMIT 1",
            (contact_id,),
        ) as cur:
            row = await cur.fetchone()
    return _row_to_client_card(row) if row else None


async def list_client_cards() -> list[ClientCard]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM client_cards ORDER BY created_at DESC") as cur:
            rows = await cur.fetchall()
    return [_row_to_client_card(r) for r in rows]


# ─────────────────────────────────────────────────────────────────
# Звонки
# ─────────────────────────────────────────────────────────────────
async def create_call(
    call_id: str,
    chat_title: str,
    started_at: Optional[str] = None,
    finished_at: Optional[str] = None,
    transcript_text: Optional[str] = None,
    summary_text: Optional[str] = None,
    participant_telegram_ids: Optional[str] = None,
) -> Call:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.execute(
            """INSERT INTO calls
               (call_id, chat_title, started_at, finished_at, transcript_text, summary_text, participant_telegram_ids)
               VALUES (?,?,?,?,?,?,?)""",
            (call_id, chat_title, started_at, finished_at, transcript_text, summary_text, participant_telegram_ids),
        )
        await conn.commit()
        async with conn.execute("SELECT * FROM calls WHERE id = last_insert_rowid()") as cur:
            return _row_to_call(await cur.fetchone())


async def get_call_by_call_id(call_id: str) -> Optional[Call]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM calls WHERE call_id = ?", (call_id,)) as cur:
            row = await cur.fetchone()
    return _row_to_call(row) if row else None


async def get_call_by_id(db_id: int) -> Optional[Call]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM calls WHERE id = ?", (db_id,)) as cur:
            row = await cur.fetchone()
    return _row_to_call(row) if row else None


async def update_call(call_id: str, **fields) -> Optional[Call]:
    allowed = {"transcript_text", "summary_text", "ai_review", "participant_telegram_ids"}
    updates = {k: v for k, v in fields.items() if k in allowed and v is not None}
    if not updates:
        return await get_call_by_call_id(call_id)
    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [call_id]
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.execute(f"UPDATE calls SET {set_clause} WHERE call_id=?", values)
        await conn.commit()
        async with conn.execute("SELECT * FROM calls WHERE call_id=?", (call_id,)) as cur:
            row = await cur.fetchone()
    return _row_to_call(row) if row else None


async def list_calls() -> list[Call]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM calls ORDER BY created_at DESC") as cur:
            rows = await cur.fetchall()
    return [_row_to_call(r) for r in rows]


async def add_call_participant(call_db_id: int, contact_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO call_participants (call_db_id, contact_id) VALUES (?,?)",
            (call_db_id, contact_id),
        )
        await conn.commit()


async def get_calls_by_contact(contact_id: int) -> list[Call]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            """SELECT c.* FROM calls c
               JOIN call_participants cp ON cp.call_db_id = c.id
               WHERE cp.contact_id = ?
               ORDER BY c.created_at DESC""",
            (contact_id,),
        ) as cur:
            rows = await cur.fetchall()
    return [_row_to_call(r) for r in rows]


async def get_call_participants(call_db_id: int) -> list[Contact]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            """SELECT ct.* FROM contacts ct
               JOIN call_participants cp ON cp.contact_id = ct.id
               WHERE cp.call_db_id = ?""",
            (call_db_id,),
        ) as cur:
            rows = await cur.fetchall()
    return [_row_to_contact(r) for r in rows]
