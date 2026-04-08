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


def _row_to_contact(row: aiosqlite.Row) -> Contact:
    return Contact(
        id=row["id"],
        name=row["name"],
        telegram_id=row["telegram_id"],
        telegram_username=row["telegram_username"],
        email=row["email"],
        bitrix_chat_id=row["bitrix_chat_id"],
    )


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
