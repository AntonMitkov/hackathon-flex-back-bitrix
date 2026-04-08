"""
Получение Telethon StringSession для заполнения TG_SESSION в .env

Запуск:
  uv run get_session.py
"""

import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

TG_API_ID   = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH", "")


async def main():
    from telethon import TelegramClient
    from telethon.sessions import StringSession

    if not TG_API_ID or not TG_API_HASH:
        print("Задайте TG_API_ID и TG_API_HASH в .env")
        return

    async with TelegramClient(StringSession(), TG_API_ID, TG_API_HASH) as client:
        session_string = client.session.save()
        print("\n✅ Скопируйте строку ниже в TG_SESSION в .env:\n")
        print(session_string)
        print()


if __name__ == "__main__":
    asyncio.run(main())
