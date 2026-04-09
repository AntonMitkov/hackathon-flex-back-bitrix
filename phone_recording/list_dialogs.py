from __future__ import annotations

import asyncio
import os

from dotenv import load_dotenv
from pyrogram import Client


load_dotenv()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1].strip()
    return value


async def async_main() -> None:
    app = Client(
        name="list_dialogs",
        api_id=int(_require_env("TELEGRAM_API_ID")),
        api_hash=_require_env("TELEGRAM_API_HASH"),
        session_string=_require_env("TELEGRAM_STRING_SESSION"),
        in_memory=True,
    )

    await app.start()
    try:
        print("Available dialogs:\n")
        async for dialog in app.get_dialogs():
            chat = dialog.chat
            username = f"@{chat.username}" if chat.username else "-"
            print(f"{chat.title or '<no title>'} | id={chat.id} | username={username} | type={chat.type}")
    finally:
        await app.stop()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
