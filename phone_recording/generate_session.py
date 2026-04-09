from __future__ import annotations

import os

from dotenv import load_dotenv
from pyrogram import Client

load_dotenv()


def main() -> None:
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    if not api_id or not api_hash:
        raise ValueError("TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in .env")

    with Client(
        name="session_generator",
        api_id=int(api_id),
        api_hash=api_hash,
        in_memory=True,
    ) as app:
        print("\nTELEGRAM_STRING_SESSION=")
        print(app.export_session_string())
