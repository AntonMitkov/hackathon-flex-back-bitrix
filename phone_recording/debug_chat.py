from __future__ import annotations

import asyncio
import os
from pprint import pformat

from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import PeerIdInvalid
from pyrogram.raw.functions.channels import GetFullChannel
from pyrogram.raw.functions.messages import GetFullChat
from pyrogram.raw.functions.phone import GetGroupCall, GetGroupCallJoinAs
from pyrogram.raw.types import InputPeerChannel, InputPeerChat


load_dotenv()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1].strip()
    return value


async def _build_dialog_cache(app: Client) -> dict[str, object]:
    cache: dict[str, object] = {}
    async for dialog in app.get_dialogs():
        chat = dialog.chat
        cache[str(chat.id)] = chat
        if str(chat.id).startswith("-100"):
            cache[f"-{str(chat.id)[4:]}"] = chat
        if chat.username:
            cache[chat.username.lower()] = chat
            cache[f"@{chat.username.lower()}"] = chat
    return cache


async def async_main() -> None:
    target = os.getenv("DEBUG_CHAT_TARGET") or os.getenv("TELEGRAM_TARGET_CHATS", "").split(",")[0].strip()
    if not target:
        raise ValueError("Set DEBUG_CHAT_TARGET or TELEGRAM_TARGET_CHATS in .env")

    app = Client(
        name="chat_debug",
        api_id=int(_require_env("TELEGRAM_API_ID")),
        api_hash=_require_env("TELEGRAM_API_HASH"),
        session_string=_require_env("TELEGRAM_STRING_SESSION"),
        in_memory=True,
    )

    await app.start()
    try:
        print(f"Target: {target}")
        dialog_cache = await _build_dialog_cache(app)
        try:
            chat = await app.get_chat(target)
        except PeerIdInvalid:
            chat = dialog_cache.get(target) or dialog_cache.get(target.lower())
            if chat is None:
                print("\nTarget was not found in this account's dialog list.")
                print("Open the chat with this Telegram account first, or use @username instead of numeric id.")
                print("\nKnown dialogs:")
                for key, value in sorted(dialog_cache.items(), key=lambda item: str(item[0])):
                    if str(key).startswith("@") or str(key).lstrip("-").isdigit():
                        print(f"- {value.title or '<no title>'} | id={value.id} | username={('@' + value.username) if value.username else '-'}")
                return

        print("\n=== Resolved Chat ===")
        print(f"Title: {chat.title or '<no title>'}")
        print(f"Username: @{chat.username}" if chat.username else "Username: <no username>")
        print(f"Resolved chat id: {chat.id}")
        print(f"Type: {chat.type}")

        input_peer = await app.resolve_peer(chat.id)
        print(f"Input peer type: {type(input_peer).__name__}")
        print(f"Input peer: {input_peer}")

        if isinstance(input_peer, InputPeerChannel):
            full = await app.invoke(GetFullChannel(channel=input_peer))
            full_chat = full.full_chat
        elif isinstance(input_peer, InputPeerChat):
            full = await app.invoke(GetFullChat(chat_id=input_peer.chat_id))
            full_chat = full.full_chat
        else:
            print("Unsupported peer type for voice chat diagnostics")
            return

        print("\nfull_chat.call:")
        print(pformat(getattr(full_chat, "call", None)))

        print("\nfull_chat.groupcall_default_join_as:")
        print(pformat(getattr(full_chat, "groupcall_default_join_as", None)))

        print("\nGetGroupCallJoinAs:")
        try:
            join_as = await app.invoke(GetGroupCallJoinAs(peer=input_peer))
            print(pformat(join_as))
        except Exception as exc:
            print(f"Failed: {exc}")

        group_call = getattr(full_chat, "call", None)
        if group_call is not None:
            print("\nGetGroupCall:")
            try:
                call_info = await app.invoke(GetGroupCall(call=group_call, limit=10))
                print(pformat(call_info))
            except Exception as exc:
                print(f"Failed: {exc}")
        else:
            print("\nNo active group call is visible in full_chat.call")
    finally:
        await app.stop()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
