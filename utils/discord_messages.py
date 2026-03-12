from __future__ import annotations

from typing import Iterable


DISCORD_MESSAGE_LIMIT = 4000


def split_discord_message(text: str, limit: int = DISCORD_MESSAGE_LIMIT) -> list[str]:
    message = str(text or "").strip()
    if not message:
        return []
    if len(message) <= limit:
        return [message]

    chunks: list[str] = []
    remaining = message
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break

        window = remaining[:limit]
        cut = window.rfind("\n\n")
        if cut < int(limit * 0.4):
            cut = window.rfind("\n")
        if cut < int(limit * 0.4):
            cut = window.rfind(" ")
        if cut <= 0:
            cut = limit

        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()

    return [chunk for chunk in chunks if chunk]


async def send_discord_text(channel: object, text: str) -> None:
    sender = getattr(channel, "send", None)
    if sender is None:
        return
    for chunk in split_discord_message(text):
        await sender(chunk)


async def send_discord_file(channel: object, *, file: object) -> None:
    sender = getattr(channel, "send", None)
    if sender is None:
        return
    await sender(file=file)


async def send_lines(channel: object, lines: Iterable[str]) -> None:
    await send_discord_text(channel, "\n".join(str(line) for line in lines if line is not None))
