from __future__ import annotations

import asyncio
import re
from typing import Iterable


DISCORD_MESSAGE_LIMIT = 2000
SAFE_DISCORD_MESSAGE_LIMIT = 1900
SENTENCE_BOUNDARY_RE = re.compile(r"(?<=[.!?])\s+")


def _find_last_cut(window: str, limit: int) -> int:
    paragraph = window.rfind("\n\n")
    if paragraph > 0:
        return paragraph + 2

    newline = window.rfind("\n")
    if newline > 0:
        return newline + 1

    sentence_cut = -1
    for match in SENTENCE_BOUNDARY_RE.finditer(window):
        sentence_cut = match.end()
    if sentence_cut > 0:
        return sentence_cut

    space = window.rfind(" ")
    if space > 0:
        return space + 1

    return min(limit, len(window))


def split_discord_message(text: str, limit: int = SAFE_DISCORD_MESSAGE_LIMIT) -> list[str]:
    message = str(text or "")
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
        cut = _find_last_cut(window, limit)
        if cut <= 0:
            cut = min(limit, len(window))

        chunks.append(remaining[:cut])
        remaining = remaining[cut:]

    return [chunk for chunk in chunks if chunk]


async def send_long_message(channel: object, text: str, *, limit: int = SAFE_DISCORD_MESSAGE_LIMIT) -> None:
    sender = getattr(channel, "send", None)
    if sender is None:
        return
    chunks = split_discord_message(text, limit=limit)
    for index, chunk in enumerate(chunks):
        if index > 0:
            await asyncio.sleep(0.3)
        await sender(chunk)


async def send_discord_text(channel: object, text: str) -> None:
    await send_long_message(channel, text)


async def send_discord_file(channel: object, *, file: object) -> None:
    sender = getattr(channel, "send", None)
    if sender is None:
        return
    await sender(file=file)


async def send_lines(channel: object, lines: Iterable[str]) -> None:
    await send_discord_text(channel, "\n".join(str(line) for line in lines if line is not None))
