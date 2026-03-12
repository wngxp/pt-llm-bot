from __future__ import annotations

import asyncio
from datetime import date
import logging
import re
from pathlib import Path
from typing import Optional

import discord
from discord.ext import commands

from config import Settings, load_settings
from db.database import Database
from llm.client import OllamaClient
from utils.discord_messages import send_discord_text


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


class PTBot(commands.Bot):
    def __init__(self, settings: Settings) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.messages = True

        super().__init__(
            command_prefix=settings.command_prefix,
            intents=intents,
            help_command=None,
        )
        self.settings = settings
        self.db = Database(settings.database_path)
        self.ollama = OllamaClient(settings.ollama_base_url, settings.ollama_model)
        self._startup_notified = False
        self._last_changelog_file = settings.database_path.parent / ".last_changelog_version"

    async def setup_hook(self) -> None:
        await self.db.init()
        for ext in [
            "cogs.programme",
            "cogs.workout",
            "cogs.activity",
            "cogs.checkin",
            "cogs.ask",
            "cogs.prs",
            "cogs.utility",
        ]:
            await self.load_extension(ext)
        logging.info("Loaded all cogs")

    async def close(self) -> None:
        await self.ollama.close()
        await super().close()

    def today(self) -> date:
        return date.today()

    async def on_ready(self) -> None:
        logging.info("Logged in as %s (%s)", self.user, getattr(self.user, "id", "?"))
        if self._startup_notified:
            return
        self._startup_notified = True
        await self._maybe_post_changelog_update()

    def _extract_latest_changelog(self) -> tuple[Optional[str], Optional[str]]:
        path = Path(__file__).with_name("CHANGELOG.md")
        if not path.exists():
            return None, None
        text = path.read_text(encoding="utf-8")
        match = re.search(
            r"^## \\[(?P<version>[^\\]]+)\\] - (?P<date>[^\\n]+)\\n(?P<body>.*?)(?=^## \\[|\\Z)",
            text,
            flags=re.MULTILINE | re.DOTALL,
        )
        if not match:
            return None, None
        version = match.group("version").strip()
        date_label = match.group("date").strip()
        body = match.group("body").strip()
        section = f"## [{version}] - {date_label}\\n{body}".strip()
        return version, section

    def _read_last_posted_version(self) -> str:
        if not self._last_changelog_file.exists():
            return ""
        return self._last_changelog_file.read_text(encoding="utf-8").strip()

    def _write_last_posted_version(self, version: str) -> None:
        self._last_changelog_file.parent.mkdir(parents=True, exist_ok=True)
        self._last_changelog_file.write_text(version.strip(), encoding="utf-8")

    async def _resolve_changelog_channel(self) -> Optional[discord.TextChannel]:
        if self.settings.changelog_channel_id:
            channel = self.get_channel(self.settings.changelog_channel_id)
            if isinstance(channel, discord.TextChannel):
                return channel
        for guild in self.guilds:
            for channel in guild.text_channels:
                if channel.name == "changelog":
                    return channel
        return None

    async def _maybe_post_changelog_update(self) -> None:
        version, section = self._extract_latest_changelog()
        if not version or not section:
            return

        last_posted = self._read_last_posted_version()
        if last_posted == version:
            return

        channel = await self._resolve_changelog_channel()
        if channel is None:
            logging.warning("Changelog channel not found; skipping startup changelog post.")
            return

        await send_discord_text(channel, f"PT-LLM Bot startup update\\n\\n{section}")
        self._write_last_posted_version(version)


async def main() -> None:
    settings = load_settings()
    bot = PTBot(settings)
    await bot.start(settings.discord_token)


if __name__ == "__main__":
    asyncio.run(main())
