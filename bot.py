from __future__ import annotations

import asyncio
from datetime import date
import logging

import discord
from discord.ext import commands

from config import Settings, load_settings
from db.database import Database
from llm.client import OllamaClient


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

        super().__init__(command_prefix=settings.command_prefix, intents=intents)
        self.settings = settings
        self.db = Database(settings.database_path)
        self.ollama = OllamaClient(settings.ollama_base_url, settings.ollama_model)

    async def setup_hook(self) -> None:
        await self.db.init()
        for ext in [
            "cogs.programme",
            "cogs.workout",
            "cogs.activity",
            "cogs.checkin",
            "cogs.ask",
            "cogs.prs",
        ]:
            await self.load_extension(ext)
        logging.info("Loaded all cogs")

    async def close(self) -> None:
        await self.ollama.close()
        await super().close()

    def today(self) -> date:
        return date.today()


async def main() -> None:
    settings = load_settings()
    bot = PTBot(settings)
    await bot.start(settings.discord_token)


if __name__ == "__main__":
    asyncio.run(main())
