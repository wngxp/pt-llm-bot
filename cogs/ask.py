from __future__ import annotations

import json

import discord
from discord.ext import commands

from llm.prompts import ASK_SYSTEM_PROMPT


class AskCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.settings = bot.settings
        self.db = bot.db

    def _is_ask_channel(self, channel: discord.abc.GuildChannel | discord.Thread | discord.abc.PrivateChannel) -> bool:
        cid = getattr(channel, "id", None)
        name = getattr(channel, "name", "")
        if self.settings.ask_channel_id:
            return cid == self.settings.ask_channel_id
        return name == "ask"

    async def _answer(self, question: str) -> str:
        context = await self.db.build_context(target_date=self.bot.today())
        prompt = {
            "question": question,
            "context": {
                "phase": context["user_state"].get("phase"),
                "readiness": context["user_state"].get("readiness"),
                "weekly_volume": context.get("weekly_volume"),
                "recent_prs": context.get("recent_prs")[:5],
            },
        }
        return await self.bot.ollama.chat(
            system=ASK_SYSTEM_PROMPT,
            user=json.dumps(prompt, ensure_ascii=False),
            temperature=0.3,
        )

    @commands.command(name="ask")
    async def ask_command(self, ctx: commands.Context, *, question: str) -> None:
        reply = await self._answer(question)
        await ctx.send(reply)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        if not self._is_ask_channel(message.channel):
            return

        content = message.content.strip()
        if not content or content.startswith(self.settings.command_prefix):
            return

        try:
            reply = await self._answer(content)
            await message.channel.send(reply)
        except Exception as exc:
            await message.channel.send(f"Couldn't reach coaching model: {exc}")


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AskCog(bot))
