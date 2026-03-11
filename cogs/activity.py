from __future__ import annotations

from datetime import date
from typing import Any

import discord
from discord.ext import commands

from llm.prompts import ACTIVITY_IMPACT_SYSTEM_PROMPT


class ActivityCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.settings = bot.settings
        self.db = bot.db

    def _is_activity_channel(self, channel: discord.abc.GuildChannel | discord.Thread | discord.abc.PrivateChannel) -> bool:
        cid = getattr(channel, "id", None)
        name = getattr(channel, "name", "")
        if self.settings.activity_channel_id:
            return cid == self.settings.activity_channel_id
        return name == "activity"

    async def _classify_activity(self, text: str) -> dict[str, Any]:
        try:
            out = await self.bot.ollama.chat_json(
                system=ACTIVITY_IMPACT_SYSTEM_PROMPT,
                user=text,
                temperature=0.1,
            )
            return {
                "activity_type": str(out.get("activity_type") or "general"),
                "intensity": str(out.get("intensity") or "moderate"),
                "muscle_groups": str(out.get("muscle_groups") or ""),
                "short_note": str(out.get("short_note") or text),
            }
        except Exception:
            return {
                "activity_type": "general",
                "intensity": "moderate",
                "muscle_groups": "",
                "short_note": text,
            }

    @commands.command(name="activity")
    async def activity_command(self, ctx: commands.Context, *, description: str) -> None:
        classification = await self._classify_activity(description)
        await self.db.add_activity(
            activity_date=date.today(),
            activity_type=classification["activity_type"],
            description=description,
            intensity=classification["intensity"],
            muscle_groups=classification["muscle_groups"],
        )
        await ctx.send(
            f"Logged activity: {classification['activity_type']} "
            f"({classification['intensity']}) affecting [{classification['muscle_groups'] or 'unspecified'}]."
        )

    @commands.command(name="readiness")
    async def readiness_command(self, ctx: commands.Context, score: int) -> None:
        score = max(1, min(10, score))
        await self.db.update_user_state(readiness=score)
        await ctx.send(f"Readiness updated to {score}/10.")

    @commands.command(name="phase")
    async def phase_command(self, ctx: commands.Context, phase: str) -> None:
        phase = phase.lower().strip()
        if phase not in {"cut", "bulk", "maintain"}:
            await ctx.send("Phase must be one of: cut, bulk, maintain.")
            return
        await self.db.update_user_state(phase=phase)
        await ctx.send(f"Phase set to {phase}.")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        if not self._is_activity_channel(message.channel):
            return

        content = message.content.strip()
        if not content or content.startswith(self.settings.command_prefix):
            return

        classification = await self._classify_activity(content)
        await self.db.add_activity(
            activity_date=date.today(),
            activity_type=classification["activity_type"],
            description=content,
            intensity=classification["intensity"],
            muscle_groups=classification["muscle_groups"],
        )
        await message.channel.send(
            f"Logged: {classification['activity_type']} ({classification['intensity']}) "
            f"for {classification['muscle_groups'] or 'general recovery context'}."
        )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ActivityCog(bot))
