from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

import discord
from discord.ext import commands

from utils.discord_messages import send_discord_text
from utils.numbers import format_standard_number


logger = logging.getLogger(__name__)


class PRsCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.settings = bot.settings
        self.db = bot.db

    async def _get_pr_channel(self) -> Optional[discord.TextChannel]:
        if self.settings.prs_channel_id:
            channel = self.bot.get_channel(self.settings.prs_channel_id)
            if isinstance(channel, discord.TextChannel):
                return channel
            try:
                fetched = await self.bot.fetch_channel(self.settings.prs_channel_id)
                if isinstance(fetched, discord.TextChannel):
                    return fetched
            except Exception as exc:
                logger.warning(
                    "Failed to fetch PR channel id=%s: %s",
                    self.settings.prs_channel_id,
                    exc,
                )

        for guild in self.bot.guilds:
            for channel in guild.text_channels:
                if channel.name == "prs":
                    return channel
        return None

    @commands.Cog.listener()
    async def on_pr_hit(self, payload: dict[str, Any]) -> None:
        logger.info(
            "on_pr_hit payload: exercise=%s first=%s e1rm=%s performer_id=%s",
            payload.get("exercise_name"),
            payload.get("is_first_benchmark"),
            payload.get("e1rm"),
            payload.get("performer_user_id"),
        )
        channel = await self._get_pr_channel()
        if not channel:
            logger.warning("PR event dropped: PRs channel could not be resolved.")
            return

        logger.info("Posting PR update to channel id=%s", channel.id)

        performer_user_id = str(payload.get("performer_user_id") or "").strip()
        performer = str(payload.get("performer_name") or "").strip()
        if performer_user_id:
            performer_prefix = f"<@{performer_user_id}> "
        elif performer:
            performer_prefix = f"{performer} "
        else:
            performer_prefix = ""

        if payload.get("is_first_benchmark"):
            lines = [
                (
                    f"📊 {performer_prefix}just logged their first {payload['exercise_name']}: "
                    f"{format_standard_number(float(payload['weight']))} {payload['unit']} x {payload['reps']}"
                ),
                f"(e1RM: {format_standard_number(float(payload['e1rm']))}) - this is their starting benchmark.",
            ]
            await send_discord_text(channel, "\n".join(lines))
            return

        previous = payload.get("previous")
        lines = [
            f"🏆 {performer_prefix}hit a new {payload['exercise_name']} PR!",
            (
                f"{format_standard_number(float(payload['weight']))} {payload['unit']} x {payload['reps']} "
                f"(estimated 1RM: {format_standard_number(float(payload['e1rm']))})"
            ),
        ]
        if previous:
            prev_e1rm = float(previous["estimated_1rm"])
            gain = payload["e1rm"] - prev_e1rm
            pct = (gain / prev_e1rm * 100.0) if prev_e1rm > 0 else 0
            lines.append(
                f"Previous: {format_standard_number(float(previous['weight']))}{previous['unit']} x {previous['reps']} "
                f"(e1RM {format_standard_number(prev_e1rm)})"
            )
            lines.append(f"Improvement: {gain:+.1f} ({pct:+.1f}%)")

        await send_discord_text(channel, "\n".join(lines))

    @commands.command(name="prs")
    async def prs_command(self, ctx: commands.Context, days: int = 14) -> None:
        rows = await self.db.get_recent_prs(max(1, min(90, days)), user_id=str(ctx.author.id))
        if not rows:
            await send_discord_text(ctx.channel, "No PRs in that window.")
            return

        lines = [f"Recent PRs ({days}d):"]
        for row in rows[:15]:
            lines.append(
                f"{row['date']}: {row['exercise_name']} "
                f"{format_standard_number(float(row['weight']))}{row['unit']} x {row['reps']} "
                f"(e1RM {format_standard_number(float(row['estimated_1rm']))})"
            )
        await send_discord_text(ctx.channel, "\n".join(lines))


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(PRsCog(bot))
