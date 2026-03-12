from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from typing import Any, Optional

import discord
from discord.ext import commands, tasks

from llm.prompts import CHECKIN_SYSTEM_PROMPT
from utils.volume import format_volume_report

READINESS_RULES: list[tuple[set[str], int, str]] = [
    ({"bad sleep", "no sleep", "havent been sleeping", "haven't been sleeping", "poor sleep"}, -2, "sleep issues"),
    ({"stressed", "stress", "anxious", "overwhelmed"}, -1, "stress"),
    ({"sick", "ill", "fever", "flu"}, -3, "illness"),
    ({"tired", "exhausted", "fatigued", "drained"}, -1, "fatigue"),
]


class CheckInCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.settings = bot.settings
        self.db = bot.db
        self.weekly_check_loop.start()

    def cog_unload(self) -> None:
        self.weekly_check_loop.cancel()

    def _is_checkin_channel(self, channel: discord.abc.GuildChannel | discord.Thread | discord.abc.PrivateChannel) -> bool:
        cid = getattr(channel, "id", None)
        name = getattr(channel, "name", "")
        if self.settings.checkin_channel_id:
            return cid == self.settings.checkin_channel_id
        return name == "check-in"

    async def _generate_summary(self) -> str:
        today = date.today()
        week_start = today - timedelta(days=today.weekday())
        week_end = week_start + timedelta(days=6)

        state = await self.db.get_user_state()
        prs = await self.db.get_recent_prs(7)
        weekly_volume = await self.db.get_weekly_volume(start_date=week_start)
        trend = await self.db.get_trend_last_4_weeks()

        sessions_count = 0
        latest_workout = await self.db.get_latest_workout_date()
        if latest_workout and latest_workout >= week_start:
            sessions_count = min(7, (today - week_start).days + 1)

        local_lines = [
            f"📊 Weekly Summary ({week_start.isoformat()} - {week_end.isoformat()})",
            "",
            f"Sessions: {sessions_count}",
            f"Streak: {state.get('current_streak', 0)}",
            f"Phase: {state.get('phase', 'maintain')}",
            "",
            f"PRs this week: {len(prs)}",
            format_volume_report(weekly_volume),
        ]

        if trend:
            local_lines.append("")
            local_lines.append("Top trends:")
            for row in trend[:3]:
                local_lines.append(
                    f"- {row['exercise_name']}: best e1RM {row['best_e1rm']:.1f}"
                )

        context: dict[str, Any] = {
            "window": [week_start.isoformat(), week_end.isoformat()],
            "state": state,
            "prs": prs,
            "weekly_volume": weekly_volume,
            "trend": trend,
            "sessions_count": sessions_count,
        }

        try:
            llm = await self.bot.ollama.chat(
                system=CHECKIN_SYSTEM_PROMPT,
                user=json.dumps(context, ensure_ascii=False),
                temperature=0.2,
            )
            return llm.strip()
        except Exception:
            return "\n".join(local_lines)

    async def _adjust_readiness_from_text(self, text: str) -> Optional[tuple[int, str]]:
        lowered = text.lower()
        for keywords, delta, reason in READINESS_RULES:
            if any(keyword in lowered for keyword in keywords):
                state = await self.db.get_user_state()
                current = int(state.get("readiness") or 7)
                next_value = max(1, min(10, current + delta))
                if next_value == current:
                    return next_value, reason
                await self.db.update_user_state(readiness=next_value)
                return next_value, reason
        return None

    async def _reply_short_checkin_chat(self, text: str) -> str:
        state = await self.db.get_user_state()
        prompt = {
            "message": text,
            "response_style": "Reply in 2-3 short sentences.",
            "state": {
                "phase": state.get("phase"),
                "readiness": state.get("readiness"),
                "current_streak": state.get("current_streak"),
            },
        }
        try:
            reply = await self.bot.ollama.chat(
                system=CHECKIN_SYSTEM_PROMPT,
                user=json.dumps(prompt, ensure_ascii=False),
                temperature=0.2,
            )
        except Exception:
            return "Got it. I logged that check-in context and will adjust suggestions accordingly."
        sentences = [part.strip() for part in re.split(r"(?<=[.!?])\\s+", reply) if part.strip()]
        if len(sentences) <= 3:
            return reply.strip()
        return " ".join(sentences[:3]).strip()

    @commands.command(name="checkin")
    async def checkin_command(self, ctx: commands.Context) -> None:
        if not self._is_checkin_channel(ctx.channel):
            return
        summary = await self._generate_summary()
        await self.db.set_last_checkin(date.today())
        await ctx.send(summary)

    async def _get_checkin_channel(self) -> Optional[discord.TextChannel]:
        if self.settings.checkin_channel_id:
            channel = self.bot.get_channel(self.settings.checkin_channel_id)
            if isinstance(channel, discord.TextChannel):
                return channel

        for guild in self.bot.guilds:
            for channel in guild.text_channels:
                if channel.name == "check-in":
                    return channel
        return None

    @tasks.loop(minutes=30)
    async def weekly_check_loop(self) -> None:
        await self.bot.wait_until_ready()

        now = datetime.now()
        if now.weekday() != 5 or now.hour < 18:
            return

        week_start = date.today() - timedelta(days=date.today().weekday())
        last_checkin = await self.db.get_last_checkin_date()
        if last_checkin and last_checkin >= week_start:
            return

        channel = await self._get_checkin_channel()
        if not channel:
            return

        summary = await self._generate_summary()
        await channel.send(
            "You haven’t checked in yet this week. Here’s your proactive summary:\n\n" + summary
        )
        await self.db.set_last_checkin(date.today())

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        if not self._is_checkin_channel(message.channel):
            return

        content = message.content.strip()
        if not content or content.startswith(self.settings.command_prefix):
            return

        readiness_update = await self._adjust_readiness_from_text(content)
        if readiness_update:
            score, reason = readiness_update
            await message.channel.send(
                f"Got it - I adjusted readiness to {score}/10 based on {reason}. I'll adapt today's suggestions."
            )
            return

        lowered = content.lower()
        if any(token in lowered for token in {"summary", "weekly", "check in", "check-in"}):
            summary = await self._generate_summary()
            await self.db.set_last_checkin(date.today())
            await message.channel.send(summary)
            return

        reply = await self._reply_short_checkin_chat(content)
        await message.channel.send(reply)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(CheckInCog(bot))
