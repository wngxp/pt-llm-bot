from __future__ import annotations

from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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

    def _settings_channel_ref(self, channel: discord.abc.Messageable) -> str | None:
        settings_id = self.settings.settings_channel_id
        if not settings_id:
            return None
        if getattr(channel, "id", None) == settings_id:
            return None
        guild = getattr(channel, "guild", None)
        if guild and isinstance(guild, discord.Guild):
            target = guild.get_channel(settings_id)
            if isinstance(target, discord.TextChannel):
                return target.mention
        return "#settings"

    async def _maybe_send_settings_tip(self, channel: discord.abc.Messageable) -> None:
        ref = self._settings_channel_ref(channel)
        if ref:
            await channel.send(f"Tip: use {ref} for utility commands like this.")

    async def _local_today(self) -> date:
        tz_name = await self.db.get_user_timezone()
        try:
            tzinfo = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            tzinfo = ZoneInfo("UTC")
        return datetime.now(tzinfo).date()

    def _normalize_activity_classification(self, text: str, classification: dict[str, Any]) -> dict[str, Any]:
        out = dict(classification)
        intensity = str(out.get("intensity") or "moderate").lower().strip()
        if intensity not in {"low", "moderate", "high"}:
            intensity = "moderate"

        lower_text = text.lower()
        if "5k" in lower_text and intensity == "high":
            hard_run_markers = {"race", "all out", "sprint", "max effort"}
            if not any(marker in lower_text for marker in hard_run_markers):
                intensity = "moderate"

        out["intensity"] = intensity
        return out

    async def _estimate_recovery_message(self, muscle_groups: str, intensity: str) -> str:
        recovery_days_map = {"low": 1, "moderate": 2, "high": 3}
        recovery_days = recovery_days_map.get(intensity, 2)

        groups = [g.strip().lower() for g in (muscle_groups or "").split(",") if g.strip()]
        if not groups:
            return f"Estimated recovery: ~{recovery_days} day(s)."

        current_index = await self.db.get_current_day_index()
        active = await self.db.get_active_program()
        if not active:
            return f"Estimated recovery: ~{recovery_days} day(s) for {', '.join(groups)}."

        days = await self.db.get_program_days(int(active["id"]))
        if not days:
            return f"Estimated recovery: ~{recovery_days} day(s) for {', '.join(groups)}."

        affected_day_name = None
        max_lookahead = min(len(days), 14)
        for offset in range(max_lookahead):
            day = days[(current_index + offset) % len(days)]
            exercises = await self.db.get_exercises_for_day(int(day["id"]))
            day_groups: set[str] = set()
            for ex in exercises:
                ex_groups = [m.strip().lower() for m in str(ex.get("muscle_groups") or "").split(",") if m.strip()]
                day_groups.update(ex_groups)
            if any(group in day_groups for group in groups):
                affected_day_name = str(day["name"])
                break

        if affected_day_name:
            return (
                f"Estimated recovery: ~{recovery_days} day(s). "
                f"Your next **{affected_day_name}** session may need lighter volume."
            )
        return f"Estimated recovery: ~{recovery_days} day(s) for {', '.join(groups)}."

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
        classification = self._normalize_activity_classification(description, classification)
        today_local = await self._local_today()
        await self.db.add_activity(
            activity_date=today_local,
            activity_type=classification["activity_type"],
            description=description,
            intensity=classification["intensity"],
            muscle_groups=classification["muscle_groups"],
        )
        recovery_note = await self._estimate_recovery_message(
            classification["muscle_groups"],
            classification["intensity"],
        )
        await ctx.send(
            f"Logged activity on {today_local.isoformat()}: {classification['activity_type']} "
            f"({classification['intensity']}) affecting [{classification['muscle_groups'] or 'unspecified'}].\n"
            f"{recovery_note}"
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

    @commands.command(name="timezone")
    async def timezone_command(self, ctx: commands.Context, *, timezone_name: str = "") -> None:
        if not timezone_name.strip():
            current = await self.db.get_user_timezone()
            await ctx.send(f"Current timezone: `{current}`")
            await self._maybe_send_settings_tip(ctx.channel)
            return

        candidate = timezone_name.strip()
        try:
            ZoneInfo(candidate)
        except ZoneInfoNotFoundError:
            await ctx.send(
                "Invalid timezone. Use an IANA timezone like `America/New_York`, `Europe/London`, or `Asia/Shanghai`."
            )
            await self._maybe_send_settings_tip(ctx.channel)
            return

        await self.db.set_user_timezone(candidate)
        await ctx.send(f"Timezone set to `{candidate}`.")
        await self._maybe_send_settings_tip(ctx.channel)

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
        classification = self._normalize_activity_classification(content, classification)
        today_local = await self._local_today()
        await self.db.add_activity(
            activity_date=today_local,
            activity_type=classification["activity_type"],
            description=content,
            intensity=classification["intensity"],
            muscle_groups=classification["muscle_groups"],
        )
        recovery_note = await self._estimate_recovery_message(
            classification["muscle_groups"],
            classification["intensity"],
        )
        await message.channel.send(
            f"Logged on {today_local.isoformat()}: {classification['activity_type']} ({classification['intensity']}) "
            f"for {classification['muscle_groups'] or 'general recovery context'}.\n"
            f"{recovery_note}"
        )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ActivityCog(bot))
