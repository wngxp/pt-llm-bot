from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import discord
from discord.ext import commands

from llm.prompts import ACTIVITY_IMPACT_SYSTEM_PROMPT
from utils.discord_messages import send_discord_text


ACTIVITY_VERB_RE = re.compile(
    r"\b(ran|run|running|played|play|went|did|climbed|hiked|swam|cycled|biked|walked|jogged|lifted|trained)\b",
    re.IGNORECASE,
)
ACTIVITY_DESCRIPTOR_RE = re.compile(
    r"(\b\d+\s*(?:min|mins|minute|minutes|hr|hrs|hour|hours|km|k|mile|miles)\b|\b(hard|easy|moderate|intense|light|long)\b)",
    re.IGNORECASE,
)
INJURY_RE = re.compile(
    r"\b(injured|injury|tore|torn|broken|sprained|strained|hurt|pain|ache|tweaked)\b",
    re.IGNORECASE,
)
MUSCLE_KEYWORDS: dict[str, str] = {
    "rotator cuff": "shoulders",
    "shoulder": "shoulders",
    "knee": "quads",
    "hamstring": "hamstrings",
    "quad": "quads",
    "lower back": "back",
    "back": "back",
    "elbow": "triceps",
    "wrist": "forearms",
    "ankle": "calves",
    "hip": "glutes",
    "pec": "chest",
    "chest": "chest",
}
ACTIVITY_MUSCLE_MAP: dict[str, str] = {
    "rock climbing": "back, forearms, biceps, core, shoulders",
    "climbing": "back, forearms, biceps, core, shoulders",
    "bouldering": "back, forearms, biceps, core, shoulders",
    "running": "quads, hamstrings, calves, cardiovascular",
    "jogging": "quads, hamstrings, calves, cardiovascular",
    "cycling": "quads, hamstrings, calves, cardiovascular",
    "swimming": "back, shoulders, chest, core, cardiovascular",
    "soccer": "quads, hamstrings, calves, core, cardiovascular",
    "football": "quads, hamstrings, calves, core, cardiovascular",
    "basketball": "quads, hamstrings, calves, shoulders, cardiovascular",
    "tennis": "shoulders, forearms, core, quads, cardiovascular",
    "boxing": "shoulders, chest, core, arms, cardiovascular",
    "kickboxing": "shoulders, chest, core, arms, quads, hamstrings, cardiovascular",
    "hiking": "quads, hamstrings, calves, core",
    "yoga": "core, flexibility, full body",
    "skiing": "quads, hamstrings, core, calves",
    "snowboarding": "quads, hamstrings, core, calves",
    "rowing": "back, biceps, core, shoulders, cardiovascular",
    "volleyball": "shoulders, quads, calves, core",
    "martial arts": "full body, cardiovascular",
    "wrestling": "full body, cardiovascular",
    "jump rope": "calves, shoulders, cardiovascular",
    "sprinting": "quads, hamstrings, glutes, calves, cardiovascular",
}
HIGH_INTENSITY_HINTS = {"hard", "intense", "all out", "max effort", "very hard", "brutal"}
LOW_INTENSITY_HINTS = {"easy", "light", "recovery", "very easy"}
MODERATE_INTENSITY_HINTS = {"moderate", "medium"}


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
            await send_discord_text(channel, f"Tip: use {ref} for utility commands like this.")

    async def _local_today(self) -> date:
        tz_name = await self.db.get_user_timezone()
        try:
            tzinfo = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            tzinfo = ZoneInfo("UTC")
        return datetime.now(tzinfo).date()

    def _looks_like_activity_report(self, text: str) -> bool:
        if self._match_known_activity(text):
            return True
        return bool(ACTIVITY_VERB_RE.search(text) and ACTIVITY_DESCRIPTOR_RE.search(text))

    def _looks_like_injury_report(self, text: str) -> bool:
        return bool(INJURY_RE.search(text))

    def _extract_injury_groups(self, text: str) -> str:
        lowered = text.lower()
        groups: set[str] = set()
        for key, group in MUSCLE_KEYWORDS.items():
            if key in lowered:
                groups.add(group)
        return ",".join(sorted(groups)) if groups else "general"

    def _match_known_activity(self, text: str) -> str | None:
        lowered = text.lower()
        for key in sorted(ACTIVITY_MUSCLE_MAP.keys(), key=len, reverse=True):
            if key in lowered:
                return key
        return None

    def _infer_intensity(self, text: str) -> str:
        lowered = text.lower()
        if any(token in lowered for token in HIGH_INTENSITY_HINTS):
            return "high"
        if any(token in lowered for token in LOW_INTENSITY_HINTS):
            return "low"
        if any(token in lowered for token in MODERATE_INTENSITY_HINTS):
            return "moderate"
        return "moderate"

    def _has_duration_or_descriptor(self, text: str) -> bool:
        return bool(ACTIVITY_DESCRIPTOR_RE.search(text))

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
        known = self._match_known_activity(text)
        if known:
            return {
                "activity_type": known,
                "intensity": self._infer_intensity(text),
                "muscle_groups": ACTIVITY_MUSCLE_MAP[known],
                "short_note": text,
            }
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

    async def _handle_activity_text(self, channel: discord.abc.Messageable, content: str) -> None:
        today_local = await self._local_today()
        known_activity = self._match_known_activity(content)
        if known_activity and "?" in content and not ACTIVITY_VERB_RE.search(content) and not self._has_duration_or_descriptor(content):
            await send_discord_text(
                channel,
                f"If you want to log {known_activity}, send something like `{known_activity} 1 hour moderate`.",
            )
            return

        if self._looks_like_injury_report(content):
            groups = self._extract_injury_groups(content)
            await self.db.add_injury(
                injury_date=today_local,
                description=content,
                muscle_groups=groups,
                severity="high",
            )
            await send_discord_text(
                channel,
                "That sounds serious. Please consult a medical professional. "
                f"I'll flag {groups} exercises as skip/substitute until you tell me you've recovered.",
            )
            return

        if known_activity or self._looks_like_activity_report(content):
            classification = await self._classify_activity(content)
            classification = self._normalize_activity_classification(content, classification)
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
            await send_discord_text(
                channel,
                f"Logged activity on {today_local.isoformat()}: {classification['activity_type']} "
                f"({classification['intensity']}) affecting [{classification['muscle_groups'] or 'unspecified'}].\n"
                f"{recovery_note}",
            )
            if known_activity and not self._has_duration_or_descriptor(content):
                await send_discord_text(
                    channel,
                    f"If you want a more accurate adjustment, reply with duration/intensity for {known_activity} "
                    "(for example `1 hour moderate`).",
                )
            return

        if "?" in content:
            await send_discord_text(
                channel,
                "I can log activities like `played soccer 2 hours hard` or `ran 30 min easy`. "
                "If this is an injury update, tell me what hurts and I’ll flag it.",
            )
            return

        await send_discord_text(
            channel,
            "I didn't log that as an activity. Use a format like `ran 30 min moderate` if you want it tracked.",
        )

    @commands.command(name="activity")
    async def activity_command(self, ctx: commands.Context, *, description: str) -> None:
        await self._handle_activity_text(ctx.channel, description)

    @commands.command(name="readiness")
    async def readiness_command(self, ctx: commands.Context, score: int) -> None:
        score = max(1, min(10, score))
        await self.db.update_user_state(readiness=score)
        await send_discord_text(ctx.channel, f"Readiness updated to {score}/10.")

    @commands.command(name="phase")
    async def phase_command(self, ctx: commands.Context, phase: str) -> None:
        phase = phase.lower().strip()
        if phase not in {"cut", "bulk", "maintain"}:
            await send_discord_text(ctx.channel, "Phase must be one of: cut, bulk, maintain.")
            return
        await self.db.update_user_state(phase=phase)
        await send_discord_text(ctx.channel, f"Phase set to {phase}.")

    @commands.command(name="timezone")
    async def timezone_command(self, ctx: commands.Context, *, timezone_name: str = "") -> None:
        if not timezone_name.strip():
            current = await self.db.get_user_timezone()
            await send_discord_text(ctx.channel, f"Current timezone: `{current}`")
            await self._maybe_send_settings_tip(ctx.channel)
            return

        candidate = timezone_name.strip()
        try:
            ZoneInfo(candidate)
        except ZoneInfoNotFoundError:
            await send_discord_text(
                ctx.channel,
                "Invalid timezone. Use an IANA timezone like `America/New_York`, `Europe/London`, or `Asia/Shanghai`.",
            )
            await self._maybe_send_settings_tip(ctx.channel)
            return

        await self.db.set_user_timezone(candidate)
        await send_discord_text(ctx.channel, f"Timezone set to `{candidate}`.")
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

        await self._handle_activity_text(message.channel, content)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ActivityCog(bot))
