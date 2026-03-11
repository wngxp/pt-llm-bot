from __future__ import annotations

import asyncio
import re
from datetime import date, timedelta

import discord
from discord.ext import commands

from llm.parser import ProgramParser


DURATION_RE = re.compile(r"(?P<num>\d+)\s*(?P<unit>day|days|week|weeks)", re.IGNORECASE)


class ProgrammeCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.settings = bot.settings
        self.db = bot.db
        self.parser = ProgramParser(bot.ollama)

    def _is_programme_channel(self, channel: discord.abc.GuildChannel | discord.Thread | discord.abc.PrivateChannel) -> bool:
        cid = getattr(channel, "id", None)
        name = getattr(channel, "name", "")
        if self.settings.programme_channel_id:
            return cid == self.settings.programme_channel_id
        return name == "programme"

    def _is_placeholder_name(self, name: str) -> bool:
        cleaned = name.strip().lower()
        if not cleaned:
            return True
        return cleaned in {"unnamed", "unnamed program", "untitled", "untitled program", "imported program"}

    def _infer_program_name_from_text(self, raw_text: str) -> str | None:
        patterns = [
            re.compile(r"\bprogram\s*[:\-]\s*(?P<name>[A-Za-z0-9][A-Za-z0-9 '\-_]{1,60})", re.IGNORECASE),
            re.compile(r"\b(?:this is|it's|it is)\s+(?:my\s+)?(?P<name>[A-Za-z0-9][A-Za-z0-9 '\-_]{1,60})\s+program\b", re.IGNORECASE),
            re.compile(r"\bmy\s+(?P<name>[A-Za-z0-9][A-Za-z0-9 '\-_]{1,60})\s+program\b", re.IGNORECASE),
        ]
        for pattern in patterns:
            match = pattern.search(raw_text)
            if match:
                return match.group("name").strip()
        return None

    async def _resolve_program_name(
        self,
        channel: discord.abc.Messageable,
        author: discord.abc.User,
        raw_text: str,
        parsed_name: str,
    ) -> str:
        if not self._is_placeholder_name(parsed_name):
            return parsed_name.strip()

        inferred = self._infer_program_name_from_text(raw_text)
        if inferred:
            return inferred

        await channel.send("What would you like to name this program?")
        try:
            reply = await self.bot.wait_for(
                "message",
                timeout=60,
                check=lambda m: m.author.id == author.id and m.channel.id == getattr(channel, "id", None),
            )
            candidate = reply.content.strip()
            if candidate:
                return candidate[:80]
        except asyncio.TimeoutError:
            pass
        return "Imported Program"

    async def _import_program(
        self,
        channel: discord.abc.Messageable,
        author: discord.abc.User,
        raw_text: str,
    ) -> int:
        parsed = await self.parser.parse_program(raw_text)
        parsed["program_name"] = await self._resolve_program_name(
            channel,
            author,
            raw_text,
            str(parsed.get("program_name") or ""),
        )
        program_id = await self.db.create_program_from_payload(parsed)

        days = parsed.get("days", [])
        total_exercises = sum(len(day.get("exercises", [])) for day in days)
        await channel.send(
            f"✅ Imported **{parsed['program_name']}** (ID {program_id}) with "
            f"{len(days)} days and {total_exercises} exercises."
        )
        return program_id

    @commands.command(name="import")
    async def import_program_command(self, ctx: commands.Context, *, text: str) -> None:
        if not self._is_programme_channel(ctx.channel):
            return
        await self._import_program(ctx.channel, ctx.author, text)

    @commands.command(name="program")
    async def show_program_command(self, ctx: commands.Context) -> None:
        program = await self.db.get_active_program()
        if not program:
            await ctx.send("No active program yet. Paste one in #programme.")
            return
        days = await self.db.get_program_days(program["id"])
        if not days:
            await ctx.send(f"Active program **{program['name']}** has no days.")
            return

        lines = [f"Active program: **{program['name']}** ({len(days)} days)"]
        for day in days:
            exercises = await self.db.get_exercises_for_day(day["id"])
            lines.append(f"Day {day['day_order'] + 1}: {day['name']} ({len(exercises)} exercises)")
        await ctx.send("\n".join(lines))

    @commands.command(name="travel")
    async def travel_program_command(self, ctx: commands.Context, *, text: str) -> None:
        if not self._is_programme_channel(ctx.channel):
            return

        active = await self.db.get_active_program()
        if not active:
            await ctx.send("You need an active base program first.")
            return

        duration_days = 14
        match = DURATION_RE.search(text)
        if match:
            num = int(match.group("num"))
            unit = match.group("unit").lower()
            duration_days = num * 7 if "week" in unit else num

        expires = (date.today() + timedelta(days=duration_days)).isoformat()
        prompt = (
            "Create a temporary workout program in JSON with this schema: "
            '{"program_name":str,"days":[{"day_order":0,"name":str,"exercises":[{"name":str,"sets":int,'
            '"rep_range_low":int|null,"rep_range_high":int|null,"category":str,"superset_group":int|null,'
            '"muscle_groups":str,"notes":str}]}]}. '
            f"Context: {text}. Keep 3-5 days cycle and use available equipment."
        )
        parsed = await self.bot.ollama.chat_json(
            system="Return only valid JSON workout program.",
            user=prompt,
            temperature=0.2,
        )

        program_id = await self.db.create_program_from_payload(
            parsed,
            temporary=True,
            parent_program_id=active["id"],
            expires_at=expires,
        )
        await ctx.send(
            f"✅ Temporary program activated (ID {program_id}) until {expires}. "
            f"Base program: **{active['name']}** will resume automatically."
        )

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        if not self._is_programme_channel(message.channel):
            return

        content = message.content.strip()
        if not content:
            return
        if content.startswith(self.settings.command_prefix):
            return

        if len(content) < 40 and len(content.splitlines()) < 2:
            return

        looks_like_program = (
            "x" in content.lower()
            and any(token in content.lower() for token in ["day", "push", "pull", "legs", "upper", "lower"])
        )
        if not looks_like_program:
            return

        try:
            await self._import_program(message.channel, message.author, content)
        except Exception as exc:
            await message.channel.send(f"Could not parse program: {exc}")


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ProgrammeCog(bot))
