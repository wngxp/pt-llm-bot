from __future__ import annotations

import asyncio
import json
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import discord
from discord.ext import commands

from llm.parser import ProgramParser


DURATION_RE = re.compile(r"(?P<num>\d+)\s*(?P<unit>day|days|week|weeks)", re.IGNORECASE)
DAY_NUMBER_RE = re.compile(r"\bday\s*(?P<day_num>\d+)\b", re.IGNORECASE)
START_DAY_INTENT_RE = re.compile(r"\b(start|begin|starting|start on|begin with)\b", re.IGNORECASE)
EDIT_INTENT_RE = re.compile(r"\b(edit|swap|replace|change|modify)\b", re.IGNORECASE)
QUESTION_HINT_RE = re.compile(r"\?|\b(what|why|how|can|should|which)\b", re.IGNORECASE)
SET_PATTERN_RE = re.compile(r"\d+\s*[xX×]\s*\d+")


class ProgrammeCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.settings = bot.settings
        self.db = bot.db
        self.parser = ProgramParser(bot.ollama)
        self.post_import_state: dict[int, dict[str, Any]] = {}

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

    def _now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

    def _set_post_import_context(self, user_id: int, channel_id: int, program_id: int) -> None:
        self.post_import_state[user_id] = {
            "program_id": program_id,
            "channel_id": channel_id,
            "expires_at": self._now_utc() + timedelta(minutes=5),
        }

    def _clear_post_import_context(self, user_id: int) -> None:
        self.post_import_state.pop(user_id, None)

    def _get_post_import_context(self, user_id: int, channel_id: int) -> Optional[dict[str, Any]]:
        state = self.post_import_state.get(user_id)
        if not state:
            return None
        if int(state.get("channel_id") or 0) != channel_id:
            return None
        expires_at = state.get("expires_at")
        if not isinstance(expires_at, datetime) or expires_at < self._now_utc():
            self._clear_post_import_context(user_id)
            return None
        return state

    def _looks_like_program_paste(self, text: str) -> bool:
        lowered = text.lower()
        if len(text.splitlines()) < 2:
            return False
        if len(SET_PATTERN_RE.findall(text)) < 2:
            return False
        day_markers = any(token in lowered for token in ["day ", "push", "pull", "legs", "upper", "lower"])
        return day_markers

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

    async def _start_day_prompt(self, channel: discord.abc.Messageable, program_id: int) -> None:
        days = await self.db.get_program_days(program_id)
        if not days:
            return
        lines = ["Which day would you like to start on?"]
        for day in days:
            lines.append(f"{day['day_order'] + 1}. {day['name']}")
        lines.append("Reply with `start on Legs` or `start on Day 3`.")
        await channel.send("\n".join(lines))

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
        self._set_post_import_context(author.id, getattr(channel, "id", 0), program_id)
        await self._start_day_prompt(channel, program_id)
        return program_id

    def _normalize_name(self, text: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", text.lower())

    def _parse_start_day_index(self, text: str, days: list[dict[str, Any]]) -> Optional[int]:
        match = DAY_NUMBER_RE.search(text)
        if match:
            idx = int(match.group("day_num")) - 1
            if 0 <= idx < len(days):
                return idx

        lowered = text.lower()
        for day in days:
            day_name = str(day.get("name") or "").strip()
            if day_name and day_name.lower() in lowered:
                return int(day["day_order"])

        normalized_text = self._normalize_name(text)
        for day in days:
            day_name = self._normalize_name(str(day.get("name") or ""))
            if day_name and day_name in normalized_text:
                return int(day["day_order"])

        return None

    async def _handle_start_day_message(self, channel: discord.abc.Messageable, text: str) -> bool:
        active = await self.db.get_active_program()
        if not active:
            return False
        days = await self.db.get_program_days(int(active["id"]))
        if not days:
            return False

        lowered = text.lower()
        if not START_DAY_INTENT_RE.search(lowered) and not DAY_NUMBER_RE.search(lowered):
            return False

        idx = self._parse_start_day_index(text, days)
        if idx is None:
            lines = ["I couldn't map that to a program day. Try one of:"]
            for day in days:
                lines.append(f"{day['day_order'] + 1}. {day['name']}")
            await channel.send("\n".join(lines))
            return True

        await self.db.set_current_day_index(idx)
        selected = next((d for d in days if int(d["day_order"]) == idx), days[idx])
        await channel.send(
            f"✅ Starting day set to **{selected['name']}** (Day {idx + 1} of {len(days)})."
        )
        return True

    async def _handle_simple_edit_request(self, channel: discord.abc.Messageable, text: str) -> bool:
        lowered = text.lower().strip()

        swap_match = re.search(r"(?:swap|replace)\s+(.+?)\s+with\s+(.+)$", text, re.IGNORECASE)
        if swap_match:
            old_name = swap_match.group(1).strip(" .")
            new_name = swap_match.group(2).strip(" .")
            rows = await self.db.update_exercise_name_in_active_program(old_name, new_name)
            if rows > 0:
                await channel.send(f"✅ Updated exercise: **{old_name}** -> **{new_name}**.")
            else:
                await channel.send(f"Couldn't find `{old_name}` in the active program.")
            return True

        change_match = re.search(r"change\s+(.+?)\s+to\s+(.+)$", text, re.IGNORECASE)
        if change_match:
            old_name = change_match.group(1).strip(" .")
            new_name = change_match.group(2).strip(" .")

            day_rows = await self.db.rename_program_day_in_active_program(old_name, new_name)
            if day_rows > 0:
                await channel.send(f"✅ Renamed day: **{old_name}** -> **{new_name}**.")
                return True

            ex_rows = await self.db.update_exercise_name_in_active_program(old_name, new_name)
            if ex_rows > 0:
                await channel.send(f"✅ Renamed exercise: **{old_name}** -> **{new_name}**.")
            else:
                await channel.send(
                    "I couldn't apply that change directly. Try `swap <old exercise> with <new exercise>`."
                )
            return True

        if EDIT_INTENT_RE.search(lowered):
            active = await self.db.get_active_program()
            if not active:
                await channel.send("No active program to edit yet.")
                return True
            days = await self.db.get_program_days(int(active["id"]))
            context = {"program": active, "days": days, "request": text}
            try:
                reply = await self.bot.ollama.chat(
                    system=(
                        "You are assisting with workout program edits. "
                        "Give a concise response and suggest a concrete edit command. Keep it under 3 sentences."
                    ),
                    user=json.dumps(context, ensure_ascii=False),
                    temperature=0.2,
                )
                await channel.send(reply.strip())
            except Exception:
                await channel.send("I couldn't process that edit request right now. Try `swap X with Y`.")
            return True

        return False

    async def _reply_programme_question(self, channel: discord.abc.Messageable, text: str) -> None:
        active = await self.db.get_active_program()
        if not active:
            await channel.send("No active program yet. Paste one to get started.")
            return
        days = await self.db.get_program_days(int(active["id"]))
        payload = {
            "question": text,
            "program": active,
            "days": days,
            "response_style": "2-3 short sentences.",
        }
        try:
            reply = await self.bot.ollama.chat(
                system="You help users discuss and understand their lifting program. Keep answers concise.",
                user=json.dumps(payload, ensure_ascii=False),
                temperature=0.2,
            )
            await channel.send(reply.strip())
        except Exception:
            await channel.send("I couldn't answer that right now. Try `!program` to view the current setup.")

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

    @commands.command(name="startday")
    async def start_day_command(self, ctx: commands.Context, *, text: str) -> None:
        if not self._is_programme_channel(ctx.channel):
            return
        handled = await self._handle_start_day_message(ctx.channel, f"start on {text}")
        if not handled:
            await ctx.send("Couldn't parse that day selection. Try `!startday Day 3` or `!startday Legs`.")

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
        self._set_post_import_context(ctx.author.id, ctx.channel.id, int(program_id))

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

        user_id = message.author.id
        channel_id = message.channel.id
        context = self._get_post_import_context(user_id, channel_id)

        if self._looks_like_program_paste(content):
            try:
                await self._import_program(message.channel, message.author, content)
            except Exception as exc:
                await message.channel.send(f"Could not parse program: {exc}")
            return

        if await self._handle_start_day_message(message.channel, content):
            if context:
                self._clear_post_import_context(user_id)
            return

        if await self._handle_simple_edit_request(message.channel, content):
            return

        if context or QUESTION_HINT_RE.search(content) or "program" in content.lower():
            await self._reply_programme_question(message.channel, content)
            return

        if context:
            await message.channel.send(
                "You can say `start on Legs`, `start on Day 3`, or request an edit like `swap RDL with Good Morning`."
            )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ProgrammeCog(bot))
