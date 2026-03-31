from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
import re
from typing import Any, Optional

import discord
from discord.ext import commands

from utils.discord_messages import send_discord_text
from utils.e1rm import epley_1rm
from utils.input_parser import parse_set_input


YES_TOKENS = {"yes", "y", "yeah", "yep", "overwrite"}
NO_TOKENS = {"no", "n", "nope", "cancel"}
BACKFILL_WEEK_DAY_RE = re.compile(r"^week\s+(?P<week>\d+)\s+day\s+(?P<day>\d+)\s*$", re.IGNORECASE)
BACKFILL_LINE_RE = re.compile(r"^(?P<index>\d+)\s*:\s*(?P<sets>.+)$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(slots=True)
class PendingBackfill:
    user_id: int
    channel_id: int
    day: dict[str, Any]
    exercises: list[dict[str, Any]]
    workout_date: date
    awaiting_overwrite_confirmation: bool = False
    existing_logged_sets: int = 0
    handled_indices: set[int] = field(default_factory=set)
    skipped_indices: set[int] = field(default_factory=set)
    logged_set_count: int = 0
    logged_exercise_indices: set[int] = field(default_factory=set)


class BackfillCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.settings = bot.settings
        self.db = bot.db
        self.pending: dict[tuple[int, int], PendingBackfill] = {}

    def clear_runtime_state(self) -> None:
        self.pending.clear()

    def _pending_key(self, user_id: int, channel_id: int) -> tuple[int, int]:
        return (int(user_id), int(channel_id))

    def _is_workout_channel(self, channel: discord.abc.GuildChannel | discord.Thread | discord.abc.PrivateChannel) -> bool:
        cid = getattr(channel, "id", None)
        name = getattr(channel, "name", "")
        if self.settings.workout_channel_ids:
            return cid in self.settings.workout_channel_ids
        return name in {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}

    def _is_backfill_command_channel(
        self,
        channel: discord.abc.GuildChannel | discord.Thread | discord.abc.PrivateChannel,
    ) -> bool:
        cid = getattr(channel, "id", None)
        name = str(getattr(channel, "name", "")).strip().lower()
        if self._is_workout_channel(channel):
            return True

        settings_id = getattr(self.settings, "settings_channel_id", None)
        programme_id = getattr(self.settings, "programme_channel_id", None)
        if settings_id and cid == settings_id:
            return True
        if programme_id and cid == programme_id:
            return True

        return name in {"settings", "commands", "programme"}

    def is_waiting_for_input(self, *, user_id: int, channel_id: int) -> bool:
        return self._pending_key(user_id, channel_id) in self.pending

    def _has_active_workout_session(self, user_id: int) -> bool:
        workout_cog = self.bot.get_cog("WorkoutCog")
        sessions = getattr(workout_cog, "sessions", None)
        if not isinstance(sessions, dict):
            return False
        return str(user_id) in sessions

    def _format_day_header(self, day: dict[str, Any]) -> str:
        block = str(day.get("block") or "").strip()
        week = int(day.get("week") or 0)
        day_number = int(day.get("day_number") or 0)
        day_name = str(day.get("name") or "Day").strip() or "Day"
        return f"{block} Week {week} Day {day_number} - {day_name}"

    def _format_rep_range(self, exercise: dict[str, Any]) -> str:
        low = exercise.get("rep_range_low")
        high = exercise.get("rep_range_high")
        if low is None or high is None:
            return "AMRAP"
        if int(low) == int(high):
            return str(int(low))
        return f"{int(low)}-{int(high)}"

    def _format_exercise_list(self, pending: PendingBackfill) -> str:
        lines = [f"Backfill: {self._format_day_header(pending.day)}"]
        for idx, exercise in enumerate(pending.exercises, start=1):
            sets = int(exercise.get("sets") or 1)
            rep_text = self._format_rep_range(exercise)
            lines.append(f"{idx}. {exercise['name']} ({sets} working sets, {rep_text} reps)")
        lines.append('Type your sets for each exercise, e.g.:')
        lines.append("1: 135x6, 135x8")
        lines.append("2: 30x10, 30x10")
        lines.append('Or type "skip" to skip an exercise, "done" to finish.')
        next_index = self._next_unhandled_index(pending)
        if next_index is not None:
            lines.append(f"Next to enter: {next_index + 1}. {pending.exercises[next_index]['name']}")
        return "\n".join(lines)

    def _next_unhandled_index(self, pending: PendingBackfill) -> Optional[int]:
        for idx in range(len(pending.exercises)):
            if idx in pending.handled_indices or idx in pending.skipped_indices:
                continue
            return idx
        return None

    async def _default_unit_for_user(self, user_id: str) -> str:
        state = await self.db.get_user_state(user_id)
        default_unit = str(state.get("default_unit") or "lbs").strip().lower()
        return "kg" if default_unit.startswith("kg") else "lbs"

    async def _record_pr_if_needed(
        self,
        *,
        exercise_name: str,
        weight: float,
        reps: int,
        unit: str,
        workout_date: date,
        workout_log_id: int,
        user_id: str,
    ) -> None:
        existing = await self.db.get_best_pr(exercise_name, user_id=user_id)
        e1rm = epley_1rm(weight, reps) if weight > 0 else 0.0
        if not existing:
            await self.db.create_pr(
                exercise_name,
                user_id=user_id,
                weight=weight,
                reps=reps,
                unit=unit,
                estimated_1rm=e1rm,
                workout_date=workout_date,
                workout_log_id=workout_log_id,
            )
            return

        prev_e1rm = float(existing.get("estimated_1rm") or 0.0)
        prev_weight = float(existing.get("weight") or 0.0)
        prev_reps = int(existing.get("reps") or 0)
        is_pr = e1rm > prev_e1rm or (weight > prev_weight and reps >= prev_reps) or (reps > prev_reps and weight >= prev_weight)
        if not is_pr:
            return

        await self.db.create_pr(
            exercise_name,
            user_id=user_id,
            weight=weight,
            reps=reps,
            unit=unit,
            estimated_1rm=e1rm,
            workout_date=workout_date,
            workout_log_id=workout_log_id,
        )

    async def _build_pending_backfill(
        self,
        *,
        user_id: str,
        week: int,
        day_number: int,
    ) -> tuple[Optional[PendingBackfill], Optional[str]]:
        day = await self.db.get_program_day_by_week_day(week=week, day_number=day_number, user_id=user_id)
        if day is None:
            return None, f"I couldn't find Week {week} Day {day_number} in your active program."
        if bool(day.get("is_rest_day")):
            return None, f"Week {week} Day {day_number} is a rest day - nothing to log."

        workout_date = await self.db.get_program_day_date(int(day["day_order"]), user_id=user_id)
        if workout_date is None:
            return None, "I need your program start date before I can backfill this. Use `!startdate YYYY-MM-DD` first."

        exercises = await self.db.get_exercises_for_day(int(day["id"]))
        if not exercises:
            return None, f"{self._format_day_header(day)} has no exercises to log."

        existing_logged_sets = await self.db.count_logged_sets_for_program_day(int(day["id"]), user_id=user_id)
        pending = PendingBackfill(
            user_id=int(user_id),
            channel_id=0,
            day=day,
            exercises=exercises,
            workout_date=workout_date,
            awaiting_overwrite_confirmation=existing_logged_sets > 0,
            existing_logged_sets=existing_logged_sets,
        )
        return pending, None

    async def _build_yesterday_backfill(
        self,
        *,
        user_id: str,
    ) -> tuple[Optional[PendingBackfill], Optional[str]]:
        yesterday = date.today() - timedelta(days=1)
        idx = await self.db.find_most_recent_unlogged_day_index(yesterday, user_id=user_id)
        if idx is None:
            start_date = await self.db.get_program_start_date(user_id)
            if start_date is None:
                return None, "I need your program start date before I can backfill yesterday. Use `!startdate YYYY-MM-DD`."
            return None, "I couldn't find an unlogged training day on or before yesterday."

        day = await self.db.get_day_for_index(idx, user_id=user_id)
        if day is None:
            return None, "I couldn't resolve the day for that backfill request."
        exercises = await self.db.get_exercises_for_day(int(day["id"]))
        workout_date = await self.db.get_program_day_date(int(day["day_order"]), user_id=user_id)
        if workout_date is None:
            return None, "I need your program start date before I can backfill yesterday. Use `!startdate YYYY-MM-DD`."
        pending = PendingBackfill(
            user_id=int(user_id),
            channel_id=0,
            day=day,
            exercises=exercises,
            workout_date=workout_date,
        )
        return pending, None

    async def _start_pending_backfill(
        self,
        channel: discord.abc.Messageable,
        *,
        user_id: int,
        pending: PendingBackfill,
    ) -> None:
        pending.channel_id = int(getattr(channel, "id", 0))
        key = self._pending_key(user_id, pending.channel_id)
        self.pending[key] = pending
        if pending.awaiting_overwrite_confirmation:
            await send_discord_text(
                channel,
                f"{self._format_day_header(pending.day)} already has logged sets ({pending.existing_logged_sets} total). Overwrite? (yes/no)",
            )
            return
        await send_discord_text(channel, self._format_exercise_list(pending))

    async def _finalize_backfill(self, channel: discord.abc.Messageable, pending: PendingBackfill) -> None:
        key = self._pending_key(pending.user_id, pending.channel_id)
        current_index = await self.db.get_current_day_index(str(pending.user_id))
        if current_index == int(pending.day["day_order"]):
            await self.db.advance_day_index(user_id=str(pending.user_id), skip_rest_days=True)
        self.pending.pop(key, None)
        await send_discord_text(
            channel,
            (
                f"Logged {pending.logged_set_count} sets across {len(pending.logged_exercise_indices)} exercises for "
                f"Week {int(pending.day['week'])} Day {int(pending.day['day_number'])}."
            ),
        )

    async def _handle_overwrite_reply(
        self,
        channel: discord.abc.Messageable,
        pending: PendingBackfill,
        content: str,
    ) -> None:
        lowered = content.strip().lower()
        if lowered in YES_TOKENS:
            await self.db.clear_logs_for_program_day(int(pending.day["id"]), user_id=str(pending.user_id))
            pending.awaiting_overwrite_confirmation = False
            pending.existing_logged_sets = 0
            await send_discord_text(channel, "Existing logs cleared. Starting backfill.")
            await send_discord_text(channel, self._format_exercise_list(pending))
            return
        if lowered in NO_TOKENS:
            self.pending.pop(self._pending_key(pending.user_id, pending.channel_id), None)
            await send_discord_text(channel, "Backfill cancelled.")
            return
        await send_discord_text(channel, "Please reply `yes` or `no`.")

    async def _handle_skip(
        self,
        channel: discord.abc.Messageable,
        pending: PendingBackfill,
    ) -> None:
        next_index = self._next_unhandled_index(pending)
        if next_index is None:
            await self._finalize_backfill(channel, pending)
            return
        pending.skipped_indices.add(next_index)
        next_index = self._next_unhandled_index(pending)
        if next_index is None:
            await self._finalize_backfill(channel, pending)
            return
        await send_discord_text(channel, f"Skipped. Next: {next_index + 1}. {pending.exercises[next_index]['name']}")

    async def _handle_entry_line(
        self,
        channel: discord.abc.Messageable,
        pending: PendingBackfill,
        line: str,
    ) -> bool:
        match = BACKFILL_LINE_RE.match(line.strip())
        if not match:
            await send_discord_text(channel, 'Use `1: 135x6, 135x8`, `skip`, or `done`.')
            return False

        index = int(match.group("index")) - 1
        if index < 0 or index >= len(pending.exercises):
            await send_discord_text(channel, f"That exercise index is out of range. Pick 1-{len(pending.exercises)}.")
            return False
        if index in pending.handled_indices or index in pending.skipped_indices:
            await send_discord_text(channel, f"Exercise {index + 1} has already been handled.")
            return False

        exercise = pending.exercises[index]
        default_unit = await self._default_unit_for_user(str(pending.user_id))
        raw_sets = [part.strip() for part in match.group("sets").split(",") if part.strip()]
        if not raw_sets:
            await send_discord_text(channel, "I couldn't find any sets in that line.")
            return False

        planned_sets = max(1, int(exercise.get("sets") or 1))
        set_count = 0
        for set_number, raw_set in enumerate(raw_sets, start=1):
            parsed = parse_set_input(raw_set)
            if parsed is None:
                await send_discord_text(channel, f"I couldn't parse `{raw_set}`. Use `weight x reps` like `135x6`.")
                return False
            unit = str(parsed.get("unit") or default_unit)
            reps = int(parsed.get("reps") or 0)
            weight = float(parsed.get("weight") or 0.0)
            is_bodyweight = bool(parsed.get("is_bodyweight"))
            note_parts = [str(parsed.get("note") or "").strip(), str(parsed.get("trailing_text") or "").strip()]
            notes = "; ".join(part for part in note_parts if part)

            if is_bodyweight and str(exercise.get("category") or "") != "bodyweight":
                await send_discord_text(channel, f"{exercise['name']} is not a bodyweight movement in the program. Please include a load.")
                return False
            if not is_bodyweight and str(exercise.get("category") or "") == "bodyweight" and weight == 0 and not notes:
                notes = "bodyweight"

            log_id = await self.db.log_set(
                exercise_id=int(exercise["id"]),
                user_id=str(pending.user_id),
                workout_date=pending.workout_date,
                set_number=set_number,
                weight=weight,
                reps=reps,
                unit=unit,
                rir=parsed.get("rir"),
                notes=notes,
                performed_exercise_name=str(exercise["name"]),
                performed_category=str(exercise.get("category") or "cable_machine"),
                performed_equipment_type=str(exercise.get("equipment_type") or "unknown"),
            )
            await self._record_pr_if_needed(
                exercise_name=str(exercise["name"]),
                weight=weight,
                reps=reps,
                unit=unit,
                workout_date=pending.workout_date,
                workout_log_id=log_id,
                user_id=str(pending.user_id),
            )
            set_count += 1

        pending.handled_indices.add(index)
        pending.logged_exercise_indices.add(index)
        pending.logged_set_count += set_count

        status = (
            f"Logged {set_count} sets for {index + 1}. {exercise['name']} "
            f"(programmed {planned_sets} working sets)."
        )
        next_index = self._next_unhandled_index(pending)
        if next_index is None:
            await send_discord_text(channel, status)
            await self._finalize_backfill(channel, pending)
            return True

        await send_discord_text(
            channel,
            f"{status}\nNext: {next_index + 1}. {pending.exercises[next_index]['name']}",
        )
        return True

    @commands.command(name="backfill")
    async def backfill_command(self, ctx: commands.Context, *, target: str) -> None:
        if not self._is_backfill_command_channel(ctx.channel):
            await send_discord_text(
                ctx.channel,
                "Use `!backfill` in a workout channel, `#commands`, `#settings`, or `#programme`.",
            )
            return
        if self._has_active_workout_session(ctx.author.id):
            await send_discord_text(ctx.channel, "Finish or pause your current workout before starting a backfill.")
            return

        user_id = str(ctx.author.id)
        lowered = target.strip().lower()
        if lowered == "yesterday":
            pending, error = await self._build_yesterday_backfill(user_id=user_id)
        else:
            match = BACKFILL_WEEK_DAY_RE.match(target.strip())
            if not match:
                await send_discord_text(ctx.channel, "Use `!backfill week <W> day <D>` or `!backfill yesterday`.")
                return
            pending, error = await self._build_pending_backfill(
                user_id=user_id,
                week=int(match.group("week")),
                day_number=int(match.group("day")),
            )

        if error:
            await send_discord_text(ctx.channel, error)
            return
        if pending is None:
            await send_discord_text(ctx.channel, "I couldn't start that backfill.")
            return

        await self._start_pending_backfill(ctx.channel, user_id=ctx.author.id, pending=pending)

    @commands.command(name="startdate")
    async def startdate_command(self, ctx: commands.Context, iso_date: str) -> None:
        if not self._is_backfill_command_channel(ctx.channel):
            await send_discord_text(
                ctx.channel,
                "Use `!startdate` in a workout channel, `#commands`, `#settings`, or `#programme`.",
            )
            return
        cleaned = iso_date.strip()
        if not DATE_RE.fullmatch(cleaned):
            await send_discord_text(ctx.channel, "Use `!startdate YYYY-MM-DD`.")
            return
        try:
            parsed = date.fromisoformat(cleaned)
        except ValueError:
            await send_discord_text(ctx.channel, "That date isn't valid. Use `YYYY-MM-DD`.")
            return
        await self.db.set_program_start_date(parsed, user_id=str(ctx.author.id))
        await send_discord_text(ctx.channel, f"Program start date set to {parsed.isoformat()}.")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        key = self._pending_key(message.author.id, message.channel.id)
        pending = self.pending.get(key)
        if pending is None:
            if not self._is_backfill_command_channel(message.channel):
                return
            if message.content.startswith(self.settings.command_prefix):
                return
            return
        if message.content.startswith(self.settings.command_prefix):
            return

        content = message.content.strip()
        if not content:
            return

        if pending.awaiting_overwrite_confirmation:
            await self._handle_overwrite_reply(message.channel, pending, content)
            return

        for raw_line in [line.strip() for line in content.splitlines() if line.strip()]:
            lowered = raw_line.lower()
            if lowered == "done":
                await self._finalize_backfill(message.channel, pending)
                return
            if lowered == "skip":
                await self._handle_skip(message.channel, pending)
                if key not in self.pending:
                    return
                continue
            handled = await self._handle_entry_line(message.channel, pending, raw_line)
            if not handled:
                return
            if key not in self.pending:
                return


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(BackfillCog(bot))
