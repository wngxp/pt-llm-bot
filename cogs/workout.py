from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import discord
from discord.ext import commands

from utils.e1rm import epley_1rm
from utils.export import write_logs_csv
from utils.formatters import format_exercise_brief, format_pr_message, format_set_log
from utils.input_parser import parse_cue, parse_extend_rest, parse_set_input
from utils.plates import plates_breakdown
from utils.progression import suggest_weight
from utils.volume import format_volume_report
from utils.warmup import generate_warmup


REST_SECONDS_BY_CATEGORY = {
    "heavy_barbell": 240,
    "light_barbell": 150,
    "dumbbell": 120,
    "cable_machine": 90,
    "bodyweight": 90,
}
SUPSERSET_ROUND_REST_SECONDS = 90


@dataclass(slots=True)
class SupersetState:
    group_id: int
    member_indices: list[int]
    max_rounds: int
    round_number: int = 1
    member_pos: int = 0


@dataclass(slots=True)
class WorkoutSession:
    channel_id: int
    day_index: int
    day: dict[str, Any]
    exercises: list[dict[str, Any]]
    started_at: datetime
    current_index: int = 0
    set_counts: dict[int, int] = field(default_factory=dict)
    superset: Optional[SupersetState] = None
    rest_task: Optional[asyncio.Task] = None
    rest_seconds: int = 0

    def current_exercise(self) -> Optional[dict[str, Any]]:
        if self.superset:
            idx = self.superset.member_indices[self.superset.member_pos]
            return self.exercises[idx]
        if self.current_index >= len(self.exercises):
            return None
        return self.exercises[self.current_index]

    def is_complete(self) -> bool:
        return self.current_index >= len(self.exercises) and self.superset is None


class WorkoutCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.settings = bot.settings
        self.db = bot.db
        self.sessions: dict[int, WorkoutSession] = {}

    def _is_workout_channel(self, channel: discord.abc.GuildChannel | discord.Thread | discord.abc.PrivateChannel) -> bool:
        cid = getattr(channel, "id", None)
        name = getattr(channel, "name", "")
        if self.settings.workout_channel_ids:
            return cid in self.settings.workout_channel_ids
        return name in {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}

    async def _start_session(self, channel: discord.abc.Messageable) -> Optional[WorkoutSession]:
        program = await self.db.get_active_program()
        if not program:
            await channel.send("No active program. Paste one in #programme first.")
            return None

        day_index = await self.db.get_current_day_index()
        day = await self.db.get_day_for_index(day_index)
        if not day:
            await channel.send("Active program has no days configured.")
            return None

        exercises = await self.db.get_exercises_for_day(int(day["id"]))
        if not exercises:
            await channel.send(f"{day['name']} has no exercises.")
            return None

        session = WorkoutSession(
            channel_id=getattr(channel, "id", 0),
            day_index=day_index,
            day=day,
            exercises=exercises,
            started_at=datetime.now(),
            set_counts={int(ex["id"]): 0 for ex in exercises},
        )
        self.sessions[session.channel_id] = session

        await channel.send(
            f"Starting **{day['name']}** (Day {day_index + 1} of {len(await self.db.get_program_days(program['id']))})"
        )

        state = await self.db.get_user_state()
        last_workout = state.get("last_workout_date")
        if last_workout:
            last = datetime.strptime(last_workout, "%Y-%m-%d").date()
            gap = (date.today() - last).days
            if gap > 1:
                await channel.send(
                    f"You missed yesterday's session. Continuing sequential order with **{day['name']}** today."
                )

        await self._prompt_current_exercise(channel, session, intro=True)
        return session

    async def _prompt_current_exercise(
        self,
        channel: discord.abc.Messageable,
        session: WorkoutSession,
        *,
        intro: bool = False,
    ) -> None:
        if session.is_complete():
            await self._complete_session(channel, session)
            return

        if session.superset is None:
            self._ensure_superset_initialized(session)

        exercise = session.current_exercise()
        if exercise is None:
            await self._complete_session(channel, session)
            return

        logs = await self.db.get_last_logs_for_exercise(int(exercise["id"]), limit=3)
        suggestion = suggest_weight(exercise, logs)

        last_lines: list[str] = []
        if logs:
            compact = ", ".join(f"{l['weight']:g}{l['unit']}x{l['reps']}" for l in logs)
            last_lines.append(f"Last: {compact}")

        warmup = None
        if logs:
            warmup = generate_warmup(
                float(logs[0]["weight"]),
                str(exercise.get("category") or "cable_machine"),
                str(logs[0].get("unit") or "lbs"),
            )

        cue = await self.db.get_latest_cue(str(exercise["name"]))

        lines = []
        if intro:
            lines.append("Let's go.")

        if session.superset:
            group = self._superset_members(session)
            if session.superset.member_pos == 0:
                names = " + ".join(f"**{m['name']}**" for m in group)
                lines.append(
                    f"Superset Round {session.superset.round_number}/{session.superset.max_rounds}: {names}"
                )

        lines.append(format_exercise_brief(exercise))
        lines.extend(last_lines)
        lines.append(f"Suggestion: {suggestion}")
        if warmup:
            lines.append(f"Warm-up: {', '.join(warmup)}")
        if cue:
            lines.append(f"💡 Cue: {cue}")

        next_set = session.set_counts[int(exercise["id"])] + 1
        lines.append(f"Log set {next_set}/{exercise['sets']} as `weight x reps` (optionally `@rir`).")
        await channel.send("\n".join(lines))

    def _ensure_superset_initialized(self, session: WorkoutSession) -> None:
        if session.current_index >= len(session.exercises):
            return

        exercise = session.exercises[session.current_index]
        group_id = exercise.get("superset_group")
        if group_id is None:
            return

        member_indices: list[int] = []
        idx = session.current_index
        while idx < len(session.exercises):
            if session.exercises[idx].get("superset_group") != group_id:
                break
            member_indices.append(idx)
            idx += 1

        members = [session.exercises[i] for i in member_indices]
        max_rounds = max(int(m["sets"]) for m in members)
        session.superset = SupersetState(
            group_id=int(group_id),
            member_indices=member_indices,
            max_rounds=max_rounds,
        )

    def _superset_members(self, session: WorkoutSession) -> list[dict[str, Any]]:
        if not session.superset:
            return []
        return [session.exercises[i] for i in session.superset.member_indices]

    async def _start_rest(
        self,
        channel: discord.abc.Messageable,
        session: WorkoutSession,
        seconds: int,
        ready_message: str,
    ) -> None:
        if session.rest_task and not session.rest_task.done():
            session.rest_task.cancel()

        session.rest_seconds = seconds
        await channel.send(f"⏱️ Rest {seconds // 60}:{seconds % 60:02d}.")

        async def _timer() -> None:
            try:
                await asyncio.sleep(seconds)
                await channel.send(f"Ready. {ready_message}")
            except asyncio.CancelledError:
                return

        session.rest_task = asyncio.create_task(_timer())

    async def _cancel_rest(self, session: WorkoutSession) -> None:
        if session.rest_task and not session.rest_task.done():
            session.rest_task.cancel()
        session.rest_task = None
        session.rest_seconds = 0

    async def _complete_session(self, channel: discord.abc.Messageable, session: WorkoutSession) -> None:
        await self._cancel_rest(session)
        streak = await self.db.mark_workout_completed(date.today())
        await self.db.advance_day_index()
        self.sessions.pop(session.channel_id, None)

        await channel.send(
            "Session complete. "
            f"🔥 Streak: {streak['current_streak']} sessions "
            f"(Longest: {streak['longest_streak']})."
        )

    async def _resolve_target_exercise(
        self,
        session: WorkoutSession,
        parsed: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        if session.superset:
            current = session.current_exercise()
            if not current:
                return None

            provided = (parsed.get("exercise") or "").strip().lower()
            if not provided:
                return current

            for member in self._superset_members(session):
                if provided in member["name"].lower():
                    return member
            return None

        current = session.current_exercise()
        if not current:
            return None

        provided = (parsed.get("exercise") or "").strip().lower()
        if not provided:
            return current

        if provided in current["name"].lower():
            return current
        return None

    async def _check_and_record_pr(
        self,
        channel: discord.abc.Messageable,
        exercise_name: str,
        weight: float,
        reps: int,
        unit: str,
        workout_log_id: int,
    ) -> None:
        e1rm = epley_1rm(weight, reps)
        existing = await self.db.get_best_pr(exercise_name)

        pr_type: Optional[str] = None
        if not existing:
            pr_type = "first"
        elif e1rm > float(existing["estimated_1rm"]):
            pr_type = "e1rm"
        elif weight > float(existing["weight"]) and reps >= int(existing["reps"]):
            pr_type = "weight"
        elif reps > int(existing["reps"]) and weight >= float(existing["weight"]):
            pr_type = "reps"

        if not pr_type:
            return

        await self.db.create_pr(
            exercise_name,
            weight=weight,
            reps=reps,
            unit=unit,
            estimated_1rm=e1rm,
            workout_date=date.today(),
            workout_log_id=workout_log_id,
        )
        msg = format_pr_message(exercise_name, weight, reps, unit, e1rm, existing)
        await channel.send(msg)

        self.bot.dispatch(
            "pr_hit",
            {
                "exercise_name": exercise_name,
                "weight": weight,
                "reps": reps,
                "unit": unit,
                "e1rm": e1rm,
                "previous": existing,
            },
        )

    async def _handle_logged_set(
        self,
        channel: discord.abc.Messageable,
        session: WorkoutSession,
        parsed: dict[str, Any],
    ) -> None:
        target = await self._resolve_target_exercise(session, parsed)
        if not target:
            await channel.send("That does not match the current exercise context.")
            return

        ex_id = int(target["id"])
        done_sets = session.set_counts.get(ex_id, 0)
        total_sets = int(target["sets"])
        if done_sets >= total_sets:
            await channel.send(f"{target['name']} is already complete for today.")
            return

        default_unit = (await self.db.get_user_state()).get("default_unit", "lbs")
        unit = parsed.get("unit") or default_unit
        set_number = done_sets + 1

        log_id = await self.db.log_set(
            exercise_id=ex_id,
            workout_date=date.today(),
            set_number=set_number,
            weight=float(parsed["weight"]),
            reps=int(parsed["reps"]),
            unit=str(unit),
            rir=parsed.get("rir"),
        )
        session.set_counts[ex_id] = set_number

        await channel.send(
            format_set_log(
                exercise_name=str(target["name"]),
                weight=float(parsed["weight"]),
                reps=int(parsed["reps"]),
                unit=str(unit),
                set_number=set_number,
            )
        )

        cue = parse_cue(parsed.get("raw", ""))
        if cue:
            await self.db.save_cue(str(target["name"]), cue)
            await channel.send(f"💡 Saved cue: {cue}")

        await self._check_and_record_pr(
            channel,
            str(target["name"]),
            float(parsed["weight"]),
            int(parsed["reps"]),
            str(unit),
            log_id,
        )

        if session.superset:
            await self._advance_superset(channel, session)
            return

        if set_number < total_sets:
            rest = REST_SECONDS_BY_CATEGORY.get(str(target.get("category") or "cable_machine"), 90)
            await self._start_rest(
                channel,
                session,
                rest,
                f"{target['name']} set {set_number + 1}/{total_sets}.",
            )
            return

        session.current_index += 1
        await self._prompt_current_exercise(channel, session)

    async def _advance_superset(self, channel: discord.abc.Messageable, session: WorkoutSession) -> None:
        superset = session.superset
        if not superset:
            return

        members = self._superset_members(session)

        if superset.member_pos < len(members) - 1:
            superset.member_pos += 1
            nxt = session.current_exercise()
            if nxt:
                await channel.send(f"Now do **{nxt['name']}** (round {superset.round_number}).")
            return

        all_done = True
        for member in members:
            ex_id = int(member["id"])
            if session.set_counts.get(ex_id, 0) < int(member["sets"]):
                all_done = False
                break

        if all_done:
            last_idx = superset.member_indices[-1]
            session.current_index = last_idx + 1
            session.superset = None
            await self._prompt_current_exercise(channel, session)
            return

        superset.round_number += 1
        superset.member_pos = 0
        next_ex = session.current_exercise()
        ready = f"Round {superset.round_number}: start with {next_ex['name']}." if next_ex else "Next round."
        await self._start_rest(channel, session, SUPSERSET_ROUND_REST_SECONDS, ready)

    @commands.command(name="start")
    async def start_workout_command(self, ctx: commands.Context) -> None:
        if not self._is_workout_channel(ctx.channel):
            return
        if ctx.channel.id in self.sessions:
            await ctx.send("Workout already in progress in this channel.")
            return
        await self._start_session(ctx.channel)

    @commands.command(name="done")
    async def finish_workout_command(self, ctx: commands.Context) -> None:
        if not self._is_workout_channel(ctx.channel):
            return
        session = self.sessions.get(ctx.channel.id)
        if not session:
            await ctx.send("No workout in progress.")
            return
        session.current_index = len(session.exercises)
        session.superset = None
        await self._complete_session(ctx.channel, session)

    @commands.command(name="plates")
    async def plates_command(self, ctx: commands.Context, weight: float, unit: str = "lbs") -> None:
        if not self._is_workout_channel(ctx.channel):
            return
        normalized = "kg" if unit.lower().startswith("kg") else "lbs"
        await ctx.send(plates_breakdown(weight, normalized))

    @commands.command(name="e1rm")
    async def e1rm_command(self, ctx: commands.Context, *, exercise_name: str) -> None:
        rows = await self.db.get_e1rm_history(exercise_name, limit=12)
        if not rows:
            await ctx.send(f"No history for {exercise_name}.")
            return

        lines = [f"{exercise_name.title()} estimated 1RM trend:"]
        for row in rows:
            lines.append(
                f"{row['date']}: {row['e1rm']:.1f} {row['unit']} ({row['weight']:g}x{row['reps']})"
            )
        delta = rows[-1]["e1rm"] - rows[0]["e1rm"]
        lines.append(f"Trend: {delta:+.1f} over {len(rows)} entries")
        await ctx.send("\n".join(lines))

    @commands.command(name="volume")
    async def volume_command(self, ctx: commands.Context) -> None:
        weekly = await self.db.get_weekly_volume()
        await ctx.send(format_volume_report(weekly))

    @commands.command(name="cue")
    async def cue_command(self, ctx: commands.Context, exercise_name: str, *, cue: str) -> None:
        await self.db.save_cue(exercise_name, cue)
        await ctx.send(f'Saved cue for {exercise_name}: "{cue}"')

    @commands.command(name="export")
    async def export_command(self, ctx: commands.Context, *, exercise_name: str = "") -> None:
        rows = await self.db.export_logs(exercise_name=exercise_name.strip() or None)
        if not rows:
            await ctx.send("No logs to export.")
            return

        stem = f"{exercise_name.strip().lower().replace(' ', '_')}_logs" if exercise_name.strip() else "workout_logs"
        csv_path = write_logs_csv(rows, stem=stem)
        await ctx.send(file=discord.File(str(csv_path)))

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        if not self._is_workout_channel(message.channel):
            return

        content = message.content.strip()
        if not content or content.startswith(self.settings.command_prefix):
            return

        session = self.sessions.get(message.channel.id)
        if not session:
            session = await self._start_session(message.channel)
            if not session:
                return

        lowered = content.lower()
        if lowered in {"skip rest", "skip"}:
            await self._cancel_rest(session)
            await message.channel.send("Rest skipped.")
            await self._prompt_current_exercise(message.channel, session)
            return

        extend = parse_extend_rest(content)
        if extend:
            if session.rest_task and not session.rest_task.done():
                await self._start_rest(
                    message.channel,
                    session,
                    session.rest_seconds + extend * 60,
                    "Extended rest complete.",
                )
                return
            await message.channel.send("No active rest timer to extend.")
            return

        parsed = parse_set_input(content)
        if parsed:
            parsed["raw"] = content
            await self._handle_logged_set(message.channel, session, parsed)
            return

        cue = parse_cue(content)
        if cue:
            current = session.current_exercise()
            if current:
                await self.db.save_cue(str(current["name"]), cue)
                await message.channel.send(f"Saved cue for {current['name']}: {cue}")
                return

        await message.channel.send("Log sets as `weight x reps`, or use commands like `!volume`, `!plates`, `!e1rm`.")


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(WorkoutCog(bot))
