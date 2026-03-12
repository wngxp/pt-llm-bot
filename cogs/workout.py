from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import discord
from discord.ext import commands

from llm.prompts import ASK_SYSTEM_PROMPT
from utils.e1rm import epley_1rm
from utils.export import write_logs_csv
from utils.formatters import format_exercise_brief, format_set_log
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
SUPERSET_ROUND_REST_SECONDS = 90
READY_TOKENS = {"ready", "start", "lets go", "let's go", "go"}
WEEKDAY_CHANNEL_NAMES = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
WEEKDAY_LABELS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
EARLY_END_INTENTS = {
    "done",
    "im done",
    "i'm done",
    "end",
    "stop",
    "done for today",
    "gotta go",
    "got to go",
    "need to go",
    "have to go",
    "leaving",
    "end workout",
    "end session",
    "quit",
}
YES_TOKENS = {"yes", "y", "yeah", "yep", "sure"}
NO_TOKENS = {"no", "n", "nah", "nope", "cancel", "continue"}
RESUME_TOKENS = {"resume", "pause", "later", "resume later"}
MOVE_ON_TOKENS = {"move on", "moveon", "advance", "next day", "end and move on"}
FATIGUE_CUE_WORDS = {
    "tired",
    "cant do any more",
    "can't do any more",
    "cannot do any more",
    "cant do more",
    "can't do more",
    "felt heavy",
    "too heavy",
    "exhausted",
    "drained",
}
QUESTION_WORDS = {
    "what",
    "why",
    "how",
    "can",
    "could",
    "should",
    "when",
    "where",
    "which",
    "who",
    "is",
    "are",
    "do",
    "does",
}


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
    presented_exercises: set[int] = field(default_factory=set)
    exercise_units: dict[int, str] = field(default_factory=dict)
    logged_sets: list[dict[str, Any]] = field(default_factory=list)
    pr_events: list[dict[str, Any]] = field(default_factory=list)
    baseline_exercises: set[str] = field(default_factory=set)
    paused: bool = False
    paused_local_date: Optional[str] = None
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
        self.user_locks: dict[int, asyncio.Lock] = {}
        self.early_end_prompts: dict[tuple[int, int], dict[str, Any]] = {}

    def _is_workout_channel(self, channel: discord.abc.GuildChannel | discord.Thread | discord.abc.PrivateChannel) -> bool:
        cid = getattr(channel, "id", None)
        name = getattr(channel, "name", "")
        if self.settings.workout_channel_ids:
            return cid in self.settings.workout_channel_ids
        return name in {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}

    def _get_user_lock(self, user_id: int) -> asyncio.Lock:
        lock = self.user_locks.get(user_id)
        if lock is None:
            lock = asyncio.Lock()
            self.user_locks[user_id] = lock
        return lock

    async def _current_local_datetime(self) -> tuple[datetime, str]:
        tz_name = await self.db.get_user_timezone()
        try:
            tzinfo = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            tz_name = "UTC"
            tzinfo = timezone.utc
            await self.db.set_user_timezone(tz_name)
        return datetime.now(tzinfo), tz_name

    def _channel_ref_for_name(self, channel: discord.abc.Messageable, expected_name: str) -> str:
        guild = getattr(channel, "guild", None)
        if guild and isinstance(guild, discord.Guild):
            expected = discord.utils.get(guild.text_channels, name=expected_name)
            if expected is not None:
                return expected.mention
        return f"#{expected_name}"

    def _settings_channel_ref(self, channel: discord.abc.Messageable) -> Optional[str]:
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

    async def _check_weekday_start_channel(self, channel: discord.abc.Messageable) -> bool:
        channel_name = str(getattr(channel, "name", "")).lower()
        if channel_name not in WEEKDAY_CHANNEL_NAMES:
            return True

        now_local, _ = await self._current_local_datetime()
        weekday_idx = now_local.weekday()
        expected_channel = WEEKDAY_CHANNEL_NAMES[weekday_idx]
        if channel_name == expected_channel:
            return True

        expected_ref = self._channel_ref_for_name(channel, expected_channel)
        weekday_label = WEEKDAY_LABELS[weekday_idx]
        await channel.send(f"It's {weekday_label} - head over to {expected_ref} to start your session.")
        return False

    def _session_progress(self, session: WorkoutSession) -> tuple[int, int, float]:
        completed_exercises = sum(
            1 for ex in session.exercises if session.set_counts.get(int(ex["id"]), 0) > 0
        )
        total_exercises = max(1, len(session.exercises))
        progress = completed_exercises / total_exercises
        return completed_exercises, total_exercises, progress

    def _is_early_end_intent(self, text: str) -> bool:
        lowered = text.strip().lower()
        if lowered in EARLY_END_INTENTS:
            return True
        return any(intent in lowered for intent in EARLY_END_INTENTS)

    def _matches_token(self, text: str, tokens: set[str]) -> bool:
        lowered = text.strip().lower()
        for token in tokens:
            if lowered == token:
                return True
            if lowered.startswith(token + " "):
                return True
        return False

    async def _begin_early_end_prompt(
        self,
        channel: discord.abc.Messageable,
        user_id: int,
        session: WorkoutSession,
    ) -> None:
        completed, total, progress = self._session_progress(session)
        self.early_end_prompts[(channel.id, user_id)] = {
            "step": "confirm",
            "completed": completed,
            "total": total,
            "progress": progress,
        }
        await channel.send(
            f"You've completed {completed}/{total} exercises. Are you sure you want to end early? Reply `yes` or `no`."
        )

    async def _end_session_early(
        self,
        channel: discord.abc.Messageable,
        session: WorkoutSession,
        *,
        progress: float,
        completed: int,
        total: int,
    ) -> None:
        await self._cancel_rest(session)
        self.sessions.pop(session.channel_id, None)
        self._clear_early_end_prompts_for_channel(session.channel_id)

        streak_line = ""
        if session.logged_sets:
            streak = await self.db.mark_workout_completed(date.today())
            streak_line = (
                f"\n🔥 Streak: {streak['current_streak']} sessions "
                f"(Longest: {streak['longest_streak']})."
            )

        await self.db.advance_day_index()
        if progress < 0.5:
            tone = (
                f"No worries - some work is better than none. You crushed those {completed} exercises. "
                "Pick up where you left off next time?"
            )
        else:
            tone = "Solid session - you got through most of it. Rest up."

        await channel.send(
            f"{tone}\nEnded early at {completed}/{total} exercises and queued the next program day.{streak_line}"
        )

    def _clear_early_end_prompts_for_channel(self, channel_id: int) -> None:
        keys = [key for key in self.early_end_prompts if key[0] == channel_id]
        for key in keys:
            self.early_end_prompts.pop(key, None)

    async def _handle_early_end_prompt(
        self,
        channel: discord.abc.Messageable,
        user_id: int,
        session: WorkoutSession,
        text: str,
    ) -> bool:
        key = (channel.id, user_id)
        prompt = self.early_end_prompts.get(key)
        lowered = text.strip().lower()

        if prompt is None:
            if not self._is_early_end_intent(text):
                return False
            await self._begin_early_end_prompt(channel, user_id, session)
            return True

        step = prompt.get("step")
        completed = int(prompt.get("completed", 0))
        total = int(prompt.get("total", len(session.exercises)))
        progress = float(prompt.get("progress", 0.0))

        if step == "confirm":
            if self._matches_token(lowered, YES_TOKENS):
                if progress < 0.5:
                    await channel.send(
                        f"No worries - some work is better than none. You crushed those {completed} exercises. "
                        "Pick up where you left off next time?\n"
                        "Reply `resume` to continue later today in this channel, or `move on` to end and advance."
                    )
                else:
                    await channel.send(
                        "Solid session - you got through most of it. Rest up.\n"
                        "Reply `resume` to continue later today in this channel, or `move on` to end and advance."
                )
                prompt["step"] = "decision"
                return True
            if self._matches_token(lowered, NO_TOKENS):
                self.early_end_prompts.pop(key, None)
                await channel.send("Continuing current session.")
                await self._prompt_current_exercise(channel, session)
                return True

            await channel.send("Please reply `yes` or `no`.")
            return True

        if step == "decision":
            if self._matches_token(lowered, RESUME_TOKENS):
                await self._cancel_rest(session)
                now_local, _ = await self._current_local_datetime()
                session.paused = True
                session.paused_local_date = now_local.date().isoformat()
                self.early_end_prompts.pop(key, None)
                await channel.send(
                    f"Session paused at {completed}/{total} exercises. "
                    "Type `ready` in this channel later today to continue."
                )
                return True

            if self._matches_token(lowered, MOVE_ON_TOKENS) or "move on" in lowered:
                self.early_end_prompts.pop(key, None)
                await self._end_session_early(
                    channel,
                    session,
                    progress=progress,
                    completed=completed,
                    total=total,
                )
                return True

            if self._matches_token(lowered, NO_TOKENS):
                self.early_end_prompts.pop(key, None)
                await channel.send("Continuing current session.")
                await self._prompt_current_exercise(channel, session)
                return True

            await channel.send("Reply `resume` or `move on`.")
            return True

        self.early_end_prompts.pop(key, None)
        return False

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
        self._clear_early_end_prompts_for_channel(session.channel_id)

        total_days = len(await self.db.get_program_days(program["id"]))
        await channel.send(
            f"Starting **{day['name']}** (Day {day_index + 1} of {total_days})."
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
        else:
            last_lines.append("Last: no history")

        warmup = None
        category = str(exercise.get("category") or "cable_machine")
        if category in {"heavy_barbell", "light_barbell"} and logs:
            basis_weight = float(logs[0]["weight"])
            basis_unit = str(logs[0].get("unit") or "lbs")
            if basis_weight > 0:
                warmup = generate_warmup(basis_weight, category, basis_unit)

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
        session.presented_exercises.add(int(exercise["id"]))
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
        *,
        prompt_on_ready: bool = False,
    ) -> None:
        if session.rest_task and not session.rest_task.done():
            session.rest_task.cancel()

        session.rest_seconds = seconds
        await channel.send(f"⏱️ Rest {seconds // 60}:{seconds % 60:02d}.")

        async def _timer() -> None:
            try:
                await asyncio.sleep(seconds)
                if self.sessions.get(session.channel_id) is not session:
                    return
                await channel.send(f"Ready. {ready_message}")
                if prompt_on_ready:
                    await self._prompt_current_exercise(channel, session)
            except asyncio.CancelledError:
                return

        session.rest_task = asyncio.create_task(_timer())

    async def _cancel_rest(self, session: WorkoutSession) -> None:
        if session.rest_task and not session.rest_task.done():
            session.rest_task.cancel()
        session.rest_task = None
        session.rest_seconds = 0

    def _build_session_summary(self, session: WorkoutSession) -> str:
        completed_exercises = 0
        for ex in session.exercises:
            if session.set_counts.get(int(ex["id"]), 0) > 0:
                completed_exercises += 1

        top_sets = sorted(
            session.logged_sets,
            key=lambda row: float(row.get("e1rm", 0.0)),
            reverse=True,
        )[:3]

        lines = [
            f"Session complete for **{session.day['name']}**.",
            f"Completed: {completed_exercises} exercises, {len(session.logged_sets)} sets.",
        ]
        if session.pr_events:
            lines.append(f"PRs hit: {len(session.pr_events)}")

        if top_sets:
            lines.append("Notable sets:")
            for row in top_sets:
                if row.get("is_bodyweight"):
                    load_display = row.get("note") or "bodyweight"
                    lines.append(
                        f"- {row['exercise_name']}: {load_display} x {row['reps']}"
                    )
                else:
                    lines.append(
                        f"- {row['exercise_name']}: {row['weight']:g}{row['unit']} x {row['reps']} "
                        f"(e1RM {row['e1rm']:.1f})"
                    )

        lines.append("Next day is queued. Type `ready` when you want to start it.")
        return "\n".join(lines)

    async def _complete_session(self, channel: discord.abc.Messageable, session: WorkoutSession) -> None:
        await self._cancel_rest(session)
        summary = self._build_session_summary(session)
        streak = await self.db.mark_workout_completed(date.today())
        await self.db.advance_day_index()
        self.sessions.pop(session.channel_id, None)
        self._clear_early_end_prompts_for_channel(session.channel_id)

        await channel.send(
            f"{summary}\n🔥 Streak: {streak['current_streak']} sessions (Longest: {streak['longest_streak']})."
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

    async def _resolve_unit(
        self,
        channel: discord.abc.Messageable,
        session: WorkoutSession,
        exercise_id: int,
        parsed: dict[str, Any],
    ) -> Optional[str]:
        if parsed.get("unit"):
            unit = str(parsed["unit"])
            session.exercise_units[exercise_id] = unit
            return unit

        if exercise_id in session.exercise_units:
            return session.exercise_units[exercise_id]

        last_log = await self.db.get_last_log_for_exercise(exercise_id)
        if last_log and last_log.get("unit"):
            unit = str(last_log["unit"])
            session.exercise_units[exercise_id] = unit
            return unit

        user_state = await self.db.get_user_state()
        default_unit = str(user_state.get("default_unit") or "").strip().lower()
        if default_unit in {"kg", "lbs"}:
            session.exercise_units[exercise_id] = default_unit
            return default_unit

        await channel.send(
            "I need a unit for this exercise. Include `kg` or `lbs` in your set entry (e.g. `80 kg x 8`)."
        )
        return None

    def _contains_fatigue_cue(self, text: str) -> bool:
        lowered = text.strip().lower()
        if not lowered:
            return False
        return any(cue in lowered for cue in FATIGUE_CUE_WORDS)

    async def _apply_fatigue_adjustment_if_needed(
        self,
        channel: discord.abc.Messageable,
        trailing_text: str,
    ) -> None:
        if not self._contains_fatigue_cue(trailing_text):
            return
        state = await self.db.get_user_state()
        readiness = int(state.get("readiness") or 7)
        next_readiness = max(1, readiness - 1)
        if next_readiness == readiness:
            return
        await self.db.update_user_state(readiness=next_readiness)
        await channel.send(
            f"Noted fatigue cue. Readiness adjusted to {next_readiness}/10 for upcoming suggestions."
        )

    async def _check_and_record_pr(
        self,
        session: WorkoutSession,
        exercise_name: str,
        weight: float,
        reps: int,
        unit: str,
        workout_log_id: int,
        category: str,
    ) -> tuple[Optional[dict[str, Any]], Optional[str]]:
        if weight <= 0:
            return None, None

        key = exercise_name.strip().lower()
        e1rm = epley_1rm(weight, reps)
        existing = await self.db.get_best_pr(exercise_name)

        if not existing:
            await self.db.create_pr(
                exercise_name,
                weight=weight,
                reps=reps,
                unit=unit,
                estimated_1rm=e1rm,
                workout_date=date.today(),
                workout_log_id=workout_log_id,
            )
            session.baseline_exercises.add(key)
            return None, None

        pr_type: Optional[str] = None
        if e1rm > float(existing["estimated_1rm"]):
            pr_type = "e1rm"
        elif weight > float(existing["weight"]) and reps >= int(existing["reps"]):
            pr_type = "weight"
        elif reps > int(existing["reps"]) and weight >= float(existing["weight"]):
            pr_type = "reps"

        if not pr_type:
            return None, None

        await self.db.create_pr(
            exercise_name,
            weight=weight,
            reps=reps,
            unit=unit,
            estimated_1rm=e1rm,
            workout_date=date.today(),
            workout_log_id=workout_log_id,
        )

        if key in session.baseline_exercises:
            return None, None

        announce_categories = {"heavy_barbell", "light_barbell", "bodyweight"}
        if category not in announce_categories:
            return None, None

        payload = {
            "exercise_name": exercise_name,
            "weight": weight,
            "reps": reps,
            "unit": unit,
            "e1rm": e1rm,
            "previous": existing,
        }
        self.bot.dispatch("pr_hit", payload)
        short_message = f"🏆 PR! {weight:g}{unit} x {reps} (e1RM: {e1rm:.1f})"
        return payload, short_message

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
            if session.superset:
                await self._advance_superset(channel, session)
                return

            current = session.current_exercise()
            if current and int(current["id"]) == ex_id:
                await channel.send(f"{target['name']} is complete. Moving to the next exercise.")
                session.current_index += 1
                if session.current_index >= len(session.exercises):
                    await self._complete_session(channel, session)
                else:
                    await self._prompt_current_exercise(channel, session)
                return
            await channel.send(f"{target['name']} is already complete for today.")
            return

        if ex_id not in session.presented_exercises:
            await self._prompt_current_exercise(channel, session)
            return

        unit = await self._resolve_unit(channel, session, ex_id, parsed)
        if not unit:
            return

        set_number = done_sets + 1
        parsed_note = str(parsed.get("note") or "").strip()
        trailing_text = str(parsed.get("trailing_text") or "").strip()
        note_parts = [part for part in [parsed_note, trailing_text] if part]
        note = "; ".join(note_parts)

        log_id = await self.db.log_set(
            exercise_id=ex_id,
            workout_date=date.today(),
            set_number=set_number,
            weight=float(parsed["weight"]),
            reps=int(parsed["reps"]),
            unit=str(unit),
            rir=parsed.get("rir"),
            notes=note,
        )
        session.set_counts[ex_id] = set_number

        category = str(target.get("category") or "cable_machine")
        pr_payload, short_pr = await self._check_and_record_pr(
            session,
            str(target["name"]),
            float(parsed["weight"]),
            int(parsed["reps"]),
            str(unit),
            log_id,
            category,
        )
        if pr_payload:
            session.pr_events.append(pr_payload)

        if parsed.get("is_bodyweight"):
            display_load = note or "bodyweight"
            set_message = f"✅ Set {set_number}: {target['name']} — {display_load} x {int(parsed['reps'])}"
        else:
            set_message = format_set_log(
                exercise_name=str(target["name"]),
                weight=float(parsed["weight"]),
                reps=int(parsed["reps"]),
                unit=str(unit),
                set_number=set_number,
            )
        if short_pr:
            set_message = f"{set_message} | {short_pr}"
        await channel.send(set_message)

        await self._apply_fatigue_adjustment_if_needed(channel, trailing_text)

        cue = parse_cue(parsed.get("raw", ""))
        if cue:
            await self.db.save_cue(str(target["name"]), cue)
            await channel.send(f"💡 Saved cue: {cue}")

        e1rm_value = epley_1rm(float(parsed["weight"]), int(parsed["reps"])) if float(parsed["weight"]) > 0 else 0.0
        session.logged_sets.append(
            {
                "exercise_name": str(target["name"]),
                "weight": float(parsed["weight"]),
                "reps": int(parsed["reps"]),
                "unit": str(unit),
                "e1rm": e1rm_value,
                "is_bodyweight": bool(parsed.get("is_bodyweight")),
                "note": note,
            }
        )

        if session.superset:
            await self._advance_superset(channel, session)
            return

        rest = REST_SECONDS_BY_CATEGORY.get(category, 90)

        is_last_set_for_exercise = set_number >= total_sets
        is_last_exercise_of_day = session.current_index >= len(session.exercises) - 1

        if not is_last_set_for_exercise:
            await self._start_rest(
                channel,
                session,
                rest,
                f"{target['name']} set {set_number + 1}/{total_sets}.",
                prompt_on_ready=False,
            )
            return

        session.current_index += 1
        if is_last_exercise_of_day:
            await self._complete_session(channel, session)
            return

        next_ex = session.current_exercise()
        next_name = next_ex["name"] if next_ex else "next exercise"
        await self._start_rest(
            channel,
            session,
            rest,
            f"Up next: {next_name}.",
            prompt_on_ready=True,
        )

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
            if session.current_index >= len(session.exercises):
                await self._complete_session(channel, session)
                return
            next_ex = session.current_exercise()
            next_name = next_ex["name"] if next_ex else "next exercise"
            await self._start_rest(
                channel,
                session,
                SUPERSET_ROUND_REST_SECONDS,
                f"Up next: {next_name}.",
                prompt_on_ready=True,
            )
            return

        superset.round_number += 1
        superset.member_pos = 0
        next_ex = session.current_exercise()
        ready = f"Round {superset.round_number}: start with {next_ex['name']}." if next_ex else "Next round."
        await self._start_rest(
            channel,
            session,
            SUPERSET_ROUND_REST_SECONDS,
            ready,
            prompt_on_ready=True,
        )

    def _is_question(self, text: str) -> bool:
        lowered = text.strip().lower()
        if "?" in lowered:
            return True
        words = [w for w in lowered.replace("'", "").split() if w]
        if not words:
            return False
        return any(word in QUESTION_WORDS for word in words)

    async def _answer_workout_question(self, session: WorkoutSession, question: str) -> str:
        state = await self.db.get_user_state()
        current = session.current_exercise()
        payload = {
            "question": question,
            "response_style": "Keep the answer to 2-3 short sentences.",
            "context": {
                "day": session.day.get("name"),
                "current_exercise": current,
                "phase": state.get("phase"),
                "readiness": state.get("readiness"),
            },
        }
        reply = await self.bot.ollama.chat(
            system=ASK_SYSTEM_PROMPT,
            user=json.dumps(payload, ensure_ascii=False),
            temperature=0.25,
        )
        sentence_chunks = [chunk.strip() for chunk in re.split(r"(?<=[.!?])\\s+", reply) if chunk.strip()]
        if len(sentence_chunks) <= 3:
            return reply.strip()
        return " ".join(sentence_chunks[:3]).strip()

    @commands.command(name="start")
    async def start_workout_command(self, ctx: commands.Context) -> None:
        if not self._is_workout_channel(ctx.channel):
            return
        existing = self.sessions.get(ctx.channel.id)
        if existing:
            if existing.paused:
                existing.paused = False
                await ctx.send("Resuming paused session.")
                await self._prompt_current_exercise(ctx.channel, existing)
            else:
                await ctx.send("Workout already in progress in this channel.")
            return
        if not await self._check_weekday_start_channel(ctx.channel):
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
            await self._maybe_send_settings_tip(ctx.channel)
            return

        lines = [f"{exercise_name.title()} estimated 1RM trend:"]
        for row in rows:
            lines.append(
                f"{row['date']}: {row['e1rm']:.1f} {row['unit']} ({row['weight']:g}x{row['reps']})"
            )
        delta = rows[-1]["e1rm"] - rows[0]["e1rm"]
        lines.append(f"Trend: {delta:+.1f} over {len(rows)} entries")
        await ctx.send("\n".join(lines))
        await self._maybe_send_settings_tip(ctx.channel)

    @commands.command(name="volume")
    async def volume_command(self, ctx: commands.Context) -> None:
        weekly = await self.db.get_weekly_volume()
        await ctx.send(format_volume_report(weekly))
        await self._maybe_send_settings_tip(ctx.channel)

    @commands.command(name="cue")
    async def cue_command(self, ctx: commands.Context, exercise_name: str, *, cue: str) -> None:
        await self.db.save_cue(exercise_name, cue)
        await ctx.send(f'Saved cue for {exercise_name}: "{cue}"')
        await self._maybe_send_settings_tip(ctx.channel)

    @commands.command(name="export")
    async def export_command(self, ctx: commands.Context, *, exercise_name: str = "") -> None:
        rows = await self.db.export_logs(exercise_name=exercise_name.strip() or None)
        if not rows:
            await ctx.send("No logs to export.")
            await self._maybe_send_settings_tip(ctx.channel)
            return

        stem = f"{exercise_name.strip().lower().replace(' ', '_')}_logs" if exercise_name.strip() else "workout_logs"
        csv_path = write_logs_csv(rows, stem=stem)
        await ctx.send(file=discord.File(str(csv_path)))
        await self._maybe_send_settings_tip(ctx.channel)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        if not self._is_workout_channel(message.channel):
            return

        content = message.content.strip()
        if not content or content.startswith(self.settings.command_prefix):
            return

        lowered = content.lower().strip()
        session = self.sessions.get(message.channel.id)

        if not session:
            if not await self._check_weekday_start_channel(message.channel):
                return
            if lowered in READY_TOKENS:
                await self._start_session(message.channel)
                return
            if self._is_question(content):
                await message.channel.send("Type `ready` to start today's workout, or ask broader questions in #ask.")
                return
            await message.channel.send("Type `ready` to start today's workout.")
            return

        lock = self._get_user_lock(message.author.id)
        async with lock:
            session = self.sessions.get(message.channel.id)
            if not session:
                await message.channel.send("Type `ready` to start today's workout.")
                return

            handled_early_end = await self._handle_early_end_prompt(
                message.channel,
                message.author.id,
                session,
                content,
            )
            if handled_early_end:
                return

            if session.paused:
                if lowered in READY_TOKENS:
                    session.paused = False
                    await message.channel.send("Resuming paused session.")
                    await self._prompt_current_exercise(message.channel, session)
                    return
                if self._matches_token(lowered, MOVE_ON_TOKENS) or "move on" in lowered:
                    completed, total, progress = self._session_progress(session)
                    await self._end_session_early(
                        message.channel,
                        session,
                        progress=progress,
                        completed=completed,
                        total=total,
                    )
                    return
                await message.channel.send(
                    "Session is paused. Type `ready` to resume or `move on` to end and advance."
                )
                return

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
                        prompt_on_ready=False,
                    )
                    return
                await message.channel.send("No active rest timer to extend.")
                return

            parsed = parse_set_input(content)
            if parsed:
                if session.rest_task and not session.rest_task.done():
                    await message.channel.send(
                        "Rest timer is active. Wait for the prompt or type `skip rest`."
                    )
                    return
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

            if self._is_question(content):
                try:
                    reply = await self._answer_workout_question(session, content)
                    await message.channel.send(reply)
                except Exception:
                    await message.channel.send("I couldn't reach the coach model right now. Try again in a moment.")
                return

            await message.channel.send(
                "I didn't catch that. Log sets as `225 x 3` or ask me a question."
            )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(WorkoutCog(bot))
