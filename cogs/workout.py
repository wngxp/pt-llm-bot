from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import discord
from discord.ext import commands

from llm.prompts import ASK_SYSTEM_PROMPT
from utils.e1rm import epley_1rm
from utils.discord_messages import send_discord_text
from utils.export import write_logs_csv
from utils.formatters import format_exercise_brief, format_set_log
from utils.input_parser import parse_cue, parse_extend_rest, parse_set_input
from utils.numbers import format_standard_number
from utils.plates import plates_breakdown
from utils.progression import suggest_weight
from utils.volume import format_volume_report
from utils.warmup import generate_warmup

logger = logging.getLogger(__name__)


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
    "move on",
    "moveon",
}
YES_TOKENS = {"yes", "y", "yeah", "yep", "sure"}
NO_TOKENS = {"no", "n", "nah", "nope", "cancel", "continue"}
RESUME_TOKENS = {"resume", "pause", "later", "resume later"}
MOVE_ON_TOKENS = {
    "move on",
    "moveon",
    "move",
    "m",
    "advance",
    "next day",
    "end and move on",
    "done",
    "end",
}
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

MAX_WEIGHT_BY_UNIT = {"lbs": 1500.0, "kg": 700.0}
MAX_REPS = 200
ZERO_WIDTH_RE = re.compile(r"[\u200B-\u200D\uFEFF]")


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
    total_exercises: int = 0
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
    day_activity_warning: Optional[str] = None

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
        self.early_end_timeout_tasks: dict[tuple[int, int], asyncio.Task] = {}

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
        total_exercises = max(1, int(session.total_exercises or len(session.exercises)))
        progress = completed_exercises / total_exercises
        return completed_exercises, total_exercises, progress

    def _normalize_user_text(self, text: str) -> str:
        cleaned = ZERO_WIDTH_RE.sub("", text or "")
        cleaned = re.sub(r"[^a-zA-Z0-9\\s']+", " ", cleaned)
        cleaned = " ".join(cleaned.strip().lower().split())
        return cleaned

    def _is_early_end_intent(self, text: str) -> bool:
        lowered = self._normalize_user_text(text)
        if lowered in EARLY_END_INTENTS:
            return True
        return any(intent in lowered for intent in EARLY_END_INTENTS)

    def _matches_token(self, text: str, tokens: set[str]) -> bool:
        lowered = self._normalize_user_text(text)
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
            "expires_at": datetime.now(timezone.utc) + timedelta(seconds=60),
        }
        self._schedule_early_end_timeout((channel.id, user_id), channel, session)
        await send_discord_text(
            channel,
            f"You've completed {completed}/{total} exercises. Are you sure you want to end early? Reply `yes` or `no`."
        )

    async def _end_session_early(
        self,
        channel: discord.abc.Messageable,
        session: WorkoutSession,
        *,
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

        next_index = await self.db.advance_day_index()
        next_day = await self.db.get_day_for_index(next_index)
        next_name = str(next_day["name"]) if next_day else "next day"
        progress_line = (
            f"No exercises completed in this session."
            if completed == 0
            else f"{session.day['name']} session ended at {completed}/{total} exercises."
        )
        await send_discord_text(
            channel,
            f"{progress_line} "
            f"Next time you type `ready`, you'll start {next_name} (Day {next_index + 1}).{streak_line}",
        )

    def _clear_early_end_prompts_for_channel(self, channel_id: int) -> None:
        keys = [key for key in self.early_end_prompts if key[0] == channel_id]
        for key in keys:
            self.early_end_prompts.pop(key, None)
            task = self.early_end_timeout_tasks.pop(key, None)
            if task and not task.done():
                task.cancel()

    def _schedule_early_end_timeout(
        self,
        key: tuple[int, int],
        channel: discord.abc.Messageable,
        session: WorkoutSession,
    ) -> None:
        existing = self.early_end_timeout_tasks.get(key)
        if existing and not existing.done():
            existing.cancel()

        async def _timeout() -> None:
            try:
                await asyncio.sleep(60)
                prompt = self.early_end_prompts.get(key)
                if not prompt:
                    return
                current_session = self.sessions.get(session.channel_id)
                if current_session is not session:
                    self.early_end_prompts.pop(key, None)
                    return
                self.early_end_prompts.pop(key, None)
                await send_discord_text(channel, "No response received. Continuing current session.")
                await self._prompt_current_exercise(channel, session)
            except asyncio.CancelledError:
                return
            finally:
                self.early_end_timeout_tasks.pop(key, None)

        self.early_end_timeout_tasks[key] = asyncio.create_task(_timeout())

    async def _handle_early_end_prompt(
        self,
        channel: discord.abc.Messageable,
        user_id: int,
        session: WorkoutSession,
        text: str,
    ) -> bool:
        key = (channel.id, user_id)
        prompt = self.early_end_prompts.get(key)
        lowered = self._normalize_user_text(text)

        if prompt is None:
            if not self._is_early_end_intent(text):
                return False
            await self._begin_early_end_prompt(channel, user_id, session)
            return True

        expires_at = prompt.get("expires_at")
        if isinstance(expires_at, datetime) and datetime.now(timezone.utc) > expires_at:
            self.early_end_prompts.pop(key, None)
            task = self.early_end_timeout_tasks.pop(key, None)
            if task and not task.done():
                task.cancel()
            await send_discord_text(channel, "Early-end prompt timed out. Continuing current session.")
            await self._prompt_current_exercise(channel, session)
            return True

        step = prompt.get("step")
        completed = int(prompt.get("completed", 0))
        total = int(prompt.get("total", session.total_exercises or len(session.exercises)))
        progress = float(prompt.get("progress", 0.0))

        if step == "confirm":
            if self._matches_token(lowered, YES_TOKENS):
                if progress < 0.5:
                    if completed == 0:
                        text = (
                            "No exercises completed yet. Want to try again later?\n"
                            "Reply `resume` to continue later today in this channel, or `move on` to end and advance."
                        )
                    else:
                        text = (
                            f"No worries - some work is better than none. You finished {completed} exercises.\n"
                            "Reply `resume` to continue later today in this channel, or `move on` to end and advance."
                        )
                    await send_discord_text(
                        channel,
                        text,
                    )
                else:
                    await send_discord_text(
                        channel,
                        "Solid session - you got through most of it. Rest up.\n"
                        "Reply `resume` to continue later today in this channel, or `move on` to end and advance."
                    )
                prompt["step"] = "decision"
                prompt["expires_at"] = datetime.now(timezone.utc) + timedelta(seconds=60)
                self._schedule_early_end_timeout(key, channel, session)
                return True
            if self._matches_token(lowered, NO_TOKENS):
                self.early_end_prompts.pop(key, None)
                task = self.early_end_timeout_tasks.pop(key, None)
                if task and not task.done():
                    task.cancel()
                await send_discord_text(channel, "Continuing current session.")
                await self._prompt_current_exercise(channel, session)
                return True

            await send_discord_text(channel, "Please reply `yes` or `no`.")
            return True

        if step == "decision":
            if self._matches_token(lowered, RESUME_TOKENS):
                await self._cancel_rest(session)
                now_local, _ = await self._current_local_datetime()
                session.paused = True
                session.paused_local_date = now_local.date().isoformat()
                self.early_end_prompts.pop(key, None)
                task = self.early_end_timeout_tasks.pop(key, None)
                if task and not task.done():
                    task.cancel()
                await send_discord_text(
                    channel,
                    f"Session paused at {completed}/{total} exercises. "
                    "Type `ready` in this channel later today to continue."
                )
                return True

            if self._matches_token(lowered, MOVE_ON_TOKENS) or "move on" in lowered:
                self.early_end_prompts.pop(key, None)
                task = self.early_end_timeout_tasks.pop(key, None)
                if task and not task.done():
                    task.cancel()
                await self._end_session_early(
                    channel,
                    session,
                    completed=completed,
                    total=total,
                )
                return True

            if self._matches_token(lowered, NO_TOKENS):
                self.early_end_prompts.pop(key, None)
                task = self.early_end_timeout_tasks.pop(key, None)
                if task and not task.done():
                    task.cancel()
                await send_discord_text(channel, "Continuing current session.")
                await self._prompt_current_exercise(channel, session)
                return True

            await send_discord_text(channel, "Reply `resume` or `move on`.")
            return True

        self.early_end_prompts.pop(key, None)
        task = self.early_end_timeout_tasks.pop(key, None)
        if task and not task.done():
            task.cancel()
        return False

    async def _start_session(self, channel: discord.abc.Messageable) -> Optional[WorkoutSession]:
        program = await self.db.get_active_program()
        if not program:
            await send_discord_text(channel, "No active program. Paste one in #programme first.")
            return None

        day_index = await self.db.get_current_day_index()
        day = await self.db.get_day_for_index(day_index)
        if not day:
            await send_discord_text(channel, "Active program has no days configured.")
            return None

        exercises = await self.db.get_exercises_for_day(int(day["id"]))
        if not exercises:
            await send_discord_text(channel, f"{day['name']} has no exercises.")
            return None

        session = WorkoutSession(
            channel_id=getattr(channel, "id", 0),
            day_index=day_index,
            day=day,
            exercises=exercises,
            started_at=datetime.now(),
            set_counts={int(ex["id"]): 0 for ex in exercises},
            total_exercises=len(exercises),
        )
        self.sessions[session.channel_id] = session
        self._clear_early_end_prompts_for_channel(session.channel_id)

        total_days = len(await self.db.get_program_days(program["id"]))
        await send_discord_text(
            channel,
            f"Starting **{day['name']}** (Day {day_index + 1} of {total_days})."
        )

        session.day_activity_warning = await self._day_activity_warning(exercises)
        if session.day_activity_warning:
            await send_discord_text(channel, session.day_activity_warning)

        state = await self.db.get_user_state()
        last_workout = state.get("last_workout_date")
        if last_workout:
            last = datetime.strptime(last_workout, "%Y-%m-%d").date()
            gap = (date.today() - last).days
            if gap > 1:
                await send_discord_text(
                    channel,
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
        activity_multiplier, activity_note = await self._activity_adjustment_for_exercise(exercise)
        state = await self.db.get_user_state()
        readiness = int(state.get("readiness") or 7)
        readiness_multiplier = 1.0
        readiness_note: Optional[str] = None
        if readiness >= 8:
            readiness_multiplier = 1.03
            readiness_note = f"Readiness is high ({readiness}/10). You can push the top end if form stays crisp."
        elif readiness >= 6:
            readiness_multiplier = 1.0
        elif readiness >= 4:
            readiness_multiplier = 0.9
            readiness_note = f"⚠️ Readiness is low ({readiness}/10). Going lighter today."
        else:
            readiness_multiplier = 0.8
            readiness_note = (
                f"⚠️ Readiness is very low ({readiness}/10). Consider a light technique session or rest."
            )

        final_multiplier = activity_multiplier * readiness_multiplier
        suggestion = suggest_weight(exercise, logs, adjustment_multiplier=final_multiplier)
        if not logs and readiness <= 5:
            suggestion = (
                f"{suggestion} Since readiness is low, use ~10% less than your typical starting load."
            )

        last_lines: list[str] = []
        if logs:
            compact = ", ".join(
                f"{format_standard_number(float(l['weight']))}{l['unit']}x{l['reps']}" for l in logs
            )
            last_lines.append(f"Last: {compact}")
        else:
            last_lines.append("Last: no history")

        warmup = None
        category = str(exercise.get("category") or "cable_machine")
        if category in {"heavy_barbell", "light_barbell"} and logs:
            basis_weight = float(logs[0]["weight"]) * final_multiplier
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

        if activity_note:
            lines.append(activity_note)
        if readiness_note:
            lines.append(readiness_note)
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
        await send_discord_text(channel, "\n".join(lines))

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
                        f"- {row['exercise_name']}: {format_standard_number(float(row['weight']))}{row['unit']} x {row['reps']} "
                        f"(e1RM {format_standard_number(float(row['e1rm']))})"
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

        await send_discord_text(
            channel,
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

        await send_discord_text(
            channel,
            "I need a unit for this exercise. Include `kg` or `lbs` in your set entry (e.g. `80 kg x 8`)."
        )
        return None

    def _split_groups(self, value: str) -> set[str]:
        groups: set[str] = set()
        for part in str(value or "").split(","):
            cleaned = part.strip().lower()
            if not cleaned:
                continue
            norm = re.sub(r"[^a-z0-9]+", "", cleaned)
            if not norm:
                continue
            groups.add(norm)
            if norm in {"leg", "legs", "lowerbody"}:
                groups.update({"legs", "quads", "hamstrings", "glutes", "calves"})
            if norm in {"quad", "quads"}:
                groups.update({"quads", "legs"})
            if norm in {"hamstring", "hamstrings"}:
                groups.update({"hamstrings", "legs"})
            if norm in {"glute", "glutes"}:
                groups.update({"glutes", "legs"})
            if norm in {"calf", "calves"}:
                groups.update({"calves", "legs"})
            if norm in {"back", "lats", "upperback", "traps"}:
                groups.update({"back", "lats", "upperback"})
            if norm in {"shoulder", "shoulders", "delts"}:
                groups.update({"shoulders", "delts"})
        return groups

    def _infer_groups_from_exercise_name(self, name: str) -> set[str]:
        lowered = name.lower()
        groups: set[str] = set()
        if any(token in lowered for token in {"squat", "deadlift", "rdl", "sldl", "lunge", "leg", "calf"}):
            groups.update({"legs", "quads", "hamstrings", "glutes", "calves"})
        if any(token in lowered for token in {"bench", "chest", "fly"}):
            groups.add("chest")
        if any(token in lowered for token in {"row", "pulldown", "pull", "chin", "lat"}):
            groups.update({"back", "lats"})
        if any(token in lowered for token in {"curl"}):
            groups.add("biceps")
        if any(token in lowered for token in {"triceps", "pushdown", "extension"}):
            groups.add("triceps")
        if any(token in lowered for token in {"press", "raise", "ohp", "shoulder"}):
            groups.update({"shoulders", "delts"})
        return groups

    async def _day_activity_warning(self, exercises: list[dict[str, Any]]) -> Optional[str]:
        activities = await self.db.get_recent_activities(hours=72)
        if not activities:
            return None

        day_groups: set[str] = set()
        for ex in exercises:
            groups = self._split_groups(str(ex.get("muscle_groups") or ""))
            if not groups:
                groups = self._infer_groups_from_exercise_name(str(ex.get("name") or ""))
            day_groups.update(groups)
        if not day_groups:
            return None

        high_hit: Optional[dict[str, Any]] = None
        moderate_hit: Optional[dict[str, Any]] = None

        for activity in activities:
            activity_groups = self._split_groups(str(activity.get("muscle_groups") or ""))
            if not activity_groups:
                activity_groups = self._infer_groups_from_exercise_name(str(activity.get("description") or ""))
            if not activity_groups:
                continue
            if day_groups.isdisjoint(activity_groups):
                continue
            intensity = str(activity.get("intensity") or "moderate").lower()
            if intensity == "high" and high_hit is None:
                high_hit = activity
            elif intensity == "moderate" and moderate_hit is None:
                moderate_hit = activity

        if high_hit:
            detail = str(high_hit.get("description") or high_hit.get("activity_type") or "recent activity")
            return f"⚠️ You logged a hard session recently ({detail}) affecting today's muscles. Consider going lighter today."
        if moderate_hit:
            detail = str(moderate_hit.get("description") or moderate_hit.get("activity_type") or "recent activity")
            return f"Recovery note: recent moderate activity overlaps with today's muscles ({detail})."
        return None

    async def _activity_adjustment_for_exercise(self, exercise: dict[str, Any]) -> tuple[float, Optional[str]]:
        exercise_groups = self._split_groups(str(exercise.get("muscle_groups") or ""))
        if not exercise_groups:
            exercise_groups = self._infer_groups_from_exercise_name(str(exercise.get("name") or ""))
        if not exercise_groups:
            return 1.0, None

        activities = await self.db.get_recent_activities(hours=72)
        if not activities:
            return 1.0, None

        high_overlap = False
        moderate_overlap = False
        high_descriptions: list[str] = []
        moderate_descriptions: list[str] = []

        for activity in activities:
            groups = self._split_groups(str(activity.get("muscle_groups") or ""))
            if not groups:
                groups = self._infer_groups_from_exercise_name(str(activity.get("description") or ""))
            if not groups:
                continue
            if exercise_groups.isdisjoint(groups):
                continue
            intensity = str(activity.get("intensity") or "moderate").lower()
            description = str(activity.get("description") or activity.get("activity_type") or "recent activity")
            if intensity == "high":
                high_overlap = True
                high_descriptions.append(description)
            elif intensity == "moderate":
                moderate_overlap = True
                moderate_descriptions.append(description)

        if high_overlap:
            detail = high_descriptions[0] if high_descriptions else "recent high-intensity activity"
            return (
                0.9,
                f"⚠️ Recovery note: recent high-intensity activity overlaps with today's muscles ({detail}). Suggestions reduced ~10%.",
            )
        if moderate_overlap:
            detail = moderate_descriptions[0] if moderate_descriptions else "recent moderate activity"
            return (
                1.0,
                f"Recovery note: recent moderate activity overlaps with today's muscles ({detail}). Keep effort honest.",
            )
        return 1.0, None

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
        await send_discord_text(
            channel,
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
            logger.debug("PR check skipped for %s because weight <= 0", exercise_name)
            return None, None

        key = exercise_name.strip().lower()
        e1rm = epley_1rm(weight, reps)
        existing = await self.db.get_best_pr(exercise_name)
        announce_categories = {"heavy_barbell", "light_barbell", "bodyweight"}

        if not existing:
            logger.debug("PR baseline created for %s: %s %s x %s", exercise_name, weight, unit, reps)
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
            if category in announce_categories:
                return (
                    None,
                    (
                        f"🏁 First {exercise_name} logged! "
                        f"{format_standard_number(weight)} {unit} x {reps} - this is your starting benchmark."
                    ),
                )
            return None, None

        pr_type: Optional[str] = None
        prev_e1rm = float(existing.get("estimated_1rm") or 0.0)
        prev_weight = float(existing.get("weight") or 0.0)
        prev_reps = int(existing.get("reps") or 0)

        if e1rm > prev_e1rm:
            pr_type = "e1rm"
        elif weight > prev_weight and reps >= prev_reps:
            pr_type = "weight"
        elif reps > prev_reps and weight >= prev_weight:
            pr_type = "reps"

        if not pr_type:
            logger.debug(
                "PR check no-hit for %s: new %.2f vs best %.2f",
                exercise_name,
                e1rm,
                prev_e1rm,
            )
            return None, None

        logger.debug(
            "PR hit for %s: type=%s new=%.2f prev=%.2f",
            exercise_name,
            pr_type,
            e1rm,
            prev_e1rm,
        )
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
        short_message = (
            f"🏆 PR! {format_standard_number(weight)}{unit} x {reps} "
            f"(e1RM: {format_standard_number(e1rm)})"
        )
        return payload, short_message

    async def _handle_logged_set(
        self,
        channel: discord.abc.Messageable,
        session: WorkoutSession,
        parsed: dict[str, Any],
    ) -> None:
        target = await self._resolve_target_exercise(session, parsed)
        if not target:
            await send_discord_text(channel, "That does not match the current exercise context.")
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
                await send_discord_text(channel, f"{target['name']} is complete. Moving to the next exercise.")
                session.current_index += 1
                if session.current_index >= len(session.exercises):
                    await self._complete_session(channel, session)
                else:
                    await self._prompt_current_exercise(channel, session)
                return
            await send_discord_text(channel, f"{target['name']} is already complete for today.")
            return

        if ex_id not in session.presented_exercises:
            await self._prompt_current_exercise(channel, session)
            return

        unit = await self._resolve_unit(channel, session, ex_id, parsed)
        if not unit:
            return

        reps = int(parsed["reps"])
        if reps > MAX_REPS:
            await send_discord_text(channel, "That rep count seems unrealistic.")
            return

        category = str(target.get("category") or "cable_machine")
        is_bodyweight = bool(parsed.get("is_bodyweight"))
        if is_bodyweight and category != "bodyweight":
            await send_discord_text(
                channel,
                f"{target['name']} is a {category.replace('_', ' ')} exercise - log a weight like `30 x 10`.",
            )
            return

        weight = float(parsed["weight"])
        max_weight = MAX_WEIGHT_BY_UNIT.get(str(unit), 1500.0)
        if abs(weight) > max_weight:
            await send_discord_text(channel, "That weight seems unrealistic. Please double-check.")
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
            weight=weight,
            reps=reps,
            unit=str(unit),
            rir=parsed.get("rir"),
            notes=note,
        )
        session.set_counts[ex_id] = set_number

        pr_payload, short_pr = await self._check_and_record_pr(
            session,
            str(target["name"]),
            weight,
            reps,
            str(unit),
            log_id,
            category,
        )
        if pr_payload:
            session.pr_events.append(pr_payload)

        if parsed.get("is_bodyweight"):
            display_load = note or "bodyweight"
            set_message = f"✅ Set {set_number}: {target['name']} — {display_load} x {reps}"
        else:
            set_message = format_set_log(
                exercise_name=str(target["name"]),
                weight=weight,
                reps=reps,
                unit=str(unit),
                set_number=set_number,
            )
        if short_pr:
            set_message = f"{set_message} | {short_pr}"
        await send_discord_text(channel, set_message)

        await self._apply_fatigue_adjustment_if_needed(channel, trailing_text)

        cue = parse_cue(parsed.get("raw", ""))
        if cue:
            await self.db.save_cue(str(target["name"]), cue)
            await send_discord_text(channel, f"💡 Saved cue: {cue}")

        e1rm_value = epley_1rm(weight, reps) if weight > 0 else 0.0
        session.logged_sets.append(
            {
                "exercise_name": str(target["name"]),
                "weight": weight,
                "reps": reps,
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
                await send_discord_text(channel, f"Now do **{nxt['name']}** (round {superset.round_number}).")
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
            max_tokens=150,
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
                await send_discord_text(ctx.channel, "Resuming paused session.")
                await self._prompt_current_exercise(ctx.channel, existing)
            else:
                await send_discord_text(ctx.channel, "Workout already in progress in this channel.")
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
            await send_discord_text(ctx.channel, "No workout in progress.")
            return
        session.current_index = len(session.exercises)
        session.superset = None
        await self._complete_session(ctx.channel, session)

    @commands.command(name="plates")
    async def plates_command(self, ctx: commands.Context, weight: float, unit: str = "") -> None:
        if not unit.strip():
            state = await self.db.get_user_state()
            unit = str(state.get("default_unit") or "lbs")
        normalized = "kg" if unit.lower().startswith("kg") else "lbs"
        if weight > MAX_WEIGHT_BY_UNIT[normalized]:
            await send_discord_text(ctx.channel, "That weight seems unrealistic. Please double-check.")
            return
        await send_discord_text(ctx.channel, plates_breakdown(weight, normalized))
        await self._maybe_send_settings_tip(ctx.channel)

    @commands.command(name="e1rm")
    async def e1rm_command(self, ctx: commands.Context, *, exercise_name: str) -> None:
        resolved = await self.db.resolve_exercise_name(exercise_name)
        target_name = resolved or exercise_name
        rows = await self.db.get_e1rm_history(target_name, limit=12)
        if not rows:
            await send_discord_text(ctx.channel, f"No history for {exercise_name}.")
            await self._maybe_send_settings_tip(ctx.channel)
            return

        lines = [f"{target_name} estimated 1RM trend:"]
        for row in rows:
            lines.append(
                f"{row['date']}: {format_standard_number(float(row['e1rm']))} {row['unit']} "
                f"({format_standard_number(float(row['weight']))}x{row['reps']})"
            )
        delta = rows[-1]["e1rm"] - rows[0]["e1rm"]
        lines.append(f"Trend: {delta:+.1f} over {len(rows)} entries")
        await send_discord_text(ctx.channel, "\n".join(lines))
        await self._maybe_send_settings_tip(ctx.channel)

    @commands.command(name="volume")
    async def volume_command(self, ctx: commands.Context) -> None:
        weekly = await self.db.get_weekly_volume()
        await send_discord_text(ctx.channel, format_volume_report(weekly))
        await self._maybe_send_settings_tip(ctx.channel)

    @commands.command(name="cue")
    async def cue_command(self, ctx: commands.Context, exercise_name: str, *, cue: str) -> None:
        await self.db.save_cue(exercise_name, cue)
        await send_discord_text(ctx.channel, f'Saved cue for {exercise_name}: "{cue}"')
        await self._maybe_send_settings_tip(ctx.channel)

    @commands.command(name="export")
    async def export_command(self, ctx: commands.Context, *, exercise_name: str = "") -> None:
        rows = await self.db.export_logs(exercise_name=exercise_name.strip() or None)
        if not rows:
            await send_discord_text(ctx.channel, "No logs to export.")
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

        lowered = self._normalize_user_text(content)
        session = self.sessions.get(message.channel.id)

        if not session:
            if not await self._check_weekday_start_channel(message.channel):
                return
            if lowered in READY_TOKENS:
                await self._start_session(message.channel)
                return
            if self._is_question(content):
                await send_discord_text(message.channel, "Type `ready` to start today's workout, or ask broader questions in #ask.")
                return
            await send_discord_text(message.channel, "Type `ready` to start today's workout.")
            return

        lock = self._get_user_lock(message.author.id)
        async with lock:
            session = self.sessions.get(message.channel.id)
            if not session:
                await send_discord_text(message.channel, "Type `ready` to start today's workout.")
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
                    await send_discord_text(message.channel, "Resuming paused session.")
                    await self._prompt_current_exercise(message.channel, session)
                    return
                if self._matches_token(lowered, MOVE_ON_TOKENS) or "move on" in lowered:
                    completed, total, _ = self._session_progress(session)
                    await self._end_session_early(
                        message.channel,
                        session,
                        completed=completed,
                        total=total,
                    )
                    return
                await send_discord_text(
                    message.channel,
                    "Session is paused. Type `ready` to resume or `move on` to end and advance."
                )
                return

            if lowered in {"skip rest", "skip"}:
                await self._cancel_rest(session)
                await send_discord_text(message.channel, "Rest skipped.")
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
                await send_discord_text(message.channel, "No active rest timer to extend.")
                return

            parsed = parse_set_input(content)
            if parsed:
                if session.rest_task and not session.rest_task.done():
                    await send_discord_text(
                        message.channel,
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
                    await send_discord_text(message.channel, f"Saved cue for {current['name']}: {cue}")
                    return

            if self._is_question(content):
                try:
                    reply = await self._answer_workout_question(session, content)
                    await send_discord_text(message.channel, reply)
                except Exception:
                    await send_discord_text(message.channel, "I couldn't reach the coach model right now. Try again in a moment.")
                return

            await send_discord_text(
                message.channel,
                "I didn't catch that. Log sets as `225 x 3` or ask me a question."
            )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(WorkoutCog(bot))
