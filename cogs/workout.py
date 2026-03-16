from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import discord
from discord.ext import commands

from llm.prompts import ASK_SYSTEM_PROMPT
from utils.e1rm import epley_1rm
from utils.discord_messages import send_discord_file, send_discord_text
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
    "smith_machine": 150,
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
SHORTHAND_SET_RE = re.compile(r"^(?P<weight>\d+(?:\.\d+)?)\s+(?P<reps>\d+)(?:\s+(?P<rir>\d+))?$")
SAME_SET_RE = re.compile(r"^same(?:\s+(?P<arg>[+-]?\d+))?$", re.IGNORECASE)
EQUIPMENT_SWITCH_RE = re.compile(
    r"\b(?:switch to|use|do this on|make it|change to)\s+"
    r"(?P<equipment>smith machine|smith|barbell|dumbbell|cable|machine|bodyweight)\b",
    re.IGNORECASE,
)
PR_QUERY_RE = re.compile(r"\b(?:what(?:'s| is)?\s+my\s+pr\s+for|pr\s+for)\s+(?P<exercise>.+)$", re.IGNORECASE)
REORDER_PATTERNS = (
    re.compile(r"^(?:skip to|go to|do)\s+(?P<exercise>.+?)\s+(?:next|now)$", re.IGNORECASE),
    re.compile(r"^(?:skip to|go to|do)\s+(?P<exercise>.+)$", re.IGNORECASE),
    re.compile(r"^(?:i want to do|let'?s do)\s+(?P<exercise>.+?)\s+(?:next|now)$", re.IGNORECASE),
)
REMAINING_QUERY_HINTS = (
    "what exercises do i have left",
    "what do i have left",
    "how many exercises left",
    "what's left",
    "whats left",
)
HISTORY_QUERY_HINTS = {"history", "show history", "what's my history", "whats my history"}


@dataclass(slots=True)
class SupersetState:
    group_id: int
    member_indices: list[int]
    max_rounds: int
    round_number: int = 1
    member_pos: int = 0


@dataclass(slots=True)
class WorkoutSession:
    user_id: str
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
    day_injury_warning: Optional[str] = None

    def current_exercise(self) -> Optional[dict[str, Any]]:
        if self.superset:
            idx = self.superset.member_indices[self.superset.member_pos]
            return self.exercises[idx]
        if self.current_index >= len(self.exercises):
            return None
        return self.exercises[self.current_index]

    def is_complete(self) -> bool:
        return self.current_index >= len(self.exercises) and self.superset is None


@dataclass(slots=True)
class LoggedSetMessageRef:
    workout_log_id: int
    exercise_id: int
    exercise_name: str
    category: str
    equipment_type: str
    weight: float
    reps: int
    unit: str
    note: str
    is_bodyweight: bool


class WorkoutCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.settings = bot.settings
        self.db = bot.db
        self.sessions: dict[str, WorkoutSession] = {}
        self.user_locks: dict[int, asyncio.Lock] = {}
        self.early_end_prompts: dict[tuple[int, int], dict[str, Any]] = {}
        self.early_end_timeout_tasks: dict[tuple[int, int], asyncio.Task] = {}
        self.message_log_map: dict[int, LoggedSetMessageRef] = {}

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

    def _session_key(self, user_id: int | str) -> str:
        return str(user_id)

    async def _current_local_datetime(self, user_id: str) -> tuple[datetime, str]:
        tz_name = await self.db.get_user_timezone(user_id=user_id)
        try:
            tzinfo = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            tz_name = "UTC"
            tzinfo = timezone.utc
            await self.db.set_user_timezone(tz_name, user_id=user_id)
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
            await send_discord_text(channel, f"Tip: use {ref} for utility commands like this.")

    async def _check_weekday_start_channel(self, channel: discord.abc.Messageable, user_id: str) -> bool:
        channel_name = str(getattr(channel, "name", "")).lower()
        if channel_name not in WEEKDAY_CHANNEL_NAMES:
            return True

        now_local, _ = await self._current_local_datetime(user_id)
        weekday_idx = now_local.weekday()
        expected_channel = WEEKDAY_CHANNEL_NAMES[weekday_idx]
        if channel_name == expected_channel:
            return True

        expected_ref = self._channel_ref_for_name(channel, expected_channel)
        weekday_label = WEEKDAY_LABELS[weekday_idx]
        await send_discord_text(channel, f"It's {weekday_label} - head over to {expected_ref} to start your session.")
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

    def _format_exercise_scheme(self, exercise: dict[str, Any]) -> str:
        low = exercise.get("rep_range_low")
        high = exercise.get("rep_range_high")
        sets = int(exercise.get("sets") or 1)
        if low is None or high is None:
            return f"{sets}xAMRAP"
        if int(low) == int(high):
            return f"{sets}x{int(low)}"
        return f"{sets}x{int(low)}-{int(high)}"

    def _exercise_display_name(self, exercise: dict[str, Any]) -> str:
        return str(exercise.get("name") or "")

    def _exercise_base_name(self, exercise: dict[str, Any]) -> str:
        return str(exercise.get("base_name") or exercise.get("name") or "")

    def _find_last_logged_set_for_current_exercise(self, session: WorkoutSession) -> Optional[dict[str, Any]]:
        current = session.current_exercise()
        if not current:
            return None
        current_id = int(current["id"])
        for row in reversed(session.logged_sets):
            if int(row.get("exercise_id") or -1) == current_id:
                return row
        return None

    def _parse_same_command(self, session: WorkoutSession, text: str) -> tuple[Optional[dict[str, Any]], Optional[str]]:
        match = SAME_SET_RE.fullmatch(text.strip())
        if not match:
            return None, None
        previous = self._find_last_logged_set_for_current_exercise(session)
        if previous is None:
            return None, "No previous set to repeat. Log your first set as `weight x reps`."

        arg = str(match.group("arg") or "").strip()
        weight = float(previous["weight"])
        reps = int(previous["reps"])
        if arg:
            if arg.startswith(("+", "-")):
                if previous.get("is_bodyweight"):
                    return None, "Weight adjustments with `same +/-n` only work for weighted exercises."
                weight += float(arg)
            else:
                reps = int(arg)

        return (
            {
                "exercise": None,
                "weight": weight,
                "reps": reps,
                "unit": previous.get("unit"),
                "unit_explicit": False,
                "rir": None,
                "is_bodyweight": bool(previous.get("is_bodyweight")),
                "note": str(previous.get("note") or ""),
                "trailing_text": "",
                "same_as_last": True,
                "raw": text,
            },
            None,
        )

    def _parse_shorthand_set(self, session: WorkoutSession, text: str) -> Optional[dict[str, Any]]:
        current = session.current_exercise()
        if current is None:
            return None
        exercise_id = int(current["id"])
        if session.set_counts.get(exercise_id, 0) < 1:
            return None
        match = SHORTHAND_SET_RE.fullmatch(text.strip())
        if not match:
            return None
        return {
            "exercise": None,
            "weight": float(match.group("weight")),
            "reps": int(match.group("reps")),
            "unit": None,
            "unit_explicit": False,
            "rir": int(match.group("rir")) if match.group("rir") else None,
            "is_bodyweight": False,
            "note": "",
            "trailing_text": "",
            "raw": text,
        }

    def _is_remaining_query(self, text: str) -> bool:
        lowered = self._normalize_user_text(text)
        if lowered in REMAINING_QUERY_HINTS:
            return True
        return "exercise" in lowered and "left" in lowered

    def _is_history_query(self, text: str) -> bool:
        lowered = self._normalize_user_text(text)
        if lowered in HISTORY_QUERY_HINTS:
            return True
        return "history" in lowered

    def _extract_pr_query(self, text: str) -> Optional[str]:
        match = PR_QUERY_RE.search(text.strip())
        if match:
            return str(match.group("exercise") or "").strip(" ?.")
        return None

    def _extract_reorder_target(self, text: str) -> Optional[str]:
        lowered = self._normalize_user_text(text)
        for pattern in REORDER_PATTERNS:
            match = pattern.match(lowered)
            if match:
                return str(match.group("exercise") or "").strip()
        return None

    def _find_remaining_exercise_match(self, session: WorkoutSession, query: str) -> Optional[int]:
        current = session.current_index
        normalized_query = self._normalize_user_text(query)
        best_idx: Optional[int] = None
        best_score = 0.0
        for idx in range(current, len(session.exercises)):
            candidate = session.exercises[idx]
            name = self._normalize_user_text(str(candidate.get("name") or ""))
            if not name:
                continue
            if normalized_query == name or normalized_query in name or name in normalized_query:
                return idx
            score = SequenceMatcher(a=normalized_query, b=name).ratio()
            if score > best_score:
                best_score = score
                best_idx = idx
        if best_idx is not None and best_score >= 0.5:
            return best_idx
        return None

    def _current_set_prompt(self, session: WorkoutSession) -> str:
        current = session.current_exercise()
        if not current:
            return "Workout complete."
        done_sets = int(session.set_counts.get(int(current["id"]), 0))
        total_sets = int(current.get("sets") or 1)
        return f"Ready for set {done_sets + 1}/{total_sets} of {current['name']} when you are."

    def _category_from_equipment_switch(self, exercise: dict[str, Any], equipment_type: str) -> str:
        normalized = str(equipment_type or "").strip().lower()
        if normalized == "smith machine":
            return "smith_machine"
        if normalized == "dumbbell":
            return "dumbbell"
        if normalized == "bodyweight":
            return "bodyweight"
        if normalized in {"cable", "machine"}:
            return "cable_machine"
        base_category = str(exercise.get("base_category") or exercise.get("category") or "").strip().lower()
        if base_category in {"heavy_barbell", "light_barbell"}:
            return base_category
        lowered_name = self._exercise_base_name(exercise).lower()
        heavy_tokens = ("squat", "bench", "deadlift", "overhead press", "ohp", "barbell row")
        return "heavy_barbell" if any(token in lowered_name for token in heavy_tokens) else "light_barbell"

    def _strip_equipment_prefix(self, exercise_name: str) -> str:
        patterns = [
            r"^smith machine\s+",
            r"^dumbbell\s+",
            r"^barbell\s+",
            r"^cable\s+",
            r"^machine\s+",
        ]
        stem = exercise_name.strip()
        for pattern in patterns:
            stem = re.sub(pattern, "", stem, flags=re.IGNORECASE)
        return stem or exercise_name.strip()

    def _build_equipment_variant_name(self, exercise: dict[str, Any], equipment_type: str) -> str:
        base_name = self._exercise_base_name(exercise)
        stem = self._strip_equipment_prefix(base_name)
        normalized = str(equipment_type or "").strip().lower()
        if normalized == "barbell":
            if stem != base_name:
                return stem
            return base_name
        if normalized == "smith machine":
            return f"Smith Machine {stem}"
        if normalized == "dumbbell":
            return f"Dumbbell {stem}"
        if normalized == "cable":
            return f"Cable {stem}"
        if normalized == "machine":
            return f"Machine {stem}"
        if normalized == "bodyweight":
            return stem if any(token in stem.lower() for token in ("pull-up", "pull up", "chin-up", "chin up", "dip", "push-up", "push up")) else f"Bodyweight {stem}"
        return base_name

    async def _preview_next_exercise(self, session: WorkoutSession) -> Optional[str]:
        next_ex = session.current_exercise()
        if next_ex is None:
            return None
        last_logs = await self.db.get_last_logs_for_named_exercise(str(next_ex["name"]), limit=1, user_id=session.user_id)
        last_line = "Last: no history"
        if last_logs:
            last = last_logs[0]
            last_line = (
                f"Last: {format_standard_number(float(last['weight']))} {last['unit']} x {last['reps']}"
                if str(last.get("logged_category") or "") != "bodyweight"
                else f"Last: {(last.get('notes') or 'bodyweight')} x {last['reps']}"
            )
        return f"⏭️ **Up next:** {next_ex['name']} — {self._format_exercise_scheme(next_ex)} ({last_line})"

    def _is_early_end_intent(self, text: str) -> bool:
        lowered = self._normalize_user_text(text)
        if lowered in EARLY_END_INTENTS:
            return True
        return any(intent in lowered for intent in EARLY_END_INTENTS)

    def _parse_bodyweight_reps_only(self, text: str) -> Optional[dict[str, Any]]:
        cleaned = text.strip()
        if not cleaned:
            return None
        lowered = cleaned.lower()
        if "x" in lowered or "×" in lowered:
            return None
        match = re.fullmatch(
            r"(?P<reps>\d+)(?:\s*(?:@|rir\s*)(?P<rir>\d+))?(?:\s+(?P<tail>.*))?",
            lowered,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        reps = int(match.group("reps"))
        rir = int(match.group("rir")) if match.group("rir") else None
        tail = str(match.group("tail") or "").strip()
        return {"reps": reps, "rir": rir, "trailing_text": tail}

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
        user_id: str,
        session: WorkoutSession,
    ) -> None:
        completed, total, progress = self._session_progress(session)
        prompt_key = (channel.id, int(user_id))
        self.early_end_prompts[prompt_key] = {
            "step": "confirm",
            "completed": completed,
            "total": total,
            "progress": progress,
            "expires_at": datetime.now(timezone.utc) + timedelta(seconds=60),
        }
        self._schedule_early_end_timeout(prompt_key, channel, session)
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
        self.sessions.pop(session.user_id, None)
        self._clear_early_end_prompt((session.channel_id, int(session.user_id)))

        streak_line = ""
        if session.logged_sets:
            streak = await self.db.mark_workout_completed(date.today(), user_id=session.user_id)
            streak_line = (
                f"\n🔥 Streak: {streak['current_streak']} sessions "
                f"(Longest: {streak['longest_streak']})."
            )

        next_index = await self.db.advance_day_index(user_id=session.user_id)
        next_day = await self.db.get_day_for_index(next_index, user_id=session.user_id)
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

    def _clear_early_end_prompt(self, key: tuple[int, int]) -> None:
        self.early_end_prompts.pop(key, None)
        task = self.early_end_timeout_tasks.pop(key, None)
        if task and not task.done():
            task.cancel()

    def _clear_early_end_prompts_for_channel(self, channel_id: int) -> None:
        keys = [key for key in self.early_end_prompts if key[0] == channel_id]
        for key in keys:
            self._clear_early_end_prompt(key)

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
                current_session = self.sessions.get(session.user_id)
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
        user_id: str,
        session: WorkoutSession,
        text: str,
    ) -> bool:
        key = (channel.id, int(user_id))
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
                now_local, _ = await self._current_local_datetime(session.user_id)
                session.paused = True
                session.paused_local_date = now_local.date().isoformat()
                self._clear_early_end_prompt(key)
                await send_discord_text(
                    channel,
                    f"Session paused at {completed}/{total} exercises. "
                    "Type `ready` in this channel later today to continue."
                )
                return True

            if self._matches_token(lowered, MOVE_ON_TOKENS) or "move on" in lowered:
                self._clear_early_end_prompt(key)
                await self._end_session_early(
                    channel,
                    session,
                    completed=completed,
                    total=total,
                )
                return True

            if self._matches_token(lowered, NO_TOKENS):
                self._clear_early_end_prompt(key)
                await send_discord_text(channel, "Continuing current session.")
                await self._prompt_current_exercise(channel, session)
                return True

            await send_discord_text(channel, "Reply `resume` or `move on`.")
            return True

        self._clear_early_end_prompt(key)
        return False

    async def _start_session(
        self,
        channel: discord.abc.Messageable,
        *,
        user_id: str,
    ) -> Optional[WorkoutSession]:
        program = await self.db.get_active_program(user_id)
        if not program:
            await send_discord_text(channel, "No active program. Paste one in #programme first.")
            return None

        day_index = await self.db.get_current_day_index(user_id)
        day = await self.db.get_day_for_index(day_index, user_id=user_id)
        if not day:
            await send_discord_text(channel, "Active program has no days configured.")
            return None

        exercises = await self.db.get_exercises_for_day(int(day["id"]))
        if not exercises:
            await send_discord_text(channel, f"{day['name']} has no exercises.")
            return None
        for exercise in exercises:
            exercise["base_name"] = str(exercise.get("name") or "")
            exercise["base_category"] = str(exercise.get("category") or "")
            exercise["base_equipment_type"] = str(exercise.get("equipment_type") or "")
            exercise["session_override"] = False

        session = WorkoutSession(
            user_id=user_id,
            channel_id=getattr(channel, "id", 0),
            day_index=day_index,
            day=day,
            exercises=exercises,
            started_at=datetime.now(),
            set_counts={int(ex["id"]): 0 for ex in exercises},
            total_exercises=len(exercises),
        )
        self.sessions[session.user_id] = session
        self._clear_early_end_prompt((session.channel_id, int(session.user_id)))

        total_days = len(await self.db.get_program_days(program["id"]))
        await send_discord_text(
            channel,
            f"Starting **{day['name']}** (Day {day_index + 1} of {total_days})."
        )

        session.day_activity_warning = await self._day_activity_warning(exercises, user_id=user_id)
        if session.day_activity_warning:
            await send_discord_text(channel, session.day_activity_warning)
        session.day_injury_warning = await self._day_injury_warning(exercises, user_id=user_id)
        if session.day_injury_warning:
            await send_discord_text(channel, session.day_injury_warning)

        state = await self.db.get_user_state(user_id)
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

        if bool(exercise.get("session_override")):
            logs = await self.db.get_last_logs_for_named_exercise(str(exercise["name"]), limit=3, user_id=session.user_id)
        else:
            logs = await self.db.get_last_logs_for_exercise(int(exercise["id"]), limit=3, user_id=session.user_id)
        activity_multiplier, activity_note = await self._activity_adjustment_for_exercise(exercise, user_id=session.user_id)
        injury_note = await self._injury_warning_for_exercise(exercise, user_id=session.user_id)
        state = await self.db.get_user_state(session.user_id)
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
        if not logs and bool(exercise.get("session_override")):
            suggestion = f"{suggestion} You don't have history for {exercise['name']} yet. Start conservative."

        last_lines: list[str] = []
        if logs:
            compact = ", ".join(
                f"{format_standard_number(float(l['weight']))}{l['unit']}x{l['reps']}" for l in logs
            )
            last_lines.append(f"Last: {compact}")
        else:
            if bool(exercise.get("session_override")):
                last_lines.append(f"Last: no history for {exercise['name']} yet")
            else:
                last_lines.append("Last: no history")

        warmup = None
        category = str(exercise.get("category") or "cable_machine")
        if category in {"heavy_barbell", "light_barbell"} and logs:
            basis_weight = float(logs[0]["weight"]) * final_multiplier
            basis_unit = str(logs[0].get("unit") or "lbs")
            if basis_weight > 0:
                warmup = generate_warmup(basis_weight, category, basis_unit)

        cue = await self.db.get_latest_cue(str(exercise["name"]), user_id=session.user_id)

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
        if injury_note:
            lines.append(injury_note)
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
        rest_message: Optional[str] = None,
    ) -> None:
        if session.rest_task and not session.rest_task.done():
            session.rest_task.cancel()

        session.rest_seconds = seconds
        await send_discord_text(channel, rest_message or f"⏱️ Rest {seconds // 60}:{seconds % 60:02d}.")

        async def _timer() -> None:
            try:
                await asyncio.sleep(seconds)
                if self.sessions.get(session.user_id) is not session:
                    return
                ready_line = f"Ready. {ready_message}".strip()
                await send_discord_text(channel, ready_line)
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
        streak = await self.db.mark_workout_completed(date.today(), user_id=session.user_id)
        await self.db.advance_day_index(user_id=session.user_id)
        self.sessions.pop(session.user_id, None)
        self._clear_early_end_prompt((session.channel_id, int(session.user_id)))

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

        last_log = await self.db.get_last_log_for_exercise(exercise_id, user_id=session.user_id)
        if last_log and last_log.get("unit"):
            unit = str(last_log["unit"])
            session.exercise_units[exercise_id] = unit
            return unit

        user_state = await self.db.get_user_state(session.user_id)
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

    async def _day_activity_warning(self, exercises: list[dict[str, Any]], *, user_id: str) -> Optional[str]:
        activities = await self.db.get_recent_activities(hours=72, user_id=user_id)
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

    async def _day_injury_warning(self, exercises: list[dict[str, Any]], *, user_id: str) -> Optional[str]:
        injuries = await self.db.get_active_injuries(user_id=user_id)
        if not injuries:
            return None
        day_groups: set[str] = set()
        for ex in exercises:
            groups = self._split_groups(str(ex.get("muscle_groups") or ""))
            if not groups:
                groups = self._infer_groups_from_exercise_name(str(ex.get("name") or ""))
            day_groups.update(groups)
        if not day_groups:
            return None

        for injury in injuries:
            injury_groups = self._split_groups(str(injury.get("muscle_groups") or ""))
            if not injury_groups:
                continue
            if day_groups.isdisjoint(injury_groups):
                continue
            desc = str(injury.get("description") or "recent injury report")
            return (
                f"🚫 Injury flag: {desc}. Exercises hitting {', '.join(sorted(injury_groups))} "
                "should be skipped or substituted today."
            )
        return None

    async def _activity_adjustment_for_exercise(
        self,
        exercise: dict[str, Any],
        *,
        user_id: str,
    ) -> tuple[float, Optional[str]]:
        exercise_groups = self._split_groups(str(exercise.get("muscle_groups") or ""))
        if not exercise_groups:
            exercise_groups = self._infer_groups_from_exercise_name(str(exercise.get("name") or ""))
        if not exercise_groups:
            return 1.0, None

        activities = await self.db.get_recent_activities(hours=72, user_id=user_id)
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

    async def _injury_warning_for_exercise(self, exercise: dict[str, Any], *, user_id: str) -> Optional[str]:
        injuries = await self.db.get_active_injuries(user_id=user_id)
        if not injuries:
            return None
        ex_groups = self._split_groups(str(exercise.get("muscle_groups") or ""))
        if not ex_groups:
            ex_groups = self._infer_groups_from_exercise_name(str(exercise.get("name") or ""))
        if not ex_groups:
            return None
        for injury in injuries:
            injury_groups = self._split_groups(str(injury.get("muscle_groups") or ""))
            if not injury_groups:
                continue
            if ex_groups.isdisjoint(injury_groups):
                continue
            desc = str(injury.get("description") or "active injury")
            return (
                f"🚫 Injury flag overlaps this exercise ({', '.join(sorted(injury_groups))}): {desc}. "
                "Skip or substitute this movement."
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
        user_id: str,
        trailing_text: str,
    ) -> None:
        if not self._contains_fatigue_cue(trailing_text):
            return
        state = await self.db.get_user_state(user_id)
        readiness = int(state.get("readiness") or 7)
        next_readiness = max(1, readiness - 1)
        if next_readiness == readiness:
            return
        await self.db.update_user_state(user_id, readiness=next_readiness)
        await send_discord_text(
            channel,
            f"Noted fatigue cue. Readiness adjusted to {next_readiness}/10 for upcoming suggestions."
        )

    def _exercise_type_label(self, exercise: dict[str, Any]) -> str:
        equipment_type = str(exercise.get("equipment_type") or "").strip().lower()
        if equipment_type:
            return equipment_type.replace("_", " ")
        return str(exercise.get("category") or "cable_machine").replace("_", " ")

    def _is_pr_announce_exercise(self, exercise_name: str, category: str, equipment_type: str) -> bool:
        lowered = exercise_name.lower()
        normalized_category = (category or "").strip().lower()
        normalized_type = (equipment_type or "").strip().lower()
        if normalized_category != "heavy_barbell":
            return False
        if normalized_type != "barbell":
            return False
        if "smith" in lowered:
            return False
        squat_patterns = ("squat", "pause squat", "front squat", "safety bar squat")
        bench_patterns = ("bench press", "pause bench", "close grip bench", "close-grip bench", "larsen press")
        deadlift_patterns = ("deadlift", "sumo deadlift", "deficit deadlift", "pause deadlift")
        if any(pattern in lowered for pattern in bench_patterns):
            return True
        if any(pattern in lowered for pattern in deadlift_patterns):
            return True
        if any(pattern in lowered for pattern in squat_patterns):
            excluded = {"split squat", "goblet squat", "hack squat", "belt squat", "cyclist squat"}
            return not any(token in lowered for token in excluded)
        return False

    async def _check_and_record_pr(
        self,
        session: WorkoutSession,
        exercise_name: str,
        weight: float,
        reps: int,
        unit: str,
        workout_log_id: int,
        category: str,
        equipment_type: str,
        user_id: str,
        performer_name: Optional[str] = None,
        performer_user_id: Optional[str] = None,
    ) -> tuple[Optional[dict[str, Any]], Optional[str]]:
        if weight <= 0:
            logger.debug("PR check skipped for %s because weight <= 0", exercise_name)
            return None, None

        key = exercise_name.strip().lower()
        e1rm = epley_1rm(weight, reps)
        existing = await self.db.get_best_pr(exercise_name, user_id=user_id)
        existing_e1rm = float(existing.get("estimated_1rm") or 0.0) if existing else 0.0
        is_new_pr_candidate = bool(existing is None or e1rm > existing_e1rm)
        prs_channel = self.bot.get_channel(self.settings.prs_channel_id) if self.settings.prs_channel_id else None
        logger.info(
            "PR check: exercise=%s, new_e1rm=%.2f, existing_best=%.2f, is_new_pr=%s",
            exercise_name,
            e1rm,
            existing_e1rm,
            is_new_pr_candidate,
        )
        logger.info(
            "PR CHECK: exercise=%s, weight=%s, reps=%s, new_e1rm=%.2f",
            exercise_name,
            weight,
            reps,
            e1rm,
        )
        logger.info("PR CHECK: existing best e1rm=%.2f", existing_e1rm)
        logger.info("PR CHECK: prs_channel=%s", getattr(prs_channel, "id", None))
        logger.info(
            "PR check for %s: new_e1rm=%.2f best=%s",
            exercise_name,
            e1rm,
            f"{existing_e1rm:.2f}" if existing else "None",
        )
        if existing is None:
            logger.info("PR CHECK: no existing personal_records row found for %s", exercise_name)
        else:
            logger.info("PR CHECK: existing personal_records row found for %s", exercise_name)

        should_announce = self._is_pr_announce_exercise(exercise_name, category, equipment_type)

        if not existing:
            logger.info("PR baseline created for %s: %s %s x %s", exercise_name, weight, unit, reps)
            pr_id = await self.db.create_pr(
                exercise_name,
                user_id=user_id,
                weight=weight,
                reps=reps,
                unit=unit,
                estimated_1rm=e1rm,
                workout_date=date.today(),
                workout_log_id=workout_log_id,
            )
            logger.info("PR CHECK: personal_records insert success id=%s", pr_id)
            session.baseline_exercises.add(key)
            if should_announce:
                logger.info("PR CHECK: is_pr=True, pr_type=first")
                payload = {
                    "exercise_name": exercise_name,
                    "weight": weight,
                    "reps": reps,
                    "unit": unit,
                    "e1rm": e1rm,
                    "previous": None,
                    "is_first_benchmark": True,
                    "performer_name": performer_name or "",
                    "performer_user_id": str(performer_user_id or user_id or "").strip(),
                }
                self.bot.dispatch("pr_hit", payload)
                return (
                    payload,
                    (
                        f"📊 First {exercise_name} logged! "
                        f"{format_standard_number(weight)} {unit} x {reps} "
                        f"(e1RM: {format_standard_number(e1rm)}) - this is your starting benchmark."
                    ),
                )
            logger.info("PR CHECK: is_pr=False, pr_type=first_non_announced")
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
            logger.info("PR CHECK: is_pr=False, pr_type=None")
            logger.debug(
                "PR check no-hit for %s: new %.2f vs best %.2f",
                exercise_name,
                e1rm,
                prev_e1rm,
            )
            return None, None

        logger.info("PR CHECK: is_pr=True, pr_type=%s", pr_type)

        logger.info(
            "PR hit for %s: type=%s new=%.2f prev=%.2f",
            exercise_name,
            pr_type,
            e1rm,
            prev_e1rm,
        )
        pr_id = await self.db.create_pr(
            exercise_name,
            user_id=user_id,
            weight=weight,
            reps=reps,
            unit=unit,
            estimated_1rm=e1rm,
            workout_date=date.today(),
            workout_log_id=workout_log_id,
        )
        logger.info("PR CHECK: personal_records insert success id=%s", pr_id)

        if not should_announce:
            return None, None

        payload = {
            "exercise_name": exercise_name,
            "weight": weight,
            "reps": reps,
            "unit": unit,
            "e1rm": e1rm,
            "previous": existing,
            "is_first_benchmark": False,
            "performer_name": performer_name or "",
            "performer_user_id": str(performer_user_id or user_id or "").strip(),
        }
        self.bot.dispatch("pr_hit", payload)
        prev_unit = str(existing.get("unit") or unit)
        prev_date = str(existing.get("date") or "previous session")
        short_message = (
            f"🏆 NEW PR! {exercise_name} {format_standard_number(weight)} {unit} x {reps} "
            f"(e1RM: {format_standard_number(e1rm)}) - Previous: "
            f"{format_standard_number(prev_weight)} {prev_unit} x {prev_reps} "
            f"(e1RM: {format_standard_number(prev_e1rm)}) on {prev_date}"
        )
        return payload, short_message

    async def _recheck_pr_after_edit(
        self,
        *,
        exercise_name: str,
        weight: float,
        reps: int,
        unit: str,
        workout_log_id: int,
        category: str,
        equipment_type: str,
        user_id: str,
        performer_name: Optional[str] = None,
        performer_user_id: Optional[str] = None,
    ) -> Optional[str]:
        if weight <= 0:
            return None
        should_announce = self._is_pr_announce_exercise(exercise_name, category, equipment_type)
        e1rm = epley_1rm(weight, reps)
        existing = await self.db.get_best_pr_excluding_log(
            exercise_name,
            user_id=user_id,
            excluded_workout_log_id=workout_log_id,
        )
        logger.info(
            "PR recheck(edit) for %s: new_e1rm=%.2f best_excluding=%s",
            exercise_name,
            e1rm,
            f"{float(existing.get('estimated_1rm') or 0.0):.2f}" if existing else "None",
        )
        logger.info(
            "PR check: exercise=%s, new_e1rm=%.2f, existing_best=%s, is_new_pr=%s",
            exercise_name,
            e1rm,
            f"{float(existing.get('estimated_1rm') or 0.0):.2f}" if existing else "None",
            bool(existing is None or e1rm > float(existing.get("estimated_1rm") or 0.0)),
        )

        if not existing:
            pr_id = await self.db.create_pr(
                exercise_name,
                user_id=user_id,
                weight=weight,
                reps=reps,
                unit=unit,
                estimated_1rm=e1rm,
                workout_date=date.today(),
                workout_log_id=workout_log_id,
            )
            logger.info("PR CHECK: personal_records insert success id=%s", pr_id)
            if should_announce:
                payload = {
                    "exercise_name": exercise_name,
                    "weight": weight,
                    "reps": reps,
                    "unit": unit,
                    "e1rm": e1rm,
                    "previous": None,
                    "is_first_benchmark": True,
                    "performer_name": performer_name or "",
                    "performer_user_id": str(performer_user_id or user_id or "").strip(),
                }
                self.bot.dispatch("pr_hit", payload)
                return (
                    f"📊 First {exercise_name} logged! "
                    f"{format_standard_number(weight)} {unit} x {reps} "
                    f"(e1RM: {format_standard_number(e1rm)}) - this is your starting benchmark."
                )
            return None

        prev_e1rm = float(existing.get("estimated_1rm") or 0.0)
        prev_weight = float(existing.get("weight") or 0.0)
        prev_reps = int(existing.get("reps") or 0)

        is_pr = e1rm > prev_e1rm or (weight > prev_weight and reps >= prev_reps) or (reps > prev_reps and weight >= prev_weight)
        if not is_pr:
            return None

        pr_id = await self.db.create_pr(
            exercise_name,
            user_id=user_id,
            weight=weight,
            reps=reps,
            unit=unit,
            estimated_1rm=e1rm,
            workout_date=date.today(),
            workout_log_id=workout_log_id,
        )
        logger.info("PR CHECK: personal_records insert success id=%s", pr_id)
        if should_announce:
            payload = {
                "exercise_name": exercise_name,
                "weight": weight,
                "reps": reps,
                "unit": unit,
                "e1rm": e1rm,
                "previous": existing,
                "is_first_benchmark": False,
                "performer_name": performer_name or "",
                "performer_user_id": str(performer_user_id or user_id or "").strip(),
            }
            self.bot.dispatch("pr_hit", payload)
            return (
                f"🏆 NEW PR! {exercise_name} {format_standard_number(weight)} {unit} x {reps} "
                f"(e1RM: {format_standard_number(e1rm)}) - Previous: "
                f"{format_standard_number(prev_weight)} {existing.get('unit') or unit} x {prev_reps} "
                f"(e1RM: {format_standard_number(prev_e1rm)}) on {existing.get('date') or 'previous session'}"
            )
        return None

    async def _handle_logged_set(
        self,
        channel: discord.abc.Messageable,
        session: WorkoutSession,
        parsed: dict[str, Any],
        *,
        message_id: Optional[int] = None,
        performer_name: Optional[str] = None,
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
        equipment_type = str(target.get("equipment_type") or "").strip() or category.replace("_", " ")
        is_bodyweight = bool(parsed.get("is_bodyweight"))
        if is_bodyweight and category != "bodyweight":
            await send_discord_text(
                channel,
                f"{target['name']} is a {equipment_type} exercise - log a weight like `30 x 10`.",
            )
            return
        if category == "bodyweight" and not is_bodyweight:
            await send_discord_text(
                channel,
                f"{target['name']} is a bodyweight exercise. Log as `bw x 10` or `bw+25 x 10` for weighted.",
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
            user_id=session.user_id,
            workout_date=date.today(),
            set_number=set_number,
            weight=weight,
            reps=reps,
            unit=str(unit),
            rir=parsed.get("rir"),
            notes=note,
            performed_exercise_name=str(target["name"]),
            performed_category=category,
            performed_equipment_type=equipment_type,
        )
        logger.info("PR CHECK: workout_log insert success id=%s exercise=%s", log_id, str(target["name"]))
        session.set_counts[ex_id] = set_number

        pr_payload, short_pr = await self._check_and_record_pr(
            session,
            str(target["name"]),
            weight,
            reps,
            str(unit),
            log_id,
            category,
            equipment_type,
            user_id=session.user_id,
            performer_name=performer_name,
            performer_user_id=session.user_id,
        )
        if pr_payload:
            session.pr_events.append(pr_payload)

        if parsed.get("is_bodyweight"):
            display_load = note or "bodyweight"
            set_message = f"✅ Set {set_number}: {target['name']} — {display_load} x {reps}"
        elif parsed.get("same_as_last"):
            set_message = (
                f"✅ Set {set_number}/{total_sets}: **{format_standard_number(weight)} {unit} x {reps}** "
                "(same as last set) - logged."
            )
        else:
            set_message = (
                f"✅ Set {set_number}/{total_sets}: **{format_standard_number(weight)} {unit} x {reps}** - logged."
            )
        if short_pr:
            set_message = f"{set_message} | {short_pr}"
        await send_discord_text(channel, set_message)

        await self._apply_fatigue_adjustment_if_needed(channel, session.user_id, trailing_text)

        cue = parse_cue(parsed.get("raw", ""))
        if cue:
            await self.db.save_cue(str(target["name"]), cue, user_id=session.user_id)
            await send_discord_text(channel, f"💡 Saved cue: {cue}")

        e1rm_value = epley_1rm(weight, reps) if weight > 0 else 0.0
        session.logged_sets.append(
            {
                "workout_log_id": log_id,
                "exercise_id": ex_id,
                "exercise_name": str(target["name"]),
                "weight": weight,
                "reps": reps,
                "unit": str(unit),
                "e1rm": e1rm_value,
                "is_bodyweight": bool(parsed.get("is_bodyweight")),
                "note": note,
            }
        )
        if message_id is not None:
            self.message_log_map[int(message_id)] = LoggedSetMessageRef(
                workout_log_id=log_id,
                exercise_id=ex_id,
                exercise_name=str(target["name"]),
                category=category,
                equipment_type=equipment_type,
                weight=weight,
                reps=reps,
                unit=str(unit),
                note=note,
                is_bodyweight=bool(parsed.get("is_bodyweight")),
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
                rest_message=f"⏱️ Rest {rest // 60}:{rest % 60:02d} before set {set_number + 1}/{total_sets} of {target['name']}...",
            )
            return

        session.current_index += 1
        if is_last_exercise_of_day:
            await self._complete_session(channel, session)
            return

        next_ex = session.current_exercise()
        next_name = next_ex["name"] if next_ex else "next exercise"
        preview = await self._preview_next_exercise(session)
        rest_message = f"⏱️ Rest {rest // 60}:{rest % 60:02d} before the next exercise..."
        if preview:
            rest_message = f"{rest_message}\n{preview}"
        await self._start_rest(
            channel,
            session,
            rest,
            f"Up next: {next_name}.",
            prompt_on_ready=True,
            rest_message=rest_message,
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
            preview = await self._preview_next_exercise(session)
            rest_message = f"⏱️ Rest {SUPERSET_ROUND_REST_SECONDS // 60}:{SUPERSET_ROUND_REST_SECONDS % 60:02d} before the next exercise..."
            if preview:
                rest_message = f"{rest_message}\n{preview}"
            await self._start_rest(
                channel,
                session,
                SUPERSET_ROUND_REST_SECONDS,
                f"Up next: {next_name}.",
                prompt_on_ready=True,
                rest_message=rest_message,
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
            rest_message=f"⏱️ Rest {SUPERSET_ROUND_REST_SECONDS // 60}:{SUPERSET_ROUND_REST_SECONDS % 60:02d} before round {superset.round_number}...",
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
        state = await self.db.get_user_state(session.user_id)
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

    async def _answer_remaining_query(self, session: WorkoutSession) -> str:
        remaining: list[str] = []
        seen: set[int] = set()
        for idx in range(session.current_index, len(session.exercises)):
            exercise = session.exercises[idx]
            ex_id = int(exercise["id"])
            if ex_id in seen:
                continue
            seen.add(ex_id)
            done = int(session.set_counts.get(ex_id, 0))
            total = int(exercise.get("sets") or 1)
            if done >= total:
                continue
            label = f"{exercise['name']} ({self._format_exercise_scheme(exercise)})"
            if idx == session.current_index and done > 0:
                label = f"{label}, {done}/{total} sets done"
            remaining.append(label)
        if not remaining:
            return "You have no exercises left. Finish the last set and I’ll wrap the session."
        prefix = "exercise" if len(remaining) == 1 else "exercises"
        return f"You have {len(remaining)} {prefix} left: {', '.join(remaining)}.\n{self._current_set_prompt(session)}"

    async def _answer_history_query(self, session: WorkoutSession) -> str:
        current = session.current_exercise()
        if current is None:
            return "No active exercise right now."
        rows = await self.db.get_recent_sessions_for_named_exercise(str(current["name"]), limit=5, user_id=session.user_id)
        if not rows:
            return f"📊 {current['name']} — no history yet.\n{self._current_set_prompt(session)}"
        lines = [f"📊 {current['name']} — Last {len(rows)} sessions:"]
        for row in rows:
            session_date = datetime.strptime(str(row["date"]), "%Y-%m-%d").strftime("%b %d")
            lines.append(f"{session_date}: {row['sets_summary']}")
        lines.append(self._current_set_prompt(session))
        return "\n".join(lines)

    async def _answer_pr_query(self, session: WorkoutSession, exercise_name: Optional[str]) -> str:
        target = (exercise_name or "").strip()
        if not target:
            current = session.current_exercise()
            if current is None:
                return "No active exercise right now."
            target = str(current["name"])
        resolved = await self.db.resolve_exercise_name(target, user_id=session.user_id)
        best = await self.db.get_best_pr(resolved or target, user_id=session.user_id)
        if not best:
            return f"No PR logged for {target} yet.\n{self._current_set_prompt(session)}"
        return (
            f"🏆 {best['exercise_name']} PR: {format_standard_number(float(best['weight']))} {best['unit']} x {best['reps']} "
            f"(e1RM: {format_standard_number(float(best['estimated_1rm'] or 0.0))}) on {best['date']}.\n"
            f"{self._current_set_prompt(session)}"
        )

    async def _handle_equipment_switch(self, channel: discord.abc.Messageable, session: WorkoutSession, text: str) -> bool:
        match = EQUIPMENT_SWITCH_RE.search(text)
        if not match:
            return False
        current = session.current_exercise()
        if current is None:
            return False
        equipment_raw = str(match.group("equipment") or "").strip().lower()
        equipment_type = "smith machine" if equipment_raw == "smith" else equipment_raw
        variant_name = self._build_equipment_variant_name(current, equipment_type)
        current["name"] = variant_name
        current["category"] = self._category_from_equipment_switch(current, equipment_type)
        current["equipment_type"] = equipment_type
        current["session_override"] = variant_name != self._exercise_base_name(current) or equipment_type != str(current.get("base_equipment_type") or "").strip().lower()

        await self._cancel_rest(session)
        history = await self.db.get_last_logs_for_named_exercise(variant_name, limit=1, user_id=session.user_id)
        if history:
            last = history[0]
            history_line = f"Last: {format_standard_number(float(last['weight']))} {last['unit']} x {last['reps']}."
        else:
            history_line = f"You don't have history for {variant_name} yet. Start conservative."
        base_name = self._exercise_base_name(current)
        base_type = self._exercise_type_label(
            {
                "equipment_type": current.get("base_equipment_type"),
                "category": current.get("base_category"),
            }
        )
        await send_discord_text(
            channel,
            f"🔄 Switching to **{variant_name}** for this session. Your program still has {base_name} ({base_type}) for next time.\n{history_line}",
        )
        await self._prompt_current_exercise(channel, session)
        return True

    async def _handle_equipment_unavailable_message(
        self,
        channel: discord.abc.Messageable,
        session: WorkoutSession,
        text: str,
    ) -> bool:
        lowered = self._normalize_user_text(text)
        if not any(token in lowered for token in ("no barbell", "barbell taken", "station taken", "rack taken", "bench taken")):
            return False
        current = session.current_exercise()
        if current is None:
            return False
        stem = self._strip_equipment_prefix(self._exercise_base_name(current))
        suggestions = [f"Dumbbell {stem}", f"Smith Machine {stem}"]
        await send_discord_text(
            channel,
            f"💡 No barbell available? You could try {suggestions[0]} or {suggestions[1]} instead.",
        )
        return True

    async def _handle_reorder_request(self, channel: discord.abc.Messageable, session: WorkoutSession, text: str) -> bool:
        target_name = self._extract_reorder_target(text)
        if not target_name:
            return False
        if session.superset:
            await send_discord_text(channel, "Finish the current superset round first, then reorder the remaining exercises.")
            return True
        match_idx = self._find_remaining_exercise_match(session, target_name)
        if match_idx is None:
            remaining = ", ".join(ex["name"] for ex in session.exercises[session.current_index:] if session.set_counts.get(int(ex["id"]), 0) < int(ex["sets"]))
            await send_discord_text(channel, f"I don't see that exercise in today's remaining lineup. You have: {remaining}")
            return True
        if match_idx == session.current_index:
            await send_discord_text(channel, f"**{session.exercises[match_idx]['name']}** is already up next.")
            return True

        moved = session.exercises.pop(match_idx)
        session.exercises.insert(session.current_index, moved)
        await self._cancel_rest(session)
        await send_discord_text(channel, f"🔀 Moving **{moved['name']}** up next. You'll do the skipped exercises after.")
        await self._prompt_current_exercise(channel, session)
        return True

    async def _skip_current_exercise(self, channel: discord.abc.Messageable, session: WorkoutSession) -> None:
        current = session.current_exercise()
        if current is None:
            await send_discord_text(channel, "No current exercise to skip.")
            return
        ex_id = int(current["id"])
        done = int(session.set_counts.get(ex_id, 0))
        total = int(current.get("sets") or 1)
        await self._cancel_rest(session)
        if session.superset:
            session.set_counts[ex_id] = total
            await send_discord_text(channel, f"{current['name']}: {done}/{total} sets completed. Moving on.")
            await self._advance_superset(channel, session)
            return
        session.current_index += 1
        await send_discord_text(channel, f"{current['name']}: {done}/{total} sets completed. Moving on.")
        if session.current_index >= len(session.exercises):
            await self._complete_session(channel, session)
            return
        await self._prompt_current_exercise(channel, session)

    async def _resolve_day_target_index(
        self,
        target: str,
        *,
        user_id: str,
    ) -> tuple[Optional[int], list[dict[str, Any]]]:
        active = await self.db.get_active_program(user_id)
        if not active:
            return None, []
        days = await self.db.get_program_days(int(active["id"]))
        if not days:
            return None, []

        cleaned = self._normalize_user_text(target)
        num_match = re.search(r"\b(\d+)\b", cleaned)
        if num_match:
            idx = int(num_match.group(1)) - 1
            if 0 <= idx < len(days):
                return idx, days

        for day in days:
            name = self._normalize_user_text(str(day.get("name") or ""))
            if name and name in cleaned:
                return int(day["day_order"]), days
        return None, days

    async def _skip_to_day(self, channel: discord.abc.Messageable, target: str, *, user_id: str) -> None:
        idx, days = await self._resolve_day_target_index(target, user_id=user_id)
        if idx is None or not days:
            if days:
                choices = ", ".join(f"{d['day_order'] + 1}:{d['name']}" for d in days)
                await send_discord_text(channel, f"Couldn't find that day. Options: {choices}")
            else:
                await send_discord_text(channel, "No active program days available.")
            return

        ended_any = False
        active_session = self.sessions.get(user_id)
        if active_session:
            await self._cancel_rest(active_session)
            self.sessions.pop(user_id, None)
            self._clear_early_end_prompt((active_session.channel_id, int(user_id)))
            ended_any = True
        if ended_any:
            await send_discord_text(channel, "Current session ended before applying day change.")

        await self.db.set_current_day_index(idx, user_id=user_id)
        await send_discord_text(
            channel,
            f"Skipped to Day {idx + 1} - {days[idx]['name']}. Type `ready` to start.",
        )

    @commands.command(name="start")
    async def start_workout_command(self, ctx: commands.Context) -> None:
        if not self._is_workout_channel(ctx.channel):
            return
        user_id = self._session_key(ctx.author.id)
        existing = self.sessions.get(user_id)
        if existing:
            if existing.paused:
                existing.paused = False
                await send_discord_text(ctx.channel, "Resuming paused session.")
                await self._prompt_current_exercise(ctx.channel, existing)
            else:
                await send_discord_text(ctx.channel, "Workout already in progress for you.")
            return
        if not await self._check_weekday_start_channel(ctx.channel, user_id):
            return
        await self._start_session(ctx.channel, user_id=user_id)

    @commands.command(name="done")
    async def finish_workout_command(self, ctx: commands.Context) -> None:
        if not self._is_workout_channel(ctx.channel):
            return
        user_id = self._session_key(ctx.author.id)
        session = self.sessions.get(user_id)
        if not session:
            await send_discord_text(ctx.channel, "No workout in progress.")
            return
        session.current_index = len(session.exercises)
        session.superset = None
        await self._complete_session(ctx.channel, session)

    @commands.command(name="skipday")
    async def skipday_command(self, ctx: commands.Context, *, target: str) -> None:
        await self._skip_to_day(ctx.channel, target, user_id=self._session_key(ctx.author.id))

    @commands.command(name="goto")
    async def goto_command(self, ctx: commands.Context, *, day_name_or_number: str) -> None:
        await self._skip_to_day(ctx.channel, day_name_or_number, user_id=self._session_key(ctx.author.id))

    @commands.command(name="plates")
    async def plates_command(self, ctx: commands.Context, weight: float, unit: str = "") -> None:
        if not unit.strip():
            state = await self.db.get_user_state(self._session_key(ctx.author.id))
            unit = str(state.get("default_unit") or "lbs")
        normalized = "kg" if unit.lower().startswith("kg") else "lbs"
        if weight > MAX_WEIGHT_BY_UNIT[normalized]:
            await send_discord_text(ctx.channel, "That weight seems unrealistic. Please double-check.")
            return
        await send_discord_text(ctx.channel, plates_breakdown(weight, normalized))
        await self._maybe_send_settings_tip(ctx.channel)

    @commands.command(name="e1rm")
    async def e1rm_command(self, ctx: commands.Context, *, exercise_name: str) -> None:
        user_id = self._session_key(ctx.author.id)
        resolved = await self.db.resolve_exercise_name(exercise_name, user_id=user_id)
        target_name = resolved or exercise_name
        rows = await self.db.get_e1rm_history(target_name, limit=12, user_id=user_id)
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
        weekly = await self.db.get_weekly_volume(user_id=self._session_key(ctx.author.id))
        await send_discord_text(ctx.channel, format_volume_report(weekly))
        await self._maybe_send_settings_tip(ctx.channel)

    @commands.command(name="cue")
    async def cue_command(self, ctx: commands.Context, exercise_name: str, *, cue: str) -> None:
        await self.db.save_cue(exercise_name, cue, user_id=self._session_key(ctx.author.id))
        await send_discord_text(ctx.channel, f'Saved cue for {exercise_name}: "{cue}"')
        await self._maybe_send_settings_tip(ctx.channel)

    @commands.command(name="export")
    async def export_command(self, ctx: commands.Context, *, exercise_name: str = "") -> None:
        rows = await self.db.export_logs(
            user_id=self._session_key(ctx.author.id),
            exercise_name=exercise_name.strip() or None,
        )
        if not rows:
            await send_discord_text(ctx.channel, "No logs to export.")
            await self._maybe_send_settings_tip(ctx.channel)
            return

        stem = f"{exercise_name.strip().lower().replace(' ', '_')}_logs" if exercise_name.strip() else "workout_logs"
        csv_path = write_logs_csv(rows, stem=stem)
        await send_discord_file(ctx.channel, file=discord.File(str(csv_path)))
        await self._maybe_send_settings_tip(ctx.channel)

    def _format_load_display(self, *, weight: float, unit: str, note: str, is_bodyweight: bool, reps: int) -> str:
        if is_bodyweight:
            load = note or "bodyweight"
            return f"{load} x {reps}"
        return f"{format_standard_number(weight)} {unit} x {reps}"

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message) -> None:
        if after.author.bot:
            return
        if before.content == after.content:
            return
        if not self._is_workout_channel(after.channel):
            return

        ref = self.message_log_map.get(after.id)
        if ref is None:
            return

        content = after.content.strip()
        if not content:
            return

        lock = self._get_user_lock(after.author.id)
        async with lock:
            ref = self.message_log_map.get(after.id)
            if ref is None:
                return

            parsed = parse_set_input(content)
            if not parsed and ref.category == "bodyweight":
                reps_only = self._parse_bodyweight_reps_only(content)
                if reps_only:
                    parsed = {
                        "exercise": None,
                        "weight": 0.0,
                        "reps": int(reps_only["reps"]),
                        "unit": None,
                        "unit_explicit": False,
                        "rir": reps_only.get("rir"),
                        "is_bodyweight": True,
                        "note": "bodyweight",
                        "trailing_text": str(reps_only.get("trailing_text") or "").strip(),
                    }
            if not parsed:
                await send_discord_text(after.channel, "I couldn't parse the edited set. Use `weight x reps`.")
                return

            category = ref.category
            is_bodyweight = bool(parsed.get("is_bodyweight"))
            if is_bodyweight and category != "bodyweight":
                await send_discord_text(
                    after.channel,
                    f"{ref.exercise_name} is a {category.replace('_', ' ')} exercise - log a weight like `30 x 10`.",
                )
                return
            if category == "bodyweight" and not is_bodyweight:
                await send_discord_text(
                    after.channel,
                    f"{ref.exercise_name} is a bodyweight exercise. Log as `bw x 10` or `bw+25 x 10` for weighted.",
                )
                return

            reps = int(parsed.get("reps") or 0)
            if reps <= 0:
                await send_discord_text(after.channel, "Reps must be greater than zero.")
                return
            if reps > MAX_REPS:
                await send_discord_text(after.channel, "That rep count seems unrealistic.")
                return

            unit = str(parsed.get("unit") or ref.unit)
            weight = float(parsed.get("weight") or 0.0)
            if category != "bodyweight" and unit not in {"lbs", "kg"}:
                unit = "lbs"
            max_weight = MAX_WEIGHT_BY_UNIT.get(unit, 1500.0)
            if abs(weight) > max_weight:
                await send_discord_text(after.channel, "That weight seems unrealistic. Please double-check.")
                return

            parsed_note = str(parsed.get("note") or "").strip()
            trailing_text = str(parsed.get("trailing_text") or "").strip()
            note_parts = [part for part in [parsed_note, trailing_text] if part]
            note = "; ".join(note_parts)

            updated = await self.db.update_workout_log(
                ref.workout_log_id,
                user_id=self._session_key(after.author.id),
                weight=weight,
                reps=reps,
                unit=unit,
                rir=parsed.get("rir"),
                notes=note,
            )
            if not updated:
                await send_discord_text(after.channel, "Couldn't update that set in the database.")
                return

            await self.db.delete_pr_for_workout_log(
                ref.workout_log_id,
                user_id=self._session_key(after.author.id),
            )
            pr_text = await self._recheck_pr_after_edit(
                exercise_name=ref.exercise_name,
                weight=weight,
                reps=reps,
                unit=unit,
                workout_log_id=ref.workout_log_id,
                category=category,
                equipment_type=ref.equipment_type,
                user_id=self._session_key(after.author.id),
                performer_name=getattr(after.author, "display_name", str(after.author)),
                performer_user_id=self._session_key(after.author.id),
            )

            old_display = self._format_load_display(
                weight=ref.weight,
                unit=ref.unit,
                note=ref.note,
                is_bodyweight=ref.is_bodyweight,
                reps=ref.reps,
            )
            new_display = self._format_load_display(
                weight=weight,
                unit=unit,
                note=note,
                is_bodyweight=is_bodyweight,
                reps=reps,
            )
            update_message = f"✏️ Updated: {ref.exercise_name} — {new_display} (was {old_display})"
            if pr_text:
                update_message = f"{update_message} | {pr_text}"
            await send_discord_text(after.channel, update_message)

            active_session = self.sessions.get(self._session_key(after.author.id))
            if active_session:
                for row in active_session.logged_sets:
                    if int(row.get("workout_log_id") or -1) != ref.workout_log_id:
                        continue
                    row["weight"] = weight
                    row["reps"] = reps
                    row["unit"] = unit
                    row["e1rm"] = epley_1rm(weight, reps) if weight > 0 else 0.0
                    row["is_bodyweight"] = is_bodyweight
                    row["note"] = note
                    break

            self.message_log_map[after.id] = LoggedSetMessageRef(
                workout_log_id=ref.workout_log_id,
                exercise_id=ref.exercise_id,
                exercise_name=ref.exercise_name,
                category=ref.category,
                equipment_type=ref.equipment_type,
                weight=weight,
                reps=reps,
                unit=unit,
                note=note,
                is_bodyweight=is_bodyweight,
            )

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
        user_id = self._session_key(message.author.id)
        session = self.sessions.get(user_id)

        if not session:
            if not await self._check_weekday_start_channel(message.channel, user_id):
                return
            if lowered in READY_TOKENS:
                await self._start_session(message.channel, user_id=user_id)
                return
            if self._is_question(content):
                await send_discord_text(message.channel, "Type `ready` to start today's workout, or ask broader questions in #ask.")
                return
            await send_discord_text(message.channel, "Type `ready` to start today's workout.")
            return

        lock = self._get_user_lock(message.author.id)
        async with lock:
            session = self.sessions.get(user_id)
            if not session:
                await send_discord_text(message.channel, "Type `ready` to start today's workout.")
                return

            handled_early_end = await self._handle_early_end_prompt(
                message.channel,
                user_id,
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

            if lowered == "skip rest":
                await self._cancel_rest(session)
                await send_discord_text(message.channel, "Rest skipped.")
                await self._prompt_current_exercise(message.channel, session)
                return

            if lowered in {"skip", "move on"}:
                await self._skip_current_exercise(message.channel, session)
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
                        rest_message=f"⏱️ Rest {(session.rest_seconds + extend * 60) // 60}:{(session.rest_seconds + extend * 60) % 60:02d}.",
                    )
                    return
                await send_discord_text(message.channel, "No active rest timer to extend.")
                return

            parsed = parse_set_input(content)
            same_parsed, same_error = self._parse_same_command(session, content)
            if same_error:
                await send_discord_text(message.channel, same_error)
                return
            if parsed is None and same_parsed is not None:
                parsed = same_parsed
            if parsed is None:
                parsed = self._parse_shorthand_set(session, content)
            if not parsed:
                current = session.current_exercise()
                current_category = str(current.get("category") or "") if current else ""
                reps_only = self._parse_bodyweight_reps_only(content) if current_category == "bodyweight" else None
                if reps_only:
                    parsed = {
                        "exercise": None,
                        "weight": 0.0,
                        "reps": int(reps_only["reps"]),
                        "unit": None,
                        "unit_explicit": False,
                        "rir": reps_only.get("rir"),
                        "is_bodyweight": True,
                        "note": "bodyweight",
                        "trailing_text": str(reps_only.get("trailing_text") or "").strip(),
                    }
            if parsed:
                if session.rest_task and not session.rest_task.done():
                    await send_discord_text(
                        message.channel,
                        "Rest timer is active. Wait for the prompt or type `skip rest`."
                    )
                    return
                parsed["raw"] = content
                await self._handle_logged_set(
                    message.channel,
                    session,
                    parsed,
                    message_id=message.id,
                    performer_name=getattr(message.author, "display_name", str(message.author)),
                )
                return

            if await self._handle_equipment_switch(message.channel, session, content):
                return

            if await self._handle_reorder_request(message.channel, session, content):
                return

            if self._is_remaining_query(content):
                await send_discord_text(message.channel, await self._answer_remaining_query(session))
                return

            if self._is_history_query(content):
                await send_discord_text(message.channel, await self._answer_history_query(session))
                return

            pr_query = self._extract_pr_query(content)
            if pr_query is not None:
                await send_discord_text(message.channel, await self._answer_pr_query(session, pr_query))
                return

            if await self._handle_equipment_unavailable_message(message.channel, session, content):
                return

            cue = parse_cue(content)
            if cue:
                current = session.current_exercise()
                if current:
                    await self.db.save_cue(str(current["name"]), cue, user_id=session.user_id)
                    await send_discord_text(message.channel, f"Saved cue for {current['name']}: {cue}")
                    return

            if self._is_question(content):
                try:
                    reply = await self._answer_workout_question(session, content)
                    await send_discord_text(message.channel, f"{reply}\n{self._current_set_prompt(session)}")
                except Exception:
                    await send_discord_text(message.channel, "I couldn't reach the coach model right now. Try again in a moment.")
                return

            await send_discord_text(
                message.channel,
                "I didn't catch that. Log sets as `225 x 3`, `100 8`, `same`, or ask a workout question."
            )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(WorkoutCog(bot))
