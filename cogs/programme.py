from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

import discord
from discord.ext import commands

from llm.parser import ProgramParser
from llm.prompts import PROGRAMME_EDIT_JSON_SYSTEM_PROMPT, PROGRAMME_IMPORT_SYSTEM_PROMPT, PROGRAMME_ROUTER_SYSTEM_PROMPT
from utils.discord_messages import send_discord_text, split_discord_message
from utils.logbook_import import LogbookImportError, parse_structured_logbook_bytes


DURATION_RE = re.compile(r"(?P<num>\d+)\s*(?P<unit>day|days|week|weeks)", re.IGNORECASE)
DAY_NUMBER_RE = re.compile(r"\bday\s*(?P<day_num>\d+)\b", re.IGNORECASE)
START_DAY_INTENT_RE = re.compile(r"\b(start|begin|starting|start on|begin with)\b", re.IGNORECASE)
SET_PATTERN_RE = re.compile(r"\d+\s*[xX×]\s*\d+")
TRAVEL_INTENT_RE = re.compile(r"\b(travel|travelling|traveling|vacation|hotel gym|limited equipment)\b", re.IGNORECASE)
BACK_INTENT_RE = re.compile(r"\b(i'?m back|im back|back from|returned|home now)\b", re.IGNORECASE)
SWAP_RE = re.compile(
    r"(?:swap|replace)\s+(?P<old>.+?)\s+with\s+(?P<new>.+?)(?:\s+on\s+(?P<day>.+))?$",
    re.IGNORECASE,
)
CHANGE_RE = re.compile(r"change\s+(.+?)\s+to\s+(.+)$", re.IGNORECASE)
INDEX_REF_DETECT_RE = re.compile(r"\b\d+\.\d+\b")
INDEX_REF_PAIR_RE = re.compile(r"\b(?P<day>\d+)\.(?P<ex>\d+)\b")
EQUIPMENT_KEYWORD_RE = re.compile(
    r"\b(?:to|is|are|should be|=)\s+(?:a\s+|an\s+)?(?P<type>heavy[_ ]barbell|light[_ ]barbell|barbell|dumbbell|cable|machine|bodyweight|smith(?:\s+machine)?)\b",
    re.IGNORECASE,
)
TYPE_CORRECTION_PATTERNS = [
    re.compile(
        r"^(?P<exercise>.+?)\s+(?:is|should be)\s+(?:a\s+|an\s+)?(?P<category>heavy[_ ]barbell|light[_ ]barbell|barbell|dumbbell|cable|machine|bodyweight|smith(?: machine)?)\b(?:.*)?$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^(?:change|set|update)\s+(?P<exercise>.+?)\s+(?:to|as)\s+(?:a\s+|an\s+)?(?P<category>heavy[_ ]barbell|light[_ ]barbell|barbell|dumbbell|cable|machine|bodyweight|smith(?: machine)?)\b(?:.*)?$",
        re.IGNORECASE,
    ),
]
CONFIRM_TOKENS = {"save", "confirm", "import", "looks good", "ship it", "yes"}
CANCEL_TOKENS = {"cancel", "stop", "never mind", "discard"}
VALID_EQUIPMENT_TYPES = {"barbell", "dumbbell", "cable", "machine", "bodyweight", "smith machine", "unknown"}
SHOW_PROGRAM_TOKENS = ("list", "show", "display", "current", "active", "what's", "whats")

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PendingProgram:
    user_id: int
    channel_id: int
    raw_text: str
    parsed_program: dict[str, Any]
    created_at: datetime
    flow_id: str = field(default_factory=lambda: uuid4().hex)
    temporary: bool = False
    parent_program_id: Optional[int] = None
    expires_at: Optional[str] = None
    stage: str = "review"
    inferred_name: str = "Imported Program"


@dataclass(slots=True)
class PendingExerciseTypePrompt:
    user_id: int
    channel_id: int
    old_name: str
    new_name: str
    day_hint: Optional[str]
    created_at: datetime
    fallback_type: str = "unknown"
    exercise_id: Optional[int] = None
    new_sets: Optional[int] = None
    new_rep_low: Optional[int] = None
    new_rep_high: Optional[int] = None


class ProgrammeCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.settings = bot.settings
        self.db = bot.db
        self.parser = ProgramParser(bot.ollama)
        self.pending_programs: dict[tuple[int, int], PendingProgram] = {}
        self.post_import_state: dict[int, dict[str, Any]] = {}
        self.pending_travel_context: dict[int, tuple[str, datetime]] = {}
        self.pending_exercise_type_prompts: dict[tuple[int, int], PendingExerciseTypePrompt] = {}
        self.user_locks: dict[int, asyncio.Lock] = {}
        self.flow_timeout_tasks: dict[tuple[int, int, str], asyncio.Task[None]] = {}

    def clear_runtime_state(self) -> None:
        for task in self.flow_timeout_tasks.values():
            if not task.done():
                task.cancel()
        self.flow_timeout_tasks.clear()
        self.pending_programs.clear()
        self.post_import_state.clear()
        self.pending_travel_context.clear()
        self.pending_exercise_type_prompts.clear()
        self.user_locks.clear()

    def _get_user_lock(self, user_id: int) -> asyncio.Lock:
        lock = self.user_locks.get(user_id)
        if lock is None:
            lock = asyncio.Lock()
            self.user_locks[user_id] = lock
        return lock

    def _pending_key(self, user_id: int, channel_id: int) -> tuple[int, int]:
        return (int(user_id), int(channel_id))

    def _timeout_key(self, user_id: int, channel_id: int, kind: str) -> tuple[int, int, str]:
        return (int(user_id), int(channel_id), kind)

    def _cancel_timeout(self, user_id: int, channel_id: int, kind: str) -> None:
        task = self.flow_timeout_tasks.pop(self._timeout_key(user_id, channel_id, kind), None)
        if task and not task.done():
            task.cancel()

    def _schedule_timeout(self, user_id: int, channel_id: int, kind: str, task: asyncio.Task[None]) -> None:
        self._cancel_timeout(user_id, channel_id, kind)
        self.flow_timeout_tasks[self._timeout_key(user_id, channel_id, kind)] = task

    def _is_programme_channel(self, channel: discord.abc.GuildChannel | discord.Thread | discord.abc.PrivateChannel) -> bool:
        cid = getattr(channel, "id", None)
        name = getattr(channel, "name", "")
        if self.settings.programme_channel_id:
            return cid == self.settings.programme_channel_id or name == "programme"
        return name == "programme"

    def _now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

    def _normalize_name(self, text: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", (text or "").lower())

    def _normalize_equipment_type(self, label: str) -> str:
        lowered = " ".join((label or "").strip().lower().split())
        if not lowered:
            return "unknown"
        if lowered in VALID_EQUIPMENT_TYPES:
            return lowered
        aliases = {
            "smith": "smith machine",
            "bw": "bodyweight",
            "body weight": "bodyweight",
            "db": "dumbbell",
            "free weight": "barbell",
            "bb": "barbell",
            "cables": "cable",
            "machines": "machine",
            "mach": "machine",
        }
        return aliases.get(lowered, "unknown")

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

    def _looks_like_program_paste(self, text: str) -> bool:
        lowered = (text or "").lower()
        if len((text or "").splitlines()) < 2:
            return False
        if len(SET_PATTERN_RE.findall(text or "")) < 2:
            return False
        return any(token in lowered for token in ["day ", "push", "pull", "legs", "upper", "lower"])

    def _exercise_type_from_name_and_category(self, exercise_name: str, category: str = "") -> str:
        lowered = exercise_name.lower()
        normalized_category = (category or "").strip().lower()

        if "smith" in lowered or normalized_category == "smith_machine":
            return "smith machine"

        bodyweight_tokens = (
            "pull-up", "pull up", "pullup", "chin-up", "chin up", "chinup",
            "push-up", "push up", "pushup", "dip", "diamond pushup",
            "leg raise", "hanging raise", "plank", "crunch", "sit-up",
            "sit up", "burpee", "lunge walk", "bodyweight", "bw ",
            "pistol squat", "muscle-up", "muscle up",
        )
        if any(token in lowered for token in bodyweight_tokens) or normalized_category == "bodyweight":
            return "bodyweight"

        dumbbell_tokens = (
            "dumbbell", "db ", "db-", "arnold press", "kroc",
            "hammer curl", "cross-body curl", "cross body curl",
            "concentration curl", "incline curl",
            "lateral raise", "front raise", "rear delt fly",
            "dumbbell row", "db row",
            "goblet squat", "dumbbell lunge", "walking lunge",
        )
        if any(token in lowered for token in dumbbell_tokens) or normalized_category == "dumbbell":
            return "dumbbell"

        cable_tokens = (
            "cable", "press-around", "press around", "face pull",
            "tricep pushdown", "pushdown", "overhead extension",
            "cross-body tricep", "cross body tricep",
            "cable fly", "cable crossover", "cable curl",
            "cable row", "cable pullover", "cable lateral",
            "rope", "v-bar", "straight bar curl",
        )
        if any(token in lowered for token in cable_tokens):
            return "cable"

        machine_tokens = (
            "machine", "pulldown", "lat pulldown", "lat pull-down",
            "leg press", "leg curl", "leg extension",
            "hack squat", "hip thrust", "seated row",
            "chest fly machine", "pec deck", "pec fly",
            "shoulder press machine", "smith",
            "seated calf", "toe press", "calf raise machine",
            "hip abduct", "hip adduct", "glute drive",
            "assisted", "preacher curl machine",
        )
        if any(token in lowered for token in machine_tokens):
            return "machine"

        if "ez-bar" in lowered or "ez bar" in lowered or "preacher curl" in lowered:
            return "barbell"

        if normalized_category in {"heavy_barbell", "light_barbell"}:
            return "barbell"
        return "unknown"

    def _db_category_from_equipment_type(self, exercise_name: str, equipment_type: str, fallback: str = "") -> str:
        normalized_type = self._normalize_equipment_type(equipment_type)
        normalized_fallback = (fallback or "").strip().lower()
        if normalized_type == "smith machine":
            return "smith_machine"
        if normalized_type == "dumbbell":
            return "dumbbell"
        if normalized_type == "bodyweight":
            return "bodyweight"
        if normalized_type in {"cable", "machine", "unknown"}:
            return "cable_machine"
        if normalized_fallback in {"heavy_barbell", "light_barbell"}:
            return normalized_fallback
        looked_up = self.parser._category_lookup(exercise_name, fallback=normalized_fallback)
        if looked_up in {"heavy_barbell", "light_barbell"}:
            return looked_up
        heavy_tokens = ("squat", "bench", "deadlift", "overhead press", "ohp", "barbell row")
        return "heavy_barbell" if any(token in exercise_name.lower() for token in heavy_tokens) else "light_barbell"

    def _category_label_from_db(self, category: str, equipment_type: str = "") -> str:
        normalized_category = (category or "").strip().lower()
        normalized_type = self._normalize_equipment_type(equipment_type)
        if normalized_type == "smith machine" or normalized_category == "smith_machine":
            return "smith"
        if normalized_type == "machine":
            return "machine"
        if normalized_type == "cable":
            return "cable"
        if normalized_type == "dumbbell" or normalized_category == "dumbbell":
            return "dumbbell"
        if normalized_type == "bodyweight" or normalized_category == "bodyweight":
            return "bodyweight"
        if normalized_category in {"heavy_barbell", "light_barbell"}:
            return normalized_category
        return normalized_type or normalized_category or "unknown"

    def _looks_like_show_program_intent(self, text: str) -> bool:
        lowered = " ".join((text or "").strip().lower().split())
        if "program" not in lowered:
            return False
        if any(token in lowered for token in ("what is my", "what's my", "whats my", "current program", "active program")):
            return True
        return any(token in lowered for token in SHOW_PROGRAM_TOKENS)

    def _extract_type_correction(self, text: str) -> Optional[tuple[str, str]]:
        for pattern in TYPE_CORRECTION_PATTERNS:
            match = pattern.match((text or "").strip())
            if not match:
                continue
            exercise_name = str(match.group("exercise") or "").strip(" .")
            category = str(match.group("category") or "").strip(" .")
            if exercise_name and category:
                return exercise_name, category
        return None

    def _normalize_program_payload(
        self,
        payload: dict[str, Any],
        *,
        fallback_name: str = "Imported Program",
    ) -> dict[str, Any]:
        days_in = payload.get("days") if isinstance(payload, dict) else []
        if not isinstance(days_in, list) or not days_in:
            raise ValueError("No days found in parsed program")

        out_days: list[dict[str, Any]] = []
        for day_idx, day in enumerate(days_in):
            if not isinstance(day, dict):
                continue
            exercises_in = day.get("exercises")
            if not isinstance(exercises_in, list):
                exercises_in = []
            out_exercises: list[dict[str, Any]] = []
            for ex_idx, exercise in enumerate(exercises_in):
                if not isinstance(exercise, dict):
                    continue
                name = str(exercise.get("name") or "").strip()
                if not name:
                    continue
                equipment_type = self._normalize_equipment_type(
                    str(exercise.get("equipment_type") or self._exercise_type_from_name_and_category(name, str(exercise.get("category") or "")))
                )
                category = self._db_category_from_equipment_type(
                    name,
                    equipment_type,
                    fallback=str(exercise.get("category") or ""),
                )
                if equipment_type == "unknown":
                    inferred = self._exercise_type_from_name_and_category(name, str(exercise.get("category") or ""))
                    if inferred != "unknown":
                        equipment_type = self._normalize_equipment_type(inferred)
                        category = self._db_category_from_equipment_type(name, equipment_type)
                out_exercises.append(
                    {
                        "name": name,
                        "display_order": ex_idx,
                        "sets": max(1, int(exercise.get("sets") or 1)),
                        "rep_range_low": self._int_or_none(exercise.get("rep_range_low")),
                        "rep_range_high": self._int_or_none(exercise.get("rep_range_high")),
                        "category": category,
                        "equipment_type": equipment_type,
                        "superset_group": self._int_or_none(exercise.get("superset_group")),
                        "muscle_groups": str(exercise.get("muscle_groups") or "").strip(),
                        "notes": str(exercise.get("notes") or "").strip(),
                    }
                )
            out_days.append(
                {
                    "day_order": day_idx,
                    "name": str(day.get("name") or f"Day {day_idx + 1}").strip() or f"Day {day_idx + 1}",
                    "exercises": out_exercises,
                }
            )

        if not out_days:
            raise ValueError("No days found in parsed program")
        return {
            "program_name": str(payload.get("program_name") or fallback_name).strip() or fallback_name,
            "days": out_days,
        }

    def _int_or_none(self, value: Any) -> Optional[int]:
        if value in {None, "", "null"}:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _format_rep_scheme(self, exercise: dict[str, Any]) -> str:
        sets = int(exercise.get("sets") or 1)
        low = exercise.get("rep_range_low")
        high = exercise.get("rep_range_high")
        notes = str(exercise.get("notes") or "").strip()
        if low is not None and high is not None:
            if int(low) == int(high):
                return f"{sets}×{int(low)}"
            return f"{sets}×{int(low)}–{int(high)}"
        if "amrap" in notes.lower():
            return f"{sets}×AMRAP"
        return f"{sets}×?"

    def _program_to_text(self, parsed_program: dict[str, Any]) -> str:
        lines: list[str] = []
        for day in parsed_program.get("days", []):
            lines.append(str(day.get("name") or "Day"))
            for exercise in day.get("exercises", []):
                lines.append(f"{exercise['name']} - {self._format_rep_scheme(exercise)}")
            lines.append("")
        return "\n".join(lines).strip()

    def _unknown_exercises(self, parsed_program: dict[str, Any]) -> list[dict[str, str]]:
        unknowns: list[dict[str, str]] = []
        for day in parsed_program.get("days", []):
            day_name = str(day.get("name") or "Day")
            for exercise in day.get("exercises", []):
                if self._normalize_equipment_type(str(exercise.get("equipment_type") or "")) != "unknown":
                    continue
                unknowns.append({"day_name": day_name, "exercise_name": str(exercise.get("name") or "")})
        return unknowns

    def _format_program_created_label(self, created_at: Any) -> Optional[str]:
        raw = str(created_at or "").strip()
        if not raw:
            return None
        candidates = [raw, raw.replace(" ", "T")]
        for candidate in candidates:
            try:
                parsed = datetime.fromisoformat(candidate)
                return parsed.strftime("imported %b %d")
            except ValueError:
                continue
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(raw, fmt)
                return parsed.strftime("imported %b %d")
            except ValueError:
                continue
        return None

    def _format_program_day_heading(
        self,
        day_number: int,
        day_name: str,
        exercise_count: int,
        *,
        block: Optional[str] = None,
        week: Optional[int] = None,
        programmed_day_number: Optional[int] = None,
    ) -> str:
        clean_name = day_name.strip()
        if block and week is not None and programmed_day_number is not None:
            label = f"{block} Week {week} Day {programmed_day_number} - {clean_name}"
        elif clean_name.lower().startswith("day "):
            label = clean_name
        else:
            label = f"Day {day_number} - {clean_name}"
        return f"{label} ({exercise_count} exercises)"

    def _attachment_looks_like_logbook(self, attachment: object) -> bool:
        filename = str(getattr(attachment, "filename", "") or "").lower()
        return filename.endswith((".xlsx", ".xlsm", ".xls", ".csv"))

    def _structured_program_name(self, payload: dict[str, Any], filename: str) -> str:
        current = str(payload.get("program_name") or "").strip()
        if current and current.lower() != "structured logbook":
            return current
        stem = Path(filename).stem.replace("-", " ").replace("_", " ").strip()
        if not stem:
            return current or "Structured Logbook"
        words = ["".join(ch for ch in word if ch.isalnum()) for word in stem.split()]
        cleaned_words = [word for word in words if word]
        if not cleaned_words:
            return current or "Structured Logbook"
        return " ".join(word.upper() if word.isupper() else word.title() for word in cleaned_words)

    def _format_first_week_schedule(self, summary: dict[str, Any]) -> str:
        schedule = summary.get("first_week_schedule") if isinstance(summary, dict) else None
        if not isinstance(schedule, list) or not schedule:
            return ""
        lines = ["Week 1 schedule:"]
        for day in schedule:
            if not isinstance(day, dict):
                continue
            day_number = int(day.get("day_number") or 0)
            day_name = str(day.get("day_name") or "Day").strip() or "Day"
            if bool(day.get("is_rest_day")):
                lines.append(f"- Day {day_number}: {day_name}")
                continue
            exercise_count = int(day.get("exercise_count") or 0)
            exercise_label = "exercise" if exercise_count == 1 else "exercises"
            lines.append(f"- Day {day_number}: {day_name} ({exercise_count} {exercise_label})")
        return "\n".join(lines)

    async def _import_structured_logbook_attachment(
        self,
        channel: discord.abc.Messageable,
        author: discord.abc.User,
        attachment: object,
    ) -> bool:
        filename = str(getattr(attachment, "filename", "") or "logbook.xlsx")
        reader = getattr(attachment, "read", None)
        if reader is None:
            await send_discord_text(channel, f"I couldn't read `{filename}`.")
            return True

        try:
            async with channel.typing():
                data = await reader()
                payload = parse_structured_logbook_bytes(data, filename)
        except LogbookImportError as exc:
            await send_discord_text(channel, f"I couldn't import `{filename}`: {exc}")
            return True
        except Exception as exc:
            await send_discord_text(channel, f"I couldn't parse `{filename}` cleanly yet: {exc}")
            return True

        user_id = str(author.id)
        payload["program_name"] = self._structured_program_name(payload, filename)
        async with channel.typing():
            recent = await self.db.get_recent_program_by_name(str(payload["program_name"]), user_id=user_id, minutes=5)
            if recent:
                display_id = int(recent.get("display_id") or recent.get("id") or 0)
                await send_discord_text(
                    channel,
                    f"Skipped duplicate import: **{payload['program_name']}** was already imported recently (ID {display_id}).",
                )
                return True

            program_id = await self.db.create_program_from_payload(payload, user_id=user_id)
            created_program = await self.db.get_program_by_id(program_id)

        summary = payload.get("import_summary") if isinstance(payload, dict) else {}
        if isinstance(summary, dict):
            first_active_idx = int(summary.get("first_active_day_index") or 0)
            await self.db.set_current_day_index(first_active_idx, user_id=user_id)
            label = str(summary.get("label") or payload["program_name"])
            total_days = int(summary.get("days") or len(payload.get("days", [])))
            training_days = int(summary.get("training_days") or 0)
            rest_days = int(summary.get("rest_days") or 0)
            total_rows = int(summary.get("source_rows") or 0)
            total_exercises = int(
                summary.get("exercises") or sum(len(day.get("exercises", [])) for day in payload.get("days", []))
            )
            total_weeks = int(summary.get("weeks") or 0)
            schedule_summary = self._format_first_week_schedule(summary)
        else:
            label = str(payload["program_name"])
            total_days = len(payload.get("days", []))
            training_days = 0
            rest_days = 0
            total_rows = 0
            total_exercises = sum(len(day.get("exercises", [])) for day in payload.get("days", []))
            total_weeks = 0
            schedule_summary = ""

        if created_program:
            display_id = int(created_program.get("display_id") or program_id)
            logger.info("Imported structured logbook %s as program %s", label, display_id)
        lines = [f"Imported **{payload['program_name']}** (ID {display_id if created_program else program_id})."]
        count_line = f"Weeks: {total_weeks} | Exercises: {total_exercises} | Total days: {total_days}"
        if total_rows and total_rows != total_exercises:
            count_line = f"{count_line} | CSV rows: {total_rows}"
        if training_days or rest_days:
            count_line = f"{count_line} | Training days: {training_days} | Rest days: {rest_days}"
        lines.append(count_line)
        if schedule_summary:
            lines.append(schedule_summary)
        await send_discord_text(channel, "\n".join(lines))
        return True

    async def _build_active_program_context(self, user_id: str) -> Optional[dict[str, Any]]:
        program = await self.db.get_active_program(user_id)
        if not program:
            return None
        days = await self.db.get_program_days(int(program["id"]))
        if not days:
            return {
                "program": program,
                "days": [],
                "exercise_map": {},
                "current_day_index": 0,
                "current_day": None,
            }

        raw_current_day = await self.db.get_current_day_index(user_id)
        current_day_index = raw_current_day % len(days)
        day_entries: list[dict[str, Any]] = []
        exercise_map: dict[str, dict[str, Any]] = {}

        for day_number, day in enumerate(days, start=1):
            exercises = await self.db.get_exercises_for_day(int(day["id"]))
            exercise_entries: list[dict[str, Any]] = []
            for exercise_number, exercise in enumerate(exercises, start=1):
                category_label = self._category_label_from_db(
                    str(exercise.get("category") or ""),
                    str(exercise.get("equipment_type") or ""),
                )
                entry = {
                    **exercise,
                    "ref": f"{day_number}.{exercise_number}",
                    "day_number": day_number,
                    "exercise_number": exercise_number,
                    "day_name": str(day["name"]),
                    "category_label": category_label,
                }
                exercise_entries.append(entry)
                exercise_map[entry["ref"]] = entry
            day_entries.append(
                {
                    **day,
                    "day_number": day_number,
                    "heading": self._format_program_day_heading(
                        day_number,
                        str(day["name"]),
                        len(exercise_entries),
                        block=str(day.get("block") or "").strip() or None,
                        week=self._int_or_none(day.get("week")),
                        programmed_day_number=self._int_or_none(day.get("day_number")),
                    ),
                    "exercises": exercise_entries,
                }
            )

        current_day = day_entries[current_day_index] if day_entries else None
        return {
            "program": program,
            "days": day_entries,
            "exercise_map": exercise_map,
            "current_day_index": current_day_index,
            "current_day": current_day,
        }

    def _format_active_program_header(self, context: dict[str, Any]) -> str:
        program = context["program"]
        imported_label = self._format_program_created_label(program.get("created_at"))
        if imported_label:
            title = f"📋 {program['name']} ({imported_label})"
        else:
            title = f"📋 {program['name']}"
        current_day = context.get("current_day")
        if current_day is None:
            return f"{title}\nCurrent day: not set"
        block = str(current_day.get("block") or "").strip()
        week = self._int_or_none(current_day.get("week"))
        programmed_day = self._int_or_none(current_day.get("day_number"))
        if block and week is not None and programmed_day is not None:
            return f"{title}\nCurrent day: {block} Week {week} Day {programmed_day} - {current_day['name']}"
        return f"{title}\nCurrent day: Day {current_day['day_number']} - {current_day['name']}"

    def _format_active_program_day_block(self, day: dict[str, Any]) -> str:
        lines = [day["heading"]]
        if not day["exercises"]:
            lines.append("  No exercises.")
            return "\n".join(lines)
        for exercise in day["exercises"]:
            lines.append(
                f"  {exercise['ref']} {exercise['name']} - {self._format_rep_scheme(exercise)} [{exercise['category_label']}]"
            )
        return "\n".join(lines)

    async def _send_active_program_summary(self, channel: discord.abc.Messageable, user_id: str) -> None:
        context = await self._build_active_program_context(user_id)
        if context is None:
            await send_discord_text(channel, "No active program yet. Paste one in #programme.")
            return
        await send_discord_text(channel, self._format_active_program_header(context))
        for day in context["days"]:
            await send_discord_text(channel, self._format_active_program_day_block(day))

    def _format_rep_scheme_from_values(
        self,
        *,
        sets: int,
        rep_low: Optional[int],
        rep_high: Optional[int],
    ) -> str:
        exercise = {
            "sets": sets,
            "rep_range_low": rep_low,
            "rep_range_high": rep_high,
            "notes": "AMRAP" if rep_low is None and rep_high is None else "",
        }
        return self._format_rep_scheme(exercise)

    async def _request_programme_action(
        self,
        channel: discord.abc.Messageable,
        *,
        user_id: str,
        content: str,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        summary_lines = [self._format_active_program_header(context)]
        for day in context["days"]:
            summary_lines.append("")
            summary_lines.append(self._format_active_program_day_block(day))
        system_prompt = PROGRAMME_ROUTER_SYSTEM_PROMPT.replace("{program_summary}", "\n".join(summary_lines).strip())
        payload = {
            "message": content,
            "current_day": context["current_day"]["day_number"] if context.get("current_day") else None,
            "exercise_refs": {
                ref: {
                    "name": exercise["name"],
                    "sets": exercise["sets"],
                    "rep_low": exercise.get("rep_range_low"),
                    "rep_high": exercise.get("rep_range_high"),
                    "type": exercise["category_label"],
                    "day": exercise["day_number"],
                }
                for ref, exercise in context["exercise_map"].items()
            },
        }
        async with channel.typing():
            response = await self.bot.ollama.chat_json(
                system=system_prompt,
                user=json.dumps(payload, ensure_ascii=False),
                temperature=0.0,
                max_tokens=1200,
            )
        if not isinstance(response, dict):
            raise ValueError("Programme router returned a non-object response")
        return response

    async def _execute_update_sets_reps(
        self,
        channel: discord.abc.Messageable,
        *,
        user_id: str,
        context: dict[str, Any],
        action: dict[str, Any],
    ) -> None:
        exercise_ref = str(action.get("exercise_ref") or "").strip()
        target = context["exercise_map"].get(exercise_ref)
        if not target:
            await send_discord_text(channel, f"I couldn't find exercise reference `{exercise_ref}`.")
            return
        sets = max(1, int(action.get("sets") or target["sets"]))
        rep_low = self._int_or_none(action.get("rep_low"))
        rep_high = self._int_or_none(action.get("rep_high"))
        if rep_low is None and rep_high is None:
            await send_discord_text(channel, "Tell me the new reps in a format like `5x10` or `4x8-10`.")
            return
        if rep_low is None:
            rep_low = rep_high
        if rep_high is None:
            rep_high = rep_low
        updated = await self.db.update_exercise_scheme_by_id(
            int(target["id"]),
            user_id=user_id,
            sets=sets,
            rep_low=rep_low,
            rep_high=rep_high,
        )
        if not updated:
            await send_discord_text(channel, f"I couldn't update `{exercise_ref}`.")
            return
        old_scheme = self._format_rep_scheme_from_values(
            sets=int(updated["old_sets"]),
            rep_low=updated["old_rep_low"],
            rep_high=updated["old_rep_high"],
        )
        new_scheme = self._format_rep_scheme_from_values(
            sets=int(updated["new_sets"]),
            rep_low=updated["new_rep_low"],
            rep_high=updated["new_rep_high"],
        )
        await send_discord_text(channel, f"✅ {exercise_ref} {updated['exercise_name']}: {old_scheme} -> {new_scheme}")

    async def _execute_update_types(
        self,
        channel: discord.abc.Messageable,
        *,
        user_id: str,
        context: dict[str, Any],
        action: dict[str, Any],
    ) -> None:
        exercises = action.get("exercises")
        if not isinstance(exercises, dict) or not exercises:
            await send_discord_text(channel, "Tell me which exercise references you want to update, for example `2.1 is cable`.")
            return
        lines: list[str] = []
        errors: list[str] = []
        for exercise_ref, requested_type in exercises.items():
            target = context["exercise_map"].get(str(exercise_ref))
            if not target:
                errors.append(f"I couldn't find `{exercise_ref}`.")
                continue
            requested_label = str(requested_type or "").strip()
            normalized_type = self._normalize_equipment_type(requested_label)
            if normalized_type == "unknown" and requested_label.lower() not in {"heavy_barbell", "light_barbell"}:
                errors.append(f"I couldn't map `{requested_label}` for {exercise_ref}.")
                continue
            updated = await self.db.update_exercise_category_by_id(
                int(target["id"]),
                requested_label if requested_label else normalized_type,
                user_id=user_id,
            )
            if not updated:
                errors.append(f"I couldn't update `{exercise_ref}`.")
                continue
            lines.append(
                f"✅ {exercise_ref} {updated['exercise_name']}: {updated['old_category']} -> {updated['new_category']}"
            )
        if lines:
            await send_discord_text(channel, "\n".join(lines))
        if errors:
            await send_discord_text(channel, "\n".join(errors))

    async def _execute_swap_exercise(
        self,
        channel: discord.abc.Messageable,
        *,
        user_id: str,
        context: dict[str, Any],
        action: dict[str, Any],
    ) -> None:
        exercise_ref = str(action.get("exercise_ref") or "").strip()
        target = context["exercise_map"].get(exercise_ref)
        if not target:
            await send_discord_text(channel, f"I couldn't find exercise reference `{exercise_ref}`.")
            return

        new_name = str(action.get("new_name") or "").strip()
        if not new_name:
            await send_discord_text(channel, "Tell me the replacement exercise name.")
            return

        requested_type = str(action.get("new_type") or "").strip()
        equipment_type = self._normalize_equipment_type(requested_type)
        if equipment_type == "unknown":
            equipment_type = self._normalize_equipment_type(
                self._exercise_type_from_name_and_category(new_name, "")
            )
        if equipment_type == "unknown":
            self.pending_exercise_type_prompts[self._pending_key(int(user_id), getattr(channel, "id", 0))] = PendingExerciseTypePrompt(
                user_id=int(user_id),
                channel_id=getattr(channel, "id", 0),
                old_name=str(target["name"]),
                new_name=new_name,
                day_hint=str(target["day_name"]),
                created_at=self._now_utc(),
                exercise_id=int(target["id"]),
                new_sets=self._int_or_none(action.get("new_sets")),
                new_rep_low=self._int_or_none(action.get("new_rep_low")),
                new_rep_high=self._int_or_none(action.get("new_rep_high")),
            )
            await send_discord_text(
                channel,
                f"What type is {new_name}? Reply with `barbell`, `dumbbell`, `cable`, `machine`, `bodyweight`, or `smith machine`.",
            )
            return

        updated = await self.db.replace_exercise_in_active_program_by_id(
            int(target["id"]),
            user_id=user_id,
            new_name=new_name,
            new_category=self._db_category_from_equipment_type(new_name, equipment_type, fallback=str(target.get("category") or "")),
            new_equipment_type=equipment_type,
            new_sets=self._int_or_none(action.get("new_sets")),
            new_rep_low=self._int_or_none(action.get("new_rep_low")),
            new_rep_high=self._int_or_none(action.get("new_rep_high")),
        )
        if not updated:
            await send_discord_text(channel, f"I couldn't replace `{exercise_ref}`.")
            return
        scheme = self._format_rep_scheme_from_values(
            sets=int(updated["new_sets"]),
            rep_low=updated["new_rep_low"],
            rep_high=updated["new_rep_high"],
        )
        type_label = self._category_label_from_db(str(updated["category"]), str(updated["equipment_type"]))
        await send_discord_text(
            channel,
            f"✅ {exercise_ref} {updated['old_name']} -> {updated['new_name']} ({scheme}) [{type_label}]",
        )

    async def _execute_remove_exercise(
        self,
        channel: discord.abc.Messageable,
        *,
        user_id: str,
        context: dict[str, Any],
        action: dict[str, Any],
    ) -> None:
        exercise_ref = str(action.get("exercise_ref") or "").strip()
        target = context["exercise_map"].get(exercise_ref)
        if not target:
            await send_discord_text(channel, f"I couldn't find exercise reference `{exercise_ref}`.")
            return
        removed = await self.db.remove_exercise_from_active_program_by_id(int(target["id"]), user_id=user_id)
        if not removed:
            await send_discord_text(channel, f"I couldn't remove `{exercise_ref}`.")
            return
        await send_discord_text(channel, f"✅ Removed {exercise_ref} {removed['exercise_name']} from {removed['day_name']}.")

    async def _execute_add_exercise(
        self,
        channel: discord.abc.Messageable,
        *,
        user_id: str,
        context: dict[str, Any],
        action: dict[str, Any],
    ) -> None:
        day_number = int(action.get("day") or 0)
        if day_number < 1 or day_number > len(context["days"]):
            await send_discord_text(channel, f"Day {day_number} doesn't exist.")
            return
        day = context["days"][day_number - 1]
        name = str(action.get("name") or "").strip()
        if not name:
            await send_discord_text(channel, "Tell me the exercise name to add.")
            return
        sets = max(1, int(action.get("sets") or 1))
        rep_low = self._int_or_none(action.get("rep_low"))
        rep_high = self._int_or_none(action.get("rep_high"))
        if rep_low is None and rep_high is None:
            await send_discord_text(channel, "Tell me the set and rep scheme too, for example `3x15`.")
            return
        if rep_low is None:
            rep_low = rep_high
        if rep_high is None:
            rep_high = rep_low
        requested_type = str(action.get("type") or "").strip()
        equipment_type = self._normalize_equipment_type(requested_type)
        if equipment_type == "unknown":
            equipment_type = self._normalize_equipment_type(self._exercise_type_from_name_and_category(name, ""))
        if equipment_type == "unknown":
            await send_discord_text(
                channel,
                f"I couldn't infer the type for {name}. Tell me if it's `barbell`, `dumbbell`, `cable`, `machine`, `bodyweight`, or `smith machine`.",
            )
            return
        added = await self.db.add_exercise_to_program_day(
            int(day["id"]),
            user_id=user_id,
            name=name,
            sets=sets,
            rep_low=rep_low,
            rep_high=rep_high,
            category=self._db_category_from_equipment_type(name, equipment_type),
            equipment_type=equipment_type,
        )
        if not added:
            await send_discord_text(channel, f"I couldn't add {name} to Day {day_number}.")
            return
        exercise_ref = f"{day_number}.{len(day['exercises']) + 1}"
        scheme = self._format_rep_scheme_from_values(sets=sets, rep_low=rep_low, rep_high=rep_high)
        await send_discord_text(channel, f"✅ Added {exercise_ref} {name} ({scheme}) [{equipment_type}] to {day['name']}.")

    async def _handle_active_programme_message_with_llm(
        self,
        channel: discord.abc.Messageable,
        content: str,
        *,
        user_id: str,
    ) -> bool:
        context = await self._build_active_program_context(user_id)
        if context is None:
            return False
        try:
            action = await self._request_programme_action(channel, user_id=user_id, content=content, context=context)
        except Exception as exc:
            await send_discord_text(channel, f"I couldn't interpret that program request cleanly yet: {exc}")
            return True

        action_name = str(action.get("action") or "conversation").strip().lower()
        if action_name == "show_program":
            await self._send_active_program_summary(channel, user_id)
            return True
        if action_name == "update_sets_reps":
            await self._execute_update_sets_reps(channel, user_id=user_id, context=context, action=action)
            return True
        if action_name == "update_type":
            await self._execute_update_types(channel, user_id=user_id, context=context, action=action)
            return True
        if action_name == "swap_exercise":
            await self._execute_swap_exercise(channel, user_id=user_id, context=context, action=action)
            return True
        if action_name == "remove_exercise":
            await self._execute_remove_exercise(channel, user_id=user_id, context=context, action=action)
            return True
        if action_name == "add_exercise":
            await self._execute_add_exercise(channel, user_id=user_id, context=context, action=action)
            return True

        response = str(action.get("response") or "").strip()
        if response:
            await send_discord_text(channel, response)
        else:
            await send_discord_text(channel, "Tell me what you want to change in the program and I'll handle it.")
        return True

    async def _send_program_preview(
        self,
        channel: discord.abc.Messageable,
        parsed_program: dict[str, Any],
        *,
        include_footer: bool = True,
    ) -> None:
        days = parsed_program.get("days", [])
        if not days:
            await send_discord_text(channel, "I couldn't break that into days yet. Re-send it with the day headers kept in place and I'll try again.")
            return

        await send_discord_text(channel, "Here's your program:")
        for idx, day in enumerate(days):
            day_name = str(day.get("name") or f"Day {idx + 1}").strip() or f"Day {idx + 1}"
            day_label = day_name if day_name.lower().startswith("day ") else f"Day {idx + 1} - {day_name}"
            lines = [f"{day_label}:"]
            for ex_idx, exercise in enumerate(day.get("exercises", []), start=1):
                ex_name = str(exercise.get("name") or "").strip()
                if not ex_name:
                    continue
                equipment_type = self._normalize_equipment_type(str(exercise.get("equipment_type") or "unknown"))
                lines.append(f"  {ex_idx}. {ex_name} ({equipment_type}) — {self._format_rep_scheme(exercise)}")
            for chunk in split_discord_message("\n".join(lines), limit=1800):
                await send_discord_text(channel, chunk)

        if not include_footer:
            return

        unknowns = self._unknown_exercises(parsed_program)
        if unknowns:
            names = ", ".join(item["exercise_name"] for item in unknowns[:6])
            await send_discord_text(
                channel,
                f"I marked {len(unknowns)} exercise type(s) as `(unknown)` ({names}). Reply with the correct types if you want them fixed before import.",
            )
        await send_discord_text(
            channel,
            "Are you happy to proceed? Reply with `save` to import, or describe any edits you'd like.",
        )

    async def _parse_program_preview(self, channel: discord.abc.Messageable, raw_text: str) -> dict[str, Any]:
        async with channel.typing():
            parsed = await self.parser.parse_program(raw_text)
        inferred_name = self._infer_program_name_from_text(raw_text) or str(parsed.get("program_name") or "Imported Program")
        normalized = self._normalize_program_payload(parsed, fallback_name=inferred_name)
        if not normalized.get("days"):
            raise ValueError("No days found in parsed program")
        return normalized

    async def _apply_pending_edit_with_llm(
        self,
        channel: discord.abc.Messageable,
        state: PendingProgram,
        user_message: str,
    ) -> tuple[str, str, Optional[dict[str, Any]]]:
        payload = {
            "program": state.parsed_program,
            "user_message": user_message,
            "unknown_exercises": self._unknown_exercises(state.parsed_program),
        }
        async with channel.typing():
            response = await self.bot.ollama.chat_json(
                system=PROGRAMME_EDIT_JSON_SYSTEM_PROMPT,
                user=json.dumps(payload, ensure_ascii=False),
                temperature=0.0,
                max_tokens=1600,
            )
        status = str(response.get("status") or "no_change").strip().lower()
        message = str(response.get("message") or "").strip()
        if status != "updated":
            return status, message, None
        program_payload = response.get("program")
        if not isinstance(program_payload, dict):
            raise ValueError("Updated program payload missing from LLM response")
        updated = self._normalize_program_payload(
            program_payload,
            fallback_name=str(state.parsed_program.get("program_name") or state.inferred_name),
        )
        return status, message, updated

    def _extract_travel_expiry(self, text: str) -> Optional[str]:
        match = DURATION_RE.search(text or "")
        if not match:
            return None
        num = int(match.group("num"))
        unit = str(match.group("unit") or "").lower()
        days = num * 7 if "week" in unit else num
        return (date.today() + timedelta(days=max(1, days))).isoformat()

    def _set_pending_program(self, state: PendingProgram) -> None:
        self.pending_programs[self._pending_key(state.user_id, state.channel_id)] = state

    def _get_pending_program(self, user_id: int, channel_id: int) -> Optional[PendingProgram]:
        pending = self.pending_programs.get(self._pending_key(user_id, channel_id))
        if not pending:
            return None
        if pending.created_at + timedelta(minutes=30) < self._now_utc():
            self.pending_programs.pop(self._pending_key(user_id, channel_id), None)
            return None
        return pending

    def _clear_pending_program(self, user_id: int, channel_id: int) -> None:
        self.pending_programs.pop(self._pending_key(user_id, channel_id), None)
        self._cancel_timeout(user_id, channel_id, "name")

    def _set_post_import_context(self, user_id: int, channel_id: int, program_id: int, display_id: int) -> None:
        self.post_import_state[user_id] = {
            "program_id": program_id,
            "display_id": display_id,
            "channel_id": channel_id,
            "expires_at": self._now_utc() + timedelta(minutes=5),
        }

    def _get_post_import_context(self, user_id: int, channel_id: int) -> Optional[dict[str, Any]]:
        state = self.post_import_state.get(user_id)
        if not state:
            return None
        if int(state.get("channel_id") or 0) != channel_id:
            return None
        expires_at = state.get("expires_at")
        if not isinstance(expires_at, datetime) or expires_at < self._now_utc():
            self._clear_post_import_context(user_id, channel_id)
            return None
        return state

    def _clear_post_import_context(self, user_id: int, channel_id: int) -> None:
        self.post_import_state.pop(user_id, None)
        self._cancel_timeout(user_id, channel_id, "day")

    def _clear_pending_exercise_type_prompt(self, user_id: int, channel_id: int) -> None:
        self.pending_exercise_type_prompts.pop(self._pending_key(user_id, channel_id), None)

    def _clear_programme_flow_state(self, user_id: int, channel_id: int) -> None:
        self._clear_pending_program(user_id, channel_id)
        self._clear_post_import_context(user_id, channel_id)
        self._clear_pending_exercise_type_prompt(user_id, channel_id)
        self.pending_travel_context.pop(user_id, None)

    async def _resolve_programme_channel(self, guild: Optional[discord.Guild]) -> Optional[discord.TextChannel]:
        if guild is None:
            return None
        if self.settings.programme_channel_id:
            channel = guild.get_channel(self.settings.programme_channel_id)
            if isinstance(channel, discord.TextChannel):
                return channel
        for channel in guild.text_channels:
            if channel.name == "programme":
                return channel
        return None

    async def start_import_handoff_from_coach(
        self,
        *,
        author: discord.abc.User,
        guild: Optional[discord.Guild],
        raw_text: str,
    ) -> Optional[discord.TextChannel]:
        target = await self._resolve_programme_channel(guild)
        if target is None:
            return None
        lock = self._get_user_lock(author.id)
        async with lock:
            await self._start_pending_program(target, author, raw_text)
        return target

    async def _start_pending_program(
        self,
        channel: discord.abc.Messageable,
        author: discord.abc.User,
        raw_text: str,
        *,
        temporary: bool = False,
        parent_program_id: Optional[int] = None,
        expires_at: Optional[str] = None,
    ) -> None:
        parsed = await self._parse_program_preview(channel, raw_text)
        state = PendingProgram(
            user_id=author.id,
            channel_id=getattr(channel, "id", 0),
            raw_text=raw_text,
            parsed_program=parsed,
            created_at=self._now_utc(),
            temporary=temporary,
            parent_program_id=parent_program_id,
            expires_at=expires_at,
            inferred_name=self._infer_program_name_from_text(raw_text) or str(parsed.get("program_name") or "Imported Program"),
        )
        self._set_pending_program(state)
        await self._send_program_preview(channel, state.parsed_program, include_footer=True)

    def _is_confirm_message(self, text: str) -> bool:
        normalized = " ".join((text or "").strip().lower().split())
        return normalized in CONFIRM_TOKENS or normalized.startswith("save")

    def _is_cancel_message(self, text: str) -> bool:
        normalized = " ".join((text or "").strip().lower().split())
        return normalized in CANCEL_TOKENS

    def _sanitize_program_name(self, text: str, fallback: str) -> str:
        candidate = (text or "").strip()
        if not candidate:
            return fallback
        normalized = candidate.lower()
        if normalized in CONFIRM_TOKENS or normalized in CANCEL_TOKENS:
            return fallback
        return candidate[:80]

    def _display_program_id(self, program: Optional[dict[str, Any]]) -> str:
        if not program:
            return "n/a"
        value = program.get("display_id")
        if value is None:
            value = program.get("id")
        return str(value)

    async def _schedule_name_timeout(
        self,
        channel: discord.abc.Messageable,
        author: discord.abc.User,
        flow_id: str,
    ) -> None:
        async def _timeout() -> None:
            try:
                await asyncio.sleep(60)
                lock = self._get_user_lock(author.id)
                async with lock:
                    pending = self._get_pending_program(author.id, getattr(channel, "id", 0))
                    if not pending or pending.flow_id != flow_id or pending.stage != "await_name":
                        return
                    await send_discord_text(channel, f"No name provided within 60 seconds, so I'll use `{pending.inferred_name}`.")
                    await self._import_pending_program(channel, author, pending, pending.inferred_name)
            except asyncio.CancelledError:
                return
            finally:
                self.flow_timeout_tasks.pop(self._timeout_key(author.id, getattr(channel, "id", 0), "name"), None)

        self._schedule_timeout(author.id, getattr(channel, "id", 0), "name", asyncio.create_task(_timeout()))

    async def _schedule_day_timeout(self, channel: discord.abc.Messageable, user_id: int) -> None:
        channel_id = getattr(channel, "id", 0)

        async def _timeout() -> None:
            try:
                await asyncio.sleep(60)
                lock = self._get_user_lock(user_id)
                async with lock:
                    context = self._get_post_import_context(user_id, channel_id)
                    if not context:
                        return
                    await self.db.set_current_day_index(0, user_id=str(user_id))
                    day = await self.db.get_day_for_index(0, user_id=str(user_id))
                    day_name = str(day["name"]) if day else "Day 1"
                    await send_discord_text(channel, f"No day selected within 60 seconds, so I'm starting you on **{day_name}** (Day 1).")
                    self._clear_post_import_context(user_id, channel_id)
                    await send_discord_text(channel, "Program saved and ready. Head to your workout channel and type `ready` to start.")
            except asyncio.CancelledError:
                return
            finally:
                self.flow_timeout_tasks.pop(self._timeout_key(user_id, channel_id, "day"), None)

        self._schedule_timeout(user_id, channel_id, "day", asyncio.create_task(_timeout()))

    async def _begin_save_flow(
        self,
        channel: discord.abc.Messageable,
        author: discord.abc.User,
        pending: PendingProgram,
    ) -> None:
        pending.stage = "await_name"
        pending.created_at = self._now_utc()
        self._set_pending_program(pending)
        await send_discord_text(
            channel,
            f"What would you like to name this program? Reply in this channel within 60 seconds. (Default: `{pending.inferred_name}`)",
        )
        await self._schedule_name_timeout(channel, author, pending.flow_id)

    async def _start_day_prompt(self, channel: discord.abc.Messageable, program_id: int) -> None:
        days = await self.db.get_program_days(program_id)
        if not days:
            return
        lines = ["Which day would you like to start on?"]
        for day in days:
            lines.append(f"{day['day_order'] + 1}. {day['name']}")
        lines.append("Reply with `start on Legs` or `start on Day 3`.")
        await send_discord_text(channel, "\n".join(lines))

    async def _import_pending_program(
        self,
        channel: discord.abc.Messageable,
        author: discord.abc.User,
        pending: PendingProgram,
        program_name: str,
    ) -> None:
        final_name = self._sanitize_program_name(program_name, pending.inferred_name)
        payload = self._normalize_program_payload(
            {**pending.parsed_program, "program_name": final_name},
            fallback_name=final_name,
        )
        user_id = str(author.id)

        async with channel.typing():
            recent = await self.db.get_recent_program_by_name(payload["program_name"], user_id=user_id, minutes=5)
            if recent:
                self._clear_pending_program(pending.user_id, pending.channel_id)
                display_id = int(recent.get("display_id") or recent.get("id") or 0)
                await send_discord_text(
                    channel,
                    f"Skipped duplicate import: **{payload['program_name']}** was already imported recently (ID {display_id}).",
                )
                self._set_post_import_context(author.id, getattr(channel, "id", 0), int(recent["id"]), display_id)
                await self._start_day_prompt(channel, int(recent["id"]))
                await self._schedule_day_timeout(channel, author.id)
                return

            program_id = await self.db.create_program_from_payload(
                payload,
                user_id=user_id,
                temporary=pending.temporary,
                parent_program_id=pending.parent_program_id,
                expires_at=pending.expires_at,
            )
            created_program = await self.db.get_program_by_id(program_id)

        self._clear_pending_program(pending.user_id, pending.channel_id)
        display_id = int(created_program.get("display_id") or program_id) if created_program else program_id
        total_days = len(payload.get("days", []))
        total_exercises = sum(len(day.get("exercises", [])) for day in payload.get("days", []))
        await send_discord_text(
            channel,
            f"Imported **{payload['program_name']}** (ID {display_id}) with {total_days} days and {total_exercises} exercises.",
        )
        self._set_post_import_context(author.id, getattr(channel, "id", 0), program_id, display_id)
        await self._start_day_prompt(channel, program_id)
        await self._schedule_day_timeout(channel, author.id)

    def _parse_start_day_index(self, text: str, days: list[dict[str, Any]]) -> Optional[int]:
        plain_num = re.fullmatch(r"\s*(\d+)\s*", text)
        if plain_num:
            idx = int(plain_num.group(1)) - 1
            if 0 <= idx < len(days):
                return idx

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

    async def _handle_start_day_message(
        self,
        channel: discord.abc.Messageable,
        text: str,
        *,
        user_id: Optional[str] = None,
        program_id: Optional[int] = None,
        allow_implicit: bool = False,
    ) -> bool:
        if program_id is None:
            active = await self.db.get_active_program(user_id)
            if not active:
                return False
            program_id = int(active["id"])

        days = await self.db.get_program_days(program_id)
        if not days:
            return False

        lowered = text.lower()
        has_intent = bool(START_DAY_INTENT_RE.search(lowered) or DAY_NUMBER_RE.search(lowered))
        if not allow_implicit and not has_intent:
            return False

        idx = self._parse_start_day_index(text, days)
        if idx is None:
            lines = ["I couldn't map that to a program day. Try one of:"]
            for day in days:
                lines.append(f"{day['day_order'] + 1}. {day['name']}")
            await send_discord_text(channel, "\n".join(lines))
            return True

        await self.db.set_current_day_index(idx, user_id=user_id)
        selected = next((day for day in days if int(day["day_order"]) == idx), days[idx])
        await send_discord_text(channel, f"Starting day set to **{selected['name']}** (Day {idx + 1} of {len(days)}).")
        return True

    def _extract_equipment_type_reply(self, text: str) -> Optional[str]:
        lowered = " ".join((text or "").strip().lower().split())
        if not lowered:
            return None
        ordered = ["smith machine", "bodyweight", "barbell", "dumbbell", "cable", "machine", "unknown"]
        for choice in ordered:
            if choice in lowered:
                return choice
        if lowered in {"smith", "bw", "db"}:
            return self._normalize_equipment_type(lowered)
        return None

    async def _apply_active_swap(
        self,
        channel: discord.abc.Messageable,
        *,
        user_id: str,
        old_name: str,
        new_name: str,
        day_hint: Optional[str],
        equipment_type: str,
    ) -> None:
        category = self._db_category_from_equipment_type(new_name, equipment_type)
        replaced = await self.db.replace_exercise_in_active_program(
            old_name=old_name,
            new_name=new_name,
            user_id=user_id,
            day_name_hint=day_hint or None,
            new_category=category,
            new_equipment_type=equipment_type,
        )
        if not replaced:
            await send_discord_text(channel, f"Couldn't find `{old_name}` in the active program.")
            return
        active = await self.db.get_active_program(user_id)
        program_name = str(active["name"]) if active else "Active Program"
        display_id = self._display_program_id(active)
        await send_discord_text(
            channel,
            f"Updated {program_name} (ID {display_id}): replaced {replaced['old_name']} with {replaced['new_name']} ({replaced['equipment_type']}) on {replaced['day_name']}.",
        )

    async def _handle_pending_exercise_type_prompt(
        self,
        channel: discord.abc.Messageable,
        author: discord.abc.User,
        content: str,
    ) -> bool:
        prompt = self.pending_exercise_type_prompts.get(self._pending_key(author.id, getattr(channel, "id", 0)))
        if not prompt:
            return False
        if prompt.created_at + timedelta(minutes=5) < self._now_utc():
            self._clear_pending_exercise_type_prompt(author.id, getattr(channel, "id", 0))
            await send_discord_text(channel, "That exercise-type prompt expired. Ask for the swap again if you still want it.")
            return True
        equipment_type = self._extract_equipment_type_reply(content)
        if equipment_type is None:
            await send_discord_text(
                channel,
                "Reply with one type: `barbell`, `dumbbell`, `cable`, `machine`, `bodyweight`, or `smith machine`.",
            )
            return True
        self._clear_pending_exercise_type_prompt(author.id, getattr(channel, "id", 0))
        if prompt.exercise_id is not None:
            updated = await self.db.replace_exercise_in_active_program_by_id(
                int(prompt.exercise_id),
                user_id=str(author.id),
                new_name=prompt.new_name,
                new_category=self._db_category_from_equipment_type(prompt.new_name, equipment_type),
                new_equipment_type=equipment_type,
                new_sets=prompt.new_sets,
                new_rep_low=prompt.new_rep_low,
                new_rep_high=prompt.new_rep_high,
            )
            if not updated:
                await send_discord_text(channel, f"Couldn't replace `{prompt.old_name}` in the active program.")
                return True
            scheme = self._format_rep_scheme_from_values(
                sets=int(updated["new_sets"]),
                rep_low=updated["new_rep_low"],
                rep_high=updated["new_rep_high"],
            )
            type_label = self._category_label_from_db(str(updated["category"]), str(updated["equipment_type"]))
            await send_discord_text(
                channel,
                f"✅ {updated['old_name']} -> {updated['new_name']} ({scheme}) [{type_label}] on {updated['day_name']}.",
            )
            return True
        await self._apply_active_swap(
            channel,
            user_id=str(author.id),
            old_name=prompt.old_name,
            new_name=prompt.new_name,
            day_hint=prompt.day_hint,
            equipment_type=equipment_type,
        )
        return True

    async def _handle_index_based_correction(
        self,
        channel: discord.abc.Messageable,
        text: str,
        *,
        user_id: str,
    ) -> bool:
        """Handle batch index-based type corrections.

        Supports formats like:
          "1.1 1.2 are dumbbell, 1.4 is machine, 1.5, 1.6, 1.7 are cable"
          "switch 1.1 to dumbbell"
          "3.7 is bodyweight"
        """
        stripped = text.strip()
        if not INDEX_REF_DETECT_RE.search(stripped):
            return False

        # Split on commas to get segments, then accumulate index refs
        # until we hit a type keyword.
        segments = [s.strip() for s in stripped.split(",") if s.strip()]

        batch: list[tuple[list[tuple[int, int]], str]] = []
        pending_refs: list[tuple[int, int]] = []

        for segment in segments:
            refs = [(int(m.group("day")), int(m.group("ex"))) for m in INDEX_REF_PAIR_RE.finditer(segment)]
            pending_refs.extend(refs)

            type_match = EQUIPMENT_KEYWORD_RE.search(segment)
            if type_match and pending_refs:
                equip_type = type_match.group("type").strip().lower()
                batch.append((list(pending_refs), equip_type))
                pending_refs = []

        # Handle leftover refs with a trailing type from the whole message
        if pending_refs:
            type_match = EQUIPMENT_KEYWORD_RE.search(stripped)
            if type_match:
                equip_type = type_match.group("type").strip().lower()
                batch.append((list(pending_refs), equip_type))
                pending_refs = []

        if not batch and not pending_refs:
            return False

        if pending_refs:
            ref_strs = [f"{d}.{e}" for d, e in pending_refs]
            await send_discord_text(
                channel,
                f"I found references {', '.join(ref_strs)} but couldn't determine the type. "
                f"Try: `{ref_strs[0]} is dumbbell` or `{', '.join(ref_strs)} are cable`.",
            )
            return True

        program = await self.db.get_active_program(user_id)
        if not program:
            await send_discord_text(channel, "No active program.")
            return True

        days = await self.db.get_program_days(int(program["id"]))
        results: list[str] = []
        errors: list[str] = []
        day_exercises_cache: dict[int, list[dict[str, Any]]] = {}

        for refs, equip_type in batch:
            normalized_type = self._normalize_equipment_type(equip_type)
            if normalized_type == "unknown":
                ref_strs = [f"{d}.{e}" for d, e in refs]
                errors.append(f"Unknown type `{equip_type}` for {', '.join(ref_strs)}.")
                continue

            for day_num, ex_num in refs:
                if day_num < 1 or day_num > len(days):
                    errors.append(f"Day {day_num} doesn't exist (program has {len(days)} days).")
                    continue

                day = days[day_num - 1]
                day_id = int(day["id"])
                if day_id not in day_exercises_cache:
                    day_exercises_cache[day_id] = await self.db.get_exercises_for_day(day_id)
                exercises = day_exercises_cache[day_id]

                if ex_num < 1 or ex_num > len(exercises):
                    errors.append(f"{day_num}.{ex_num} doesn't exist ({day['name']} has {len(exercises)} exercises).")
                    continue

                exercise = exercises[ex_num - 1]
                exercise_name = str(exercise["name"])

                updated = await self.db.update_exercise_category(
                    exercise_name=exercise_name,
                    new_category=normalized_type,
                    user_id=user_id,
                )
                if updated:
                    results.append(
                        f"✅ {day_num}.{ex_num} {updated['exercise_name']}: "
                        f"`{updated['old_category']}` → `{updated['new_category']}`"
                    )
                else:
                    errors.append(f"Failed to update {exercise_name} ({day_num}.{ex_num}).")

        if results:
            await send_discord_text(channel, "\n".join(results))
        if errors:
            await send_discord_text(channel, "\n".join(errors))

        return True

    async def _handle_type_correction_request(
        self,
        channel: discord.abc.Messageable,
        text: str,
        *,
        user_id: str,
    ) -> bool:
        correction = self._extract_type_correction(text)
        if correction is None:
            return False
        exercise_name, new_category = correction
        updated = await self.db.update_exercise_category(
            exercise_name=exercise_name,
            new_category=new_category,
            user_id=user_id,
        )
        if not updated:
            await send_discord_text(channel, f"I couldn't find `{exercise_name}` in your active program.")
            return True
        await send_discord_text(
            channel,
            f"✅ Updated {updated['exercise_name']} from `{updated['old_category']}` -> `{updated['new_category']}`.",
        )
        return True

    async def _handle_simple_edit_request(
        self,
        channel: discord.abc.Messageable,
        text: str,
        *,
        user_id: str,
        requester_id: int,
    ) -> bool:
        swap_match = SWAP_RE.search(text)
        if swap_match:
            old_name = str(swap_match.group("old") or "").strip(" .")
            new_name = str(swap_match.group("new") or "").strip(" .")
            day_hint = str(swap_match.group("day") or "").strip(" .") or None

            existing_new = await self.db.get_exercise_by_name_in_current_program(new_name, user_id=user_id)
            if existing_new:
                equipment_type = self._normalize_equipment_type(str(existing_new.get("equipment_type") or ""))
                if equipment_type == "unknown":
                    equipment_type = self._exercise_type_from_name_and_category(new_name, str(existing_new.get("category") or ""))
                await self._apply_active_swap(
                    channel,
                    user_id=user_id,
                    old_name=old_name,
                    new_name=new_name,
                    day_hint=day_hint,
                    equipment_type=equipment_type,
                )
                return True

            fallback_type = self._exercise_type_from_name_and_category(new_name, self.parser._category_lookup(new_name))
            self.pending_exercise_type_prompts[self._pending_key(requester_id, getattr(channel, "id", 0))] = PendingExerciseTypePrompt(
                user_id=requester_id,
                channel_id=getattr(channel, "id", 0),
                old_name=old_name,
                new_name=new_name,
                day_hint=day_hint,
                created_at=self._now_utc(),
                fallback_type=fallback_type,
            )
            await send_discord_text(
                channel,
                f"`{new_name}` isn't in your current program yet. What type is it? Reply with `barbell`, `dumbbell`, `cable`, `machine`, `bodyweight`, or `smith machine`.",
            )
            return True

        change_match = CHANGE_RE.search(text)
        if change_match:
            old_name = change_match.group(1).strip(" .")
            new_name = change_match.group(2).strip(" .")
            day_rows = await self.db.rename_program_day_in_active_program(old_name, new_name, user_id=user_id)
            if day_rows > 0:
                await send_discord_text(channel, f"Renamed day: **{old_name}** -> **{new_name}**.")
                return True
            ex_rows = await self.db.update_exercise_name_in_active_program(old_name, new_name, user_id=user_id)
            if ex_rows > 0:
                await send_discord_text(channel, f"Renamed exercise: **{old_name}** -> **{new_name}**.")
            else:
                await send_discord_text(channel, "I couldn't apply that change directly. Try `swap <old exercise> with <new exercise>`.")
            return True

        return False

    async def _generate_travel_draft(self, channel: discord.abc.Messageable, base_program_text: str, travel_note: str) -> str:
        payload = {
            "base_program": base_program_text,
            "constraints": travel_note,
            "instruction": "Apply only the explicit travel or equipment constraints. Keep the same day structure and preserve rep schemes when possible. Return only program text with day headers and exercise lines.",
        }
        async with channel.typing():
            result = await self.bot.ollama.chat(
                system=(
                    f"{PROGRAMME_IMPORT_SYSTEM_PROMPT}\n"
                    "The user explicitly requested a temporary travel or limited-equipment version. "
                    "Apply only those constraints and return only the updated program text."
                ),
                user=json.dumps(payload, ensure_ascii=False),
                temperature=0.2,
                max_tokens=900,
            )
        cleaned = result.strip()
        return cleaned or base_program_text

    async def _start_travel_pending_program(
        self,
        channel: discord.abc.Messageable,
        author: discord.abc.User,
        *,
        travel_note: str,
    ) -> bool:
        user_id = str(author.id)
        active = await self.db.get_active_program(user_id)
        if not active:
            await send_discord_text(channel, "No active program to adapt yet. Paste a program first.")
            return False

        equipment_hints = {"dumbbell", "db", "cable", "machine", "smith", "barbell", "bodyweight", "bands", "kettlebell"}
        if not any(token in travel_note.lower() for token in equipment_hints):
            self.pending_travel_context[author.id] = (travel_note, self._now_utc() + timedelta(minutes=10))
            await send_discord_text(channel, "What equipment will you have while traveling? For example: `dumbbells up to 30kg and a cable machine`.")
            return False

        days = await self.db.get_program_days(int(active["id"]))
        lines: list[str] = []
        for day in days:
            lines.append(str(day.get("name") or f"Day {int(day.get('day_order', 0)) + 1}"))
            exercises = await self.db.get_exercises_for_day(int(day["id"]))
            for exercise in exercises:
                low = exercise.get("rep_range_low")
                high = exercise.get("rep_range_high")
                if low is not None and high is not None:
                    scheme = f"{exercise['sets']}x{low}" if int(low) == int(high) else f"{exercise['sets']}x{low}-{high}"
                else:
                    scheme = f"{exercise['sets']}xAMRAP"
                lines.append(f"{exercise['name']} - {scheme}")
            lines.append("")
        base_program_text = "\n".join(lines).strip()
        draft_text = await self._generate_travel_draft(channel, base_program_text, travel_note)
        expires_at = self._extract_travel_expiry(travel_note)
        await send_discord_text(channel, "I drafted a temporary version based on those constraints:")
        await self._start_pending_program(
            channel,
            author,
            draft_text,
            temporary=True,
            parent_program_id=int(active["id"]),
            expires_at=expires_at,
        )
        if expires_at:
            await send_discord_text(channel, f"This will save as a temporary program until **{expires_at}** once you reply `save`.")
        else:
            await send_discord_text(channel, "Reply with the trip duration too, like `1 week`, if you want it to auto-revert.")
        self.pending_travel_context.pop(author.id, None)
        return True

    async def _reply_programme_message(self, channel: discord.abc.Messageable) -> None:
        await send_discord_text(
            channel,
            "Paste a full program and I'll list it back exactly as written. You can also say `show`, `change 2.1 to 5x10`, `swap 2.1 with pull-ups`, or `2.1, 2.2 are cable. 2.6 is dumbbell`.",
        )

    @commands.command(name="import")
    async def import_program_command(self, ctx: commands.Context, *, text: str) -> None:
        if not self._is_programme_channel(ctx.channel):
            return
        lock = self._get_user_lock(ctx.author.id)
        async with lock:
            try:
                await self._start_pending_program(ctx.channel, ctx.author, text)
            except Exception as exc:
                await send_discord_text(ctx.channel, f"I couldn't parse that cleanly yet: {exc}")

    @commands.command(name="program")
    async def show_program_command(self, ctx: commands.Context) -> None:
        await self._send_active_program_summary(ctx.channel, str(ctx.author.id))

    @commands.command(name="startday")
    async def start_day_command(self, ctx: commands.Context, *, text: str) -> None:
        if not self._is_programme_channel(ctx.channel):
            return
        lock = self._get_user_lock(ctx.author.id)
        async with lock:
            context = self._get_post_import_context(ctx.author.id, ctx.channel.id)
            handled = await self._handle_start_day_message(
                ctx.channel,
                f"start on {text}",
                user_id=str(ctx.author.id),
                program_id=int(context["program_id"]) if context else None,
                allow_implicit=bool(context),
            )
            if not handled:
                await send_discord_text(ctx.channel, "Couldn't parse that day selection. Try `!startday Day 3` or `!startday Legs`.")
                return
            if context:
                self._clear_post_import_context(ctx.author.id, ctx.channel.id)
                await send_discord_text(ctx.channel, "Program saved and ready. Head to your workout channel and type `ready` to start.")

    @commands.command(name="travel")
    async def travel_program_command(self, ctx: commands.Context, *, text: str) -> None:
        if not self._is_programme_channel(ctx.channel):
            return
        lock = self._get_user_lock(ctx.author.id)
        async with lock:
            await self._start_travel_pending_program(ctx.channel, ctx.author, travel_note=text)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        if not self._is_programme_channel(message.channel):
            return

        content = message.content.strip()
        attachments = list(getattr(message, "attachments", []) or [])
        if not content and not attachments:
            return
        if content.startswith(self.settings.command_prefix):
            return

        user_id = message.author.id
        channel_id = message.channel.id
        lock = self._get_user_lock(user_id)
        async with lock:
            reverted = await self.db.check_and_revert_expired_temporary_program(str(user_id))
            if reverted:
                parent_name = reverted.get("parent_program_name") or "your previous program"
                display_id = reverted.get("parent_display_id")
                suffix = f" (ID {display_id})" if display_id else ""
                await send_discord_text(message.channel, f"Welcome back! Reverted to {parent_name}{suffix}.")

            logbook_attachment = next((attachment for attachment in attachments if self._attachment_looks_like_logbook(attachment)), None)
            if logbook_attachment is not None:
                imported = await self._import_structured_logbook_attachment(message.channel, message.author, logbook_attachment)
                if imported:
                    return

            pending_travel = self.pending_travel_context.get(user_id)
            if pending_travel:
                original_note, expires_at = pending_travel
                if expires_at < self._now_utc():
                    self.pending_travel_context.pop(user_id, None)
                elif not self._get_pending_program(user_id, channel_id) and not self._looks_like_program_paste(content):
                    combined_note = f"{original_note}. Equipment: {content}"
                    started = await self._start_travel_pending_program(message.channel, message.author, travel_note=combined_note)
                    if started:
                        return

            if BACK_INTENT_RE.search(content):
                reverted_now = await self.db.revert_from_temporary_program(str(user_id))
                if reverted_now:
                    active = await self.db.get_active_program(str(user_id))
                    if active:
                        day_index = await self.db.get_current_day_index(str(user_id))
                        day = await self.db.get_day_for_index(day_index, user_id=str(user_id))
                        day_name = str(day["name"]) if day else f"Day {day_index + 1}"
                        await send_discord_text(
                            message.channel,
                            f"Welcome back! Reverted to **{active['name']}** (ID {self._display_program_id(active)}). You left off on {day_name} (Day {day_index + 1}).",
                        )
                    else:
                        await send_discord_text(message.channel, "Welcome back! Reverted to your base program.")
                else:
                    await send_discord_text(message.channel, "No active temporary travel program found.")
                return

            if await self._handle_pending_exercise_type_prompt(message.channel, message.author, content):
                return

            pending = self._get_pending_program(user_id, channel_id)
            if pending:
                if self._is_cancel_message(content):
                    self._clear_programme_flow_state(user_id, channel_id)
                    await send_discord_text(message.channel, "Pending program import discarded.")
                    return
                if pending.stage == "await_name":
                    await self._import_pending_program(message.channel, message.author, pending, content)
                    return
                if self._is_confirm_message(content):
                    await self._begin_save_flow(message.channel, message.author, pending)
                    return
                try:
                    status, llm_message, updated_program = await self._apply_pending_edit_with_llm(message.channel, pending, content)
                except Exception as exc:
                    await send_discord_text(message.channel, f"I couldn't apply that edit cleanly yet: {exc}")
                    return
                if status == "updated" and updated_program is not None:
                    pending.parsed_program = updated_program
                    pending.raw_text = self._program_to_text(updated_program)
                    pending.created_at = self._now_utc()
                    self._set_pending_program(pending)
                    if llm_message:
                        await send_discord_text(message.channel, llm_message)
                    await self._send_program_preview(message.channel, pending.parsed_program, include_footer=True)
                    return
                if llm_message:
                    await send_discord_text(message.channel, llm_message)
                else:
                    await send_discord_text(message.channel, "Tell me the exact change you want and I'll apply only that.")
                return

            if self._looks_like_program_paste(content):
                try:
                    await self._start_pending_program(message.channel, message.author, content)
                except Exception:
                    await send_discord_text(
                        message.channel,
                        "I couldn't cleanly parse that yet. Re-send it with the day headers and exercise lines intact and I'll try again.",
                    )
                return

            if TRAVEL_INTENT_RE.search(content):
                started = await self._start_travel_pending_program(message.channel, message.author, travel_note=content)
                if started:
                    return

            context = self._get_post_import_context(user_id, channel_id)
            handled_start_day = await self._handle_start_day_message(
                message.channel,
                content,
                user_id=str(user_id),
                program_id=int(context["program_id"]) if context else None,
                allow_implicit=bool(context),
            )
            if handled_start_day:
                if context:
                    self._clear_post_import_context(user_id, channel_id)
                    await send_discord_text(message.channel, "Program saved and ready. Head to your workout channel and type `ready` to start.")
                return

            if await self._handle_active_programme_message_with_llm(message.channel, content, user_id=str(user_id)):
                return

            await self._reply_programme_message(message.channel)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ProgrammeCog(bot))
