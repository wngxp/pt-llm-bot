from __future__ import annotations

from collections import Counter
import re
from typing import Any, Optional

from llm.client import OllamaClient
from llm.prompts import PROGRAM_PARSER_SYSTEM_PROMPT


REP_RANGE_RE = re.compile(r"(?P<sets>\d+)\s*[xX×]\s*(?P<low>\d+)(?:\s*[-–]\s*(?P<high>\d+))?")
DAY_HEADER_RE = re.compile(
    r"^(?:day\s*\d+\b.*|(?:push|pull|legs?|upper|lower)\b(?:\s*day)?\b.*)$",
    re.IGNORECASE,
)
BULLET_PREFIX_RE = re.compile(r"^(?:[-*•]+|\d+[\).])\s*")
NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")

KNOWN_CATEGORY_KEYWORDS: list[tuple[tuple[str, ...], str]] = [
    (("romanian deadlift", "rdl", "pause squat", "larsen", "sldl", "ez-bar curl", "ez bar curl"), "light_barbell"),
    (("back squat", "front squat", "squat", "bench press", "deadlift", "overhead press", "ohp", "barbell row"), "heavy_barbell"),
    (("pull-up", "pull up", "chin-up", "chin up", "push-up", "push up", "dip"), "bodyweight"),
    (("lat pulldown", "pulldown", "leg press", "machine", "cable", "row machine", "leg curl", "leg extension"), "cable_machine"),
    (("dumbbell", "db ", " db"), "dumbbell"),
]

VALID_CATEGORIES = {"heavy_barbell", "light_barbell", "dumbbell", "cable_machine", "bodyweight"}


class ProgramParser:
    def __init__(self, client: OllamaClient) -> None:
        self.client = client

    async def parse_program(self, raw_program_text: str) -> dict[str, Any]:
        if not raw_program_text.strip():
            raise ValueError("Program text is empty")

        try:
            payload = await self.client.chat_json(
                system=PROGRAM_PARSER_SYSTEM_PROMPT,
                user=raw_program_text,
                temperature=0.0,
            )
            normalized = self._normalize_program(payload)
            return self._post_process_program(normalized, raw_program_text)
        except Exception:
            fallback = self._fallback_parse(raw_program_text)
            normalized = self._normalize_program(fallback)
            return self._post_process_program(normalized, raw_program_text)

    def _post_process_program(self, parsed: dict[str, Any], raw_text: str) -> dict[str, Any]:
        day_blocks = self._extract_day_blocks(raw_text)
        if day_blocks:
            parsed = self._rebuild_from_day_blocks(parsed, day_blocks)
            parsed = self._repair_duplicate_names_from_raw(parsed, day_blocks)

        for day in parsed["days"]:
            for ex in day["exercises"]:
                self._apply_category_fallback(ex)
                self._flag_or_repair_rep_ranges(ex, raw_text)
            self._flag_suspicious_adjacent_duplicates(day)

        parsed["days"].sort(key=lambda d: d["day_order"])
        for idx, day in enumerate(parsed["days"]):
            day["day_order"] = idx
            for ex_idx, ex in enumerate(day["exercises"]):
                ex["display_order"] = ex_idx
        return parsed

    def _repair_duplicate_names_from_raw(
        self,
        parsed: dict[str, Any],
        blocks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        for day_idx, day in enumerate(parsed["days"]):
            if day_idx >= len(blocks):
                break
            block = blocks[day_idx]
            parsed_exercises = day.get("exercises", [])
            raw_exercises = block.get("exercises", [])
            if not parsed_exercises or not raw_exercises:
                continue

            raw_norms = [self._normalize_name(ex.get("name", "")) for ex in raw_exercises]
            parsed_norms = [self._normalize_name(ex.get("name", "")) for ex in parsed_exercises]
            raw_counter = Counter(raw_norms)
            parsed_counter = Counter(parsed_norms)

            missing_norms: list[str] = []
            for norm, count in raw_counter.items():
                missing = count - parsed_counter.get(norm, 0)
                if missing > 0:
                    missing_norms.extend([norm] * missing)
            if not missing_norms:
                continue

            extra_indexes: list[int] = []
            seen_counter: Counter[str] = Counter()
            for idx, norm in enumerate(parsed_norms):
                seen_counter[norm] += 1
                if seen_counter[norm] > raw_counter.get(norm, 0):
                    extra_indexes.append(idx)

            if not extra_indexes:
                continue

            used_raw_indexes: set[int] = set()
            for idx in extra_indexes:
                if not missing_norms:
                    break
                target_norm = missing_norms.pop(0)
                raw_idx = self._find_raw_index_by_norm(raw_exercises, target_norm, used_raw_indexes)
                if raw_idx is None:
                    continue
                used_raw_indexes.add(raw_idx)
                raw_ex = raw_exercises[raw_idx]
                parsed_ex = parsed_exercises[idx]

                parsed_ex["name"] = str(raw_ex.get("name") or parsed_ex.get("name") or "").strip()
                parsed_ex["sets"] = int(raw_ex.get("sets") or parsed_ex.get("sets") or 1)
                parsed_ex["rep_range_low"] = raw_ex.get("rep_range_low")
                parsed_ex["rep_range_high"] = raw_ex.get("rep_range_high")
                note = str(parsed_ex.get("notes") or "").strip()
                repair_tag = "parse_repaired_duplicate_name"
                parsed_ex["notes"] = f"{note}; {repair_tag}".strip("; ")
                self._apply_category_fallback(parsed_ex)

        return parsed

    def _find_raw_index_by_norm(
        self,
        raw_exercises: list[dict[str, Any]],
        target_norm: str,
        used: set[int],
    ) -> Optional[int]:
        for idx, raw in enumerate(raw_exercises):
            if idx in used:
                continue
            if self._normalize_name(raw.get("name", "")) == target_norm:
                return idx
        return None

    def _flag_suspicious_adjacent_duplicates(self, day: dict[str, Any]) -> None:
        exercises = day.get("exercises", [])
        for idx in range(1, len(exercises)):
            prev = exercises[idx - 1]
            curr = exercises[idx]
            same_name = self._normalize_name(prev.get("name", "")) == self._normalize_name(curr.get("name", ""))
            same_sets = int(prev.get("sets", 0) or 0) == int(curr.get("sets", 0) or 0)
            same_low = prev.get("rep_range_low") == curr.get("rep_range_low")
            same_high = prev.get("rep_range_high") == curr.get("rep_range_high")
            if not (same_name and same_sets and same_low and same_high):
                continue
            note = str(curr.get("notes") or "").strip()
            warning = "parse_warning: duplicate adjacent exercise name/scheme"
            if warning not in note.lower():
                curr["notes"] = f"{note}; {warning}".strip("; ")

    def _normalize_program(self, payload: dict[str, Any]) -> dict[str, Any]:
        program_name = str(payload.get("program_name") or "Imported Program").strip()
        days_in = payload.get("days")
        if not isinstance(days_in, list) or not days_in:
            raise ValueError("No days found in parsed program")

        normalized_days: list[dict[str, Any]] = []
        for day_idx, day in enumerate(days_in):
            exercises_in = day.get("exercises") if isinstance(day, dict) else []
            if not isinstance(exercises_in, list):
                exercises_in = []

            normalized_exercises: list[dict[str, Any]] = []
            for ex_idx, ex in enumerate(exercises_in):
                if not isinstance(ex, dict):
                    continue
                normalized_exercises.append(
                    {
                        "name": str(ex.get("name") or f"Exercise {ex_idx + 1}").strip(),
                        "display_order": int(ex.get("display_order", ex_idx)),
                        "sets": max(1, int(ex.get("sets", 1))),
                        "rep_range_low": self._int_or_none(ex.get("rep_range_low")),
                        "rep_range_high": self._int_or_none(ex.get("rep_range_high")),
                        "category": str(ex.get("category") or "cable_machine").strip(),
                        "superset_group": self._int_or_none(ex.get("superset_group")),
                        "muscle_groups": str(ex.get("muscle_groups") or "").strip(),
                        "notes": str(ex.get("notes") or "").strip(),
                    }
                )

            normalized_days.append(
                {
                    "day_order": int(day.get("day_order", day_idx)) if isinstance(day, dict) else day_idx,
                    "name": str(day.get("name") or f"Day {day_idx + 1}").strip() if isinstance(day, dict) else f"Day {day_idx + 1}",
                    "exercises": normalized_exercises,
                }
            )

        normalized_days.sort(key=lambda d: d["day_order"])
        for idx, day in enumerate(normalized_days):
            day["day_order"] = idx
        return {
            "program_name": program_name or "Imported Program",
            "days": normalized_days,
        }

    def _fallback_parse(self, text: str) -> dict[str, Any]:
        blocks = self._extract_day_blocks(text)
        if not blocks:
            raise ValueError("Unable to parse program")

        days: list[dict[str, Any]] = []
        for idx, block in enumerate(blocks):
            exercises: list[dict[str, Any]] = []
            for ex_idx, ex in enumerate(block["exercises"]):
                exercises.append(
                    {
                        "name": ex["name"],
                        "display_order": ex_idx,
                        "sets": ex["sets"],
                        "rep_range_low": ex["rep_range_low"],
                        "rep_range_high": ex["rep_range_high"],
                        "category": self._category_lookup(ex["name"]),
                        "superset_group": None,
                        "muscle_groups": "",
                        "notes": ex["notes"],
                    }
                )
            days.append({"day_order": idx, "name": block["name"], "exercises": exercises})

        return {
            "program_name": "Imported Program",
            "days": days,
        }

    def _rebuild_from_day_blocks(self, parsed: dict[str, Any], blocks: list[dict[str, Any]]) -> dict[str, Any]:
        llm_days = parsed.get("days", [])
        rebuilt_days: list[dict[str, Any]] = []

        for idx, block in enumerate(blocks):
            llm_day = llm_days[idx] if idx < len(llm_days) else {"exercises": []}
            llm_exercises = llm_day.get("exercises", [])
            rebuilt_exercises: list[dict[str, Any]] = []
            used_llm_indexes: set[int] = set()

            for ex_idx, raw_ex in enumerate(block["exercises"]):
                match_idx, match = self._find_matching_exercise(raw_ex["name"], llm_exercises)
                if match_idx is not None:
                    used_llm_indexes.add(match_idx)
                rebuilt_exercises.append(
                    self._compose_exercise(
                        raw_ex,
                        match,
                        display_order=ex_idx,
                    )
                )

            rebuilt_days.append(
                {
                    "day_order": idx,
                    "name": block["name"] or llm_day.get("name") or f"Day {idx + 1}",
                    "exercises": rebuilt_exercises,
                }
            )

        return {"program_name": parsed["program_name"], "days": rebuilt_days}

    def _merge_missing_exercises(self, parsed: dict[str, Any], blocks: list[dict[str, Any]]) -> dict[str, Any]:
        days_out: list[dict[str, Any]] = []
        for idx, day in enumerate(parsed["days"]):
            block = blocks[idx]
            exercises = day.get("exercises", [])
            existing_keys = {self._normalize_name(ex.get("name", "")) for ex in exercises}

            for raw_ex in block["exercises"]:
                key = self._normalize_name(raw_ex["name"])
                if key in existing_keys:
                    continue
                exercises.append(self._compose_exercise(raw_ex, None, display_order=len(exercises)))
                existing_keys.add(key)

            repaired: list[dict[str, Any]] = []
            for ex in exercises:
                match_idx, raw_match = self._find_matching_raw_exercise(ex.get("name", ""), block["exercises"])
                repaired.append(self._compose_exercise(raw_match, ex, display_order=len(repaired)))
            days_out.append({"day_order": idx, "name": day.get("name") or block["name"], "exercises": repaired})

        return {"program_name": parsed["program_name"], "days": days_out}

    def _compose_exercise(
        self,
        raw_ex: Optional[dict[str, Any]],
        llm_ex: Optional[dict[str, Any]],
        *,
        display_order: int,
    ) -> dict[str, Any]:
        name = ""
        if raw_ex and raw_ex.get("name"):
            name = str(raw_ex["name"]).strip()
        elif llm_ex and llm_ex.get("name"):
            name = str(llm_ex["name"]).strip()
        if not name:
            name = f"Exercise {display_order + 1}"

        raw_notes = str(raw_ex.get("notes") or "") if raw_ex else ""
        llm_notes = str(llm_ex.get("notes") or "") if llm_ex else ""
        notes = "; ".join([n for n in [llm_notes, raw_notes] if n]).strip()

        rep_low = raw_ex.get("rep_range_low") if raw_ex else None
        rep_high = raw_ex.get("rep_range_high") if raw_ex else None
        sets = raw_ex.get("sets") if raw_ex else None

        if sets is None and llm_ex:
            sets = self._int_or_none(llm_ex.get("sets"))
        if rep_low is None and llm_ex:
            rep_low = self._int_or_none(llm_ex.get("rep_range_low"))
        if rep_high is None and llm_ex:
            rep_high = self._int_or_none(llm_ex.get("rep_range_high"))

        if sets is None:
            sets = 1

        category = ""
        if llm_ex:
            category = str(llm_ex.get("category") or "").strip()
        category = self._category_lookup(name, fallback=category)

        exercise = {
            "name": name,
            "display_order": display_order,
            "sets": max(1, int(sets)),
            "rep_range_low": rep_low,
            "rep_range_high": rep_high,
            "category": category,
            "superset_group": self._int_or_none(llm_ex.get("superset_group")) if llm_ex else None,
            "muscle_groups": str(llm_ex.get("muscle_groups") or "").strip() if llm_ex else "",
            "notes": notes,
        }
        return exercise

    def _flag_or_repair_rep_ranges(self, exercise: dict[str, Any], raw_text: str) -> None:
        low = exercise.get("rep_range_low")
        high = exercise.get("rep_range_high")
        notes = str(exercise.get("notes") or "")
        if low is not None or high is not None:
            return
        if "amrap" in notes.lower():
            return

        inferred = self._infer_rep_scheme_from_raw(exercise.get("name", ""), raw_text)
        if inferred:
            exercise["sets"], exercise["rep_range_low"], exercise["rep_range_high"] = inferred
            existing = notes.strip()
            exercise["notes"] = f"{existing}; parse_repaired_rep_range".strip("; ")
            return

        existing = notes.strip()
        warning = "parse_warning: missing rep range in source"
        if warning not in existing.lower():
            exercise["notes"] = f"{existing}; {warning}".strip("; ")

    def _apply_category_fallback(self, exercise: dict[str, Any]) -> None:
        name = str(exercise.get("name") or "")
        parsed_category = str(exercise.get("category") or "")
        exercise["category"] = self._category_lookup(name, fallback=parsed_category)

    def _category_lookup(self, name: str, fallback: str = "") -> str:
        lower_name = name.lower()
        for keywords, category in KNOWN_CATEGORY_KEYWORDS:
            if any(keyword in lower_name for keyword in keywords):
                return category
        if fallback in VALID_CATEGORIES:
            return fallback
        return "cable_machine"

    def _find_matching_exercise(
        self,
        raw_name: str,
        llm_exercises: list[dict[str, Any]],
    ) -> tuple[Optional[int], Optional[dict[str, Any]]]:
        raw_key = self._normalize_name(raw_name)
        for idx, ex in enumerate(llm_exercises):
            ex_key = self._normalize_name(ex.get("name", ""))
            if raw_key == ex_key or (raw_key and raw_key in ex_key) or (ex_key and ex_key in raw_key):
                return idx, ex
        return None, None

    def _find_matching_raw_exercise(
        self,
        name: str,
        raw_exercises: list[dict[str, Any]],
    ) -> tuple[Optional[int], Optional[dict[str, Any]]]:
        target = self._normalize_name(name)
        for idx, ex in enumerate(raw_exercises):
            key = self._normalize_name(ex.get("name", ""))
            if target == key or (target and target in key) or (key and key in target):
                return idx, ex
        return None, None

    def _infer_rep_scheme_from_raw(self, exercise_name: str, raw_text: str) -> Optional[tuple[int, int, int]]:
        target = self._normalize_name(exercise_name)
        if not target:
            return None

        for line in raw_text.splitlines():
            cleaned = line.strip()
            if not cleaned:
                continue
            if target not in self._normalize_name(cleaned):
                continue
            parsed = self._parse_exercise_line(cleaned)
            if not parsed:
                continue
            low = parsed.get("rep_range_low")
            high = parsed.get("rep_range_high")
            sets = parsed.get("sets")
            if sets and low is not None and high is not None:
                return int(sets), int(low), int(high)
        return None

    def _extract_day_blocks(self, text: str) -> list[dict[str, Any]]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        blocks: list[dict[str, Any]] = []
        current: Optional[dict[str, Any]] = None

        for line in lines:
            maybe_header = self._detect_day_header(line)
            if maybe_header:
                if current:
                    blocks.append(current)
                current = {"name": maybe_header, "exercises": []}
                continue

            parsed_ex = self._parse_exercise_line(line)
            if not parsed_ex:
                continue

            if current is None:
                current = {"name": "Day 1", "exercises": []}
            current["exercises"].append(parsed_ex)

        if current:
            blocks.append(current)

        # Reject tiny accidental parses.
        valid_blocks = [b for b in blocks if b["exercises"]]
        return valid_blocks

    def _detect_day_header(self, line: str) -> Optional[str]:
        cleaned = BULLET_PREFIX_RE.sub("", line).strip("-: ")
        if not cleaned:
            return None
        if REP_RANGE_RE.search(cleaned) or "amrap" in cleaned.lower():
            return None
        if len(cleaned.split()) > 8:
            return None
        if not DAY_HEADER_RE.match(cleaned):
            return None
        return cleaned

    def _parse_exercise_line(self, line: str) -> Optional[dict[str, Any]]:
        cleaned = BULLET_PREFIX_RE.sub("", line).strip()
        if not cleaned:
            return None

        amrap_match = re.search(r"(?P<sets>\d+)\s*[xX×]\s*amrap", cleaned, flags=re.IGNORECASE)
        if amrap_match:
            sets = int(amrap_match.group("sets"))
            name = cleaned[: amrap_match.start()].strip(" -–:")
            if not name:
                return None
            return {
                "name": name,
                "sets": sets,
                "rep_range_low": None,
                "rep_range_high": None,
                "notes": "AMRAP",
            }

        scheme = REP_RANGE_RE.search(cleaned)
        if not scheme:
            return None

        sets = int(scheme.group("sets"))
        low = int(scheme.group("low"))
        high = int(scheme.group("high") or scheme.group("low"))

        name = cleaned[: scheme.start()].strip(" -–:")
        if not name:
            # Handle reverse ordering like "2x8 Bench Press"
            tail = cleaned[scheme.end() :].strip(" -–:")
            name = tail
        if not name:
            return None

        return {
            "name": name,
            "sets": sets,
            "rep_range_low": low,
            "rep_range_high": high,
            "notes": "",
        }

    @staticmethod
    def _normalize_name(value: str) -> str:
        lowered = value.strip().lower()
        return NON_ALNUM_RE.sub("", lowered)

    @staticmethod
    def _int_or_none(value: Any) -> int | None:
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None
