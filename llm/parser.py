from __future__ import annotations

import json
import re
from typing import Any

from llm.client import OllamaClient
from llm.prompts import PROGRAM_PARSER_SYSTEM_PROMPT


REP_RANGE_RE = re.compile(r"(?P<sets>\d+)\s*[xX×]\s*(?P<low>\d+)(?:\s*[-–]\s*(?P<high>\d+))?")


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
            return self._normalize_program(payload)
        except Exception:
            fallback = self._fallback_parse(raw_program_text)
            return self._normalize_program(fallback)

    def _normalize_program(self, payload: dict[str, Any]) -> dict[str, Any]:
        program_name = str(payload.get("program_name") or "Imported Program")
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
                        "name": str(ex.get("name") or f"Exercise {ex_idx + 1}"),
                        "display_order": int(ex.get("display_order", ex_idx)),
                        "sets": int(ex.get("sets", 1)),
                        "rep_range_low": self._int_or_none(ex.get("rep_range_low")),
                        "rep_range_high": self._int_or_none(ex.get("rep_range_high")),
                        "category": str(ex.get("category") or "cable_machine"),
                        "superset_group": self._int_or_none(ex.get("superset_group")),
                        "muscle_groups": str(ex.get("muscle_groups") or ""),
                        "notes": str(ex.get("notes") or ""),
                    }
                )

            normalized_days.append(
                {
                    "day_order": int(day.get("day_order", day_idx)) if isinstance(day, dict) else day_idx,
                    "name": str(day.get("name") or f"Day {day_idx + 1}") if isinstance(day, dict) else f"Day {day_idx + 1}",
                    "exercises": normalized_exercises,
                }
            )

        normalized_days.sort(key=lambda d: d["day_order"])
        for idx, day in enumerate(normalized_days):
            day["day_order"] = idx
        return {
            "program_name": program_name,
            "days": normalized_days,
        }

    def _fallback_parse(self, text: str) -> dict[str, Any]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        days: list[dict[str, Any]] = []
        current_day: dict[str, Any] | None = None

        for line in lines:
            if line.lower().startswith(("day ", "push", "pull", "legs", "upper", "lower")) and len(line.split()) <= 6:
                if current_day:
                    days.append(current_day)
                current_day = {
                    "day_order": len(days),
                    "name": line,
                    "exercises": [],
                }
                continue

            if current_day is None:
                current_day = {
                    "day_order": 0,
                    "name": "Day 1",
                    "exercises": [],
                }

            sets, low, high = self._extract_scheme(line)
            name = line
            if sets is not None:
                name = line.split(maxsplit=1)[0] if line[0].isdigit() else line.rsplit(" ", 1)[0]
            current_day["exercises"].append(
                {
                    "name": name.strip("-: "),
                    "display_order": len(current_day["exercises"]),
                    "sets": sets or 3,
                    "rep_range_low": low,
                    "rep_range_high": high,
                    "category": "cable_machine",
                    "superset_group": None,
                    "muscle_groups": "",
                    "notes": "fallback parse",
                }
            )

        if current_day:
            days.append(current_day)

        if not days:
            raise ValueError("Unable to parse program")

        return {
            "program_name": "Imported Program",
            "days": days,
        }

    @staticmethod
    def _extract_scheme(line: str) -> tuple[int | None, int | None, int | None]:
        match = REP_RANGE_RE.search(line)
        if not match:
            return None, None, None
        sets = int(match.group("sets"))
        low = int(match.group("low"))
        high = int(match.group("high") or low)
        return sets, low, high

    @staticmethod
    def _int_or_none(value: Any) -> int | None:
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None
