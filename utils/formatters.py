from __future__ import annotations

from datetime import date
from typing import Any, Optional

from utils.numbers import format_standard_number

def format_rep_target(low: Optional[int], high: Optional[int]) -> str:
    if low is None or high is None:
        return "AMRAP"
    if low == high:
        return str(low)
    return f"{low}-{high}"


def format_exercise_brief(exercise: dict[str, Any]) -> str:
    target = format_rep_target(exercise.get("rep_range_low"), exercise.get("rep_range_high"))
    return (
        f"**{exercise['name']}** ({exercise['sets']} sets x {target})\n"
        f"Category: {exercise.get('category', 'cable_machine').replace('_', ' ')}"
    )


def format_set_log(exercise_name: str, weight: float, reps: int, unit: str, set_number: int) -> str:
    return f"✅ Set {set_number}: {exercise_name} — {format_standard_number(weight)} {unit} x {reps}"


def format_pr_message(
    exercise_name: str,
    weight: float,
    reps: int,
    unit: str,
    e1rm: float,
    previous: dict[str, Any] | None,
) -> str:
    line = (
        f"🏆 NEW PR! {exercise_name} "
        f"{format_standard_number(weight)}{unit} x {reps} (e1RM: {format_standard_number(e1rm)})"
    )
    if not previous:
        return line
    prev_date = previous.get("date", "unknown")
    return (
        f"{line}\n"
        f"Previous best: {format_standard_number(float(previous['weight']))}{previous['unit']} x {previous['reps']} "
        f"(e1RM: {format_standard_number(float(previous['estimated_1rm']))}) on {prev_date}"
    )


def format_week_window(start: date, end: date) -> str:
    return f"{start.isoformat()} - {end.isoformat()}"
