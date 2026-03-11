from __future__ import annotations

from typing import Any


def suggest_weight(exercise: dict[str, Any], last_logs: list[dict[str, Any]]) -> str:
    if not last_logs:
        return "No history yet. Start with a conservative working weight you can control."

    last_weight = float(last_logs[0]["weight"])
    last_reps = int(last_logs[0]["reps"])
    rep_low = exercise.get("rep_range_low")
    rep_high = exercise.get("rep_range_high")

    if rep_low is None or rep_high is None:
        return f"Last set: {last_weight:g} × {last_reps}. Try to beat it with clean form."

    if last_reps >= rep_high:
        category = exercise.get("category", "")
        increment = 5.0 if category in {"heavy_barbell", "light_barbell", "dumbbell"} else 2.5
        return f"Try {last_weight + increment:g}, aim for {rep_low}-{rep_high} reps."

    if last_reps >= rep_low:
        next_reps = min(rep_high, last_reps + 1)
        return f"Keep {last_weight:g}, aim for {next_reps}-{rep_high} reps."

    return f"Repeat {last_weight:g}, aim for at least {rep_low} reps."


def apply_readiness_adjustment(base_weight: float, readiness: int) -> tuple[float, str]:
    if readiness >= 8:
        return base_weight, "Readiness high: push top of rep range or add a small load increase."
    if readiness >= 6:
        return base_weight, "Readiness normal: follow standard programming."
    if readiness >= 4:
        adjusted = round(base_weight * 0.93, 2)
        return adjusted, "Readiness moderate-low: reduce load ~5-10% and keep volume."

    adjusted = round(base_weight * 0.85, 2)
    return adjusted, "Readiness very low: consider technique-focused light work or rest."
