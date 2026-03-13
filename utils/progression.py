from __future__ import annotations

from typing import Any

from utils.numbers import format_standard_number, round_training_weight


def suggest_weight(
    exercise: dict[str, Any],
    last_logs: list[dict[str, Any]],
    *,
    adjustment_multiplier: float = 1.0,
) -> str:
    if not last_logs:
        return "No history yet. Start with a conservative working weight you can control."

    last_weight = float(last_logs[0]["weight"])
    last_reps = int(last_logs[0]["reps"])
    unit = str(last_logs[0].get("unit") or "lbs")
    rep_low = exercise.get("rep_range_low")
    rep_high = exercise.get("rep_range_high")

    def _adj(weight: float) -> float:
        return round_training_weight(weight * adjustment_multiplier, unit)

    if rep_low is None or rep_high is None:
        adjusted = _adj(last_weight)
        return f"Last set: {format_standard_number(last_weight)} x {last_reps}. Try around {format_standard_number(adjusted)} with clean form."

    if last_reps >= rep_high:
        category = exercise.get("category", "")
        increment = 5.0 if category in {"heavy_barbell", "light_barbell", "smith_machine", "dumbbell"} else 2.5
        target = _adj(last_weight + increment)
        return f"Try {format_standard_number(target)}, aim for {rep_low}-{rep_high} reps."

    if last_reps >= rep_low:
        next_reps = min(rep_high, last_reps + 1)
        target = _adj(last_weight)
        return f"Keep {format_standard_number(target)}, aim for {next_reps}-{rep_high} reps."

    target = _adj(last_weight)
    return f"Repeat {format_standard_number(target)}, aim for at least {rep_low} reps."


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
