from __future__ import annotations

from typing import Optional

from utils.numbers import format_standard_number


def generate_warmup(working_weight: float, category: str, unit: str = "lbs") -> Optional[list[str]]:
    bar = 45 if unit == "lbs" else 20
    round_to = 5 if unit == "lbs" else 2.5

    def rnd(weight: float) -> float:
        return round(weight / round_to) * round_to

    if working_weight <= 0:
        return None

    if category == "heavy_barbell":
        sets = ["bar x 12"]
        for pct, rep in ((0.4, 8), (0.6, 5), (0.75, 3), (0.9, 1)):
            w = rnd(working_weight * pct)
            if w > bar:
                sets.append(f"{format_standard_number(w)} x {rep}")
        return sets

    if category == "light_barbell":
        sets = ["bar x 10"]
        for pct, rep in ((0.5, 6), (0.75, 3)):
            w = rnd(working_weight * pct)
            if w > bar:
                sets.append(f"{format_standard_number(w)} x {rep}")
        return sets

    if category == "dumbbell":
        sets = []
        for pct, rep in ((0.5, 8), (0.75, 5)):
            w = rnd(working_weight * pct)
            if w > 0:
                sets.append(f"{format_standard_number(w)} x {rep}")
        return sets or None

    if category == "cable_machine":
        w = rnd(working_weight * 0.5)
        return [f"{format_standard_number(w)} x 10"] if w > 0 else None

    if category == "bodyweight":
        return ["1-2 easy ramp sets"]

    return None


def generate_pyramid_warmup(working_weight: float, warmup_sets: int, unit: str = "lbs") -> Optional[list[str]]:
    if working_weight <= 0:
        return None

    count = max(1, min(4, int(warmup_sets or 0)))
    step = 1.0 if str(unit).lower().startswith("kg") else 2.5
    schemes = {
        1: [(0.60, "6-10")],
        2: [(0.50, "6-10"), (0.70, "4-6")],
        3: [(0.45, "6-10"), (0.65, "4-6"), (0.85, "3-4")],
        4: [(0.45, "6-10"), (0.60, "4-6"), (0.75, "3-5"), (0.85, "2-4")],
    }

    def rnd(weight: float) -> float:
        return round(weight / step) * step

    lines: list[str] = []
    for index, (pct, rep_target) in enumerate(schemes[count], start=1):
        rounded = rnd(working_weight * pct)
        rep_text = rep_target.replace("-", "–")
        lines.append(f"Warm-up set {index}: ~{format_standard_number(rounded)} {unit} × {rep_text} reps")
    return lines
