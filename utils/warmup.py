from __future__ import annotations

from typing import Optional


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
                sets.append(f"{w:g} x {rep}")
        return sets

    if category == "light_barbell":
        sets = ["bar x 10"]
        for pct, rep in ((0.5, 6), (0.75, 3)):
            w = rnd(working_weight * pct)
            if w > bar:
                sets.append(f"{w:g} x {rep}")
        return sets

    if category == "dumbbell":
        sets = []
        for pct, rep in ((0.5, 8), (0.75, 5)):
            w = rnd(working_weight * pct)
            if w > 0:
                sets.append(f"{w:g} x {rep}")
        return sets or None

    if category == "cable_machine":
        w = rnd(working_weight * 0.5)
        return [f"{w:g} x 10"] if w > 0 else None

    if category == "bodyweight":
        return ["1-2 easy ramp sets"]

    return None
