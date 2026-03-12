from __future__ import annotations

from utils.numbers import format_standard_number


def plates_breakdown(total_weight: float, unit: str = "lbs") -> str:
    bar = 45 if unit == "lbs" else 20
    if total_weight <= bar:
        return f"{format_standard_number(total_weight)} {unit} = just the bar ({bar})"

    per_side = (total_weight - bar) / 2
    plates = [45, 25, 10, 5, 2.5] if unit == "lbs" else [20, 10, 5, 2.5, 1.25]

    result: list[tuple[int, float]] = []
    remaining = per_side
    for plate in plates:
        count = int(remaining // plate)
        if count > 0:
            result.append((count, plate))
            remaining -= count * plate

    if unit == "lbs" and result == [(2, 45)]:
        return f"{format_standard_number(total_weight)} lbs = bar (45) + 2x45 per side"
    if unit == "lbs" and result == [(1, 45)]:
        return f"{format_standard_number(total_weight)} lbs = bar (45) + 1x45 per side"

    if not result:
        return f"{format_standard_number(total_weight)} {unit} = micro plates needed"

    parts = [f"{count}x{format_standard_number(weight)}" for count, weight in result]
    return (
        f"{format_standard_number(total_weight)} {unit} = "
        f"bar ({bar}) + {' + '.join(parts)} per side"
    )
