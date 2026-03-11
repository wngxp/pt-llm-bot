from __future__ import annotations


def plates_breakdown(total_weight: float, unit: str = "lbs") -> str:
    bar = 45 if unit == "lbs" else 20
    if total_weight <= bar:
        return "just the bar"

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
        return "2 plates per side"
    if unit == "lbs" and result == [(1, 45)]:
        return "1 plate per side"

    if not result:
        return "micro plates needed"

    parts = [f"{count}x{weight:g}" for count, weight in result]
    return " + ".join(parts) + " per side"
