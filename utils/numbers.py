from __future__ import annotations

import math


def round_training_weight(value: float, unit: str) -> float:
    step = 2.5 if str(unit).lower().startswith("kg") else 5.0
    if value == 0:
        return 0.0
    rounded = round(value / step) * step
    return round(rounded, 1)


def format_standard_number(value: float, decimals: int = 1) -> str:
    if not math.isfinite(value):
        return "0"
    rounded = round(float(value), max(0, decimals))
    if float(rounded).is_integer():
        return str(int(rounded))
    return f"{rounded:.{decimals}f}"
