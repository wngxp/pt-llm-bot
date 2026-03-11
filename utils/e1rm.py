from __future__ import annotations


def epley_1rm(weight: float, reps: int) -> float:
    return weight * (1 + reps / 30.0)
