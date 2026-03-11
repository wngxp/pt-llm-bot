from __future__ import annotations

from datetime import date


def is_streak_broken(last_workout_date: date | None, today: date) -> bool:
    if not last_workout_date:
        return False
    return (today - last_workout_date).days > 1


def format_streak(current_streak: int, longest_streak: int) -> str:
    return f"🔥 Streak: {current_streak} sessions | Longest: {longest_streak}"
