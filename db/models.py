from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(slots=True)
class Program:
    id: int
    name: str
    active: bool
    temporary: bool
    parent_program_id: Optional[int]
    expires_at: Optional[str]


@dataclass(slots=True)
class ProgramDay:
    id: int
    program_id: int
    day_order: int
    name: str


@dataclass(slots=True)
class Exercise:
    id: int
    program_day_id: int
    name: str
    display_order: int
    sets: int
    rep_range_low: Optional[int]
    rep_range_high: Optional[int]
    category: str
    superset_group: Optional[int]
    notes: Optional[str]
    muscle_groups: Optional[str]


@dataclass(slots=True)
class WorkoutLog:
    id: int
    exercise_id: int
    date: date
    set_number: int
    weight: float
    reps: int
    unit: str
    rir: Optional[int]
    notes: Optional[str]


@dataclass(slots=True)
class PersonalRecord:
    id: int
    exercise_name: str
    weight: float
    reps: int
    unit: str
    estimated_1rm: float
    date: date
    workout_log_id: Optional[int]
