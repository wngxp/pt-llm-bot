from __future__ import annotations

import re

VALID_EQUIPMENT_TYPES = {"barbell", "dumbbell", "cable", "machine", "bodyweight", "smith machine", "unknown"}


def normalize_equipment_type(label: str) -> str:
    lowered = " ".join((label or "").strip().lower().split())
    if not lowered:
        return "unknown"
    if lowered in VALID_EQUIPMENT_TYPES:
        return lowered
    aliases = {
        "smith": "smith machine",
        "bw": "bodyweight",
        "body weight": "bodyweight",
        "db": "dumbbell",
        "free weight": "barbell",
        "bb": "barbell",
        "cables": "cable",
        "machines": "machine",
        "mach": "machine",
    }
    return aliases.get(lowered, "unknown")


def infer_equipment_type_from_name_and_category(exercise_name: str, category: str = "") -> str:
    lowered = exercise_name.lower()
    normalized_category = (category or "").strip().lower()

    if "smith" in lowered or normalized_category == "smith_machine":
        return "smith machine"

    bodyweight_tokens = (
        "pull-up",
        "pull up",
        "pullup",
        "chin-up",
        "chin up",
        "chinup",
        "push-up",
        "push up",
        "pushup",
        "dip",
        "diamond pushup",
        "leg raise",
        "hanging raise",
        "plank",
        "crunch",
        "sit-up",
        "sit up",
        "burpee",
        "lunge walk",
        "bodyweight",
        "bw ",
        "pistol squat",
        "muscle-up",
        "muscle up",
    )
    if any(token in lowered for token in bodyweight_tokens) or normalized_category == "bodyweight":
        return "bodyweight"

    dumbbell_tokens = (
        "dumbbell",
        "db ",
        "db-",
        "arnold press",
        "kroc",
        "hammer curl",
        "cross-body curl",
        "cross body curl",
        "concentration curl",
        "incline curl",
        "lateral raise",
        "front raise",
        "rear delt fly",
        "dumbbell row",
        "db row",
        "goblet squat",
        "dumbbell lunge",
        "walking lunge",
    )
    if any(token in lowered for token in dumbbell_tokens) or normalized_category == "dumbbell":
        return "dumbbell"

    cable_tokens = (
        "cable",
        "press-around",
        "press around",
        "face pull",
        "tricep pushdown",
        "pushdown",
        "overhead extension",
        "cross-body tricep",
        "cross body tricep",
        "cable fly",
        "cable crossover",
        "cable curl",
        "cable row",
        "cable pullover",
        "cable lateral",
        "rope",
        "v-bar",
        "straight bar curl",
    )
    if any(token in lowered for token in cable_tokens):
        return "cable"

    machine_tokens = (
        "machine",
        "pulldown",
        "lat pulldown",
        "lat pull-down",
        "leg press",
        "leg curl",
        "leg extension",
        "hack squat",
        "hip thrust",
        "seated row",
        "chest fly machine",
        "pec deck",
        "pec fly",
        "shoulder press machine",
        "seated calf",
        "toe press",
        "calf raise machine",
        "hip abduct",
        "hip adduct",
        "glute drive",
        "assisted",
        "preacher curl machine",
    )
    if any(token in lowered for token in machine_tokens):
        return "machine"

    if "ez-bar" in lowered or "ez bar" in lowered or "preacher curl" in lowered:
        return "barbell"

    if normalized_category in {"heavy_barbell", "light_barbell"}:
        return "barbell"
    return "unknown"


def db_category_from_equipment_type(exercise_name: str, equipment_type: str, fallback: str = "") -> str:
    normalized_type = normalize_equipment_type(equipment_type)
    normalized_fallback = (fallback or "").strip().lower()
    if normalized_type == "smith machine":
        return "smith_machine"
    if normalized_type == "dumbbell":
        return "dumbbell"
    if normalized_type == "bodyweight":
        return "bodyweight"
    if normalized_type in {"cable", "machine", "unknown"}:
        return "cable_machine"
    if normalized_fallback in {"heavy_barbell", "light_barbell"}:
        return normalized_fallback
    lowered_name = exercise_name.lower()
    heavy_tokens = ("squat", "bench", "deadlift", "overhead press", "ohp", "barbell row")
    return "heavy_barbell" if any(token in lowered_name for token in heavy_tokens) else "light_barbell"


def category_label_from_db(category: str, equipment_type: str = "") -> str:
    normalized_category = (category or "").strip().lower()
    normalized_type = normalize_equipment_type(equipment_type)
    if normalized_type == "smith machine" or normalized_category == "smith_machine":
        return "smith"
    if normalized_type == "machine":
        return "machine"
    if normalized_type == "cable":
        return "cable"
    if normalized_type == "dumbbell" or normalized_category == "dumbbell":
        return "dumbbell"
    if normalized_type == "bodyweight" or normalized_category == "bodyweight":
        return "bodyweight"
    if normalized_category in {"heavy_barbell", "light_barbell"}:
        return normalized_category
    return normalized_type or normalized_category or "unknown"


def normalize_name_token(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())
