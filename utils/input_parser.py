from __future__ import annotations

import re
from typing import Any, Optional

SET_PATTERN = re.compile(
    r"^(?:(?P<exercise>[a-zA-Z0-9 _\-']+?)\s+)?(?P<weight>\d+(?:\.\d+)?)\s*(?P<unit>kg|kgs|lb|lbs)?\s*[x×]\s*(?P<reps>\d+)(?:\s*@\s*(?P<rir>\d+))?$",
    re.IGNORECASE,
)

CUE_PATTERN = re.compile(r"(?:remind me to|cue:?|remember to)\s+(.+)$", re.IGNORECASE)
EXTEND_REST_PATTERN = re.compile(r"^extend\s+(?P<minutes>\d+)\s*(?:m|min|minute|minutes)$", re.IGNORECASE)


def normalize_unit(unit: Optional[str], fallback: str = "lbs") -> str:
    if not unit:
        return fallback
    if unit.lower().startswith("kg"):
        return "kg"
    return "lbs"


def parse_set_input(text: str) -> Optional[dict[str, Any]]:
    match = SET_PATTERN.match(text.strip())
    if not match:
        return None

    exercise = (match.group("exercise") or "").strip()
    weight = float(match.group("weight"))
    reps = int(match.group("reps"))
    rir = match.group("rir")
    return {
        "exercise": exercise or None,
        "weight": weight,
        "reps": reps,
        "unit": normalize_unit(match.group("unit")),
        "rir": int(rir) if rir is not None else None,
    }


def parse_cue(text: str) -> Optional[str]:
    match = CUE_PATTERN.search(text.strip())
    if not match:
        return None
    cue = match.group(1).strip().strip(".")
    return cue if cue else None


def parse_extend_rest(text: str) -> Optional[int]:
    match = EXTEND_REST_PATTERN.match(text.strip())
    if not match:
        return None
    return int(match.group("minutes"))
