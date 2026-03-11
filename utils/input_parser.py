from __future__ import annotations

import re
from typing import Any, Optional

PREFIX_PATTERN = re.compile(
    r"^(?:i\s+hit|just\s+did|i\s+did|did|got)\s+",
    re.IGNORECASE,
)

SET_PATTERN = re.compile(
    r"""
    ^
    (?:(?P<exercise>[a-zA-Z][a-zA-Z0-9 _\-']*?)\s+)?
    (?P<weight_token>
        bodyweight(?:\s*[+-]\s*\d+(?:\.\d+)?)?
        |
        bw(?:\s*[+-]\s*\d+(?:\.\d+)?)?
        |
        \d+(?:\.\d+)?
    )
    \s*(?P<unit>kg|kgs|lb|lbs)?
    \s*(?:x|×|for)\s*
    (?P<reps>\d+)
    $
    """,
    re.IGNORECASE | re.VERBOSE,
)

RIR_TRAILING_PATTERN = re.compile(
    r"""
    (?:
        @\s*(?P<at_rir>\d+)\s*(?:rir)?
        |
        \brir\s*[:=]?\s*(?P<label_rir>\d+)
    )
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

BW_TOKEN_PATTERN = re.compile(
    r"^(?P<base>bw|bodyweight)(?:(?P<op>[+-])(?P<offset>\d+(?:\.\d+)?))?$",
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


def _normalize_text(text: str) -> str:
    cleaned = text.strip()
    cleaned = PREFIX_PATTERN.sub("", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"\bbody\s+weight\b", "bodyweight", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bbw\s*([+-])\s*(\d+(?:\.\d+)?)\b", r"bw\1\2", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.rstrip(".,!?")
    return cleaned.strip()


def _extract_rir(text: str) -> tuple[str, Optional[int]]:
    match = RIR_TRAILING_PATTERN.search(text)
    if not match:
        return text, None
    rir_raw = match.group("at_rir") or match.group("label_rir")
    stripped = text[: match.start()].strip()
    return stripped, int(rir_raw)


def _parse_weight_token(token: str) -> tuple[float, bool, str]:
    normalized = token.replace(" ", "").lower()
    bw_match = BW_TOKEN_PATTERN.match(normalized)
    if not bw_match:
        return float(normalized), False, ""

    op = bw_match.group("op")
    offset = bw_match.group("offset")
    if not op or not offset:
        return 0.0, True, "bodyweight"

    offset_value = float(offset)
    sign = "+" if op == "+" else "-"
    clean_offset = int(offset_value) if offset_value.is_integer() else offset_value
    return 0.0, True, f"bw{sign}{clean_offset}"


def parse_set_input(text: str) -> Optional[dict[str, Any]]:
    cleaned = _normalize_text(text)
    cleaned, extracted_rir = _extract_rir(cleaned)

    match = SET_PATTERN.match(cleaned)
    if not match:
        return None

    exercise = (match.group("exercise") or "").strip()
    weight, is_bodyweight, bw_note = _parse_weight_token(match.group("weight_token"))
    reps = int(match.group("reps"))
    unit_raw = match.group("unit")

    note = bw_note if is_bodyweight else ""
    return {
        "exercise": exercise or None,
        "weight": weight,
        "reps": reps,
        "unit": normalize_unit(unit_raw) if unit_raw else None,
        "unit_explicit": bool(unit_raw),
        "rir": extracted_rir,
        "is_bodyweight": is_bodyweight,
        "note": note,
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
