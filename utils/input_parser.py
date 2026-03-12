from __future__ import annotations

import re
from typing import Any, Optional

PREFIX_PATTERN = re.compile(
    r"^(?:i\s+hit|just\s+did|i\s+did|did|got|i\s+got)\s+",
    re.IGNORECASE,
)

SET_SEARCH_PATTERN = re.compile(
    r"""
    (?P<weight_token>
        bodyweight(?:\s*[+-]\s*\d+(?:\.\d+)?(?:\s*(?:kg|kgs|lb|lbs))?)?
        |
        bw(?:\s*[+-]\s*\d+(?:\.\d+)?(?:\s*(?:kg|kgs|lb|lbs))?)?
        |
        \d+(?:\.\d+)?
    )
    \s*(?P<unit>kg|kgs|lb|lbs)?
    \s*(?:x|×|for)\s*
    (?P<reps>\d+)
    """,
    re.IGNORECASE | re.VERBOSE,
)

RIR_ANY_PATTERN = re.compile(
    r"""
    (?:
        @\s*(?P<at_rir>\d+)\s*(?:rir)?
        |
        \brir\s*[:=]?\s*(?P<label_rir>\d+)
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

BW_TOKEN_PATTERN = re.compile(
    r"^(?P<base>bw|bodyweight)(?:(?P<op>[+-])(?P<offset>\d+(?:\.\d+)?)(?P<offset_unit>kg|kgs|lb|lbs)?)?$",
    re.IGNORECASE,
)

EXERCISE_NOISE_WORDS = {
    "i",
    "im",
    "i'm",
    "got",
    "bro",
    "dude",
    "today",
    "yesterday",
    "just",
    "did",
    "felt",
    "feeling",
    "and",
    "but",
    "then",
}

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
    cleaned = cleaned.replace(",", "")
    cleaned = PREFIX_PATTERN.sub("", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"\bbody\s+weight\b", "bodyweight", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bbw\s*([+-])\s*(\d+(?:\.\d+)?)\b", r"bw\1\2", cleaned, flags=re.IGNORECASE)
    return cleaned.strip().rstrip(".,!?")


def _parse_weight_token(token: str) -> tuple[float, bool, str]:
    normalized = token.replace(" ", "").lower()
    bw_match = BW_TOKEN_PATTERN.match(normalized)
    if not bw_match:
        return float(normalized), False, ""

    op = bw_match.group("op")
    offset = bw_match.group("offset")
    if not op or not offset:
        return 0.0, True, "bodyweight"

    offset_value = float(offset.rstrip("kgslb"))
    sign = "+" if op == "+" else "-"
    clean_offset = int(offset_value) if offset_value.is_integer() else offset_value
    if sign == "+":
        return float(offset_value), True, f"bw+{clean_offset}"
    return 0.0, True, f"bw-{clean_offset}"


def _extract_inline_unit_from_weight_token(token: str) -> Optional[str]:
    lowered = token.strip().lower().replace(" ", "")
    if lowered.endswith(("kg", "kgs")):
        return "kg"
    if lowered.endswith(("lb", "lbs")):
        return "lbs"
    return None


def _extract_exercise_candidate(prefix_text: str) -> tuple[Optional[str], str]:
    candidate = prefix_text.strip(" -,:;")
    if not candidate:
        return None, ""

    candidate = PREFIX_PATTERN.sub("", candidate).strip()
    if not candidate:
        return None, ""

    words = candidate.split()
    if len(words) > 6:
        return None, candidate

    if any(any(ch.isdigit() for ch in word) for word in words):
        return None, candidate

    if len(words) == 1 and words[0].lower() in EXERCISE_NOISE_WORDS:
        return None, candidate
    if any(word.lower() in EXERCISE_NOISE_WORDS for word in words):
        return None, candidate

    return candidate, ""


def parse_set_input(text: str) -> Optional[dict[str, Any]]:
    cleaned = _normalize_text(text)
    match = SET_SEARCH_PATTERN.search(cleaned)
    if not match:
        return None

    before = cleaned[: match.start()].strip()
    after = cleaned[match.end() :].strip()

    exercise, leftover_prefix = _extract_exercise_candidate(before)

    weight, is_bodyweight, bw_note = _parse_weight_token(match.group("weight_token"))
    reps = int(match.group("reps"))
    unit_raw = match.group("unit")
    inline_unit = _extract_inline_unit_from_weight_token(match.group("weight_token"))
    if not unit_raw and inline_unit:
        unit_raw = inline_unit

    tail_parts = [part for part in [leftover_prefix, after] if part]
    trailing_text = " ".join(tail_parts).strip()

    rir = None
    if trailing_text:
        rir_match = RIR_ANY_PATTERN.search(trailing_text)
        if rir_match:
            rir_raw = rir_match.group("at_rir") or rir_match.group("label_rir")
            rir = int(rir_raw)
            trailing_text = RIR_ANY_PATTERN.sub(" ", trailing_text)
            trailing_text = re.sub(r"\s+", " ", trailing_text).strip(" -,:;")

    note = bw_note if is_bodyweight else ""
    return {
        "exercise": exercise or None,
        "weight": weight,
        "reps": reps,
        "unit": normalize_unit(unit_raw) if unit_raw else None,
        "unit_explicit": bool(unit_raw),
        "rir": rir,
        "is_bodyweight": is_bodyweight,
        "note": note,
        "trailing_text": trailing_text,
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
