from __future__ import annotations

import csv
import tempfile
from pathlib import Path

from utils.numbers import format_standard_number


def _is_weight_valid(weight: float, unit: str) -> bool:
    normalized = str(unit or "lbs").lower()
    if normalized.startswith("kg"):
        return weight <= 700
    return weight <= 1500


def _sanitize_row(row: dict) -> dict | None:
    out = dict(row)
    try:
        weight = float(out.get("weight") or 0.0)
    except (TypeError, ValueError):
        return None
    unit = str(out.get("unit") or "lbs")
    if not _is_weight_valid(weight, unit):
        return None

    e1rm = out.get("e1rm")
    if e1rm is not None:
        try:
            e1rm = float(e1rm)
        except (TypeError, ValueError):
            e1rm = 0.0

    out["weight"] = format_standard_number(weight)
    if e1rm is not None:
        out["e1rm"] = format_standard_number(e1rm)
    return out


def write_logs_csv(rows: list[dict], *, stem: str = "workout_logs") -> Path:
    fields = ["date", "exercise", "set_number", "weight", "unit", "reps", "rir", "e1rm", "notes"]
    temp_dir = Path(tempfile.gettempdir())
    file_path = temp_dir / f"{stem}.csv"

    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            sanitized = _sanitize_row(row)
            if sanitized is None:
                continue
            out = {k: sanitized.get(k) for k in fields}
            writer.writerow(out)

    return file_path
