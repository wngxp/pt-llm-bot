from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd

from utils.exercise_types import db_category_from_equipment_type, infer_equipment_type_from_name_and_category

LOGBOOK_REQUIRED_COLUMNS = {
    "block",
    "week",
    "day_number",
    "day_name",
    "exercise",
    "technique",
    "warmup_sets_low",
    "warmup_sets_high",
    "working_sets",
    "reps_low",
    "reps_high",
    "early_rpe_low",
    "early_rpe_high",
    "last_rpe_low",
    "last_rpe_high",
    "rest_low",
    "rest_high",
    "sub1",
    "sub2",
    "notes",
}
CONFIG_REQUIRED_COLUMNS = {"block", "repeat_weeks", "repeat_from_week"}
REST_DAY_NAME = "Rest Day"


class LogbookImportError(ValueError):
    pass


def parse_structured_logbook_bytes(data: bytes, filename: str) -> dict[str, Any]:
    suffix = Path(filename).suffix.lower()
    if suffix == ".csv":
        logbook_df = _normalize_frame(pd.read_csv(BytesIO(data), sep=";", engine="python"))
        config_df: pd.DataFrame | None = None
    elif suffix in {".xlsx", ".xlsm", ".xls"}:
        workbook = pd.read_excel(BytesIO(data), sheet_name=None)
        logbook_df = _normalize_frame(workbook.get("Logbook"))
        config_sheet = workbook.get("Config")
        config_df = _normalize_frame(config_sheet) if config_sheet is not None else None
    else:
        raise LogbookImportError("Unsupported file type. Upload a `.xlsx` logbook or compatible `.csv` export.")

    _validate_frame(logbook_df, required_columns=LOGBOOK_REQUIRED_COLUMNS, sheet_name="Logbook")
    if config_df is not None and not config_df.empty:
        _validate_frame(config_df, required_columns=CONFIG_REQUIRED_COLUMNS, sheet_name="Config")

    rows, block_order = _apply_config_duplication(logbook_df, config_df)
    if not rows:
        raise LogbookImportError("No usable logbook rows found.")

    payload = _build_program_payload(rows, block_order)
    if not payload["days"]:
        raise LogbookImportError("No importable training days found.")
    return payload


def _normalize_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    normalized = frame.copy()
    normalized.columns = [str(col or "").strip().lower() for col in normalized.columns]
    normalized = normalized.dropna(how="all")
    normalized = normalized.reset_index(drop=True)
    return normalized


def _validate_frame(frame: pd.DataFrame, *, required_columns: set[str], sheet_name: str) -> None:
    if frame.empty:
        raise LogbookImportError(f"`{sheet_name}` sheet is empty.")
    missing = sorted(required_columns - set(frame.columns))
    if missing:
        raise LogbookImportError(f"`{sheet_name}` is missing required columns: {', '.join(missing)}.")


def _clean_value(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return value


def _text(value: Any) -> str | None:
    cleaned = _clean_value(value)
    if cleaned is None:
        return None
    return str(cleaned).strip() or None


def _int_or_none(value: Any) -> int | None:
    cleaned = _clean_value(value)
    if cleaned is None:
        return None
    try:
        return int(float(cleaned))
    except (TypeError, ValueError):
        return None


def _parse_repeat_weeks(value: Any) -> list[int]:
    text = _text(value)
    if not text:
        return []
    weeks: list[int] = []
    for part in text.split(","):
        cleaned = part.strip()
        if not cleaned:
            continue
        try:
            weeks.append(int(cleaned))
        except ValueError as exc:
            raise LogbookImportError(f"Invalid repeat week `{cleaned}` in Config sheet.") from exc
    return weeks


def _frame_to_rows(frame: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    block_order: dict[str, int] = {}
    for index, record in enumerate(frame.to_dict(orient="records")):
        cleaned = {key: _clean_value(value) for key, value in record.items()}
        if not any(value is not None for value in cleaned.values()):
            continue
        block = _text(cleaned.get("block"))
        day_name = _text(cleaned.get("day_name"))
        if not block or not day_name:
            continue
        if block not in block_order:
            block_order[block] = len(block_order)
        cleaned["__row_order"] = index
        rows.append(cleaned)
    return rows, block_order


def _apply_config_duplication(
    logbook_df: pd.DataFrame,
    config_df: pd.DataFrame | None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows, block_order = _frame_to_rows(logbook_df)
    if not rows:
        return [], block_order
    if config_df is None or config_df.empty:
        return rows, block_order

    base_rows = list(rows)
    for record in config_df.to_dict(orient="records"):
        block = _text(record.get("block"))
        repeat_from_week = _int_or_none(record.get("repeat_from_week"))
        repeat_weeks = _parse_repeat_weeks(record.get("repeat_weeks"))
        if not block or repeat_from_week is None or not repeat_weeks:
            continue
        if block not in block_order:
            block_order[block] = len(block_order)

        source_rows = [
            row for row in base_rows
            if _text(row.get("block")) == block and _int_or_none(row.get("week")) == repeat_from_week
        ]
        if not source_rows:
            raise LogbookImportError(
                f"Config requested `{block}` week {repeat_from_week}, but no matching Logbook rows were found."
            )

        rows = [
            row for row in rows
            if not (_text(row.get("block")) == block and _int_or_none(row.get("week")) in set(repeat_weeks))
        ]

        for repeated_week in repeat_weeks:
            for source in source_rows:
                duplicated = dict(source)
                duplicated["week"] = repeated_week
                rows.append(duplicated)

    rows.sort(
        key=lambda row: (
            block_order.get(_text(row.get("block")) or "", 10_000),
            _int_or_none(row.get("week")) or 0,
            _int_or_none(row.get("day_number")) or 0,
            int(row.get("__row_order") or 0),
        )
    )
    return rows, block_order


def _block_summary_label(block_names: list[str]) -> str:
    if not block_names:
        return "Structured Logbook"
    if len(block_names) == 1:
        return block_names[0]
    preview = " / ".join(block_names[:2])
    if len(block_names) > 2:
        return f"{preview} +{len(block_names) - 2} more"
    return preview


def _build_program_payload(rows: list[dict[str, Any]], block_order: dict[str, int]) -> dict[str, Any]:
    days: list[dict[str, Any]] = []
    day_lookup: dict[tuple[str, int, int, str], dict[str, Any]] = {}
    block_names = [block for block, _ in sorted(block_order.items(), key=lambda item: item[1])]

    for row in rows:
        block = _text(row.get("block"))
        week = _int_or_none(row.get("week"))
        day_number = _int_or_none(row.get("day_number"))
        day_name = _text(row.get("day_name"))
        if not block or week is None or day_number is None or not day_name:
            continue

        key = (block, week, day_number, day_name)
        day = day_lookup.get(key)
        if day is None:
            day = {
                "day_order": len(days),
                "name": day_name,
                "block": block,
                "week": week,
                "day_number": day_number,
                "is_rest_day": int(day_name.lower() == REST_DAY_NAME.lower()),
                "exercises": [],
            }
            day_lookup[key] = day
            days.append(day)

        if bool(day["is_rest_day"]):
            continue

        exercise_name = _text(row.get("exercise"))
        if not exercise_name:
            raise LogbookImportError(f"{block} week {week} day {day_number} is missing an exercise name.")

        equipment_type = infer_equipment_type_from_name_and_category(exercise_name, "")
        category = db_category_from_equipment_type(exercise_name, equipment_type)
        day["exercises"].append(
            {
                "name": exercise_name,
                "display_order": len(day["exercises"]),
                "sets": max(1, _int_or_none(row.get("working_sets")) or 1),
                "rep_range_low": _int_or_none(row.get("reps_low")),
                "rep_range_high": _int_or_none(row.get("reps_high")),
                "category": category,
                "equipment_type": equipment_type,
                "superset_group": None,
                "notes": _text(row.get("notes")) or "",
                "muscle_groups": "",
                "technique": _text(row.get("technique")) or "N/A",
                "warmup_sets_low": _int_or_none(row.get("warmup_sets_low")),
                "warmup_sets_high": _int_or_none(row.get("warmup_sets_high")),
                "early_rpe_low": _int_or_none(row.get("early_rpe_low")),
                "early_rpe_high": _int_or_none(row.get("early_rpe_high")),
                "last_rpe_low": _int_or_none(row.get("last_rpe_low")),
                "last_rpe_high": _int_or_none(row.get("last_rpe_high")),
                "rest_low": _int_or_none(row.get("rest_low")),
                "rest_high": _int_or_none(row.get("rest_high")),
                "sub1": _text(row.get("sub1")) or "",
                "sub2": _text(row.get("sub2")) or "",
            }
        )

    unique_weeks = {(int(day["week"]), str(day["block"])) for day in days if day.get("week") is not None}
    total_exercises = sum(len(day.get("exercises", [])) for day in days)
    first_active_index = next((idx for idx, day in enumerate(days) if not bool(day.get("is_rest_day"))), 0)
    summary = {
        "label": _block_summary_label(block_names),
        "days": len(days),
        "exercises": total_exercises,
        "weeks": len(unique_weeks),
        "first_active_day_index": first_active_index,
        "blocks": block_names,
    }
    program_name = block_names[0] if len(block_names) == 1 else "Structured Logbook"
    return {
        "program_name": program_name,
        "days": days,
        "import_summary": summary,
    }
