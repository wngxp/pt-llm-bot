from __future__ import annotations

import csv
import tempfile
from pathlib import Path


def write_logs_csv(rows: list[dict], *, stem: str = "workout_logs") -> Path:
    fields = ["date", "exercise", "set_number", "weight", "unit", "reps", "rir", "e1rm", "notes"]
    temp_dir = Path(tempfile.gettempdir())
    file_path = temp_dir / f"{stem}.csv"

    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            out = {k: row.get(k) for k in fields}
            writer.writerow(out)

    return file_path
