from __future__ import annotations

RECOMMENDED_VOLUME = {
    "chest": (10, 20),
    "back": (10, 20),
    "shoulders": (10, 20),
    "quads": (10, 16),
    "hamstrings": (10, 16),
    "biceps": (8, 14),
    "triceps": (8, 14),
    "calves": (8, 16),
}


def format_volume_report(weekly_volume: dict[str, int]) -> str:
    lines = ["Weekly volume (sets per muscle group):"]
    for group, (low, high) in RECOMMENDED_VOLUME.items():
        sets = weekly_volume.get(group, 0)
        if sets < low:
            status = "⚠️ low"
        elif sets > high:
            status = "⚠️ high"
        else:
            status = "✅"
        lines.append(f"{group.title():<11} {sets:>2} sets (recommended {low}-{high}) {status}")
    return "\n".join(lines)
