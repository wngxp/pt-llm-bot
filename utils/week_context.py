from __future__ import annotations

from typing import Any


FOUNDATION_BLOCK_DESCRIPTION = (
    "Foundation block. Build skill, consistency, and baseline performance before pushing hard."
)
RAMPING_BLOCK_DESCRIPTION = (
    "Ramping block. Hypertrophy-focused progression with higher effort, more aggressive overload, and closer-to-limit work."
)
DEFAULT_BLOCK_DESCRIPTION = (
    "Structured training block. Match the programmed rep ranges, manage fatigue well, and progress with good form."
)

WEEK_GUIDANCE_MAP: dict[tuple[str, int | str], str] = {
    (
        "foundation",
        1,
    ): (
        "This is the ramp-up week. The user is getting accustomed to the exercises, movement patterns, and volume. "
        "Suggest conservative weight selection (RPE 6-7 even if the program says 7-8). Emphasize form, tempo, and "
        "mind-muscle connection over load. It's okay to leave reps in the tank. The goal is to establish baselines, "
        "not to push hard."
    ),
    (
        "foundation",
        2,
    ): (
        "Second week - the user should be more comfortable with the movements. Encourage finding working weights that "
        "land in the prescribed RPE range. This week sets the baseline for progressive overload in weeks 3-5."
    ),
    (
        "foundation",
        "default",
    ): (
        "Progressive overload phase. The user should aim to add small increments of weight or reps each week compared "
        "to the previous week. Encourage beating last week's numbers by at least 1 rep or a small weight increase "
        "(2.5-5 lbs / 1-2.5 kg). If the user is consistently hitting the top of the rep range at the target RPE, "
        "suggest a weight increase."
    ),
    (
        "ramping",
        6,
    ): (
        "Transition to the Ramping block. Exercise selection shifts to Hypertrophy Focus days. New movements may "
        "appear - encourage the user to treat this like a mini ramp-up for unfamiliar exercises while maintaining "
        "intensity on carried-over movements."
    ),
    (
        "ramping",
        7,
    ): (
        "Establishing the Ramping block baseline. The user should find working weights for all new exercises. "
        "Intensity should match prescribed RPEs."
    ),
    (
        "ramping",
        "default",
    ): (
        "Intensification phase. Push hard - the user should be aiming for the top of the RPE range on most "
        "exercises. Encourage progressive overload week over week. By weeks 11-12, the user should be hitting true "
        "RPE 9-10 on final sets of compound movements. Watch for fatigue accumulation - if the user reports feeling "
        "beaten up, suggest a lighter session rather than skipping entirely."
    ),
}

INTRO_MESSAGE_MAP: dict[tuple[str, int | str], str] = {
    ("foundation", 1): "Ramp-up week - focus on form and finding your working weights.",
    ("foundation", 2): "Baseline week - dial in honest loads that match the programmed RPE.",
    ("foundation", "default"): "Overload week - try to beat last week's numbers with clean execution.",
    ("ramping", 6): "Transition week - treat new lifts like a mini ramp-up while staying sharp on familiar ones.",
    ("ramping", 7): "New baseline week - lock in working weights for the Ramping block.",
    ("ramping", "default"): "Intensification week - push the top end of the target effort range and manage fatigue well.",
}

WEIGHT_NOTE_MAP: dict[tuple[str, int | str], str] = {
    ("foundation", 1): (
        "Ramp-up guidance: if you have prior numbers, match them or stay slightly under while you groove the movement."
    ),
    ("foundation", 2): (
        "Baseline guidance: pick a load that lands inside the prescribed RPE range and gives you a solid reference point."
    ),
    ("foundation", "default"): (
        "Overload guidance: if last week was in range, consider adding 2.5-5 lbs (1-2.5 kg) or squeezing out 1 more rep."
    ),
    ("ramping", 6): (
        "Transition guidance: keep familiar movements moving, but be conservative on brand-new variations until the pattern feels good."
    ),
    ("ramping", 7): (
        "Baseline guidance: use this week to settle on dependable working weights for the new block."
    ),
    ("ramping", "default"): (
        "Intensification guidance: if last week was solid, push the load slightly or aim for the top of the rep range."
    ),
}

SUMMARY_NOTE_MAP: dict[tuple[str, int | str], str] = {
    ("foundation", 1): "Solid ramp-up session. You're building clean baselines that will drive the next few weeks.",
    ("foundation", 2): "Good baseline work. These numbers give us something clear to progress from next week.",
    ("foundation", "default"): "Nice overload work. Keep chasing small wins week to week without sacrificing form.",
    ("ramping", 6): "Strong transition session. You’re settling into the new block while learning the new movements.",
    ("ramping", 7): "Good baseline session for the Ramping block. These loads should guide the harder weeks ahead.",
    ("ramping", "default"): "Solid intensification work. Keep pushing the top end when recovery and form are there.",
}


def _normalize_block(block: str | None) -> str:
    return str(block or "").strip().lower()


def _lookup_context_text(mapping: dict[tuple[str, int | str], str], block: str | None, week: int | None) -> str:
    normalized_block = _normalize_block(block)
    if week is not None and (normalized_block, int(week)) in mapping:
        return mapping[(normalized_block, int(week))]
    return mapping.get((normalized_block, "default"), "")


def get_week_context(block: str, week: int) -> dict[str, str]:
    normalized_block = _normalize_block(block)
    if normalized_block == "foundation":
        block_description = FOUNDATION_BLOCK_DESCRIPTION
    elif normalized_block == "ramping":
        block_description = RAMPING_BLOCK_DESCRIPTION
    else:
        block_description = DEFAULT_BLOCK_DESCRIPTION

    week_guidance = _lookup_context_text(WEEK_GUIDANCE_MAP, normalized_block, week) or (
        "Follow the programmed rep targets and RPEs, prioritize good execution, and progress conservatively."
    )
    return {
        "block_description": block_description,
        "week_guidance": week_guidance,
    }


def get_week_intro_message(block: str | None, week: int | None) -> str | None:
    message = _lookup_context_text(INTRO_MESSAGE_MAP, block, week)
    return message or None


def get_weight_progression_note(block: str | None, week: int | None) -> str | None:
    note = _lookup_context_text(WEIGHT_NOTE_MAP, block, week)
    return note or None


def get_session_summary_note(block: str | None, week: int | None) -> str | None:
    note = _lookup_context_text(SUMMARY_NOTE_MAP, block, week)
    return note or None


def build_program_context_block(
    *,
    program_name: str | None,
    block: str | None,
    week: int | None,
    day_number: int | None,
    day_name: str | None,
) -> str:
    if not block or week is None or day_number is None or not day_name:
        return ""

    context = get_week_context(str(block), int(week))
    label = str(program_name or "Active Program").strip() or "Active Program"
    return (
        "PROGRAM CONTEXT:\n"
        f"- Program: {label}\n"
        f"- Current position: {block} block, Week {int(week)}, Day {int(day_number)} ({day_name})\n"
        f"- Block description: {context['block_description']}\n"
        f"- Week guidance: {context['week_guidance']}"
    )


def inject_program_context(
    system_prompt: str,
    *,
    program_name: str | None,
    block: str | None,
    week: int | None,
    day_number: int | None,
    day_name: str | None,
) -> str:
    context_block = build_program_context_block(
        program_name=program_name,
        block=block,
        week=week,
        day_number=day_number,
        day_name=day_name,
    )
    if not context_block:
        return system_prompt
    return f"{system_prompt}\n\n{context_block}"


def get_program_position(day: dict[str, Any] | None, state: dict[str, Any] | None) -> tuple[str | None, int | None, int | None, str | None]:
    day = day or {}
    state = state or {}
    block = str(day.get("block") or state.get("current_block") or "").strip() or None
    week_raw = day.get("week") if day.get("week") is not None else state.get("current_week")
    day_number_raw = day.get("day_number") if day.get("day_number") is not None else state.get("current_day_number")
    week = int(week_raw) if week_raw is not None else None
    day_number = int(day_number_raw) if day_number_raw is not None else None
    day_name = str(day.get("name") or "").strip() or None
    return block, week, day_number, day_name
