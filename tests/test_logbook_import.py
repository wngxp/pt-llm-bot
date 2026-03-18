from __future__ import annotations

import asyncio
import sys
import tempfile
import types
import unittest
from datetime import date
from io import BytesIO
from pathlib import Path

import pandas as pd

if "discord" not in sys.modules:
    discord_module = types.ModuleType("discord")
    discord_abc_module = types.ModuleType("discord.abc")
    discord_ext_module = types.ModuleType("discord.ext")
    commands_module = types.ModuleType("discord.ext.commands")

    class _Cog:
        @staticmethod
        def listener(*args, **kwargs):  # noqa: ANN002, ANN003
            def decorator(func):
                return func

            return decorator

    def _command(*args, **kwargs):  # noqa: ANN002, ANN003
        def decorator(func):
            return func

        return decorator

    discord_module.abc = discord_abc_module
    discord_module.Guild = type("Guild", (), {})
    discord_module.TextChannel = type("TextChannel", (), {})
    discord_module.Thread = type("Thread", (), {})
    discord_module.Message = type("Message", (), {})
    discord_module.Member = type("Member", (), {})
    discord_module.File = type("File", (), {})
    discord_module.utils = types.SimpleNamespace(get=lambda *args, **kwargs: None)
    discord_abc_module.GuildChannel = object
    discord_abc_module.Thread = object
    discord_abc_module.PrivateChannel = object
    discord_abc_module.Messageable = object
    discord_abc_module.User = object
    commands_module.Cog = _Cog
    commands_module.Bot = object
    commands_module.Context = object
    commands_module.command = _command
    discord_ext_module.commands = commands_module

    sys.modules["discord"] = discord_module
    sys.modules["discord.abc"] = discord_abc_module
    sys.modules["discord.ext"] = discord_ext_module
    sys.modules["discord.ext.commands"] = commands_module

from cogs.workout import WorkoutCog
from db.database import Database
from utils.logbook_import import parse_structured_logbook_bytes
from utils.warmup import generate_pyramid_warmup


class _DummySettings:
    command_prefix = "!"
    prs_channel_id = None
    workout_channel_ids: set[int] = set()
    settings_channel_id = None


class _DummyBot:
    def __init__(self, db: Database) -> None:
        self.settings = _DummySettings()
        self.db = db
        self.ollama = None

    def dispatch(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        return None

    def get_channel(self, channel_id: int) -> None:  # noqa: ARG002
        return None


class _FakeChannel:
    def __init__(self) -> None:
        self.id = 1
        self.name = "mon"
        self.messages: list[str] = []

    async def send(self, text: str | None = None, **kwargs) -> None:  # noqa: ANN003
        if text is not None:
            self.messages.append(text)
        elif "file" in kwargs:
            self.messages.append("<file>")


def _build_workbook_bytes() -> bytes:
    logbook = pd.DataFrame(
        [
            {
                "block": "Foundation",
                "week": 2,
                "day_number": 1,
                "day_name": "Upper Strength",
                "exercise": "Neutral-Grip Lat Pulldown",
                "technique": "N/A",
                "warmup_sets_low": 2,
                "warmup_sets_high": 3,
                "working_sets": 2,
                "reps_low": 8,
                "reps_high": 10,
                "early_rpe_low": 7,
                "early_rpe_high": 8,
                "last_rpe_low": 8,
                "last_rpe_high": 9,
                "rest_low": 2,
                "rest_high": 3,
                "sub1": "Wide-Grip Pull-Up",
                "sub2": "Dual-Handle Lat Pulldown",
                "notes": "Pull elbows toward your hips.",
            },
            {
                "block": "Foundation",
                "week": 2,
                "day_number": 2,
                "day_name": "Rest Day",
                "exercise": None,
                "technique": None,
                "warmup_sets_low": None,
                "warmup_sets_high": None,
                "working_sets": None,
                "reps_low": None,
                "reps_high": None,
                "early_rpe_low": None,
                "early_rpe_high": None,
                "last_rpe_low": None,
                "last_rpe_high": None,
                "rest_low": None,
                "rest_high": None,
                "sub1": None,
                "sub2": None,
                "notes": None,
            },
        ]
    )
    config = pd.DataFrame(
        [
            {
                "block": "Foundation",
                "repeat_weeks": "3,4,5",
                "repeat_from_week": 2,
            }
        ]
    )
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        logbook.to_excel(writer, sheet_name="Logbook", index=False)
        config.to_excel(writer, sheet_name="Config", index=False)
    return output.getvalue()


async def _create_structured_program(db: Database) -> tuple[int, int, int]:
    payload = {
        "program_name": "Structured Logbook",
        "days": [
            {
                "day_order": 0,
                "name": "Upper Strength",
                "block": "Foundation",
                "week": 1,
                "day_number": 1,
                "is_rest_day": 0,
                "exercises": [
                    {
                        "name": "Neutral-Grip Lat Pulldown",
                        "display_order": 0,
                        "sets": 2,
                        "rep_range_low": 8,
                        "rep_range_high": 10,
                        "category": "cable_machine",
                        "equipment_type": "machine",
                        "technique": "Failure",
                        "warmup_sets_low": 2,
                        "warmup_sets_high": 3,
                        "early_rpe_low": 7,
                        "early_rpe_high": 8,
                        "last_rpe_low": 9,
                        "last_rpe_high": 10,
                        "rest_low": 0,
                        "rest_high": 0,
                        "sub1": "Wide-Grip Pull-Up",
                        "sub2": "Dual-Handle Lat Pulldown",
                        "notes": "Drive elbows down.",
                        "muscle_groups": "back,lats",
                    }
                ],
            },
            {
                "day_order": 1,
                "name": "Rest Day",
                "block": "Foundation",
                "week": 1,
                "day_number": 2,
                "is_rest_day": 1,
                "exercises": [],
            },
            {
                "day_order": 2,
                "name": "Lower Hypertrophy",
                "block": "Foundation",
                "week": 1,
                "day_number": 3,
                "is_rest_day": 0,
                "exercises": [
                    {
                        "name": "Leg Press",
                        "display_order": 0,
                        "sets": 1,
                        "rep_range_low": 10,
                        "rep_range_high": 12,
                        "category": "cable_machine",
                        "equipment_type": "machine",
                        "technique": "N/A",
                        "warmup_sets_low": 1,
                        "warmup_sets_high": 1,
                        "early_rpe_low": None,
                        "early_rpe_high": None,
                        "last_rpe_low": 10,
                        "last_rpe_high": None,
                        "rest_low": 0,
                        "rest_high": 0,
                        "sub1": "",
                        "sub2": "",
                        "notes": "",
                        "muscle_groups": "quads",
                    }
                ],
            },
        ],
    }
    program_id = await db.create_program_from_payload(payload, user_id="1")
    days = await db.get_program_days(program_id)
    day_one_exercises = await db.get_exercises_for_day(int(days[0]["id"]))
    first_exercise_id = int(day_one_exercises[0]["id"])
    await db.log_set(
        exercise_id=first_exercise_id,
        user_id="1",
        workout_date=date(2026, 3, 1),
        set_number=1,
        weight=120.0,
        reps=10,
        unit="lbs",
        performed_exercise_name="Neutral-Grip Lat Pulldown",
        performed_category="cable_machine",
        performed_equipment_type="machine",
    )
    await db.log_set(
        exercise_id=first_exercise_id,
        user_id="1",
        workout_date=date(2026, 3, 2),
        set_number=1,
        weight=125.0,
        reps=9,
        unit="lbs",
        performed_exercise_name="Dual-Handle Lat Pulldown",
        performed_category="cable_machine",
        performed_equipment_type="machine",
    )
    return program_id, int(days[0]["id"]), first_exercise_id


class StructuredImportTests(unittest.TestCase):
    def test_parser_applies_config_duplication_and_marks_rest_days(self) -> None:
        payload = parse_structured_logbook_bytes(_build_workbook_bytes(), "foundation.xlsx")

        self.assertEqual(payload["program_name"], "Foundation")
        self.assertEqual(payload["import_summary"]["days"], 8)
        self.assertEqual(payload["import_summary"]["exercises"], 4)
        self.assertEqual(payload["import_summary"]["weeks"], 4)

        imported_weeks = {(day["block"], day["week"]) for day in payload["days"]}
        self.assertEqual(imported_weeks, {("Foundation", 2), ("Foundation", 3), ("Foundation", 4), ("Foundation", 5)})

        rest_days = [day for day in payload["days"] if day["name"] == "Rest Day"]
        self.assertEqual(len(rest_days), 4)
        self.assertTrue(all(day["is_rest_day"] == 1 for day in rest_days))
        self.assertTrue(all(not day["exercises"] for day in rest_days))

    def test_database_persists_structured_programme_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Database(Path(tmpdir) / "test.sqlite3")
            asyncio.run(db.init())

            payload = parse_structured_logbook_bytes(_build_workbook_bytes(), "foundation.xlsx")
            program_id = asyncio.run(db.create_program_from_payload(payload, user_id="1"))
            days = asyncio.run(db.get_program_days(program_id))
            exercises = asyncio.run(db.get_exercises_for_day(int(days[0]["id"])))
            state = asyncio.run(db.get_user_state("1"))

            self.assertEqual(days[0]["block"], "Foundation")
            self.assertEqual(days[0]["week"], 2)
            self.assertEqual(days[0]["day_number"], 1)
            self.assertEqual(days[1]["is_rest_day"], 1)
            self.assertEqual(exercises[0]["technique"], "N/A")
            self.assertEqual(exercises[0]["warmup_sets_low"], 2)
            self.assertEqual(exercises[0]["warmup_sets_high"], 3)
            self.assertEqual(exercises[0]["early_rpe_low"], 7)
            self.assertEqual(exercises[0]["last_rpe_high"], 9)
            self.assertEqual(exercises[0]["sub2"], "Dual-Handle Lat Pulldown")
            self.assertEqual(state["current_block"], "Foundation")
            self.assertEqual(state["current_week"], 2)
            self.assertEqual(state["current_day_number"], 1)

    def test_database_finds_day_by_week_and_day_number(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Database(Path(tmpdir) / "test.sqlite3")
            asyncio.run(db.init())
            payload = parse_structured_logbook_bytes(_build_workbook_bytes(), "foundation.xlsx")
            asyncio.run(db.create_program_from_payload(payload, user_id="1"))

            idx = asyncio.run(db.find_day_index_by_week_day(week=4, day_number=2, user_id="1", block="Foundation"))
            self.assertEqual(idx, 5)


class WarmupTests(unittest.TestCase):
    def test_generate_pyramid_warmup_variants(self) -> None:
        self.assertEqual(generate_pyramid_warmup(200, 1, "lbs"), ["Warm-up set 1: ~120 lbs × 6–10 reps"])
        self.assertEqual(
            generate_pyramid_warmup(200, 2, "lbs"),
            [
                "Warm-up set 1: ~100 lbs × 6–10 reps",
                "Warm-up set 2: ~140 lbs × 4–6 reps",
            ],
        )
        self.assertEqual(
            generate_pyramid_warmup(200, 3, "lbs"),
            [
                "Warm-up set 1: ~90 lbs × 6–10 reps",
                "Warm-up set 2: ~130 lbs × 4–6 reps",
                "Warm-up set 3: ~170 lbs × 3–4 reps",
            ],
        )
        self.assertEqual(
            generate_pyramid_warmup(200, 4, "lbs"),
            [
                "Warm-up set 1: ~90 lbs × 6–10 reps",
                "Warm-up set 2: ~120 lbs × 4–6 reps",
                "Warm-up set 3: ~150 lbs × 3–5 reps",
                "Warm-up set 4: ~170 lbs × 2–4 reps",
            ],
        )

    def test_rpe_prompt_logic_handles_single_set_exercises(self) -> None:
        cog = WorkoutCog(_DummyBot(db=None))  # type: ignore[arg-type]
        multi = {"sets": 2, "early_rpe_low": 7, "early_rpe_high": 8, "last_rpe_low": 9, "last_rpe_high": 10}
        single = {"sets": 1, "early_rpe_low": 7, "early_rpe_high": 8, "last_rpe_low": 10, "last_rpe_high": None}

        self.assertEqual(
            cog._rpe_prompt_for_set(multi, 1),
            ("RPE check after set 1: target ~7-8. Reply with your RPE or log the next set when ready.", True),
        )
        self.assertEqual(cog._rpe_prompt_for_set(single, 1), ("Last-set RPE target: ~10.", False))


class StructuredWorkoutFlowTests(unittest.TestCase):
    def test_structured_session_posts_general_warmup_subs_and_skips_rest_days(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Database(Path(tmpdir) / "test.sqlite3")
            asyncio.run(db.init())
            asyncio.run(_create_structured_program(db))

            bot = _DummyBot(db)
            cog = WorkoutCog(bot)
            channel = _FakeChannel()

            session = asyncio.run(cog._start_session(channel, user_id="1"))
            assert session is not None
            self.assertIn("General warm-up", channel.messages[-1])

            asyncio.run(cog._handle_general_warmup_message(channel, session, "done"))
            self.assertIn("**Next up: Neutral-Grip Lat Pulldown**", channel.messages[-1])
            self.assertIn("Warm-up: 2-3 sets · Sub options: Wide-Grip Pull-Up | Dual-Handle Lat Pulldown", channel.messages[-1])

            asyncio.run(cog._handle_substitution_request(channel, session, "sub 2"))
            self.assertEqual(session.current_exercise()["name"], "Dual-Handle Lat Pulldown")
            self.assertIn("Switched to Dual-Handle Lat Pulldown.", channel.messages[-2])
            self.assertIn("Warm-up set 1", channel.messages[-1])

            asyncio.run(cog._handle_exercise_warmup_message(channel, session, "done"))
            self.assertIn("Warm-up set 2", channel.messages[-1])
            asyncio.run(cog._handle_exercise_warmup_message(channel, session, "done"))
            self.assertIn("Ready for set 1/2 of Dual-Handle Lat Pulldown", channel.messages[-1])

            asyncio.run(
                cog._handle_logged_set(
                    channel,
                    session,
                    {
                        "exercise": None,
                        "weight": 125.0,
                        "reps": 9,
                        "unit": None,
                        "unit_explicit": False,
                        "rir": None,
                        "is_bodyweight": False,
                        "note": "",
                        "trailing_text": "",
                        "raw": "125 x 9",
                    },
                )
            )
            self.assertTrue(any("RPE check after set 1: target ~7-8." in message for message in channel.messages))

            asyncio.run(cog._cancel_rest(session))
            asyncio.run(cog._prompt_current_exercise(channel, session))
            self.assertIn("Last set - Failure. Take the set to failure with clean form.", channel.messages[-1])

            asyncio.run(
                cog._handle_logged_set(
                    channel,
                    session,
                    {
                        "exercise": None,
                        "weight": 130.0,
                        "reps": 8,
                        "unit": None,
                        "unit_explicit": False,
                        "rir": None,
                        "is_bodyweight": False,
                        "note": "",
                        "trailing_text": "",
                        "raw": "130 x 8",
                    },
                )
            )

            self.assertTrue(any("Last-set RPE target: ~9-10." in message for message in channel.messages))
            self.assertTrue(any("Session complete for **Upper Strength**." in message for message in channel.messages))

            state = asyncio.run(db.get_user_state("1"))
            self.assertEqual(asyncio.run(db.get_current_day_index("1")), 2)
            self.assertEqual(state["current_block"], "Foundation")
            self.assertEqual(state["current_week"], 1)
            self.assertEqual(state["current_day_number"], 3)


if __name__ == "__main__":
    unittest.main()
