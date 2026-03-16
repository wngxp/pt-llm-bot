from __future__ import annotations

import sys
import types
import unittest
from datetime import datetime

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

from cogs.programme import ProgrammeCog
from cogs.workout import WorkoutCog, WorkoutSession


class _DummySettings:
    programme_channel_id = None
    coach_channel_id = None
    command_prefix = "!"
    prs_channel_id = None
    workout_channel_ids: set[int] = set()


class _DummyBot:
    def __init__(self) -> None:
        self.settings = _DummySettings()
        self.db = None
        self.ollama = None


class ProgrammeAndPRTests(unittest.TestCase):
    def test_programme_normalization_preserves_confirmed_equipment_type(self) -> None:
        cog = ProgrammeCog(_DummyBot())
        normalized = cog._normalize_program_payload(
            {
                "program_name": "Push",
                "days": [
                    {
                        "name": "Push",
                        "exercises": [
                            {
                                "name": "Dumbbell Bench Press",
                                "sets": 3,
                                "rep_range_low": 8,
                                "rep_range_high": 10,
                                "category": "heavy_barbell",
                                "equipment_type": "dumbbell",
                            }
                        ],
                    }
                ],
            }
        )

        exercise = normalized["days"][0]["exercises"][0]
        self.assertEqual(exercise["equipment_type"], "dumbbell")
        self.assertEqual(exercise["category"], "dumbbell")

    def test_pr_announcements_are_limited_to_sbd_variations(self) -> None:
        cog = WorkoutCog(_DummyBot())

        self.assertTrue(cog._is_pr_announce_exercise("Bench Press", "heavy_barbell", "barbell"))
        self.assertTrue(cog._is_pr_announce_exercise("Pause Squat", "heavy_barbell", "barbell"))
        self.assertTrue(cog._is_pr_announce_exercise("Sumo Deadlift", "heavy_barbell", "barbell"))
        self.assertFalse(cog._is_pr_announce_exercise("Smith Machine Bench Press", "heavy_barbell", "smith machine"))
        self.assertFalse(cog._is_pr_announce_exercise("Dumbbell Bench Press", "dumbbell", "dumbbell"))
        self.assertFalse(cog._is_pr_announce_exercise("Pull-Ups", "bodyweight", "bodyweight"))
        self.assertFalse(cog._is_pr_announce_exercise("Hack Squat", "heavy_barbell", "barbell"))
        self.assertFalse(cog._is_pr_announce_exercise("Bulgarian Split Squat", "heavy_barbell", "barbell"))

    def test_programme_detects_show_program_intent(self) -> None:
        cog = ProgrammeCog(_DummyBot())

        self.assertTrue(cog._looks_like_show_program_intent("show my program"))
        self.assertTrue(cog._looks_like_show_program_intent("what's my current program?"))
        self.assertFalse(cog._looks_like_show_program_intent("save program"))

    def test_programme_extracts_type_correction(self) -> None:
        cog = ProgrammeCog(_DummyBot())

        self.assertEqual(cog._extract_type_correction("leg raises is bodyweight not cable"), ("leg raises", "bodyweight"))
        self.assertEqual(cog._extract_type_correction("change smith deadlift to smith machine"), ("smith deadlift", "smith machine"))

    def test_same_command_reuses_previous_set(self) -> None:
        cog = WorkoutCog(_DummyBot())
        session = WorkoutSession(
            user_id="1",
            channel_id=1,
            day_index=0,
            day={"name": "Push"},
            exercises=[{"id": 10, "name": "Bench Press", "sets": 4}],
            started_at=datetime.now(),
            set_counts={10: 2},
            total_exercises=1,
            logged_sets=[
                {
                    "workout_log_id": 1,
                    "exercise_id": 10,
                    "exercise_name": "Bench Press",
                    "weight": 185.0,
                    "reps": 5,
                    "unit": "lbs",
                    "e1rm": 215.8,
                    "is_bodyweight": False,
                    "note": "",
                }
            ],
        )

        parsed, error = cog._parse_same_command(session, "same -5")
        self.assertIsNone(error)
        assert parsed is not None
        self.assertEqual(parsed["weight"], 180.0)
        self.assertEqual(parsed["reps"], 5)

    def test_shorthand_set_requires_existing_context(self) -> None:
        cog = WorkoutCog(_DummyBot())
        session = WorkoutSession(
            user_id="1",
            channel_id=1,
            day_index=0,
            day={"name": "Push"},
            exercises=[{"id": 10, "name": "Bench Press", "sets": 4}],
            started_at=datetime.now(),
            set_counts={10: 1},
            total_exercises=1,
        )

        parsed = cog._parse_shorthand_set(session, "100 8 2")
        assert parsed is not None
        self.assertEqual(parsed["weight"], 100.0)
        self.assertEqual(parsed["reps"], 8)
        self.assertEqual(parsed["rir"], 2)


if __name__ == "__main__":
    unittest.main()
