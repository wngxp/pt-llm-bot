from __future__ import annotations

import sys
import types
import unittest

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
from cogs.workout import WorkoutCog


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

        self.assertTrue(cog._is_pr_announce_exercise("Bench Press", "barbell"))
        self.assertTrue(cog._is_pr_announce_exercise("Pause Squat", "barbell"))
        self.assertTrue(cog._is_pr_announce_exercise("Smith Machine Bench Press", "smith machine"))
        self.assertTrue(cog._is_pr_announce_exercise("Sumo Deadlift", "barbell"))
        self.assertFalse(cog._is_pr_announce_exercise("Dumbbell Bench Press", "dumbbell"))
        self.assertFalse(cog._is_pr_announce_exercise("Pull-Ups", "bodyweight"))
        self.assertFalse(cog._is_pr_announce_exercise("Hack Squat", "machine"))
        self.assertFalse(cog._is_pr_announce_exercise("Bulgarian Split Squat", "barbell"))


if __name__ == "__main__":
    unittest.main()
