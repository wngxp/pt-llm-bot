from __future__ import annotations

import asyncio
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


class _FakeProgrammeDB:
    def __init__(self) -> None:
        self.updated: tuple[str, str, str] | None = None
        self.current_day_index = 1
        self.program = {"id": 1, "name": "JN PPLUL v2", "created_at": "2026-03-15 12:00:00"}
        self.days = [
            {"id": 10, "day_order": 0, "name": "Push"},
            {"id": 11, "day_order": 1, "name": "Pull"},
        ]
        self.exercises_by_day = {
            10: [
                {"id": 100, "name": "Smith Machine Bench Press", "sets": 1, "rep_range_low": 3, "rep_range_high": 5, "category": "heavy_barbell", "equipment_type": "barbell"},
                {"id": 101, "name": "Arnold Press", "sets": 3, "rep_range_low": 8, "rep_range_high": 10, "category": "dumbbell", "equipment_type": "dumbbell"},
            ],
            11: [
                {"id": 200, "name": "Lat Pulldowns", "sets": 4, "rep_range_low": 10, "rep_range_high": 10, "category": "cable_machine", "equipment_type": "unknown"},
                {"id": 201, "name": "Cable Row", "sets": 3, "rep_range_low": 10, "rep_range_high": 12, "category": "cable_machine", "equipment_type": "unknown"},
                {"id": 202, "name": "Cable Pullover", "sets": 3, "rep_range_low": 12, "rep_range_high": 15, "category": "cable_machine", "equipment_type": "unknown"},
                {"id": 203, "name": "Face Pulls", "sets": 3, "rep_range_low": 15, "rep_range_high": 15, "category": "cable_machine", "equipment_type": "unknown"},
                {"id": 204, "name": "Rear Delt Fly", "sets": 3, "rep_range_low": 15, "rep_range_high": 20, "category": "dumbbell", "equipment_type": "dumbbell"},
                {"id": 205, "name": "Preacher Curl", "sets": 3, "rep_range_low": 10, "rep_range_high": 12, "category": "cable_machine", "equipment_type": "unknown"},
            ],
        }

    async def get_active_program(self, user_id: str) -> dict[str, object] | None:
        return dict(self.program)

    async def get_program_days(self, program_id: int) -> list[dict[str, object]]:
        return [dict(day) for day in self.days]

    async def get_exercises_for_day(self, day_id: int) -> list[dict[str, object]]:
        return [dict(ex) for ex in self.exercises_by_day.get(day_id, [])]

    async def get_current_day_index(self, user_id: str) -> int:
        return self.current_day_index

    async def update_exercise_category(self, exercise_name: str, new_category: str, user_id: str) -> dict[str, object] | None:
        self.updated = (exercise_name, new_category, user_id)
        return {
            "exercise_name": exercise_name,
            "old_category": "heavy_barbell",
            "new_category": new_category,
        }

    async def update_exercise_scheme_by_id(
        self,
        exercise_id: int,
        *,
        user_id: str,
        sets: int,
        rep_low: int | None,
        rep_high: int | None,
    ) -> dict[str, object] | None:
        for day in self.days:
            for exercise in self.exercises_by_day[day["id"]]:
                if int(exercise["id"]) != exercise_id:
                    continue
                result = {
                    "exercise_id": exercise_id,
                    "exercise_name": exercise["name"],
                    "day_name": day["name"],
                    "old_sets": exercise["sets"],
                    "old_rep_low": exercise["rep_range_low"],
                    "old_rep_high": exercise["rep_range_high"],
                    "new_sets": sets,
                    "new_rep_low": rep_low,
                    "new_rep_high": rep_high,
                }
                exercise["sets"] = sets
                exercise["rep_range_low"] = rep_low
                exercise["rep_range_high"] = rep_high
                return result
        return None

    async def update_exercise_category_by_id(self, exercise_id: int, new_category: str, *, user_id: str) -> dict[str, object] | None:
        new_label = str(new_category).strip().lower().replace("smith machine", "smith machine")
        for day in self.days:
            for exercise in self.exercises_by_day[day["id"]]:
                if int(exercise["id"]) != exercise_id:
                    continue
                old = exercise["equipment_type"]
                exercise["equipment_type"] = "smith machine" if new_label == "smith machine" else new_label
                if exercise["equipment_type"] == "dumbbell":
                    exercise["category"] = "dumbbell"
                elif exercise["equipment_type"] == "bodyweight":
                    exercise["category"] = "bodyweight"
                elif exercise["equipment_type"] == "smith machine":
                    exercise["category"] = "smith_machine"
                elif exercise["equipment_type"] in {"cable", "machine"}:
                    exercise["category"] = "cable_machine"
                else:
                    exercise["category"] = "heavy_barbell"
                return {
                    "exercise_id": exercise_id,
                    "exercise_name": exercise["name"],
                    "day_name": day["name"],
                    "old_category": old,
                    "new_category": exercise["equipment_type"],
                    "updated_rows": 1,
                }
        return None

    async def replace_exercise_in_active_program_by_id(
        self,
        exercise_id: int,
        *,
        user_id: str,
        new_name: str,
        new_category: str,
        new_equipment_type: str,
        new_sets: int | None = None,
        new_rep_low: int | None = None,
        new_rep_high: int | None = None,
    ) -> dict[str, object] | None:
        for day in self.days:
            for exercise in self.exercises_by_day[day["id"]]:
                if int(exercise["id"]) != exercise_id:
                    continue
                result = {
                    "exercise_id": exercise_id,
                    "old_name": exercise["name"],
                    "new_name": new_name,
                    "day_name": day["name"],
                    "old_sets": exercise["sets"],
                    "old_rep_low": exercise["rep_range_low"],
                    "old_rep_high": exercise["rep_range_high"],
                    "new_sets": new_sets if new_sets is not None else exercise["sets"],
                    "new_rep_low": new_rep_low if new_rep_low is not None else exercise["rep_range_low"],
                    "new_rep_high": new_rep_high if new_rep_high is not None else exercise["rep_range_high"],
                    "category": new_category,
                    "equipment_type": new_equipment_type,
                }
                exercise["name"] = new_name
                exercise["category"] = new_category
                exercise["equipment_type"] = new_equipment_type
                exercise["sets"] = result["new_sets"]
                exercise["rep_range_low"] = result["new_rep_low"]
                exercise["rep_range_high"] = result["new_rep_high"]
                return result
        return None

    async def add_exercise_to_program_day(
        self,
        day_id: int,
        *,
        user_id: str,
        name: str,
        sets: int,
        rep_low: int | None,
        rep_high: int | None,
        category: str,
        equipment_type: str,
        notes: str = "",
        muscle_groups: str = "",
    ) -> dict[str, object] | None:
        new_id = max(ex["id"] for rows in self.exercises_by_day.values() for ex in rows) + 1
        exercise = {
            "id": new_id,
            "name": name,
            "sets": sets,
            "rep_range_low": rep_low,
            "rep_range_high": rep_high,
            "category": category,
            "equipment_type": equipment_type,
        }
        self.exercises_by_day.setdefault(day_id, []).append(exercise)
        day_name = next(day["name"] for day in self.days if int(day["id"]) == day_id)
        return {
            "exercise_id": new_id,
            "exercise_name": name,
            "day_name": day_name,
            "display_order": len(self.exercises_by_day[day_id]) - 1,
            "sets": sets,
            "rep_range_low": rep_low,
            "rep_range_high": rep_high,
            "category": category,
            "equipment_type": equipment_type,
        }

    async def remove_exercise_from_active_program_by_id(self, exercise_id: int, *, user_id: str) -> dict[str, object] | None:
        for day in self.days:
            exercises = self.exercises_by_day[day["id"]]
            for index, exercise in enumerate(exercises):
                if int(exercise["id"]) != exercise_id:
                    continue
                removed = exercises.pop(index)
                return {
                    "exercise_id": exercise_id,
                    "exercise_name": removed["name"],
                    "day_name": day["name"],
                }
        return None


class _FakeTyping:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


class _FakeOllama:
    def __init__(self, responses: list[dict[str, object]]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str]] = []

    async def chat_json(self, *, system: str, user: str, temperature: float = 0.0, max_tokens: int | None = None) -> dict[str, object]:  # noqa: ARG002
        self.calls.append((system, user))
        return dict(self.responses.pop(0))


class _FakeChannel:
    def __init__(self) -> None:
        self.id = 999
        self.messages: list[str] = []

    async def send(self, text: str) -> None:
        self.messages.append(text)

    def typing(self) -> _FakeTyping:
        return _FakeTyping()


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

    def test_programme_equipment_aliases_expand(self) -> None:
        cog = ProgrammeCog(_DummyBot())

        self.assertEqual(cog._normalize_equipment_type("bb"), "barbell")
        self.assertEqual(cog._normalize_equipment_type("mach"), "machine")
        self.assertEqual(cog._normalize_equipment_type("cables"), "cable")

    def test_programme_fallback_classifier_covers_common_exercises(self) -> None:
        cog = ProgrammeCog(_DummyBot())

        self.assertEqual(cog._exercise_type_from_name_and_category("Leg Raises"), "bodyweight")
        self.assertEqual(cog._exercise_type_from_name_and_category("Lat Pulldown"), "machine")
        self.assertEqual(cog._exercise_type_from_name_and_category("Cable Row"), "cable")
        self.assertEqual(cog._exercise_type_from_name_and_category("Preacher Curl"), "barbell")
        self.assertEqual(cog._exercise_type_from_name_and_category("Walking Lunge"), "dumbbell")

    def test_programme_normalization_second_pass_rescues_unknown_equipment(self) -> None:
        cog = ProgrammeCog(_DummyBot())
        normalized = cog._normalize_program_payload(
            {
                "program_name": "Abs",
                "days": [
                    {
                        "name": "Core",
                        "exercises": [
                            {
                                "name": "Leg Raises",
                                "sets": 3,
                                "rep_range_low": 12,
                                "rep_range_high": 15,
                                "category": "",
                                "equipment_type": "",
                            }
                        ],
                    }
                ],
            }
        )

        exercise = normalized["days"][0]["exercises"][0]
        self.assertEqual(exercise["equipment_type"], "bodyweight")
        self.assertEqual(exercise["category"], "bodyweight")

    def test_index_based_correction_updates_referenced_exercise(self) -> None:
        bot = _DummyBot()
        bot.db = _FakeProgrammeDB()
        cog = ProgrammeCog(bot)
        channel = _FakeChannel()

        handled = asyncio.run(
            cog._handle_index_based_correction(channel, "1.1 to dumbbell", user_id="123")
        )

        self.assertTrue(handled)
        self.assertEqual(bot.db.updated, ("Smith Machine Bench Press", "dumbbell", "123"))
        self.assertIn("1.1 Smith Machine Bench Press", channel.messages[-1])

    def test_index_based_correction_rejects_invalid_day(self) -> None:
        bot = _DummyBot()
        bot.db = _FakeProgrammeDB()
        cog = ProgrammeCog(bot)
        channel = _FakeChannel()

        handled = asyncio.run(
            cog._handle_index_based_correction(channel, "99.1 to cable", user_id="123")
        )

        self.assertTrue(handled)
        self.assertEqual(channel.messages[-1], "Day 99 doesn't exist (program has 2 days).")

    def test_programme_router_updates_sets_reps(self) -> None:
        bot = _DummyBot()
        bot.db = _FakeProgrammeDB()
        bot.ollama = _FakeOllama(
            [{"action": "update_sets_reps", "exercise_ref": "2.1", "sets": 5, "rep_low": 10, "rep_high": 10}]
        )
        cog = ProgrammeCog(bot)
        channel = _FakeChannel()

        handled = asyncio.run(
            cog._handle_active_programme_message_with_llm(channel, "change 2.1 to 5x10", user_id="123")
        )

        self.assertTrue(handled)
        updated = bot.db.exercises_by_day[11][0]
        self.assertEqual(updated["sets"], 5)
        self.assertEqual(updated["rep_range_low"], 10)
        self.assertEqual(channel.messages[-1], "✅ 2.1 Lat Pulldowns: 4×10 -> 5×10")

    def test_programme_router_updates_types_per_ref(self) -> None:
        bot = _DummyBot()
        bot.db = _FakeProgrammeDB()
        bot.ollama = _FakeOllama(
            [
                {
                    "action": "update_type",
                    "exercises": {"2.1": "cable", "2.2": "cable", "2.3": "cable", "2.4": "cable", "2.6": "dumbbell"},
                }
            ]
        )
        cog = ProgrammeCog(bot)
        channel = _FakeChannel()

        handled = asyncio.run(
            cog._handle_active_programme_message_with_llm(
                channel,
                "2.1, 2.2, 2.3, 2.4 are cable. 2.6 is dumbbell",
                user_id="123",
            )
        )

        self.assertTrue(handled)
        self.assertEqual(bot.db.exercises_by_day[11][0]["equipment_type"], "cable")
        self.assertEqual(bot.db.exercises_by_day[11][1]["equipment_type"], "cable")
        self.assertEqual(bot.db.exercises_by_day[11][2]["equipment_type"], "cable")
        self.assertEqual(bot.db.exercises_by_day[11][3]["equipment_type"], "cable")
        self.assertEqual(bot.db.exercises_by_day[11][5]["equipment_type"], "dumbbell")
        self.assertIn("2.6 Preacher Curl: unknown -> dumbbell", channel.messages[-1])

    def test_programme_router_shows_full_summary(self) -> None:
        bot = _DummyBot()
        bot.db = _FakeProgrammeDB()
        bot.ollama = _FakeOllama([{"action": "show_program"}])
        cog = ProgrammeCog(bot)
        channel = _FakeChannel()

        handled = asyncio.run(
            cog._handle_active_programme_message_with_llm(channel, "show", user_id="123")
        )

        self.assertTrue(handled)
        self.assertIn("📋 JN PPLUL v2 (imported Mar 15)", channel.messages[0])
        self.assertIn("Current day: Day 2 - Pull", channel.messages[0])
        self.assertIn("2.1 Lat Pulldowns - 4×10 [unknown]", "\n".join(channel.messages))

    def test_programme_router_swaps_exercise(self) -> None:
        bot = _DummyBot()
        bot.db = _FakeProgrammeDB()
        bot.ollama = _FakeOllama(
            [
                {
                    "action": "swap_exercise",
                    "exercise_ref": "2.1",
                    "new_name": "Pull-Ups",
                    "new_sets": None,
                    "new_rep_low": None,
                    "new_rep_high": None,
                    "new_type": "bodyweight",
                }
            ]
        )
        cog = ProgrammeCog(bot)
        channel = _FakeChannel()

        handled = asyncio.run(
            cog._handle_active_programme_message_with_llm(channel, "swap 2.1 with pull-ups", user_id="123")
        )

        self.assertTrue(handled)
        updated = bot.db.exercises_by_day[11][0]
        self.assertEqual(updated["name"], "Pull-Ups")
        self.assertEqual(updated["equipment_type"], "bodyweight")
        self.assertEqual(channel.messages[-1], "✅ 2.1 Lat Pulldowns -> Pull-Ups (4×10) [bodyweight]")

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
