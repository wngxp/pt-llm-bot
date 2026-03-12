from __future__ import annotations

import unittest

from llm.parser import ProgramParser


class _FakeClient:
    async def chat_json(self, *, system: str, user: str, temperature: float = 0.0):  # noqa: ARG002
        # Simulate the mis-parse observed in live tests:
        # Pause Squat gets renamed to Squat and rep scheme copied incorrectly.
        return {
            "program_name": "Test Legs",
            "days": [
                {
                    "day_order": 0,
                    "name": "Legs",
                    "exercises": [
                        {
                            "name": "Squat",
                            "sets": 1,
                            "rep_range_low": 2,
                            "rep_range_high": 4,
                            "category": "heavy_barbell",
                            "muscle_groups": "quads,glutes",
                        },
                        {
                            "name": "Squat",
                            "sets": 1,
                            "rep_range_low": 2,
                            "rep_range_high": 4,
                            "category": "heavy_barbell",
                            "muscle_groups": "quads,glutes",
                        },
                        {
                            "name": "RDL",
                            "sets": 3,
                            "rep_range_low": 8,
                            "rep_range_high": 10,
                            "category": "dumbbell",
                            "muscle_groups": "hamstrings,glutes",
                        },
                    ],
                }
            ],
        }


class ProgramParserTests(unittest.IsolatedAsyncioTestCase):
    async def test_pause_squat_is_preserved_as_distinct_exercise(self) -> None:
        raw = """
Legs
Squat - 1x2-4
Pause Squat - 2x5
RDL - 3x8-10
""".strip()

        parser = ProgramParser(_FakeClient())
        parsed = await parser.parse_program(raw)

        day = parsed["days"][0]
        names = [ex["name"] for ex in day["exercises"]]
        self.assertEqual(names, ["Squat", "Pause Squat", "RDL"])

        pause = day["exercises"][1]
        self.assertEqual(pause["sets"], 2)
        self.assertEqual(pause["rep_range_low"], 5)
        self.assertEqual(pause["rep_range_high"], 5)

    async def test_complex_rep_scheme_exercises_are_not_dropped(self) -> None:
        raw = """
Upper
Close-Grip Incline Press - 3x(8, 5, 12)
Lateral Raises - 3x(5+15)
""".strip()

        parser = ProgramParser(_FakeClient())
        parsed = await parser.parse_program(raw)

        day = parsed["days"][0]
        names = [ex["name"] for ex in day["exercises"]]
        self.assertEqual(names, ["Close-Grip Incline Press", "Lateral Raises"])

        cgi = day["exercises"][0]
        self.assertEqual(cgi["sets"], 3)
        self.assertIn("varying reps", str(cgi["notes"]).lower())

        laterals = day["exercises"][1]
        self.assertEqual(laterals["sets"], 3)
        self.assertIn("5 eccentric + 15 constant tension", str(laterals["notes"]).lower())

    async def test_category_lookup_handles_smith_swap_sensitive_exercises(self) -> None:
        parser = ProgramParser(_FakeClient())
        self.assertEqual(parser._category_lookup("Close-Grip Incline Press"), "light_barbell")
        self.assertEqual(parser._category_lookup("Arnold Press"), "dumbbell")
        self.assertEqual(parser._category_lookup("Cable Y-Raises"), "cable_machine")
        self.assertEqual(parser._category_lookup("Walking Lunge"), "bodyweight")
        self.assertEqual(parser._category_lookup("DB Walking Lunge"), "dumbbell")


if __name__ == "__main__":
    unittest.main()
