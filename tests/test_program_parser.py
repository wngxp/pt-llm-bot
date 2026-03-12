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


if __name__ == "__main__":
    unittest.main()
