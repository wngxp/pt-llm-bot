from __future__ import annotations

import unittest

from utils.discord_messages import DISCORD_MESSAGE_LIMIT, split_discord_message


class DiscordMessageTests(unittest.TestCase):
    def test_split_respects_discord_limit(self) -> None:
        text = "A" * (DISCORD_MESSAGE_LIMIT + 50)
        chunks = split_discord_message(text)

        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(chunk) <= DISCORD_MESSAGE_LIMIT for chunk in chunks))
        self.assertEqual("".join(chunks), text)

    def test_split_prefers_newlines_when_possible(self) -> None:
        line = "A" * 500
        text = "\n".join([line] * 6)
        chunks = split_discord_message(text)

        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(chunk) <= DISCORD_MESSAGE_LIMIT for chunk in chunks))
        self.assertIn("\n", chunks[0])


if __name__ == "__main__":
    unittest.main()
