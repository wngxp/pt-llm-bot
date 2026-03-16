from __future__ import annotations

import unittest

from utils.discord_messages import DISCORD_MESSAGE_LIMIT, SAFE_DISCORD_MESSAGE_LIMIT, split_discord_message


class DiscordMessageTests(unittest.TestCase):
    def test_split_respects_discord_limit(self) -> None:
        text = "A" * (DISCORD_MESSAGE_LIMIT + 50)
        chunks = split_discord_message(text)

        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(chunk) <= SAFE_DISCORD_MESSAGE_LIMIT for chunk in chunks))
        self.assertEqual("".join(chunks), text)

    def test_split_prefers_newlines_when_possible(self) -> None:
        line = "A" * 500
        text = "\n".join([line] * 6)
        chunks = split_discord_message(text)

        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(chunk) <= SAFE_DISCORD_MESSAGE_LIMIT for chunk in chunks))
        self.assertIn("\n", chunks[0])

    def test_split_prefers_sentence_boundaries_when_possible(self) -> None:
        sentence = "This is a sentence that should stay intact. "
        text = sentence * 120

        chunks = split_discord_message(text)

        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(chunk) <= SAFE_DISCORD_MESSAGE_LIMIT for chunk in chunks))
        self.assertTrue(all(chunk.endswith((" ", ".")) for chunk in chunks[:-1]))
        self.assertEqual("".join(chunks), text)


if __name__ == "__main__":
    unittest.main()
