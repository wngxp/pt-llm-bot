from __future__ import annotations

import json
import re
from typing import Optional

import discord
from discord.ext import commands

from llm.prompts import COACH_SYSTEM_PROMPT
from utils.conversation_memory import ConversationMemory
from utils.discord_messages import send_discord_text
from utils.week_context import get_program_position, inject_program_context


DAY_HINT_RE = re.compile(r"^\s*(day\s*\d+|push|pull|legs?|upper|lower)\b", re.IGNORECASE | re.MULTILINE)
SET_HINT_RE = re.compile(r"\d+\s*[xX×]\s*\d+")
IMPORT_INTENT_RE = re.compile(r"\b(import this|use this|save this|import it)\b", re.IGNORECASE)


class CoachCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.settings = bot.settings
        self.db = bot.db
        self.memory = ConversationMemory(max_messages=10, ttl_minutes=30)
        self._last_program_text: dict[tuple[int, int], str] = {}

    def clear_runtime_state(self) -> None:
        self.memory.clear_all()
        self._last_program_text.clear()

    def _is_coach_channel(self, channel: discord.abc.GuildChannel | discord.Thread | discord.abc.PrivateChannel) -> bool:
        cid = getattr(channel, "id", None)
        name = getattr(channel, "name", "")
        if self.settings.coach_channel_id:
            return cid == self.settings.coach_channel_id or name == "coach"
        return name == "coach"

    def _looks_like_program_text(self, text: str) -> bool:
        return bool(DAY_HINT_RE.search(text or "") and SET_HINT_RE.search(text or ""))

    async def _answer(self, message: discord.Message, content: str) -> str:
        user_id = str(message.author.id)
        context = await self.db.build_context(target_date=self.bot.today(), user_id=user_id)
        history = self.memory.get(user_id=message.author.id, channel_id=message.channel.id)
        state = context.get("user_state") or {}
        current_day = context.get("current_day") or {}
        current_program = context.get("current_program") or {}
        block, week, day_number, day_name = get_program_position(current_day, state)
        system_prompt = inject_program_context(
            COACH_SYSTEM_PROMPT,
            program_name=str(current_program.get("name") or "").strip() or None,
            block=block,
            week=week,
            day_number=day_number,
            day_name=day_name,
        )
        payload = {
            "message": content,
            "history": history,
            "context": {
                "current_program": current_program,
                "current_day": current_day,
                "user_state": state,
                "recent_prs": context.get("recent_prs", [])[:10],
                "recent_activities": context.get("recent_activities", [])[:10],
                "weekly_volume": context.get("weekly_volume"),
                "recent_performance_trend": context.get("recent_performance_trend", [])[:10],
                "program_total_weeks": context.get("program_total_weeks"),
            },
        }
        return await self.bot.ollama.chat(
            system=system_prompt,
            user=json.dumps(payload, ensure_ascii=False),
            temperature=0.25,
            max_tokens=900,
        )

    async def _handoff_program_import(self, message: discord.Message) -> None:
        key = (message.author.id, message.channel.id)
        program_text = self._last_program_text.get(key)
        if not program_text:
            await send_discord_text(
                message.channel,
                "I don't have a recent program draft in this thread yet. Ask me to build one first.",
            )
            return
        programme_cog = self.bot.get_cog("ProgrammeCog")
        if programme_cog is None:
            await send_discord_text(message.channel, "Programme import is unavailable right now.")
            return
        handoff = getattr(programme_cog, "start_import_handoff_from_coach", None)
        if not callable(handoff):
            await send_discord_text(message.channel, "Programme import handoff is unavailable right now.")
            return

        target_channel = await handoff(
            author=message.author,
            guild=message.guild,
            raw_text=program_text,
        )
        if target_channel is not None:
            await send_discord_text(
                message.channel,
                f"Program draft sent to {target_channel.mention}. Review there and reply `save` when ready.",
            )
            return
        await send_discord_text(
            message.channel,
            "I couldn't find #programme to hand this off. Please paste the draft there and reply `save`.",
        )

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        if not self._is_coach_channel(message.channel):
            return

        content = message.content.strip()
        if not content or content.startswith(self.settings.command_prefix):
            return

        if IMPORT_INTENT_RE.search(content):
            await self._handoff_program_import(message)
            return

        self.memory.append(user_id=message.author.id, channel_id=message.channel.id, role="user", content=content)
        try:
            async with message.channel.typing():
                reply = (await self._answer(message, content)).strip()
        except Exception as exc:
            await send_discord_text(message.channel, f"Couldn't reach coach model: {exc}")
            return
        self.memory.append(user_id=message.author.id, channel_id=message.channel.id, role="assistant", content=reply)
        if self._looks_like_program_text(reply):
            self._last_program_text[(message.author.id, message.channel.id)] = reply
        await send_discord_text(message.channel, reply)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(CoachCog(bot))
