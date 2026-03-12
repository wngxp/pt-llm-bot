from __future__ import annotations

import json
import re

import discord
from discord.ext import commands

from llm.prompts import ASK_SYSTEM_PROMPT
from utils.conversation_memory import ConversationMemory
from utils.discord_messages import send_discord_text


QUESTION_WORD_RE = re.compile(r"\b(what|why|how|can|should|when|where|which|who)\b", re.IGNORECASE)


class AskCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.settings = bot.settings
        self.db = bot.db
        self.memory = ConversationMemory(max_messages=10, ttl_minutes=30)

    def _is_ask_channel(self, channel: discord.abc.GuildChannel | discord.Thread | discord.abc.PrivateChannel) -> bool:
        cid = getattr(channel, "id", None)
        name = getattr(channel, "name", "")
        if self.settings.ask_channel_id:
            return cid == self.settings.ask_channel_id or name == "ask"
        return name == "ask"

    def _is_reply_to_bot(self, message: discord.Message) -> bool:
        ref = message.reference
        if not ref:
            return False
        resolved = ref.resolved
        if isinstance(resolved, discord.Message) and self.bot.user:
            return resolved.author.id == self.bot.user.id
        return False

    def _should_respond(self, message: discord.Message, content: str) -> bool:
        if self.bot.user and self.bot.user in message.mentions:
            return True
        if self._is_reply_to_bot(message):
            return True
        if "?" in content:
            return True
        return bool(QUESTION_WORD_RE.search(content))

    async def _answer(self, question: str, *, user_id: int, channel_id: int) -> str:
        context = await self.db.build_context(target_date=self.bot.today())
        history = self.memory.get(user_id=user_id, channel_id=channel_id)
        prompt = {
            "question": question,
            "history": history,
            "context": {
                "phase": context["user_state"].get("phase"),
                "readiness": context["user_state"].get("readiness"),
                "weekly_volume": context.get("weekly_volume"),
                "recent_prs": context.get("recent_prs")[:5],
            },
        }
        return await self.bot.ollama.chat(
            system=ASK_SYSTEM_PROMPT,
            user=json.dumps(prompt, ensure_ascii=False),
            temperature=0.3,
            max_tokens=150,
        )

    @commands.command(name="ask")
    async def ask_command(self, ctx: commands.Context, *, question: str) -> None:
        self.memory.append(user_id=ctx.author.id, channel_id=ctx.channel.id, role="user", content=question)
        reply = await self._answer(question, user_id=ctx.author.id, channel_id=ctx.channel.id)
        self.memory.append(user_id=ctx.author.id, channel_id=ctx.channel.id, role="assistant", content=reply)
        await send_discord_text(ctx.channel, reply)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        if not self._is_ask_channel(message.channel):
            return

        content = message.content.strip()
        if not content or content.startswith(self.settings.command_prefix):
            return
        if not self._should_respond(message, content):
            return

        try:
            self.memory.append(user_id=message.author.id, channel_id=message.channel.id, role="user", content=content)
            reply = await self._answer(content, user_id=message.author.id, channel_id=message.channel.id)
            self.memory.append(
                user_id=message.author.id,
                channel_id=message.channel.id,
                role="assistant",
                content=reply,
            )
            await send_discord_text(message.channel, reply)
        except Exception as exc:
            await message.channel.send(f"Couldn't reach coaching model: {exc}")


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AskCog(bot))
