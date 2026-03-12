from __future__ import annotations

import discord
from discord.ext import commands


BOT_VERSION = "0.2.2"


class UtilityCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.settings = bot.settings

    def _settings_channel_ref(self, channel: discord.abc.Messageable) -> str | None:
        settings_id = self.settings.settings_channel_id
        if not settings_id:
            return None
        if getattr(channel, "id", None) == settings_id:
            return None
        guild = getattr(channel, "guild", None)
        if guild and isinstance(guild, discord.Guild):
            target = guild.get_channel(settings_id)
            if isinstance(target, discord.TextChannel):
                return target.mention
        return "#settings"

    async def _maybe_send_tip(self, channel: discord.abc.Messageable) -> None:
        ref = self._settings_channel_ref(channel)
        if ref:
            await channel.send(f"Tip: use {ref} for utility commands like this.")

    @commands.command(name="version")
    async def version_command(self, ctx: commands.Context) -> None:
        await ctx.send(f"PT-LLM Bot v{BOT_VERSION}")
        await self._maybe_send_tip(ctx.channel)

    @commands.command(name="help")
    async def help_command(self, ctx: commands.Context) -> None:
        lines = [
            "Core commands:",
            "- !start / ready",
            "- !done",
            "- !plates <weight> [lbs|kg]",
            "- !volume",
            "- !e1rm <exercise>",
            "- !export [exercise]",
            "- !cue <exercise> <text>",
            "- !timezone [IANA timezone]",
            "- !checkin",
            "- !version",
        ]
        await ctx.send("\n".join(lines))
        await self._maybe_send_tip(ctx.channel)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(UtilityCog(bot))
