from __future__ import annotations

import discord
from discord.ext import commands


BOT_VERSION = "0.3.0"


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

    @commands.command(name="pthelp")
    async def help_command(self, ctx: commands.Context) -> None:
        lines = [
            "Core commands:",
            "- Workout-only: `ready`, `skip rest`, set logs like `225 x 3`",
            "- Utility (global): `!timezone`, `!volume`, `!e1rm`, `!export`, `!cue`, `!plates`, `!help`",
            "- `!start` / `!done` in workout channels",
            "- !plates <weight> [lbs|kg]",
            "- !e1rm <exercise>",
            "- !checkin / !summary in #check-in",
            "- !version",
        ]
        await ctx.send("\n".join(lines))
        await self._maybe_send_tip(ctx.channel)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(UtilityCog(bot))
