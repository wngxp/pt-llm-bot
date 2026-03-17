from __future__ import annotations

from datetime import datetime, timedelta, timezone

import discord
from discord.ext import commands

from utils.discord_messages import send_discord_text


BOT_VERSION = "0.6.4"
HELP_ENTRIES = {
    "help": {
        "syntax": "!help [command]",
        "description": "Show the command list or detailed help for one command.",
        "channel": "Any channel",
    },
    "version": {
        "syntax": "!version",
        "description": "Show the current bot version.",
        "channel": "Any channel",
    },
    "timezone": {
        "syntax": "!timezone <tz>",
        "description": "Set your timezone, for example `!timezone Asia/Shanghai`.",
        "channel": "#activity or utility channel",
    },
    "volume": {
        "syntax": "!volume",
        "description": "Show weekly set volume by muscle group.",
        "channel": "Workout channels",
    },
    "e1rm": {
        "syntax": "!e1rm <exercise>",
        "description": "Show estimated 1RM history for an exercise.",
        "channel": "Workout channels",
    },
    "export": {
        "syntax": "!export [exercise]",
        "description": "Export all workout logs, or one exercise, as CSV.",
        "channel": "Workout channels",
    },
    "plates": {
        "syntax": "!plates <weight> [kg|lbs]",
        "description": "Show a plate breakdown for a target weight.",
        "channel": "Workout channels",
    },
    "cue": {
        "syntax": "!cue <exercise> <text>",
        "description": "Save a personal lifting cue for an exercise.",
        "channel": "Workout channels",
    },
    "reset": {
        "syntax": "!reset confirm",
        "description": "Wipe workout data after a 60-second confirmation window.",
        "channel": "Admin/utility channel",
    },
    "summary": {
        "syntax": "!summary",
        "description": "Generate a weekly check-in summary.",
        "channel": "#check-in",
    },
    "checkin": {
        "syntax": "!checkin",
        "description": "Generate the current weekly check-in summary.",
        "channel": "#check-in",
    },
    "import": {
        "syntax": "!import <program text>",
        "description": "Start a programme import preview in #programme.",
        "channel": "#programme",
    },
    "program": {
        "syntax": "!program",
        "description": "Show your active program with day-by-day exercise details.",
        "channel": "#programme",
    },
    "startday": {
        "syntax": "!startday <day name or number>",
        "description": "Set the day you want to start from after importing or editing a program.",
        "channel": "#programme",
    },
    "travel": {
        "syntax": "!travel <constraints>",
        "description": "Draft a temporary travel version of the active program.",
        "channel": "#programme",
    },
    "start": {
        "syntax": "!start",
        "description": "Start today's workout session in the current weekday channel.",
        "channel": "Workout channels",
    },
    "done": {
        "syntax": "!done",
        "description": "Finish the current workout session immediately.",
        "channel": "Workout channels",
    },
    "skipday": {
        "syntax": "!skipday <day name or number>",
        "description": "Skip ahead to a different program day.",
        "channel": "Workout channels",
    },
    "goto": {
        "syntax": "!goto <day name or number>",
        "description": "Jump to a different program day.",
        "channel": "Workout channels",
    },
    "activity": {
        "syntax": "!activity <description>",
        "description": "Log an activity with recovery impact tracking.",
        "channel": "#activity",
    },
    "readiness": {
        "syntax": "!readiness <1-10>",
        "description": "Set your readiness score for training suggestions.",
        "channel": "#activity",
    },
    "phase": {
        "syntax": "!phase <cut|bulk|maintain>",
        "description": "Set your current training phase.",
        "channel": "#activity",
    },
    "prs": {
        "syntax": "!prs [days]",
        "description": "Show recently logged PR entries.",
        "channel": "#prs or utility channel",
    },
    "debug": {
        "syntax": "!debug",
        "description": "Show current state and active program info.",
        "channel": "Admin/utility channel",
    },
    "setday": {
        "syntax": "!setday <n>",
        "description": "Force the current program day index.",
        "channel": "Admin/utility channel",
    },
    "deleteprogram": {
        "syntax": "!deleteprogram confirm",
        "description": "Delete the active program after confirmation.",
        "channel": "Admin/utility channel",
    },
    "ask": {
        "syntax": "!ask <question>",
        "description": "Ask a fitness question in command form.",
        "channel": "#ask",
    },
}


class UtilityCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.settings = bot.settings
        self.db = bot.db
        self.pending_confirms: dict[tuple[int, str], datetime] = {}

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
            await send_discord_text(channel, f"Tip: use {ref} for utility commands like this.")

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _is_admin(self, ctx: commands.Context) -> bool:
        if not ctx.guild:
            return False
        if ctx.guild.owner_id == ctx.author.id:
            return True
        role_id = self.settings.admin_role_id
        if role_id and isinstance(ctx.author, discord.Member):
            return any(r.id == role_id for r in ctx.author.roles)
        return False

    async def _require_admin(self, ctx: commands.Context) -> bool:
        if self._is_admin(ctx):
            return True
        await send_discord_text(ctx.channel, "This command is restricted to the server owner/admin role.")
        return False

    def _start_confirm(self, user_id: int, action: str) -> None:
        self.pending_confirms[(user_id, action)] = self._now() + timedelta(seconds=60)

    def _consume_confirm(self, user_id: int, action: str) -> bool:
        key = (user_id, action)
        expires = self.pending_confirms.get(key)
        if not expires:
            return False
        if expires < self._now():
            self.pending_confirms.pop(key, None)
            return False
        self.pending_confirms.pop(key, None)
        return True

    async def _clear_active_workout_sessions(self, *, user_id: int | None = None) -> bool:
        workout_cog = self.bot.get_cog("WorkoutCog")
        if workout_cog is None:
            return False
        cleared = False
        sessions = getattr(workout_cog, "sessions", None)
        if isinstance(sessions, dict):
            if user_id is None:
                if sessions:
                    sessions.clear()
                    cleared = True
            else:
                removed = sessions.pop(str(user_id), None)
                if removed is not None:
                    cleared = True
        early_prompts = getattr(workout_cog, "early_end_prompts", None)
        if isinstance(early_prompts, dict):
            if user_id is None:
                if early_prompts:
                    early_prompts.clear()
                    cleared = True
            else:
                keys = [k for k in early_prompts if int(k[1]) == int(user_id)]
                for key in keys:
                    early_prompts.pop(key, None)
                    cleared = True
        timeout_tasks = getattr(workout_cog, "early_end_timeout_tasks", None)
        if isinstance(timeout_tasks, dict):
            if user_id is None:
                if timeout_tasks:
                    cleared = True
                for task in timeout_tasks.values():
                    if hasattr(task, "done") and not task.done():
                        task.cancel()
                timeout_tasks.clear()
            else:
                keys = [k for k in timeout_tasks if int(k[1]) == int(user_id)]
                for key in keys:
                    task = timeout_tasks.pop(key, None)
                    if task and hasattr(task, "done") and not task.done():
                        task.cancel()
                    cleared = True
        message_map = getattr(workout_cog, "message_log_map", None)
        if isinstance(message_map, dict) and message_map:
            if user_id is None:
                message_map.clear()
                cleared = True
        user_locks = getattr(workout_cog, "user_locks", None)
        if isinstance(user_locks, dict) and user_locks:
            if user_id is None:
                user_locks.clear()
            else:
                user_locks.pop(int(user_id), None)
        return cleared

    def _clear_runtime_memory_state(self, *, user_id: int | None = None) -> None:
        for cog_name in ("ProgrammeCog", "AskCog", "CheckInCog"):
            cog = self.bot.get_cog(cog_name)
            if cog is None:
                continue
            if user_id is None:
                clear_runtime = getattr(cog, "clear_runtime_state", None)
                if callable(clear_runtime):
                    clear_runtime()
                    continue
            memory = getattr(cog, "memory", None)
            if memory and hasattr(memory, "clear_all"):
                if user_id is None:
                    memory.clear_all()
                elif hasattr(memory, "clear_user"):
                    memory.clear_user(user_id=user_id)

    @commands.command(name="version")
    async def version_command(self, ctx: commands.Context) -> None:
        await send_discord_text(ctx.channel, f"PT-LLM Bot v{BOT_VERSION}")
        await self._maybe_send_tip(ctx.channel)

    @commands.command(name="help", aliases=["pthelp", "commands"])
    async def help_command(self, ctx: commands.Context, *, command_name: str = "") -> None:
        normalized = command_name.strip().lower().lstrip("!")
        if normalized:
            entry = HELP_ENTRIES.get(normalized)
            if entry is None:
                await send_discord_text(ctx.channel, f"No help entry for `{command_name.strip()}`. Try `!help`.")
                return
            embed = discord.Embed(
                title=f"!{normalized}",
                description=entry["description"],
                color=discord.Color.blue(),
            )
            embed.add_field(name="Syntax", value=entry["syntax"], inline=False)
            embed.add_field(name="Where", value=entry["channel"], inline=False)
            embed.set_footer(text="See COMMANDS.md in the repo root for the full user manual.")
            await ctx.channel.send(embed=embed)
            await self._maybe_send_tip(ctx.channel)
            return

        embed = discord.Embed(
            title="PT-LLM Command Guide",
            description="Hard commands are listed below. I also understand natural language in the channel where you use me.",
            color=discord.Color.blue(),
        )
        embed.add_field(
            name="Workout",
            value=(
                "`!start` start today's workout\n"
                "`!done` finish the current session\n"
                "`!skipday` / `!goto` change the queued program day\n"
                "`!plates` plate math\n"
                "`!e1rm` estimated 1RM history\n"
                "`!volume` weekly volume\n"
                "`!cue` save a cue\n"
                "`!export` export logs"
            ),
            inline=False,
        )
        embed.add_field(
            name="Programme",
            value=(
                "`!import` preview a pasted program\n"
                "`!program` show the active program\n"
                "`!startday` choose the starting day\n"
                "`!travel` draft a temporary travel program\n"
                "`!deleteprogram` delete the active program"
            ),
            inline=False,
        )
        embed.add_field(
            name="Check-In / Activity",
            value=(
                "`!checkin` / `!summary` weekly summary\n"
                "`!activity` log outside activity\n"
                "`!readiness` set readiness\n"
                "`!phase` set cut/bulk/maintain\n"
                "`!timezone` set timezone"
            ),
            inline=False,
        )
        embed.add_field(
            name="Utility / Admin",
            value=(
                "`!help` command guide\n"
                "`!version` bot version\n"
                "`!prs` recent PRs\n"
                "`!ask` command-style question\n"
                "`!debug` admin state dump\n"
                "`!setday` admin day override\n"
                "`!reset` reset workout data"
            ),
            inline=False,
        )
        embed.add_field(
            name="Natural Language",
            value="Try `what exercises do I have left?`, `same`, `switch to dumbbell`, or `show my program`.",
            inline=False,
        )
        embed.set_footer(text="Use !help <command> for details. Full manual: COMMANDS.md in the repo root.")
        await ctx.channel.send(embed=embed)
        await self._maybe_send_tip(ctx.channel)

    @commands.command(name="debug")
    async def debug_command(self, ctx: commands.Context) -> None:
        if not await self._require_admin(ctx):
            return
        user_id = str(ctx.author.id)
        state = await self.db.get_user_state(user_id)
        program = await self.db.get_active_program(user_id)
        program_name = str(program["name"]) if program else "None"
        lines = [
            "Debug state:",
            f"- active_program: {program_name}",
            f"- current_day_index: {state.get('current_day_index', 0)}",
            f"- readiness: {state.get('readiness', 7)}",
            f"- streak: {state.get('current_streak', 0)} (longest {state.get('longest_streak', 0)})",
            f"- last_workout_date: {state.get('last_workout_date') or 'None'}",
            f"- phase: {state.get('phase', 'maintain')}",
            f"- timezone: {state.get('timezone', 'UTC')}",
        ]
        await send_discord_text(ctx.channel, "\n".join(lines))

    @commands.command(name="setday")
    async def setday_command(self, ctx: commands.Context, day_number: int) -> None:
        if not await self._require_admin(ctx):
            return
        user_id = str(ctx.author.id)
        active = await self.db.get_active_program(user_id)
        if not active:
            await send_discord_text(ctx.channel, "No active program.")
            return
        days = await self.db.get_program_days(int(active["id"]))
        if not days:
            await send_discord_text(ctx.channel, "Active program has no days.")
            return
        idx = day_number - 1
        if idx < 0 or idx >= len(days):
            await send_discord_text(ctx.channel, f"Day must be between 1 and {len(days)}.")
            return
        ended = await self._clear_active_workout_sessions(user_id=ctx.author.id)
        await self.db.set_current_day_index(idx, user_id=user_id)
        if ended:
            await send_discord_text(
                ctx.channel,
                f"Current session ended. Day set to {day_number} ({days[idx]['name']}). Type `ready` to start.",
            )
            return
        await send_discord_text(ctx.channel, f"Set current day to Day {day_number} - {days[idx]['name']}.")

    @commands.command(name="reset")
    async def reset_command(self, ctx: commands.Context, confirm: str = "") -> None:
        if not await self._require_admin(ctx):
            return
        if confirm.strip().lower() == "confirm":
            if not self._consume_confirm(ctx.author.id, "reset"):
                await send_discord_text(ctx.channel, "Reset confirmation expired. Run `!reset` again.")
                return
            await self.db.wipe_workout_data_preserve_settings(user_id=str(ctx.author.id))
            await self._clear_active_workout_sessions(user_id=ctx.author.id)
            self._clear_runtime_memory_state(user_id=ctx.author.id)
            await send_discord_text(ctx.channel, "All workout data wiped. Timezone/settings were preserved.")
            return
        self._start_confirm(ctx.author.id, "reset")
        await send_discord_text(
            ctx.channel,
            "This will wipe workout logs, activities, PRs, injuries, and streak data. "
            "Run `!reset confirm` within 60 seconds to proceed.",
        )

    @commands.command(name="deleteprogram")
    async def deleteprogram_command(self, ctx: commands.Context, confirm: str = "") -> None:
        if not await self._require_admin(ctx):
            return
        if confirm.strip().lower() == "confirm":
            if not self._consume_confirm(ctx.author.id, "deleteprogram"):
                await send_discord_text(ctx.channel, "Delete confirmation expired. Run `!deleteprogram` again.")
                return
            ok = await self.db.delete_active_program(user_id=str(ctx.author.id))
            await self._clear_active_workout_sessions(user_id=ctx.author.id)
            if ok:
                await send_discord_text(ctx.channel, "Active program deleted.")
            else:
                await send_discord_text(ctx.channel, "No active program to delete.")
            return
        self._start_confirm(ctx.author.id, "deleteprogram")
        await send_discord_text(
            ctx.channel,
            "This will delete the active program and its linked exercises/logs. "
            "Run `!deleteprogram confirm` within 60 seconds to proceed.",
        )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(UtilityCog(bot))
