from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from uuid import uuid4

import discord
from discord.ext import commands

from llm.parser import ProgramParser
from llm.prompts import FITNESS_ONLY_GUARDRAIL
from utils.conversation_memory import ConversationMemory
from utils.discord_messages import send_discord_text


DURATION_RE = re.compile(r"(?P<num>\d+)\s*(?P<unit>day|days|week|weeks)", re.IGNORECASE)
DAY_NUMBER_RE = re.compile(r"\bday\s*(?P<day_num>\d+)\b", re.IGNORECASE)
START_DAY_INTENT_RE = re.compile(r"\b(start|begin|starting|start on|begin with)\b", re.IGNORECASE)
EDIT_INTENT_RE = re.compile(r"\b(edit|swap|replace|change|modify)\b", re.IGNORECASE)
SET_PATTERN_RE = re.compile(r"\d+\s*[xX×]\s*\d+")
PENDING_CONFIRM_TOKENS = {"save", "confirm", "import", "looks good", "ship it", "yes"}
PENDING_CANCEL_TOKENS = {"cancel", "stop", "never mind", "discard"}

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PendingProgram:
    user_id: int
    channel_id: int
    raw_text: str
    created_at: datetime
    notes: list[str] = field(default_factory=list)
    latest_suggestions: str = ""
    flow_id: str = field(default_factory=lambda: uuid4().hex)


class ProgrammeCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.settings = bot.settings
        self.db = bot.db
        self.parser = ProgramParser(bot.ollama)
        self.post_import_state: dict[int, dict[str, Any]] = {}
        self.pending_programs: dict[tuple[int, int], PendingProgram] = {}
        self.review_tasks: dict[tuple[int, int], asyncio.Task[list[str]]] = {}
        self.closed_flows: dict[tuple[int, int], tuple[str, datetime]] = {}
        self.user_locks: dict[int, asyncio.Lock] = {}
        self.memory = ConversationMemory(max_messages=10, ttl_minutes=30)

    def _get_user_lock(self, user_id: int) -> asyncio.Lock:
        lock = self.user_locks.get(user_id)
        if lock is None:
            lock = asyncio.Lock()
            self.user_locks[user_id] = lock
        return lock

    def _is_programme_channel(self, channel: discord.abc.GuildChannel | discord.Thread | discord.abc.PrivateChannel) -> bool:
        cid = getattr(channel, "id", None)
        name = getattr(channel, "name", "")
        if self.settings.programme_channel_id:
            return cid == self.settings.programme_channel_id or name == "programme"
        return name == "programme"

    def _is_placeholder_name(self, name: str) -> bool:
        cleaned = name.strip().lower()
        if not cleaned:
            return True
        return cleaned in {"unnamed", "unnamed program", "untitled", "untitled program", "imported program"}

    def _infer_program_name_from_text(self, raw_text: str) -> str | None:
        patterns = [
            re.compile(r"\bprogram\s*[:\-]\s*(?P<name>[A-Za-z0-9][A-Za-z0-9 '\-_]{1,60})", re.IGNORECASE),
            re.compile(r"\b(?:this is|it's|it is)\s+(?:my\s+)?(?P<name>[A-Za-z0-9][A-Za-z0-9 '\-_]{1,60})\s+program\b", re.IGNORECASE),
            re.compile(r"\bmy\s+(?P<name>[A-Za-z0-9][A-Za-z0-9 '\-_]{1,60})\s+program\b", re.IGNORECASE),
        ]
        for pattern in patterns:
            match = pattern.search(raw_text)
            if match:
                return match.group("name").strip()
        return None

    def _now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

    def _set_post_import_context(self, user_id: int, channel_id: int, program_id: int) -> None:
        self.post_import_state[user_id] = {
            "program_id": program_id,
            "channel_id": channel_id,
            "expires_at": self._now_utc() + timedelta(minutes=5),
        }

    def _clear_post_import_context(self, user_id: int) -> None:
        self.post_import_state.pop(user_id, None)

    def _get_post_import_context(self, user_id: int, channel_id: int) -> Optional[dict[str, Any]]:
        state = self.post_import_state.get(user_id)
        if not state:
            return None
        if int(state.get("channel_id") or 0) != channel_id:
            return None
        expires_at = state.get("expires_at")
        if not isinstance(expires_at, datetime) or expires_at < self._now_utc():
            self._clear_post_import_context(user_id)
            return None
        return state

    def _looks_like_program_paste(self, text: str) -> bool:
        lowered = text.lower()
        if len(text.splitlines()) < 2:
            return False
        if len(SET_PATTERN_RE.findall(text)) < 2:
            return False
        day_markers = any(token in lowered for token in ["day ", "push", "pull", "legs", "upper", "lower"])
        return day_markers

    def _normalize_name(self, text: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", text.lower())

    def _parse_start_day_index(self, text: str, days: list[dict[str, Any]]) -> Optional[int]:
        plain_num = re.fullmatch(r"\s*(\d+)\s*", text)
        if plain_num:
            idx = int(plain_num.group(1)) - 1
            if 0 <= idx < len(days):
                return idx

        match = DAY_NUMBER_RE.search(text)
        if match:
            idx = int(match.group("day_num")) - 1
            if 0 <= idx < len(days):
                return idx

        lowered = text.lower()
        for day in days:
            day_name = str(day.get("name") or "").strip()
            if day_name and day_name.lower() in lowered:
                return int(day["day_order"])

        normalized_text = self._normalize_name(text)
        for day in days:
            day_name = self._normalize_name(str(day.get("name") or ""))
            if day_name and day_name in normalized_text:
                return int(day["day_order"])

        return None

    def _pending_key(self, user_id: int, channel_id: int) -> tuple[int, int]:
        return (int(user_id), int(channel_id))

    def _get_pending_program(self, user_id: int, channel_id: int) -> Optional[PendingProgram]:
        key = self._pending_key(user_id, channel_id)
        state = self.pending_programs.get(key)
        if not state:
            return None
        if state.created_at + timedelta(minutes=30) < self._now_utc():
            self.pending_programs.pop(key, None)
            return None
        return state

    def _set_pending_program(self, state: PendingProgram) -> None:
        key = self._pending_key(state.user_id, state.channel_id)
        self.pending_programs[key] = state
        closed = self.closed_flows.get(key)
        if closed and closed[0] == state.flow_id:
            self.closed_flows.pop(key, None)

    def _clear_pending_program(self, user_id: int, channel_id: int) -> None:
        self.pending_programs.pop(self._pending_key(user_id, channel_id), None)

    def _is_flow_closed(self, user_id: int, channel_id: int, flow_id: str) -> bool:
        key = self._pending_key(user_id, channel_id)
        record = self.closed_flows.get(key)
        if not record:
            return False
        closed_flow, expires_at = record
        if expires_at < self._now_utc():
            self.closed_flows.pop(key, None)
            return False
        return closed_flow == flow_id

    def _mark_flow_closed(self, user_id: int, channel_id: int, flow_id: str) -> None:
        self.closed_flows[self._pending_key(user_id, channel_id)] = (
            flow_id,
            self._now_utc() + timedelta(minutes=10),
        )

    def _is_pending_flow_active(self, state: PendingProgram) -> bool:
        if self._is_flow_closed(state.user_id, state.channel_id, state.flow_id):
            return False
        current = self._get_pending_program(state.user_id, state.channel_id)
        if not current:
            return False
        return current.flow_id == state.flow_id

    def _cancel_review_task(self, user_id: int, channel_id: int) -> None:
        key = self._pending_key(user_id, channel_id)
        task = self.review_tasks.pop(key, None)
        if task and not task.done():
            task.cancel()

    def _extract_exercise_summary(self, raw_text: str) -> list[dict[str, Any]]:
        blocks = self.parser._extract_day_blocks(raw_text)
        summary: list[dict[str, Any]] = []
        for block in blocks:
            day_name = str(block.get("name") or "Day")
            for ex in block.get("exercises", []):
                name = str(ex.get("name") or "").strip()
                if not name:
                    continue
                summary.append(
                    {
                        "day": day_name,
                        "name": name,
                        "sets": ex.get("sets"),
                        "rep_range_low": ex.get("rep_range_low"),
                        "rep_range_high": ex.get("rep_range_high"),
                        "notes": ex.get("notes") or "",
                        "category": self.parser._category_lookup(name),
                    }
                )
        return summary

    def _clear_programme_flow_state(self, user_id: int, channel_id: int) -> None:
        pending = self._get_pending_program(user_id, channel_id)
        if pending:
            self._mark_flow_closed(user_id, channel_id, pending.flow_id)
        self._cancel_review_task(user_id, channel_id)
        self._clear_pending_program(user_id, channel_id)
        self._clear_post_import_context(user_id)
        self.memory.clear(user_id=user_id, channel_id=channel_id)

    def clear_runtime_state(self) -> None:
        for task in self.review_tasks.values():
            if task and not task.done():
                task.cancel()
        self.review_tasks.clear()
        self.post_import_state.clear()
        self.pending_programs.clear()
        self.closed_flows.clear()
        self.user_locks.clear()
        self.memory.clear_all()

    def _is_confirm_message(self, text: str) -> bool:
        normalized = " ".join(text.strip().lower().split())
        return normalized in PENDING_CONFIRM_TOKENS or normalized.startswith("save")

    def _is_cancel_message(self, text: str) -> bool:
        normalized = " ".join(text.strip().lower().split())
        return normalized in PENDING_CANCEL_TOKENS

    async def _resolve_program_name(
        self,
        channel: discord.abc.Messageable,
        author: discord.abc.User,
        raw_text: str,
        parsed_name: str,
    ) -> str:
        if not self._is_placeholder_name(parsed_name):
            return parsed_name.strip()

        inferred = self._infer_program_name_from_text(raw_text)
        if inferred:
            return inferred

        await send_discord_text(channel, "What would you like to name this program?")
        try:
            reply = await self.bot.wait_for(
                "message",
                timeout=60,
                check=lambda m: m.author.id == author.id and m.channel.id == getattr(channel, "id", None),
            )
            candidate = reply.content.strip()
            if candidate:
                return candidate[:80]
        except asyncio.TimeoutError:
            pass
        return "Imported Program"

    async def _start_day_prompt(self, channel: discord.abc.Messageable, program_id: int) -> None:
        days = await self.db.get_program_days(program_id)
        if not days:
            return
        lines = ["Which day would you like to start on?"]
        for day in days:
            lines.append(f"{day['day_order'] + 1}. {day['name']}")
        lines.append("Reply with `start on Legs` or `start on Day 3`.")
        await send_discord_text(channel, "\n".join(lines))

    async def _handle_start_day_message(
        self,
        channel: discord.abc.Messageable,
        text: str,
        *,
        program_id: Optional[int] = None,
        allow_implicit: bool = False,
        user_id: Optional[int] = None,
    ) -> bool:
        if program_id is None:
            active = await self.db.get_active_program()
            if not active:
                return False
            program_id = int(active["id"])

        days = await self.db.get_program_days(program_id)
        if not days:
            return False

        lowered = text.lower()
        has_intent = bool(START_DAY_INTENT_RE.search(lowered) or DAY_NUMBER_RE.search(lowered))
        if not allow_implicit and not has_intent:
            return False

        idx = self._parse_start_day_index(text, days)
        if idx is None:
            lines = ["I couldn't map that to a program day. Try one of:"]
            for day in days:
                lines.append(f"{day['day_order'] + 1}. {day['name']}")
            await send_discord_text(channel, "\n".join(lines))
            return True

        await self.db.set_current_day_index(idx)
        logger.info("Set current_day_index to %s for user %s", idx, user_id if user_id is not None else "unknown")
        selected = next((d for d in days if int(d["day_order"]) == idx), days[idx])
        await send_discord_text(
            channel,
            f"✅ Starting day set to **{selected['name']}** (Day {idx + 1} of {len(days)}).",
        )
        return True

    async def _handle_simple_edit_request(self, channel: discord.abc.Messageable, text: str) -> bool:
        lowered = text.lower().strip()

        swap_match = re.search(r"(?:swap|replace)\s+(.+?)\s+with\s+(.+)$", text, re.IGNORECASE)
        if swap_match:
            old_name = swap_match.group(1).strip(" .")
            new_name = swap_match.group(2).strip(" .")
            rows = await self.db.update_exercise_name_in_active_program(old_name, new_name)
            if rows > 0:
                await send_discord_text(channel, f"✅ Updated exercise: **{old_name}** -> **{new_name}**.")
            else:
                await send_discord_text(channel, f"Couldn't find `{old_name}` in the active program.")
            return True

        change_match = re.search(r"change\s+(.+?)\s+to\s+(.+)$", text, re.IGNORECASE)
        if change_match:
            old_name = change_match.group(1).strip(" .")
            new_name = change_match.group(2).strip(" .")

            day_rows = await self.db.rename_program_day_in_active_program(old_name, new_name)
            if day_rows > 0:
                await send_discord_text(channel, f"✅ Renamed day: **{old_name}** -> **{new_name}**.")
                return True

            ex_rows = await self.db.update_exercise_name_in_active_program(old_name, new_name)
            if ex_rows > 0:
                await send_discord_text(channel, f"✅ Renamed exercise: **{old_name}** -> **{new_name}**.")
            else:
                await send_discord_text(
                    channel,
                    "I couldn't apply that change directly. Try `swap <old exercise> with <new exercise>`.",
                )
            return True

        if EDIT_INTENT_RE.search(lowered):
            active = await self.db.get_active_program()
            if not active:
                await send_discord_text(channel, "No active program to edit yet.")
                return True
            days = await self.db.get_program_days(int(active["id"]))
            context = {"program": active, "days": days, "request": text}
            try:
                reply = await self.bot.ollama.chat(
                    system=(
                        f"{FITNESS_ONLY_GUARDRAIL}\n"
                        "You are assisting with workout program edits. "
                        "Give a concise response and suggest a concrete edit command. Keep it under 3 sentences."
                    ),
                    user=json.dumps(context, ensure_ascii=False),
                    temperature=0.2,
                    max_tokens=220,
                )
                await send_discord_text(channel, reply.strip())
            except Exception:
                await send_discord_text(channel, "I couldn't process that edit request right now. Try `swap X with Y`.")
            return True

        return False

    async def _suggest_pending_changes(self, state: PendingProgram) -> str:
        history = self.memory.get(user_id=state.user_id, channel_id=state.channel_id)
        exercise_summary = self._extract_exercise_summary(state.raw_text)
        payload = {
            "program_text": state.raw_text,
            "exercise_summary": exercise_summary,
            "user_constraints": state.notes,
            "history": history,
            "task": "Review this program and propose practical swaps/edits based on constraints.",
            "format": "Use concise bullet points, then a 1-line question.",
        }
        reply = await self.bot.ollama.chat(
            system=(
                f"{FITNESS_ONLY_GUARDRAIL}\n"
                "You are reviewing a workout program before import. "
                "Focus on feasibility with equipment constraints and training goals. "
                "If discussing Smith Machine swaps: only suggest Smith alternatives for heavy_barbell/light_barbell exercises. "
                "Never relabel dumbbell/cable/machine/bodyweight lifts as Smith Machine."
            ),
            user=json.dumps(payload, ensure_ascii=False),
            temperature=0.2,
            max_tokens=380,
        )
        return reply.strip()

    async def _suggest_pending_changes_by_day(self, state: PendingProgram) -> list[str]:
        history = self.memory.get(user_id=state.user_id, channel_id=state.channel_id)
        day_blocks = self.parser._extract_day_blocks(state.raw_text)
        if not day_blocks:
            single = await self._suggest_pending_changes(state)
            return [single]

        messages: list[str] = []
        for idx, block in enumerate(day_blocks):
            day_name = str(block.get("name") or f"Day {idx + 1}")
            exercises = block.get("exercises") or []
            payload = {
                "day_name": day_name,
                "day_order": idx + 1,
                "exercises": exercises,
                "user_constraints": state.notes,
                "history": history,
                "task": "Give focused swap suggestions for this day only.",
                "format": "Use 3-6 short bullets. Mention only actionable edits.",
            }
            try:
                review = await self.bot.ollama.chat(
                    system=(
                        f"{FITNESS_ONLY_GUARDRAIL}\n"
                        "You are reviewing one training day before import. "
                        "Focus on practical swaps and consistency. "
                        "If discussing Smith Machine swaps: only suggest Smith alternatives for heavy_barbell/light_barbell exercises. "
                        "Never relabel dumbbell/cable/machine/bodyweight lifts as Smith Machine."
                    ),
                    user=json.dumps(payload, ensure_ascii=False),
                    temperature=0.2,
                    max_tokens=260,
                )
                body = review.strip() or "No major swaps needed for this day."
            except Exception:
                body = "No major swaps needed for this day."
            messages.append(f"**Day {idx + 1} - {day_name}**\n{body}")
        return messages

    async def _send_day_reviews(
        self,
        channel: discord.abc.Messageable,
        day_reviews: list[str],
        *,
        updated: bool = False,
    ) -> None:
        header = "Updated review by day:" if updated else "I reviewed your program before importing:"
        await send_discord_text(channel, header)
        for review in day_reviews:
            await send_discord_text(channel, review)
        await send_discord_text(
            channel,
            "Reply with edits if you want changes, `save` to import, or `cancel` to discard.",
        )

    async def _render_program_for_import(self, state: PendingProgram) -> str:
        if not state.notes:
            return state.raw_text

        exercise_summary = self._extract_exercise_summary(state.raw_text)
        payload = {
            "program_text": state.raw_text,
            "exercise_summary": exercise_summary,
            "requested_changes": state.notes,
            "instructions": "Return only the finalized program text with day headers and exercise lines. No commentary.",
        }
        rewritten = await self.bot.ollama.chat(
            system=(
                f"{FITNESS_ONLY_GUARDRAIL}\n"
                "Rewrite the workout program text by applying the requested changes. "
                "Preserve clear day/exercise structure and sets x reps. "
                "Only apply Smith Machine renames to heavy_barbell/light_barbell categories."
            ),
            user=json.dumps(payload, ensure_ascii=False),
            temperature=0.15,
            max_tokens=900,
        )
        final_text = rewritten.strip() or state.raw_text
        combined_notes = " ".join(state.notes).lower()
        if "smith" in combined_notes:
            final_text = self._enforce_smith_machine_consistency(final_text, exercise_summary)
        return final_text

    def _enforce_smith_machine_consistency(
        self,
        program_text: str,
        summary: list[dict[str, Any]],
    ) -> str:
        lines = program_text.splitlines()
        heavy_or_light: dict[str, str] = {}
        not_smith: dict[str, str] = {}
        for ex in summary:
            original = str(ex.get("name") or "").strip()
            if not original:
                continue
            key = original.lower()
            if str(ex.get("category") or "") in {"heavy_barbell", "light_barbell"}:
                heavy_or_light[key] = original
            else:
                not_smith[key] = original

        out_lines: list[str] = []
        for line in lines:
            updated = line
            lower_line = updated.lower()
            for name, original in heavy_or_light.items():
                if name in lower_line and "smith machine" not in lower_line:
                    pattern = re.compile(re.escape(name), re.IGNORECASE)
                    updated = pattern.sub(f"Smith Machine {original}", updated, count=1)
                    lower_line = updated.lower()
                    break
            for name, original in not_smith.items():
                smith_name = f"smith machine {name}"
                if smith_name in lower_line:
                    pattern = re.compile(re.escape(smith_name), re.IGNORECASE)
                    updated = pattern.sub(original, updated, count=1)
                    lower_line = updated.lower()
                    break
            out_lines.append(updated)
        return "\n".join(out_lines)

    async def _start_pending_program(
        self,
        channel: discord.abc.Messageable,
        author: discord.abc.User,
        raw_text: str,
    ) -> None:
        self._cancel_review_task(author.id, getattr(channel, "id", 0))
        state = PendingProgram(
            user_id=author.id,
            channel_id=getattr(channel, "id", 0),
            raw_text=raw_text,
            created_at=self._now_utc(),
        )
        self._set_pending_program(state)
        day_reviews = await self._suggest_pending_changes_by_day(state)
        if not self._is_pending_flow_active(state):
            return
        state.latest_suggestions = "\n\n".join(day_reviews)
        self._set_pending_program(state)
        await self._send_day_reviews(channel, day_reviews, updated=False)

    async def _finalize_pending_program(
        self,
        channel: discord.abc.Messageable,
        author: discord.abc.User,
        state: PendingProgram,
    ) -> None:
        self._cancel_review_task(state.user_id, state.channel_id)
        final_text = state.raw_text
        try:
            final_text = await self._render_program_for_import(state)
        except Exception:
            final_text = state.raw_text

        parsed = await self.parser.parse_program(final_text)
        parsed = self._recover_missing_exercises_from_original(parsed, state.raw_text)
        parsed["program_name"] = await self._resolve_program_name(
            channel,
            author,
            final_text,
            str(parsed.get("program_name") or ""),
        )
        recent = await self.db.get_recent_program_by_name(parsed["program_name"], minutes=5)
        if recent:
            self._mark_flow_closed(state.user_id, state.channel_id, state.flow_id)
            self._clear_pending_program(state.user_id, state.channel_id)
            await send_discord_text(
                channel,
                f"⚠️ Skipped duplicate import: **{parsed['program_name']}** was already imported recently (ID {recent['id']}).",
            )
            self._set_post_import_context(author.id, getattr(channel, "id", 0), int(recent["id"]))
            await self._start_day_prompt(channel, int(recent["id"]))
            return

        program_id = await self.db.create_program_from_payload(parsed)

        days = parsed.get("days", [])
        total_exercises = sum(len(day.get("exercises", [])) for day in days)
        self._mark_flow_closed(state.user_id, state.channel_id, state.flow_id)
        self._clear_pending_program(state.user_id, state.channel_id)
        await send_discord_text(
            channel,
            f"✅ Imported **{parsed['program_name']}** (ID {program_id}) with "
            f"{len(days)} days and {total_exercises} exercises.",
        )
        self._set_post_import_context(author.id, getattr(channel, "id", 0), program_id)
        await self._start_day_prompt(channel, program_id)

    def _recover_missing_exercises_from_original(
        self,
        parsed: dict[str, Any],
        original_text: str,
    ) -> dict[str, Any]:
        original_blocks = self.parser._extract_day_blocks(original_text)
        if not original_blocks:
            return parsed

        parsed_days = parsed.get("days", [])
        if not parsed_days:
            return parsed

        for day_idx, original_day in enumerate(original_blocks):
            if day_idx >= len(parsed_days):
                break
            parsed_day = parsed_days[day_idx]
            parsed_ex = parsed_day.get("exercises", [])
            original_ex = original_day.get("exercises", [])
            if len(parsed_ex) >= len(original_ex):
                continue
            existing_names = {str(ex.get("name") or "").strip().lower() for ex in parsed_ex}
            for raw_ex in original_ex:
                name = str(raw_ex.get("name") or "").strip()
                if not name:
                    continue
                if name.lower() in existing_names:
                    continue
                parsed_ex.append(
                    {
                        "name": name,
                        "display_order": len(parsed_ex),
                        "sets": int(raw_ex.get("sets") or 1),
                        "rep_range_low": raw_ex.get("rep_range_low"),
                        "rep_range_high": raw_ex.get("rep_range_high"),
                        "category": self.parser._category_lookup(name),
                        "superset_group": None,
                        "muscle_groups": "",
                        "notes": str(raw_ex.get("notes") or "recovered_from_source"),
                    }
                )
                existing_names.add(name.lower())
            parsed_day["exercises"] = parsed_ex
        return parsed

    async def _reply_programme_message(
        self,
        channel: discord.abc.Messageable,
        text: str,
        *,
        user_id: int,
    ) -> None:
        active = await self.db.get_active_program()
        if not active:
            await send_discord_text(channel, "No active program yet. Paste one to get started.")
            return

        days = await self.db.get_program_days(int(active["id"]))
        history = self.memory.get(user_id=user_id, channel_id=getattr(channel, "id", 0))
        payload = {
            "message": text,
            "program": active,
            "days": days,
            "history": history,
            "response_style": "2-4 short sentences.",
        }
        try:
            reply = await self.bot.ollama.chat(
                system=(
                    f"{FITNESS_ONLY_GUARDRAIL}\n"
                    "You help users discuss and update their lifting program. "
                    "Answer concisely and practically."
                ),
                user=json.dumps(payload, ensure_ascii=False),
                temperature=0.2,
                max_tokens=300,
            )
            reply_text = reply.strip()
            self.memory.append(
                user_id=user_id,
                channel_id=getattr(channel, "id", 0),
                role="assistant",
                content=reply_text,
            )
            await send_discord_text(channel, reply_text)
        except Exception:
            await send_discord_text(channel, "I couldn't answer that right now. Try `!program` to view the current setup.")

    @commands.command(name="import")
    async def import_program_command(self, ctx: commands.Context, *, text: str) -> None:
        if not self._is_programme_channel(ctx.channel):
            return
        lock = self._get_user_lock(ctx.author.id)
        async with lock:
            await self._start_pending_program(ctx.channel, ctx.author, text)

    @commands.command(name="program")
    async def show_program_command(self, ctx: commands.Context) -> None:
        program = await self.db.get_active_program()
        if not program:
            await send_discord_text(ctx.channel, "No active program yet. Paste one in #programme.")
            return
        days = await self.db.get_program_days(program["id"])
        if not days:
            await send_discord_text(ctx.channel, f"Active program **{program['name']}** has no days.")
            return

        lines = [f"Active program: **{program['name']}** ({len(days)} days)"]
        for day in days:
            exercises = await self.db.get_exercises_for_day(day["id"])
            lines.append(f"Day {day['day_order'] + 1}: {day['name']} ({len(exercises)} exercises)")
        await send_discord_text(ctx.channel, "\n".join(lines))

    @commands.command(name="startday")
    async def start_day_command(self, ctx: commands.Context, *, text: str) -> None:
        if not self._is_programme_channel(ctx.channel):
            return
        lock = self._get_user_lock(ctx.author.id)
        async with lock:
            context = self._get_post_import_context(ctx.author.id, ctx.channel.id)
            handled = await self._handle_start_day_message(
                ctx.channel,
                f"start on {text}",
                program_id=int(context["program_id"]) if context else None,
                allow_implicit=bool(context),
                user_id=ctx.author.id,
            )
            if not handled:
                await send_discord_text(ctx.channel, "Couldn't parse that day selection. Try `!startday Day 3` or `!startday Legs`.")
                return
            if context:
                self._clear_programme_flow_state(ctx.author.id, ctx.channel.id)
                await send_discord_text(
                    ctx.channel,
                    "Program saved and ready. Head to your workout channel and type `ready` to start.",
                )

    @commands.command(name="travel")
    async def travel_program_command(self, ctx: commands.Context, *, text: str) -> None:
        if not self._is_programme_channel(ctx.channel):
            return

        active = await self.db.get_active_program()
        if not active:
            await send_discord_text(ctx.channel, "You need an active base program first.")
            return

        duration_days = 14
        match = DURATION_RE.search(text)
        if match:
            num = int(match.group("num"))
            unit = match.group("unit").lower()
            duration_days = num * 7 if "week" in unit else num

        expires = (date.today() + timedelta(days=duration_days)).isoformat()
        prompt = (
            "Create a temporary workout program in JSON with this schema: "
            '{"program_name":str,"days":[{"day_order":0,"name":str,"exercises":[{"name":str,"sets":int,'
            '"rep_range_low":int|null,"rep_range_high":int|null,"category":str,"superset_group":int|null,'
            '"muscle_groups":str,"notes":str}]}]}. '
            f"Context: {text}. Keep 3-5 days cycle and use available equipment."
        )
        parsed = await self.bot.ollama.chat_json(
            system=f"{FITNESS_ONLY_GUARDRAIL}\nReturn only valid JSON workout program.",
            user=prompt,
            temperature=0.2,
        )

        program_id = await self.db.create_program_from_payload(
            parsed,
            temporary=True,
            parent_program_id=active["id"],
            expires_at=expires,
        )
        await send_discord_text(
            ctx.channel,
            f"✅ Temporary program activated (ID {program_id}) until {expires}. "
            f"Base program: **{active['name']}** will resume automatically.",
        )
        self._set_post_import_context(ctx.author.id, ctx.channel.id, int(program_id))

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        if not self._is_programme_channel(message.channel):
            return

        content = message.content.strip()
        if not content or content.startswith(self.settings.command_prefix):
            return

        user_id = message.author.id
        channel_id = message.channel.id
        lock = self._get_user_lock(user_id)
        async with lock:
            context = self._get_post_import_context(user_id, channel_id)
            self.memory.append(user_id=user_id, channel_id=channel_id, role="user", content=content)

            pending = self._get_pending_program(user_id, channel_id)

            if self._looks_like_program_paste(content):
                try:
                    await self._start_pending_program(message.channel, message.author, content)
                except Exception as exc:
                    await send_discord_text(message.channel, f"Could not process program draft: {exc}")
                return

            if pending:
                if self._is_cancel_message(content):
                    self._clear_programme_flow_state(user_id, channel_id)
                    await send_discord_text(message.channel, "Pending program import discarded.")
                    return
                if self._is_confirm_message(content):
                    try:
                        await self._finalize_pending_program(message.channel, message.author, pending)
                    except Exception as exc:
                        await send_discord_text(message.channel, f"Could not import program: {exc}")
                    return
                pending.notes.append(content)
                pending.created_at = self._now_utc()
                self._set_pending_program(pending)
                key = self._pending_key(user_id, channel_id)
                self._cancel_review_task(user_id, channel_id)
                review_task = asyncio.create_task(self._suggest_pending_changes_by_day(pending))
                self.review_tasks[key] = review_task
                try:
                    day_reviews = await review_task
                    if not self._is_pending_flow_active(pending):
                        return
                    pending.latest_suggestions = "\n\n".join(day_reviews)
                    self._set_pending_program(pending)
                    await self._send_day_reviews(message.channel, day_reviews, updated=True)
                except asyncio.CancelledError:
                    return
                except Exception:
                    await send_discord_text(
                        message.channel,
                        "I noted that change request. Reply `save` when you're ready to import, or keep editing.",
                    )
                finally:
                    if self.review_tasks.get(key) is review_task:
                        self.review_tasks.pop(key, None)
                return

            handled_start_day = await self._handle_start_day_message(
                message.channel,
                content,
                program_id=int(context["program_id"]) if context else None,
                allow_implicit=bool(context),
                user_id=user_id,
            )
            if handled_start_day:
                if context:
                    self._clear_programme_flow_state(user_id, channel_id)
                    await send_discord_text(
                        message.channel,
                        "Program saved and ready. Head to your workout channel and type `ready` to start.",
                    )
                return

            if await self._handle_simple_edit_request(message.channel, content):
                return

            await self._reply_programme_message(message.channel, content, user_id=user_id)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ProgrammeCog(bot))
