from __future__ import annotations
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, AsyncIterator, Iterable, Optional

import aiosqlite


class Database:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[aiosqlite.Connection]:
        conn = await aiosqlite.connect(self.db_path)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
        finally:
            await conn.close()

    async def _fetchone(
        self,
        conn: aiosqlite.Connection,
        query: str,
        params: tuple[Any, ...] | list[Any] = (),
    ) -> Optional[aiosqlite.Row]:
        cursor = await conn.execute(query, params)
        row = await cursor.fetchone()
        await cursor.close()
        return row

    async def _fetchall(
        self,
        conn: aiosqlite.Connection,
        query: str,
        params: tuple[Any, ...] | list[Any] = (),
    ) -> list[aiosqlite.Row]:
        cursor = await conn.execute(query, params)
        rows = await cursor.fetchall()
        await cursor.close()
        return rows

    async def init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        schema_path = Path(__file__).with_name("schema.sql")
        schema = schema_path.read_text(encoding="utf-8")
        async with self.connect() as conn:
            await conn.executescript(schema)
            await self._ensure_user_state_timezone_column(conn)
            await conn.execute(
                """
                INSERT OR IGNORE INTO user_state (id)
                VALUES (1)
                """
            )
            system_tz = self._detect_system_timezone()
            await conn.execute(
                """
                UPDATE user_state
                SET timezone = COALESCE(NULLIF(timezone, ''), ?)
                WHERE id = 1
                """,
                (system_tz,),
            )
            await conn.commit()

    async def _ensure_user_state_timezone_column(self, conn: aiosqlite.Connection) -> None:
        rows = await self._fetchall(conn, "PRAGMA table_info(user_state)")
        columns = {str(r["name"]).lower() for r in rows}
        if "timezone" in columns:
            return
        await conn.execute("ALTER TABLE user_state ADD COLUMN timezone TEXT DEFAULT 'UTC'")

    def _detect_system_timezone(self) -> str:
        tzinfo = datetime.now().astimezone().tzinfo
        if tzinfo is None:
            return "UTC"
        key = getattr(tzinfo, "key", None)
        if isinstance(key, str) and key:
            return key
        name = datetime.now().astimezone().tzname()
        if isinstance(name, str) and "/" in name:
            return name
        return "UTC"

    async def get_user_state(self) -> dict[str, Any]:
        async with self.connect() as conn:
            row = await self._fetchone(conn, "SELECT * FROM user_state WHERE id = 1")
            return dict(row) if row else {}

    async def update_user_state(self, **fields: Any) -> None:
        if not fields:
            return
        columns = ", ".join(f"{key} = ?" for key in fields)
        values = list(fields.values())
        values.append(1)
        async with self.connect() as conn:
            await conn.execute(f"UPDATE user_state SET {columns} WHERE id = ?", values)
            await conn.commit()

    async def get_current_day_index(self) -> int:
        state = await self.get_user_state()
        return int(state.get("current_day_index") or 0)

    async def get_user_timezone(self) -> str:
        state = await self.get_user_state()
        tz = str(state.get("timezone") or "").strip()
        return tz or "UTC"

    async def set_user_timezone(self, timezone_name: str) -> None:
        await self.update_user_state(timezone=timezone_name)

    async def set_current_day_index(self, day_index: int) -> None:
        await self.update_user_state(current_day_index=max(0, day_index))

    async def set_current_day_for_active_program(self, day_order: int) -> bool:
        program = await self.get_active_program()
        if not program:
            return False
        days = await self.get_program_days(int(program["id"]))
        if not days:
            return False
        if day_order < 0 or day_order >= len(days):
            return False
        await self.set_current_day_index(day_order)
        return True

    async def get_active_program(self) -> Optional[dict[str, Any]]:
        await self._revert_expired_temporary_program_if_needed()
        async with self.connect() as conn:
            row = await self._fetchone(conn, 
                """
                SELECT p.*
                FROM programs p
                JOIN user_state u ON u.current_program_id = p.id
                WHERE p.active = 1
                LIMIT 1
                """
            )
            if row:
                return dict(row)

            row = await self._fetchone(conn, 
                "SELECT * FROM programs WHERE active = 1 ORDER BY id DESC LIMIT 1"
            )
            if row:
                await conn.execute(
                    "UPDATE user_state SET current_program_id = ? WHERE id = 1", (row["id"],)
                )
                await conn.commit()
                return dict(row)
            return None

    async def _revert_expired_temporary_program_if_needed(self) -> None:
        today = date.today().isoformat()
        async with self.connect() as conn:
            temporary = await self._fetchone(conn, 
                """
                SELECT * FROM programs
                WHERE active = 1 AND temporary = 1 AND expires_at IS NOT NULL AND expires_at <= ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (today,),
            )
            if not temporary:
                return

            parent_id = temporary["parent_program_id"]
            await conn.execute("UPDATE programs SET active = 0 WHERE id = ?", (temporary["id"],))
            if parent_id:
                await conn.execute("UPDATE programs SET active = 1 WHERE id = ?", (parent_id,))
                await conn.execute(
                    "UPDATE user_state SET current_program_id = ? WHERE id = 1", (parent_id,)
                )
            await conn.commit()

    async def create_program_from_payload(
        self,
        payload: dict[str, Any],
        *,
        temporary: bool = False,
        parent_program_id: Optional[int] = None,
        expires_at: Optional[str] = None,
    ) -> int:
        program_name = payload.get("program_name") or "Untitled Program"
        days = payload.get("days") or []
        async with self.connect() as conn:
            await conn.execute("UPDATE programs SET active = 0 WHERE active = 1")
            cursor = await conn.execute(
                """
                INSERT INTO programs (name, active, temporary, parent_program_id, expires_at)
                VALUES (?, 1, ?, ?, ?)
                """,
                (program_name, int(temporary), parent_program_id, expires_at),
            )
            program_id = cursor.lastrowid

            for day in days:
                day_order = int(day.get("day_order", 0))
                day_name = day.get("name") or f"Day {day_order + 1}"
                day_cursor = await conn.execute(
                    "INSERT INTO program_days (program_id, day_order, name) VALUES (?, ?, ?)",
                    (program_id, day_order, day_name),
                )
                day_id = day_cursor.lastrowid
                exercises = day.get("exercises") or []
                for idx, ex in enumerate(exercises):
                    await conn.execute(
                        """
                        INSERT INTO exercises (
                            program_day_id, name, display_order, sets, rep_range_low,
                            rep_range_high, category, superset_group, notes, muscle_groups
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            day_id,
                            ex.get("name") or f"Exercise {idx + 1}",
                            int(ex.get("display_order", idx)),
                            int(ex.get("sets", 1)),
                            ex.get("rep_range_low"),
                            ex.get("rep_range_high"),
                            ex.get("category") or "cable_machine",
                            ex.get("superset_group"),
                            ex.get("notes") or "",
                            ex.get("muscle_groups") or "",
                        ),
                    )

            await conn.execute(
                "UPDATE user_state SET current_program_id = ?, current_day_index = 0 WHERE id = 1",
                (program_id,),
            )
            await conn.commit()
            return int(program_id)

    async def get_program_days(self, program_id: int) -> list[dict[str, Any]]:
        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                "SELECT * FROM program_days WHERE program_id = ? ORDER BY day_order",
                (program_id,),
            )
        return [dict(r) for r in rows]

    async def get_exercises_for_day(self, day_id: int) -> list[dict[str, Any]]:
        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                "SELECT * FROM exercises WHERE program_day_id = ? ORDER BY display_order",
                (day_id,),
            )
        return [dict(r) for r in rows]

    async def get_exercises_for_day_index(self, day_index: int) -> list[dict[str, Any]]:
        program = await self.get_active_program()
        if not program:
            return []
        days = await self.get_program_days(int(program["id"]))
        if not days:
            return []
        normalized = day_index % len(days)
        day = days[normalized]
        return await self.get_exercises_for_day(int(day["id"]))

    async def get_day_for_index(self, day_index: int) -> Optional[dict[str, Any]]:
        program = await self.get_active_program()
        if not program:
            return None
        days = await self.get_program_days(int(program["id"]))
        if not days:
            return None
        return days[day_index % len(days)]

    async def advance_day_index(self) -> int:
        program = await self.get_active_program()
        if not program:
            return 0
        days = await self.get_program_days(int(program["id"]))
        if not days:
            return 0

        current = await self.get_current_day_index()
        nxt = (current + 1) % len(days)
        await self.update_user_state(current_day_index=nxt)
        return nxt

    async def get_last_logs_for_exercise(self, exercise_id: int, limit: int = 6) -> list[dict[str, Any]]:
        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                """
                SELECT *
                FROM workout_logs
                WHERE exercise_id = ?
                ORDER BY date DESC, set_number DESC
                LIMIT ?
                """,
                (exercise_id, limit),
            )
        return [dict(r) for r in rows]

    async def log_set(
        self,
        exercise_id: int,
        *,
        workout_date: date,
        set_number: int,
        weight: float,
        reps: int,
        unit: str,
        rir: Optional[int] = None,
        notes: str = "",
    ) -> int:
        async with self.connect() as conn:
            cursor = await conn.execute(
                """
                INSERT INTO workout_logs (exercise_id, date, set_number, weight, reps, unit, rir, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (exercise_id, workout_date.isoformat(), set_number, weight, reps, unit, rir, notes),
            )
            log_id = cursor.lastrowid
            await conn.commit()
        return int(log_id)

    async def get_best_pr(self, exercise_name: str) -> Optional[dict[str, Any]]:
        async with self.connect() as conn:
            row = await self._fetchone(conn, 
                """
                SELECT *
                FROM personal_records
                WHERE LOWER(exercise_name) = LOWER(?)
                ORDER BY estimated_1rm DESC, date DESC
                LIMIT 1
                """,
                (exercise_name,),
            )
            if row:
                return dict(row)

            fallback = await self._fetchone(
                conn,
                """
                SELECT
                    e.name AS exercise_name,
                    wl.weight AS weight,
                    wl.reps AS reps,
                    wl.unit AS unit,
                    (wl.weight * (1 + wl.reps / 30.0)) AS estimated_1rm,
                    wl.date AS date
                FROM workout_logs wl
                JOIN exercises e ON e.id = wl.exercise_id
                WHERE LOWER(e.name) = LOWER(?)
                ORDER BY estimated_1rm DESC, wl.date DESC
                LIMIT 1
                """,
                (exercise_name,),
            )
            if fallback:
                return dict(fallback)
        return None

    async def create_pr(
        self,
        exercise_name: str,
        *,
        weight: float,
        reps: int,
        unit: str,
        estimated_1rm: float,
        workout_date: date,
        workout_log_id: Optional[int],
    ) -> int:
        async with self.connect() as conn:
            cursor = await conn.execute(
                """
                INSERT INTO personal_records (
                    exercise_name, weight, reps, unit, estimated_1rm, date, workout_log_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    exercise_name,
                    weight,
                    reps,
                    unit,
                    estimated_1rm,
                    workout_date.isoformat(),
                    workout_log_id,
                ),
            )
            await conn.commit()
        return int(cursor.lastrowid)

    async def get_recent_prs(self, days: int = 14) -> list[dict[str, Any]]:
        threshold = (date.today() - timedelta(days=days)).isoformat()
        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                """
                SELECT *
                FROM personal_records
                WHERE date >= ?
                ORDER BY date DESC, estimated_1rm DESC
                """,
                (threshold,),
            )
        return [dict(r) for r in rows]

    async def get_last_logs_for_day_index(self, day_index: int) -> list[dict[str, Any]]:
        day = await self.get_day_for_index(day_index)
        if not day:
            return []

        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                """
                SELECT e.name AS exercise_name, wl.*
                FROM workout_logs wl
                JOIN exercises e ON e.id = wl.exercise_id
                WHERE e.program_day_id = ?
                ORDER BY wl.date DESC, wl.set_number DESC
                LIMIT 50
                """,
                (day["id"],),
            )
        return [dict(r) for r in rows]

    async def add_activity(
        self,
        *,
        activity_date: date,
        activity_type: str,
        description: str,
        intensity: str,
        muscle_groups: str,
    ) -> int:
        async with self.connect() as conn:
            cursor = await conn.execute(
                """
                INSERT INTO activity_logs (date, activity_type, description, intensity, muscle_groups)
                VALUES (?, ?, ?, ?, ?)
                """,
                (activity_date.isoformat(), activity_type, description, intensity, muscle_groups),
            )
            await conn.commit()
        return int(cursor.lastrowid)

    async def get_activities_last_7_days(self) -> list[dict[str, Any]]:
        threshold = (date.today() - timedelta(days=7)).isoformat()
        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                "SELECT * FROM activity_logs WHERE date >= ? ORDER BY date DESC, id DESC",
                (threshold,),
            )
        return [dict(r) for r in rows]

    async def save_cue(self, exercise_name: str, cue: str) -> int:
        async with self.connect() as conn:
            cursor = await conn.execute(
                "INSERT INTO exercise_cues (exercise_name, cue) VALUES (?, ?)",
                (exercise_name, cue.strip()),
            )
            await conn.commit()
        return int(cursor.lastrowid)

    async def get_latest_cue(self, exercise_name: str) -> Optional[str]:
        async with self.connect() as conn:
            row = await self._fetchone(conn, 
                """
                SELECT cue
                FROM exercise_cues
                WHERE LOWER(exercise_name) = LOWER(?)
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (exercise_name,),
            )
        return str(row["cue"]) if row else None

    async def get_weekly_volume(self, *, start_date: Optional[date] = None) -> dict[str, int]:
        if start_date is None:
            today = date.today()
            start_date = today - timedelta(days=today.weekday())
        end_date = start_date + timedelta(days=6)

        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                """
                SELECT e.muscle_groups, COUNT(DISTINCT wl.id) AS set_count
                FROM workout_logs wl
                JOIN exercises e ON e.id = wl.exercise_id
                WHERE wl.date BETWEEN ? AND ?
                GROUP BY e.muscle_groups
                """,
                (start_date.isoformat(), end_date.isoformat()),
            )

        volume: dict[str, int] = {}
        for row in rows:
            groups = (row["muscle_groups"] or "").split(",")
            groups = [g.strip().lower() for g in groups if g.strip()]
            if not groups:
                continue
            share = max(1, int(row["set_count"]))
            for group in groups:
                volume[group] = volume.get(group, 0) + share
        return volume

    async def get_trend_last_4_weeks(self) -> list[dict[str, Any]]:
        threshold = (date.today() - timedelta(days=28)).isoformat()
        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                """
                SELECT e.name AS exercise_name,
                       MAX(wl.weight * (1 + wl.reps / 30.0)) AS best_e1rm,
                       MIN(wl.date) AS start_date,
                       MAX(wl.date) AS end_date
                FROM workout_logs wl
                JOIN exercises e ON e.id = wl.exercise_id
                WHERE wl.date >= ?
                GROUP BY e.name
                ORDER BY best_e1rm DESC
                """,
                (threshold,),
            )
        return [dict(r) for r in rows]

    async def get_e1rm_history(self, exercise_name: str, limit: int = 12) -> list[dict[str, Any]]:
        resolved = await self.resolve_exercise_name(exercise_name)
        target = resolved or exercise_name
        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                """
                SELECT wl.date,
                       wl.weight,
                       wl.reps,
                       wl.unit,
                       (wl.weight * (1 + wl.reps / 30.0)) AS e1rm
                FROM workout_logs wl
                JOIN exercises e ON e.id = wl.exercise_id
                WHERE LOWER(e.name) = LOWER(?)
                ORDER BY wl.date ASC, wl.set_number ASC
                LIMIT ?
                """,
                (target, limit),
            )
        return [dict(r) for r in rows]

    async def export_logs(self, *, exercise_name: Optional[str] = None) -> list[dict[str, Any]]:
        resolved_target: Optional[str] = None
        if exercise_name:
            resolved_target = await self.resolve_exercise_name(exercise_name)

        async with self.connect() as conn:
            if exercise_name:
                target = resolved_target or exercise_name
                rows = await self._fetchall(conn, 
                    """
                    SELECT wl.date,
                           e.name AS exercise,
                           wl.set_number,
                           wl.weight,
                           wl.unit,
                           wl.reps,
                           wl.rir,
                           (wl.weight * (1 + wl.reps / 30.0)) AS e1rm,
                           wl.notes
                    FROM workout_logs wl
                    JOIN exercises e ON e.id = wl.exercise_id
                    WHERE LOWER(e.name) = LOWER(?)
                    ORDER BY wl.date, e.name, wl.set_number
                    """,
                    (target,),
                )
            else:
                rows = await self._fetchall(conn, 
                    """
                    SELECT wl.date,
                           e.name AS exercise,
                           wl.set_number,
                           wl.weight,
                           wl.unit,
                           wl.reps,
                           wl.rir,
                           (wl.weight * (1 + wl.reps / 30.0)) AS e1rm,
                           wl.notes
                    FROM workout_logs wl
                    JOIN exercises e ON e.id = wl.exercise_id
                    ORDER BY wl.date, e.name, wl.set_number
                    """
                )
        return [dict(r) for r in rows]

    async def resolve_exercise_name(self, query: str) -> Optional[str]:
        cleaned = query.strip()
        if not cleaned:
            return None

        async with self.connect() as conn:
            exact = await self._fetchone(
                conn,
                """
                SELECT e.name AS name, COUNT(*) AS cnt
                FROM workout_logs wl
                JOIN exercises e ON e.id = wl.exercise_id
                WHERE LOWER(e.name) = LOWER(?)
                GROUP BY e.name
                ORDER BY cnt DESC, LENGTH(e.name) ASC
                LIMIT 1
                """,
                (cleaned,),
            )
            if exact:
                return str(exact["name"])

            like = f"%{cleaned.lower()}%"
            partial = await self._fetchone(
                conn,
                """
                SELECT e.name AS name, COUNT(*) AS cnt
                FROM workout_logs wl
                JOIN exercises e ON e.id = wl.exercise_id
                WHERE LOWER(e.name) LIKE ?
                GROUP BY e.name
                ORDER BY cnt DESC, LENGTH(e.name) ASC
                LIMIT 1
                """,
                (like,),
            )
            if partial:
                return str(partial["name"])

            program_partial = await self._fetchone(
                conn,
                """
                SELECT e.name AS name
                FROM exercises e
                WHERE LOWER(e.name) LIKE ?
                ORDER BY LENGTH(e.name) ASC
                LIMIT 1
                """,
                (like,),
            )
            if program_partial:
                return str(program_partial["name"])
        return None

    async def get_recent_activities(self, hours: int = 72) -> list[dict[str, Any]]:
        days = max(1, int(hours / 24))
        threshold = (date.today() - timedelta(days=days)).isoformat()
        async with self.connect() as conn:
            rows = await self._fetchall(
                conn,
                """
                SELECT *
                FROM activity_logs
                WHERE date >= ?
                ORDER BY date DESC, id DESC
                """,
                (threshold,),
            )
        return [dict(r) for r in rows]

    async def mark_workout_completed(self, workout_date: date) -> dict[str, int]:
        state = await self.get_user_state()
        today = workout_date
        last_raw = state.get("last_workout_date")
        current_streak = int(state.get("current_streak") or 0)
        longest_streak = int(state.get("longest_streak") or 0)

        if last_raw:
            last_date = datetime.strptime(last_raw, "%Y-%m-%d").date()
            delta = (today - last_date).days
            if delta == 0:
                pass
            elif delta == 1:
                current_streak += 1
            else:
                current_streak = 1
        else:
            current_streak = 1

        if current_streak > longest_streak:
            longest_streak = current_streak

        await self.update_user_state(
            current_streak=current_streak,
            longest_streak=longest_streak,
            last_workout_date=today.isoformat(),
        )
        return {
            "current_streak": current_streak,
            "longest_streak": longest_streak,
        }

    async def build_context(self, target_date: date) -> dict[str, Any]:
        day_index = await self.get_current_day_index()
        return {
            "current_program": await self.get_active_program(),
            "todays_exercises": await self.get_exercises_for_day_index(day_index),
            "last_session_logs": await self.get_last_logs_for_day_index(day_index),
            "recent_activities": await self.get_activities_last_7_days(),
            "user_state": await self.get_user_state(),
            "weekly_volume": await self.get_weekly_volume(),
            "recent_performance_trend": await self.get_trend_last_4_weeks(),
            "recent_prs": await self.get_recent_prs(days=14),
            "target_date": target_date.isoformat(),
        }

    async def set_last_checkin(self, checkin_date: date) -> None:
        await self.update_user_state(last_checkin_date=checkin_date.isoformat())

    async def get_last_checkin_date(self) -> Optional[date]:
        state = await self.get_user_state()
        raw = state.get("last_checkin_date")
        if not raw:
            return None
        return datetime.strptime(raw, "%Y-%m-%d").date()

    async def get_exercise_by_name_in_current_program(self, name: str) -> Optional[dict[str, Any]]:
        program = await self.get_active_program()
        if not program:
            return None
        async with self.connect() as conn:
            row = await self._fetchone(conn, 
                """
                SELECT e.*
                FROM exercises e
                JOIN program_days d ON d.id = e.program_day_id
                WHERE d.program_id = ? AND LOWER(e.name) = LOWER(?)
                ORDER BY d.day_order, e.display_order
                LIMIT 1
                """,
                (program["id"], name),
            )
        return dict(row) if row else None

    async def update_exercise_name_in_active_program(self, old_name: str, new_name: str) -> int:
        program = await self.get_active_program()
        if not program:
            return 0
        async with self.connect() as conn:
            cursor = await conn.execute(
                """
                UPDATE exercises
                SET name = ?
                WHERE id = (
                    SELECT e.id
                    FROM exercises e
                    JOIN program_days d ON d.id = e.program_day_id
                    WHERE d.program_id = ? AND LOWER(e.name) = LOWER(?)
                    ORDER BY d.day_order, e.display_order
                    LIMIT 1
                )
                """,
                (new_name, int(program["id"]), old_name),
            )
            await conn.commit()
            return int(cursor.rowcount or 0)

    async def rename_program_day_in_active_program(self, old_name: str, new_name: str) -> int:
        program = await self.get_active_program()
        if not program:
            return 0
        async with self.connect() as conn:
            cursor = await conn.execute(
                """
                UPDATE program_days
                SET name = ?
                WHERE id = (
                    SELECT id
                    FROM program_days
                    WHERE program_id = ? AND LOWER(name) = LOWER(?)
                    ORDER BY day_order
                    LIMIT 1
                )
                """,
                (new_name, int(program["id"]), old_name),
            )
            await conn.commit()
            return int(cursor.rowcount or 0)

    async def get_latest_workout_date(self) -> Optional[date]:
        async with self.connect() as conn:
            row = await self._fetchone(conn, "SELECT MAX(date) AS max_date FROM workout_logs")
        if not row or not row["max_date"]:
            return None
        return datetime.strptime(row["max_date"], "%Y-%m-%d").date()

    async def get_last_log_for_exercise(self, exercise_id: int) -> Optional[dict[str, Any]]:
        logs = await self.get_last_logs_for_exercise(exercise_id, limit=1)
        return logs[0] if logs else None

    async def get_last_logs_grouped_for_day(self, day_id: int) -> dict[int, list[dict[str, Any]]]:
        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                """
                SELECT wl.*
                FROM workout_logs wl
                JOIN exercises e ON e.id = wl.exercise_id
                WHERE e.program_day_id = ?
                ORDER BY wl.date DESC, wl.set_number DESC
                """,
                (day_id,),
            )
        grouped: dict[int, list[dict[str, Any]]] = {}
        for row in rows:
            ex_id = int(row["exercise_id"])
            grouped.setdefault(ex_id, []).append(dict(row))
        return grouped

    async def get_exercises_lookup(self, exercise_ids: Iterable[int]) -> dict[int, dict[str, Any]]:
        ids = list(set(int(i) for i in exercise_ids))
        if not ids:
            return {}
        placeholders = ",".join("?" for _ in ids)
        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                f"SELECT * FROM exercises WHERE id IN ({placeholders})",
                ids,
            )
        return {int(r["id"]): dict(r) for r in rows}
