from __future__ import annotations
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, AsyncIterator, Iterable, Optional

import aiosqlite

DEFAULT_USER_ID = "legacy"


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
            await self._migrate_user_state_table(conn)
            await self._ensure_multi_user_columns(conn)
            system_tz = self._detect_system_timezone()
            await conn.execute(
                """
                UPDATE user_state
                SET timezone = COALESCE(NULLIF(timezone, ''), ?)
                WHERE timezone IS NULL OR TRIM(timezone) = ''
                """,
                (system_tz,),
            )
            await self._ensure_user_state(conn, DEFAULT_USER_ID, timezone_default=system_tz)
            await conn.commit()

    def _normalize_user_id(self, user_id: Optional[str | int]) -> str:
        if user_id is None:
            return DEFAULT_USER_ID
        cleaned = str(user_id).strip()
        return cleaned or DEFAULT_USER_ID

    async def _ensure_user_state_timezone_column(self, conn: aiosqlite.Connection) -> None:
        rows = await self._fetchall(conn, "PRAGMA table_info(user_state)")
        columns = {str(r["name"]).lower() for r in rows}
        if "timezone" in columns:
            return
        await conn.execute("ALTER TABLE user_state ADD COLUMN timezone TEXT DEFAULT 'UTC'")

    async def _migrate_user_state_table(self, conn: aiosqlite.Connection) -> None:
        row = await self._fetchone(
            conn,
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='user_state'",
        )
        if not row:
            return
        create_sql = str(row["sql"] or "").lower()
        needs_rebuild = "check (id = 1" in create_sql or "check(id = 1" in create_sql
        if not needs_rebuild:
            await self._ensure_user_state_timezone_column(conn)
            table_info = await self._fetchall(conn, "PRAGMA table_info(user_state)")
            cols = {str(r["name"]).lower() for r in table_info}
            if "user_id" not in cols:
                await conn.execute(f"ALTER TABLE user_state ADD COLUMN user_id TEXT DEFAULT '{DEFAULT_USER_ID}'")
                await conn.execute(
                    "UPDATE user_state SET user_id = COALESCE(NULLIF(TRIM(user_id), ''), ?)",
                    (DEFAULT_USER_ID,),
                )
            await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_user_state_user_id ON user_state(user_id)")
            return

        old_rows = await self._fetchall(conn, "SELECT * FROM user_state")
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_state_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL UNIQUE,
                current_program_id INTEGER,
                current_day_index INTEGER DEFAULT 0,
                phase TEXT DEFAULT 'maintain',
                default_unit TEXT DEFAULT 'lbs',
                timezone TEXT DEFAULT 'UTC',
                readiness INTEGER DEFAULT 7,
                weeks_since_deload INTEGER DEFAULT 0,
                current_streak INTEGER DEFAULT 0,
                longest_streak INTEGER DEFAULT 0,
                last_workout_date DATE,
                last_checkin_date DATE,
                FOREIGN KEY (current_program_id) REFERENCES programs(id)
            )
            """
        )

        if old_rows:
            source = dict(old_rows[0])
            await conn.execute(
                """
                INSERT OR REPLACE INTO user_state_new (
                    user_id, current_program_id, current_day_index, phase, default_unit, timezone,
                    readiness, weeks_since_deload, current_streak, longest_streak,
                    last_workout_date, last_checkin_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self._normalize_user_id(source.get("user_id")),
                    source.get("current_program_id"),
                    source.get("current_day_index") or 0,
                    source.get("phase") or "maintain",
                    source.get("default_unit") or "lbs",
                    source.get("timezone") or "UTC",
                    source.get("readiness") or 7,
                    source.get("weeks_since_deload") or 0,
                    source.get("current_streak") or 0,
                    source.get("longest_streak") or 0,
                    source.get("last_workout_date"),
                    source.get("last_checkin_date"),
                ),
            )

        await conn.execute("DROP TABLE user_state")
        await conn.execute("ALTER TABLE user_state_new RENAME TO user_state")
        await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_user_state_user_id ON user_state(user_id)")

    async def _ensure_column_with_default(
        self,
        conn: aiosqlite.Connection,
        *,
        table: str,
        column: str,
        default_value: str = DEFAULT_USER_ID,
    ) -> None:
        rows = await self._fetchall(conn, f"PRAGMA table_info({table})")
        cols = {str(r["name"]).lower() for r in rows}
        if column.lower() not in cols:
            await conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} TEXT DEFAULT '{default_value}'")
        await conn.execute(
            f"UPDATE {table} SET {column} = COALESCE(NULLIF(TRIM({column}), ''), ?)",
            (default_value,),
        )

    async def _ensure_multi_user_columns(self, conn: aiosqlite.Connection) -> None:
        await self._ensure_column_with_default(conn, table="programs", column="user_id")
        await self._ensure_column_with_default(conn, table="workout_logs", column="user_id")
        await self._ensure_column_with_default(conn, table="activity_logs", column="user_id")
        await self._ensure_column_with_default(conn, table="personal_records", column="user_id")
        await self._ensure_column_with_default(conn, table="exercise_cues", column="user_id")
        await self._ensure_column_with_default(conn, table="injuries", column="user_id")
        await self._ensure_column_with_default(conn, table="workout_sessions", column="user_id")

    async def _ensure_user_state(
        self,
        conn: aiosqlite.Connection,
        user_id: str,
        *,
        timezone_default: Optional[str] = None,
    ) -> None:
        normalized = self._normalize_user_id(user_id)
        tz_value = (timezone_default or self._detect_system_timezone() or "UTC").strip() or "UTC"
        await conn.execute(
            """
            INSERT OR IGNORE INTO user_state (
                user_id, current_day_index, phase, default_unit, timezone, readiness,
                weeks_since_deload, current_streak, longest_streak
            ) VALUES (?, 0, 'maintain', 'lbs', ?, 7, 0, 0, 0)
            """,
            (normalized, tz_value),
        )

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

    async def get_or_create_user_state(self, user_id: Optional[str | int] = None) -> dict[str, Any]:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            await self._ensure_user_state(conn, normalized)
            await conn.commit()
        return await self.get_user_state(normalized)

    async def list_user_ids(self) -> list[str]:
        async with self.connect() as conn:
            rows = await self._fetchall(
                conn,
                """
                SELECT DISTINCT user_id
                FROM user_state
                WHERE user_id IS NOT NULL AND TRIM(user_id) != ''
                ORDER BY user_id
                """,
            )
        return [str(row["user_id"]) for row in rows if str(row["user_id"]).strip()]

    async def has_any_program(self) -> bool:
        async with self.connect() as conn:
            row = await self._fetchone(
                conn,
                "SELECT 1 AS has_program FROM programs LIMIT 1",
            )
        return bool(row)

    async def get_user_state(self, user_id: Optional[str | int] = None) -> dict[str, Any]:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            await self._ensure_user_state(conn, normalized)
            row = await self._fetchone(
                conn,
                "SELECT * FROM user_state WHERE user_id = ? ORDER BY id DESC LIMIT 1",
                (normalized,),
            )
            if row:
                return dict(row)
            await conn.commit()
        return {}

    async def update_user_state(self, user_id: Optional[str | int] = None, **fields: Any) -> None:
        if not fields:
            return
        normalized = self._normalize_user_id(user_id)
        columns = ", ".join(f"{key} = ?" for key in fields)
        values = list(fields.values())
        async with self.connect() as conn:
            await self._ensure_user_state(conn, normalized)
            values.append(normalized)
            await conn.execute(f"UPDATE user_state SET {columns} WHERE user_id = ?", values)
            await conn.commit()

    async def get_current_day_index(self, user_id: Optional[str | int] = None) -> int:
        state = await self.get_user_state(user_id)
        return int(state.get("current_day_index") or 0)

    async def get_user_timezone(self, user_id: Optional[str | int] = None) -> str:
        state = await self.get_user_state(user_id)
        tz = str(state.get("timezone") or "").strip()
        return tz or "UTC"

    async def set_user_timezone(self, timezone_name: str, user_id: Optional[str | int] = None) -> None:
        await self.update_user_state(user_id, timezone=timezone_name)

    async def set_current_day_index(self, day_index: int, user_id: Optional[str | int] = None) -> None:
        await self.update_user_state(user_id, current_day_index=max(0, day_index))

    async def set_current_day_for_active_program(self, day_order: int, user_id: Optional[str | int] = None) -> bool:
        program = await self.get_active_program(user_id)
        if not program:
            return False
        days = await self.get_program_days(int(program["id"]))
        if not days:
            return False
        if day_order < 0 or day_order >= len(days):
            return False
        await self.set_current_day_index(day_order, user_id=user_id)
        return True

    async def get_active_program(self, user_id: Optional[str | int] = None) -> Optional[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        await self.check_and_revert_expired_temporary_program(normalized)
        async with self.connect() as conn:
            await self._ensure_user_state(conn, normalized)
            row = await self._fetchone(conn, 
                """
                SELECT p.*
                FROM programs p
                JOIN user_state u ON u.current_program_id = p.id
                WHERE p.active = 1
                  AND p.user_id = ?
                  AND u.user_id = ?
                LIMIT 1
                """,
                (normalized, normalized),
            )
            if row:
                return dict(row)

            row = await self._fetchone(conn, 
                "SELECT * FROM programs WHERE active = 1 AND user_id = ? ORDER BY id DESC LIMIT 1",
                (normalized,),
            )
            if row:
                await conn.execute(
                    "UPDATE user_state SET current_program_id = ? WHERE user_id = ?",
                    (row["id"], normalized),
                )
                await conn.commit()
                return dict(row)
            return None

    async def check_and_revert_expired_temporary_program(
        self,
        user_id: Optional[str | int] = None,
    ) -> Optional[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        today = date.today().isoformat()
        async with self.connect() as conn:
            temporary = await self._fetchone(
                conn,
                """
                SELECT *
                FROM programs
                WHERE active = 1 AND temporary = 1 AND expires_at IS NOT NULL AND expires_at <= ?
                  AND user_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (today, normalized),
            )
            if not temporary:
                return None

            parent_id = temporary["parent_program_id"]
            parent = None
            if parent_id:
                parent = await self._fetchone(
                    conn,
                    "SELECT * FROM programs WHERE id = ?",
                    (parent_id,),
                )

            await conn.execute("UPDATE programs SET active = 0 WHERE id = ?", (temporary["id"],))
            if parent_id:
                await conn.execute("UPDATE programs SET active = 1 WHERE id = ?", (parent_id,))
                await conn.execute(
                    "UPDATE user_state SET current_program_id = ? WHERE user_id = ?",
                    (parent_id, normalized),
                )
            await conn.commit()
            return {
                "expired_program_name": str(temporary["name"]),
                "parent_program_name": str(parent["name"]) if parent else None,
            }

    async def _revert_expired_temporary_program_if_needed(self, user_id: Optional[str | int] = None) -> None:
        normalized = self._normalize_user_id(user_id)
        today = date.today().isoformat()
        async with self.connect() as conn:
            temporary = await self._fetchone(conn, 
                """
                SELECT * FROM programs
                WHERE active = 1 AND temporary = 1 AND expires_at IS NOT NULL AND expires_at <= ?
                  AND user_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (today, normalized),
            )
            if not temporary:
                return

            parent_id = temporary["parent_program_id"]
            await conn.execute("UPDATE programs SET active = 0 WHERE id = ?", (temporary["id"],))
            if parent_id:
                await conn.execute("UPDATE programs SET active = 1 WHERE id = ?", (parent_id,))
                await conn.execute(
                    "UPDATE user_state SET current_program_id = ? WHERE user_id = ?",
                    (parent_id, normalized),
                )
            await conn.commit()

    async def revert_from_temporary_program(self, user_id: Optional[str | int] = None) -> bool:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            temporary = await self._fetchone(
                conn,
                """
                SELECT *
                FROM programs
                WHERE user_id = ? AND active = 1 AND temporary = 1
                ORDER BY id DESC
                LIMIT 1
                """,
                (normalized,),
            )
            if not temporary:
                return False
            parent_id = temporary["parent_program_id"]
            await conn.execute("UPDATE programs SET active = 0 WHERE id = ?", (temporary["id"],))
            if parent_id:
                await conn.execute("UPDATE programs SET active = 1 WHERE id = ?", (parent_id,))
                await conn.execute(
                    "UPDATE user_state SET current_program_id = ? WHERE user_id = ?",
                    (parent_id, normalized),
                )
            await conn.commit()
        return True

    async def create_program_from_payload(
        self,
        payload: dict[str, Any],
        *,
        user_id: Optional[str | int] = None,
        temporary: bool = False,
        parent_program_id: Optional[int] = None,
        expires_at: Optional[str] = None,
    ) -> int:
        normalized = self._normalize_user_id(user_id)
        program_name = payload.get("program_name") or "Untitled Program"
        days = payload.get("days") or []
        async with self.connect() as conn:
            await self._ensure_user_state(conn, normalized)
            await conn.execute("UPDATE programs SET active = 0 WHERE active = 1 AND user_id = ?", (normalized,))
            cursor = await conn.execute(
                """
                INSERT INTO programs (user_id, name, active, temporary, parent_program_id, expires_at)
                VALUES (?, ?, 1, ?, ?, ?)
                """,
                (normalized, program_name, int(temporary), parent_program_id, expires_at),
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
                "UPDATE user_state SET current_program_id = ?, current_day_index = 0 WHERE user_id = ?",
                (program_id, normalized),
            )
            await conn.commit()
            return int(program_id)

    async def get_recent_program_by_name(
        self,
        name: str,
        *,
        user_id: Optional[str | int] = None,
        minutes: int = 5,
    ) -> Optional[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        cleaned = name.strip()
        if not cleaned:
            return None
        async with self.connect() as conn:
            row = await self._fetchone(
                conn,
                """
                SELECT *
                FROM programs
                WHERE LOWER(name) = LOWER(?)
                  AND user_id = ?
                  AND created_at >= datetime('now', ?)
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (cleaned, normalized, f"-{max(1, minutes)} minutes"),
            )
        return dict(row) if row else None

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

    async def get_exercises_for_day_index(self, day_index: int, user_id: Optional[str | int] = None) -> list[dict[str, Any]]:
        program = await self.get_active_program(user_id)
        if not program:
            return []
        days = await self.get_program_days(int(program["id"]))
        if not days:
            return []
        normalized = day_index % len(days)
        day = days[normalized]
        return await self.get_exercises_for_day(int(day["id"]))

    async def get_day_for_index(self, day_index: int, user_id: Optional[str | int] = None) -> Optional[dict[str, Any]]:
        program = await self.get_active_program(user_id)
        if not program:
            return None
        days = await self.get_program_days(int(program["id"]))
        if not days:
            return None
        return days[day_index % len(days)]

    async def advance_day_index(self, user_id: Optional[str | int] = None) -> int:
        program = await self.get_active_program(user_id)
        if not program:
            return 0
        days = await self.get_program_days(int(program["id"]))
        if not days:
            return 0

        current = await self.get_current_day_index(user_id)
        nxt = (current + 1) % len(days)
        await self.update_user_state(user_id, current_day_index=nxt)
        return nxt

    async def get_last_logs_for_exercise(
        self,
        exercise_id: int,
        limit: int = 6,
        user_id: Optional[str | int] = None,
    ) -> list[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                """
                SELECT *
                FROM workout_logs
                WHERE exercise_id = ?
                  AND user_id = ?
                ORDER BY date DESC, set_number DESC
                LIMIT ?
                """,
                (exercise_id, normalized, limit),
            )
        return [dict(r) for r in rows]

    async def log_set(
        self,
        exercise_id: int,
        *,
        user_id: Optional[str | int] = None,
        workout_date: date,
        set_number: int,
        weight: float,
        reps: int,
        unit: str,
        rir: Optional[int] = None,
        notes: str = "",
    ) -> int:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            cursor = await conn.execute(
                """
                INSERT INTO workout_logs (user_id, exercise_id, date, set_number, weight, reps, unit, rir, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (normalized, exercise_id, workout_date.isoformat(), set_number, weight, reps, unit, rir, notes),
            )
            log_id = cursor.lastrowid
            await conn.commit()
        return int(log_id)

    async def get_workout_log(self, log_id: int, user_id: Optional[str | int] = None) -> Optional[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            row = await self._fetchone(
                conn,
                """
                SELECT *
                FROM workout_logs
                WHERE id = ?
                  AND user_id = ?
                LIMIT 1
                """,
                (log_id, normalized),
            )
        return dict(row) if row else None

    async def update_workout_log(
        self,
        log_id: int,
        *,
        user_id: Optional[str | int] = None,
        weight: float,
        reps: int,
        unit: str,
        rir: Optional[int],
        notes: str,
    ) -> bool:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            cur = await conn.execute(
                """
                UPDATE workout_logs
                SET weight = ?, reps = ?, unit = ?, rir = ?, notes = ?
                WHERE id = ? AND user_id = ?
                """,
                (weight, reps, unit, rir, notes, log_id, normalized),
            )
            await conn.commit()
        return int(cur.rowcount or 0) > 0

    async def get_best_pr(self, exercise_name: str, user_id: Optional[str | int] = None) -> Optional[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            row = await self._fetchone(conn, 
                """
                SELECT *
                FROM personal_records
                WHERE LOWER(exercise_name) = LOWER(?)
                  AND user_id = ?
                ORDER BY estimated_1rm DESC, date DESC
                LIMIT 1
                """,
                (exercise_name, normalized),
            )
        return dict(row) if row else None

    async def create_pr(
        self,
        exercise_name: str,
        *,
        user_id: Optional[str | int] = None,
        weight: float,
        reps: int,
        unit: str,
        estimated_1rm: float,
        workout_date: date,
        workout_log_id: Optional[int],
    ) -> int:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            cursor = await conn.execute(
                """
                INSERT INTO personal_records (
                    user_id, exercise_name, weight, reps, unit, estimated_1rm, date, workout_log_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized,
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

    async def delete_pr_for_workout_log(self, workout_log_id: int, user_id: Optional[str | int] = None) -> None:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            await conn.execute(
                "DELETE FROM personal_records WHERE workout_log_id = ? AND user_id = ?",
                (workout_log_id, normalized),
            )
            await conn.commit()

    async def get_best_pr_excluding_log(
        self,
        exercise_name: str,
        *,
        user_id: Optional[str | int] = None,
        excluded_workout_log_id: int,
    ) -> Optional[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            row = await self._fetchone(
                conn,
                """
                SELECT *
                FROM personal_records
                WHERE LOWER(exercise_name) = LOWER(?)
                  AND user_id = ?
                  AND (workout_log_id IS NULL OR workout_log_id != ?)
                ORDER BY estimated_1rm DESC, date DESC
                LIMIT 1
                """,
                (exercise_name, normalized, excluded_workout_log_id),
            )
        return dict(row) if row else None

    async def get_recent_prs(self, days: int = 14, user_id: Optional[str | int] = None) -> list[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        threshold = (date.today() - timedelta(days=days)).isoformat()
        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                """
                SELECT *
                FROM personal_records
                WHERE date >= ?
                  AND user_id = ?
                ORDER BY date DESC, estimated_1rm DESC
                """,
                (threshold, normalized),
            )
        return [dict(r) for r in rows]

    async def get_last_logs_for_day_index(
        self,
        day_index: int,
        user_id: Optional[str | int] = None,
    ) -> list[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        day = await self.get_day_for_index(day_index, user_id=normalized)
        if not day:
            return []

        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                """
                SELECT e.name AS exercise_name, wl.*
                FROM workout_logs wl
                JOIN exercises e ON e.id = wl.exercise_id
                WHERE e.program_day_id = ?
                  AND wl.user_id = ?
                ORDER BY wl.date DESC, wl.set_number DESC
                LIMIT 50
                """,
                (day["id"], normalized),
            )
        return [dict(r) for r in rows]

    async def add_activity(
        self,
        *,
        user_id: Optional[str | int] = None,
        activity_date: date,
        activity_type: str,
        description: str,
        intensity: str,
        muscle_groups: str,
    ) -> int:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            cursor = await conn.execute(
                """
                INSERT INTO activity_logs (user_id, date, activity_type, description, intensity, muscle_groups)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (normalized, activity_date.isoformat(), activity_type, description, intensity, muscle_groups),
            )
            await conn.commit()
        return int(cursor.lastrowid)

    async def add_injury(
        self,
        *,
        user_id: Optional[str | int] = None,
        injury_date: date,
        description: str,
        muscle_groups: str,
        severity: str = "moderate",
    ) -> int:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            cursor = await conn.execute(
                """
                INSERT INTO injuries (user_id, date, description, muscle_groups, severity, active)
                VALUES (?, ?, ?, ?, ?, 1)
                """,
                (normalized, injury_date.isoformat(), description, muscle_groups, severity),
            )
            await conn.commit()
        return int(cursor.lastrowid)

    async def get_active_injuries(self, user_id: Optional[str | int] = None) -> list[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            rows = await self._fetchall(
                conn,
                """
                SELECT *
                FROM injuries
                WHERE active = 1
                  AND user_id = ?
                ORDER BY date DESC, id DESC
                """,
                (normalized,),
            )
        return [dict(r) for r in rows]

    async def resolve_injuries(self, *, user_id: Optional[str | int] = None, muscle_group: Optional[str] = None) -> int:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            if muscle_group:
                cursor = await conn.execute(
                    """
                    UPDATE injuries
                    SET active = 0, resolved_date = ?
                    WHERE active = 1 AND user_id = ? AND LOWER(muscle_groups) LIKE ?
                    """,
                    (date.today().isoformat(), normalized, f"%{muscle_group.lower()}%"),
                )
            else:
                cursor = await conn.execute(
                    """
                    UPDATE injuries
                    SET active = 0, resolved_date = ?
                    WHERE active = 1 AND user_id = ?
                    """,
                    (date.today().isoformat(), normalized),
                )
            await conn.commit()
        return int(cursor.rowcount or 0)

    async def get_activities_last_7_days(self, user_id: Optional[str | int] = None) -> list[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        threshold = (date.today() - timedelta(days=7)).isoformat()
        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                "SELECT * FROM activity_logs WHERE date >= ? AND user_id = ? ORDER BY date DESC, id DESC",
                (threshold, normalized),
            )
        return [dict(r) for r in rows]

    async def save_cue(self, exercise_name: str, cue: str, user_id: Optional[str | int] = None) -> int:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            cursor = await conn.execute(
                "INSERT INTO exercise_cues (user_id, exercise_name, cue) VALUES (?, ?, ?)",
                (normalized, exercise_name, cue.strip()),
            )
            await conn.commit()
        return int(cursor.lastrowid)

    async def get_latest_cue(self, exercise_name: str, user_id: Optional[str | int] = None) -> Optional[str]:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            row = await self._fetchone(conn, 
                """
                SELECT cue
                FROM exercise_cues
                WHERE LOWER(exercise_name) = LOWER(?)
                  AND user_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (exercise_name, normalized),
            )
        return str(row["cue"]) if row else None

    async def get_weekly_volume(
        self,
        *,
        user_id: Optional[str | int] = None,
        start_date: Optional[date] = None,
    ) -> dict[str, int]:
        normalized = self._normalize_user_id(user_id)
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
                  AND wl.user_id = ?
                GROUP BY e.muscle_groups
                """,
                (start_date.isoformat(), end_date.isoformat(), normalized),
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

    async def get_trend_last_4_weeks(self, user_id: Optional[str | int] = None) -> list[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
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
                  AND wl.user_id = ?
                GROUP BY e.name
                ORDER BY best_e1rm DESC
                """,
                (threshold, normalized),
            )
        return [dict(r) for r in rows]

    async def get_e1rm_history(
        self,
        exercise_name: str,
        limit: int = 12,
        user_id: Optional[str | int] = None,
    ) -> list[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        resolved = await self.resolve_exercise_name(exercise_name, user_id=normalized)
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
                  AND wl.user_id = ?
                ORDER BY wl.date ASC, wl.set_number ASC
                LIMIT ?
                """,
                (target, normalized, limit),
            )
        return [dict(r) for r in rows]

    async def export_logs(
        self,
        *,
        user_id: Optional[str | int] = None,
        exercise_name: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        resolved_target: Optional[str] = None
        if exercise_name:
            resolved_target = await self.resolve_exercise_name(exercise_name, user_id=normalized)

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
                      AND wl.user_id = ?
                    ORDER BY wl.date, e.name, wl.set_number
                    """,
                    (target, normalized),
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
                    WHERE wl.user_id = ?
                    ORDER BY wl.date, e.name, wl.set_number
                    """,
                    (normalized,),
                )
        return [dict(r) for r in rows]

    async def resolve_exercise_name(self, query: str, user_id: Optional[str | int] = None) -> Optional[str]:
        normalized = self._normalize_user_id(user_id)
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
                  AND wl.user_id = ?
                GROUP BY e.name
                ORDER BY cnt DESC, LENGTH(e.name) ASC
                LIMIT 1
                """,
                (cleaned, normalized),
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
                  AND wl.user_id = ?
                GROUP BY e.name
                ORDER BY cnt DESC, LENGTH(e.name) ASC
                LIMIT 1
                """,
                (like, normalized),
            )
            if partial:
                return str(partial["name"])

            program_partial = await self._fetchone(
                conn,
                """
                SELECT e.name AS name
                FROM exercises e
                JOIN program_days d ON d.id = e.program_day_id
                JOIN programs p ON p.id = d.program_id
                WHERE LOWER(e.name) LIKE ?
                  AND p.user_id = ?
                ORDER BY LENGTH(e.name) ASC
                LIMIT 1
                """,
                (like, normalized),
            )
            if program_partial:
                return str(program_partial["name"])
        return None

    async def get_recent_activities(self, hours: int = 72, user_id: Optional[str | int] = None) -> list[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        days = max(1, int(hours / 24))
        threshold = (date.today() - timedelta(days=days)).isoformat()
        async with self.connect() as conn:
            rows = await self._fetchall(
                conn,
                """
                SELECT *
                FROM activity_logs
                WHERE date >= ?
                  AND user_id = ?
                ORDER BY date DESC, id DESC
                """,
                (threshold, normalized),
            )
        return [dict(r) for r in rows]

    async def wipe_workout_data_preserve_settings(self, user_id: Optional[str | int] = None) -> None:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            await conn.execute("DELETE FROM workout_logs WHERE user_id = ?", (normalized,))
            await conn.execute("DELETE FROM activity_logs WHERE user_id = ?", (normalized,))
            await conn.execute("DELETE FROM personal_records WHERE user_id = ?", (normalized,))
            await conn.execute("DELETE FROM exercise_cues WHERE user_id = ?", (normalized,))
            await conn.execute("DELETE FROM injuries WHERE user_id = ?", (normalized,))
            await conn.execute("DELETE FROM workout_sessions WHERE user_id = ?", (normalized,))
            await conn.execute(
                """
                UPDATE user_state
                SET current_streak = 0,
                    longest_streak = 0,
                    last_workout_date = NULL,
                    last_checkin_date = NULL,
                    current_day_index = 0,
                    current_program_id = NULL
                WHERE user_id = ?
                """
                ,
                (normalized,),
            )
            await conn.execute("UPDATE programs SET active = 0 WHERE user_id = ?", (normalized,))
            await conn.commit()

    async def delete_active_program(self, user_id: Optional[str | int] = None) -> bool:
        normalized = self._normalize_user_id(user_id)
        program = await self.get_active_program(normalized)
        if not program:
            return False
        program_id = int(program["id"])
        async with self.connect() as conn:
            day_rows = await self._fetchall(
                conn,
                "SELECT id FROM program_days WHERE program_id = ?",
                (program_id,),
            )
            day_ids = [int(r["id"]) for r in day_rows]
            ex_ids: list[int] = []
            if day_ids:
                placeholders = ",".join("?" for _ in day_ids)
                ex_rows = await self._fetchall(
                    conn,
                    f"SELECT id FROM exercises WHERE program_day_id IN ({placeholders})",
                    day_ids,
                )
                ex_ids = [int(r["id"]) for r in ex_rows]

            if ex_ids:
                placeholders = ",".join("?" for _ in ex_ids)
                await conn.execute(
                    f"DELETE FROM workout_logs WHERE exercise_id IN ({placeholders}) AND user_id = ?",
                    [*ex_ids, normalized],
                )

            if day_ids:
                placeholders = ",".join("?" for _ in day_ids)
                await conn.execute(
                    f"DELETE FROM exercises WHERE program_day_id IN ({placeholders})",
                    day_ids,
                )
                await conn.execute(
                    f"DELETE FROM program_days WHERE id IN ({placeholders})",
                    day_ids,
                )

            await conn.execute("DELETE FROM programs WHERE id = ?", (program_id,))
            await conn.execute(
                "UPDATE user_state SET current_program_id = NULL, current_day_index = 0 WHERE user_id = ?",
                (normalized,),
            )
            await conn.commit()
        return True

    async def mark_workout_completed(self, workout_date: date, user_id: Optional[str | int] = None) -> dict[str, int]:
        normalized = self._normalize_user_id(user_id)
        state = await self.get_user_state(normalized)
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
            normalized,
            current_streak=current_streak,
            longest_streak=longest_streak,
            last_workout_date=today.isoformat(),
        )
        return {
            "current_streak": current_streak,
            "longest_streak": longest_streak,
        }

    async def build_context(self, target_date: date, user_id: Optional[str | int] = None) -> dict[str, Any]:
        normalized = self._normalize_user_id(user_id)
        day_index = await self.get_current_day_index(normalized)
        return {
            "current_program": await self.get_active_program(normalized),
            "todays_exercises": await self.get_exercises_for_day_index(day_index, user_id=normalized),
            "last_session_logs": await self.get_last_logs_for_day_index(day_index, user_id=normalized),
            "recent_activities": await self.get_activities_last_7_days(user_id=normalized),
            "user_state": await self.get_user_state(normalized),
            "weekly_volume": await self.get_weekly_volume(user_id=normalized),
            "recent_performance_trend": await self.get_trend_last_4_weeks(user_id=normalized),
            "recent_prs": await self.get_recent_prs(days=14, user_id=normalized),
            "target_date": target_date.isoformat(),
        }

    async def set_last_checkin(self, checkin_date: date, user_id: Optional[str | int] = None) -> None:
        await self.update_user_state(user_id, last_checkin_date=checkin_date.isoformat())

    async def get_last_checkin_date(self, user_id: Optional[str | int] = None) -> Optional[date]:
        state = await self.get_user_state(user_id)
        raw = state.get("last_checkin_date")
        if not raw:
            return None
        return datetime.strptime(raw, "%Y-%m-%d").date()

    async def get_exercise_by_name_in_current_program(
        self,
        name: str,
        user_id: Optional[str | int] = None,
    ) -> Optional[dict[str, Any]]:
        program = await self.get_active_program(user_id)
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

    async def update_exercise_name_in_active_program(
        self,
        old_name: str,
        new_name: str,
        user_id: Optional[str | int] = None,
    ) -> int:
        program = await self.get_active_program(user_id)
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

    async def replace_exercise_in_active_program(
        self,
        old_name: str,
        new_name: str,
        *,
        user_id: Optional[str | int] = None,
        day_name_hint: Optional[str] = None,
        new_category: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        program = await self.get_active_program(user_id)
        if not program:
            return None
        params: list[Any] = [int(program["id"]), old_name]
        day_filter_sql = ""
        if day_name_hint and day_name_hint.strip():
            day_filter_sql = "AND LOWER(d.name) LIKE LOWER(?)"
            params.append(f"%{day_name_hint.strip()}%")

        async with self.connect() as conn:
            row = await self._fetchone(
                conn,
                f"""
                SELECT e.id AS exercise_id, e.name AS exercise_name, e.category AS category, d.name AS day_name
                FROM exercises e
                JOIN program_days d ON d.id = e.program_day_id
                WHERE d.program_id = ?
                  AND LOWER(e.name) = LOWER(?)
                  {day_filter_sql}
                ORDER BY d.day_order, e.display_order
                LIMIT 1
                """,
                tuple(params),
            )
            if not row:
                return None

            ex_id = int(row["exercise_id"])
            next_category = str(new_category or row["category"] or "cable_machine")
            await conn.execute(
                "UPDATE exercises SET name = ?, category = ? WHERE id = ?",
                (new_name, next_category, ex_id),
            )
            await conn.commit()
            return {
                "exercise_id": ex_id,
                "old_name": str(row["exercise_name"]),
                "new_name": new_name,
                "day_name": str(row["day_name"]),
                "category": next_category,
            }

    async def rename_program_day_in_active_program(
        self,
        old_name: str,
        new_name: str,
        user_id: Optional[str | int] = None,
    ) -> int:
        program = await self.get_active_program(user_id)
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

    async def get_latest_workout_date(self, user_id: Optional[str | int] = None) -> Optional[date]:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            row = await self._fetchone(conn, "SELECT MAX(date) AS max_date FROM workout_logs WHERE user_id = ?", (normalized,))
        if not row or not row["max_date"]:
            return None
        return datetime.strptime(row["max_date"], "%Y-%m-%d").date()

    async def get_last_log_for_exercise(
        self,
        exercise_id: int,
        user_id: Optional[str | int] = None,
    ) -> Optional[dict[str, Any]]:
        logs = await self.get_last_logs_for_exercise(exercise_id, limit=1, user_id=user_id)
        return logs[0] if logs else None

    async def get_last_logs_grouped_for_day(
        self,
        day_id: int,
        user_id: Optional[str | int] = None,
    ) -> dict[int, list[dict[str, Any]]]:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            rows = await self._fetchall(conn, 
                """
                SELECT wl.*
                FROM workout_logs wl
                JOIN exercises e ON e.id = wl.exercise_id
                WHERE e.program_day_id = ?
                  AND wl.user_id = ?
                ORDER BY wl.date DESC, wl.set_number DESC
                """,
                (day_id, normalized),
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
