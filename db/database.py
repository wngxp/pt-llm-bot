from __future__ import annotations
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, AsyncIterator, Iterable, Optional

import aiosqlite

DEFAULT_USER_ID = "legacy"
ARCHIVE_DAY_NAME = "__ARCHIVE__"


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
            await self._ensure_program_display_ids(conn)
            await self._ensure_program_day_tracking_columns(conn)
            await self._ensure_exercise_equipment_types(conn)
            await self._ensure_exercise_programme_columns(conn)
            await self._ensure_user_state_programme_columns(conn)
            await self._ensure_workout_log_snapshot_columns(conn)
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

    async def _ensure_column(
        self,
        conn: aiosqlite.Connection,
        *,
        table: str,
        column: str,
        definition: str,
    ) -> None:
        rows = await self._fetchall(conn, f"PRAGMA table_info({table})")
        cols = {str(r["name"]).lower() for r in rows}
        if column.lower() not in cols:
            await conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    async def _ensure_multi_user_columns(self, conn: aiosqlite.Connection) -> None:
        await self._ensure_column_with_default(conn, table="programs", column="user_id")
        await self._ensure_column_with_default(conn, table="workout_logs", column="user_id")
        await self._ensure_column_with_default(conn, table="activity_logs", column="user_id")
        await self._ensure_column_with_default(conn, table="personal_records", column="user_id")
        await self._ensure_column_with_default(conn, table="exercise_cues", column="user_id")
        await self._ensure_column_with_default(conn, table="injuries", column="user_id")
        await self._ensure_column_with_default(conn, table="workout_sessions", column="user_id")

    async def _ensure_program_display_ids(self, conn: aiosqlite.Connection) -> None:
        rows = await self._fetchall(conn, "PRAGMA table_info(programs)")
        columns = {str(r["name"]).lower() for r in rows}
        if "display_id" not in columns:
            await conn.execute("ALTER TABLE programs ADD COLUMN display_id INTEGER")

        users = await self._fetchall(
            conn,
            """
            SELECT DISTINCT user_id
            FROM programs
            WHERE user_id IS NOT NULL AND TRIM(user_id) != ''
            ORDER BY user_id
            """,
        )
        for row in users:
            user_id = str(row["user_id"])
            programs = await self._fetchall(
                conn,
                """
                SELECT id, display_id
                FROM programs
                WHERE user_id = ?
                ORDER BY COALESCE(created_at, ''), id
                """,
                (user_id,),
            )
            next_display_id = 1
            for program in programs:
                current = program["display_id"]
                if current is None or int(current or 0) <= 0:
                    await conn.execute(
                        "UPDATE programs SET display_id = ? WHERE id = ?",
                        (next_display_id, int(program["id"])),
                    )
                    next_display_id += 1
                    continue
                next_display_id = max(next_display_id, int(current) + 1)
        await conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_programs_user_display_id ON programs(user_id, display_id)"
        )

    def _infer_equipment_type(self, name: str, category: str) -> str:
        lowered = str(name or "").strip().lower()
        normalized_category = str(category or "").strip().lower()
        if "smith" in lowered or normalized_category == "smith_machine":
            return "smith machine"
        if normalized_category in {"heavy_barbell", "light_barbell"}:
            return "barbell"
        if normalized_category == "dumbbell" or any(token in lowered for token in ("dumbbell", "db ")):
            return "dumbbell"
        if normalized_category == "bodyweight" or any(
            token in lowered for token in ("pull-up", "pull up", "chin-up", "chin up", "push-up", "push up", "dip")
        ):
            return "bodyweight"
        if "cable" in lowered or any(token in lowered for token in ("press-around", "press around", "face pull")):
            return "cable"
        if any(
            token in lowered
            for token in ("machine", "pulldown", "lat pulldown", "leg press", "leg curl", "leg extension", "hack squat")
        ):
            return "machine"
        return "unknown"

    async def _ensure_exercise_equipment_types(self, conn: aiosqlite.Connection) -> None:
        rows = await self._fetchall(conn, "PRAGMA table_info(exercises)")
        columns = {str(r["name"]).lower() for r in rows}
        if "equipment_type" not in columns:
            await conn.execute("ALTER TABLE exercises ADD COLUMN equipment_type TEXT DEFAULT 'unknown'")

        exercises = await self._fetchall(
            conn,
            """
            SELECT id, name, category, equipment_type
            FROM exercises
            ORDER BY id
            """,
        )
        for exercise in exercises:
            current = str(exercise["equipment_type"] or "").strip().lower()
            if current in {"barbell", "dumbbell", "cable", "machine", "bodyweight", "smith machine", "unknown"}:
                continue
            equipment_type = self._infer_equipment_type(str(exercise["name"] or ""), str(exercise["category"] or ""))
            await conn.execute(
                "UPDATE exercises SET equipment_type = ? WHERE id = ?",
                (equipment_type, int(exercise["id"])),
            )

    async def _ensure_program_day_tracking_columns(self, conn: aiosqlite.Connection) -> None:
        await self._ensure_column(conn, table="program_days", column="block", definition="TEXT")
        await self._ensure_column(conn, table="program_days", column="week", definition="INTEGER")
        await self._ensure_column(conn, table="program_days", column="day_number", definition="INTEGER")
        await self._ensure_column(conn, table="program_days", column="is_rest_day", definition="BOOLEAN DEFAULT 0")
        await conn.execute(
            """
            UPDATE program_days
            SET day_number = COALESCE(day_number, day_order + 1),
                is_rest_day = COALESCE(is_rest_day, 0)
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_program_days_program_block_week_day
            ON program_days(program_id, block, week, day_number)
            """
        )

    async def _ensure_exercise_programme_columns(self, conn: aiosqlite.Connection) -> None:
        await self._ensure_column(conn, table="exercises", column="technique", definition="TEXT")
        await self._ensure_column(conn, table="exercises", column="warmup_sets_low", definition="INTEGER")
        await self._ensure_column(conn, table="exercises", column="warmup_sets_high", definition="INTEGER")
        await self._ensure_column(conn, table="exercises", column="early_rpe_low", definition="INTEGER")
        await self._ensure_column(conn, table="exercises", column="early_rpe_high", definition="INTEGER")
        await self._ensure_column(conn, table="exercises", column="last_rpe_low", definition="INTEGER")
        await self._ensure_column(conn, table="exercises", column="last_rpe_high", definition="INTEGER")
        await self._ensure_column(conn, table="exercises", column="rest_low", definition="INTEGER")
        await self._ensure_column(conn, table="exercises", column="rest_high", definition="INTEGER")
        await self._ensure_column(conn, table="exercises", column="sub1", definition="TEXT")
        await self._ensure_column(conn, table="exercises", column="sub2", definition="TEXT")

    async def _ensure_user_state_programme_columns(self, conn: aiosqlite.Connection) -> None:
        await self._ensure_column(conn, table="user_state", column="current_block", definition="TEXT")
        await self._ensure_column(conn, table="user_state", column="current_week", definition="INTEGER")
        await self._ensure_column(conn, table="user_state", column="current_day_number", definition="INTEGER")

    def _position_fields_for_day(self, day: Optional[dict[str, Any]], *, fallback_index: int = 0) -> dict[str, Any]:
        if not day:
            return {
                "current_block": None,
                "current_week": None,
                "current_day_number": fallback_index + 1,
            }
        return {
            "current_block": day.get("block"),
            "current_week": day.get("week"),
            "current_day_number": day.get("day_number") if day.get("day_number") is not None else fallback_index + 1,
        }

    async def _ensure_workout_log_snapshot_columns(self, conn: aiosqlite.Connection) -> None:
        rows = await self._fetchall(conn, "PRAGMA table_info(workout_logs)")
        columns = {str(r["name"]).lower() for r in rows}
        if "performed_exercise_name" not in columns:
            await conn.execute("ALTER TABLE workout_logs ADD COLUMN performed_exercise_name TEXT")
        if "performed_category" not in columns:
            await conn.execute("ALTER TABLE workout_logs ADD COLUMN performed_category TEXT")
        if "performed_equipment_type" not in columns:
            await conn.execute("ALTER TABLE workout_logs ADD COLUMN performed_equipment_type TEXT")

    async def _snapshot_exercise_logs(
        self,
        conn: aiosqlite.Connection,
        *,
        exercise_id: int,
        user_id: str,
        exercise_name: str,
        category: str,
        equipment_type: str,
    ) -> None:
        await conn.execute(
            """
            UPDATE workout_logs
            SET performed_exercise_name = COALESCE(performed_exercise_name, ?),
                performed_category = COALESCE(performed_category, ?),
                performed_equipment_type = COALESCE(performed_equipment_type, ?)
            WHERE exercise_id = ?
              AND user_id = ?
            """,
            (exercise_name, category, equipment_type, exercise_id, user_id),
        )

    async def _get_or_create_archive_day(self, conn: aiosqlite.Connection, *, program_id: int) -> int:
        row = await self._fetchone(
            conn,
            """
            SELECT id
            FROM program_days
            WHERE program_id = ?
              AND name = ?
            LIMIT 1
            """,
            (program_id, ARCHIVE_DAY_NAME),
        )
        if row:
            return int(row["id"])

        max_order_row = await self._fetchone(
            conn,
            "SELECT COALESCE(MAX(day_order), -1) AS max_day_order FROM program_days WHERE program_id = ?",
            (program_id,),
        )
        next_order = int(max_order_row["max_day_order"] or -1) + 1 if max_order_row else 0
        cursor = await conn.execute(
            "INSERT INTO program_days (program_id, day_order, name) VALUES (?, ?, ?)",
            (program_id, next_order, ARCHIVE_DAY_NAME),
        )
        return int(cursor.lastrowid)

    def _category_label_from_storage(self, category: str, equipment_type: str) -> str:
        normalized_category = str(category or "").strip().lower()
        normalized_type = str(equipment_type or "").strip().lower()
        if normalized_type == "smith machine" or normalized_category == "smith_machine":
            return "smith"
        if normalized_type == "machine":
            return "machine"
        if normalized_type == "cable":
            return "cable"
        if normalized_type == "dumbbell" or normalized_category == "dumbbell":
            return "dumbbell"
        if normalized_type == "bodyweight" or normalized_category == "bodyweight":
            return "bodyweight"
        if normalized_category in {"heavy_barbell", "light_barbell"}:
            return normalized_category
        return normalized_type or normalized_category or "unknown"

    def _normalize_requested_category(
        self,
        exercise_name: str,
        requested: str,
        *,
        current_category: str = "",
    ) -> Optional[str]:
        lowered = " ".join(str(requested or "").strip().lower().split())
        if not lowered:
            return None
        aliases = {
            "bw": "bodyweight",
            "body weight": "bodyweight",
            "db": "dumbbell",
            "smith machine": "smith",
            "smith": "smith",
            "heavy barbell": "heavy_barbell",
            "light barbell": "light_barbell",
        }
        normalized = aliases.get(lowered, lowered.replace(" ", "_"))
        if normalized in {"dumbbell", "cable", "machine", "smith", "bodyweight", "heavy_barbell", "light_barbell"}:
            return normalized
        if normalized == "barbell":
            current = str(current_category or "").strip().lower()
            if current in {"heavy_barbell", "light_barbell"}:
                return current
            inferred = self._infer_equipment_type(exercise_name, current)
            if inferred == "barbell":
                lowered_name = exercise_name.strip().lower()
                heavy_tokens = ("squat", "bench", "deadlift", "overhead press", "ohp", "barbell row")
                return "heavy_barbell" if any(token in lowered_name for token in heavy_tokens) else "light_barbell"
        return None

    def _storage_category_and_equipment(
        self,
        exercise_name: str,
        requested_category: str,
        *,
        current_category: str = "",
    ) -> tuple[str, str]:
        normalized = self._normalize_requested_category(
            exercise_name,
            requested_category,
            current_category=current_category,
        )
        if normalized == "heavy_barbell":
            return "heavy_barbell", "barbell"
        if normalized == "light_barbell":
            return "light_barbell", "barbell"
        if normalized == "dumbbell":
            return "dumbbell", "dumbbell"
        if normalized == "bodyweight":
            return "bodyweight", "bodyweight"
        if normalized == "smith":
            return "smith_machine", "smith machine"
        if normalized == "machine":
            return "cable_machine", "machine"
        return "cable_machine", "cable"

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
        normalized = self._normalize_user_id(user_id)
        safe_index = max(0, int(day_index))
        program = await self.get_active_program(normalized)
        if not program:
            await self.update_user_state(
                normalized,
                current_day_index=safe_index,
                current_block=None,
                current_week=None,
                current_day_number=safe_index + 1,
            )
            return

        days = await self.get_program_days(int(program["id"]))
        if not days:
            await self.update_user_state(
                normalized,
                current_day_index=safe_index,
                current_block=None,
                current_week=None,
                current_day_number=safe_index + 1,
            )
            return

        normalized_index = safe_index % len(days)
        position = self._position_fields_for_day(days[normalized_index], fallback_index=normalized_index)
        await self.update_user_state(
            normalized,
            current_day_index=normalized_index,
            current_block=position["current_block"],
            current_week=position["current_week"],
            current_day_number=position["current_day_number"],
        )

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
                "parent_display_id": int(parent["display_id"]) if parent and parent["display_id"] is not None else None,
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
            display_id_row = await self._fetchone(
                conn,
                "SELECT COALESCE(MAX(display_id), 0) + 1 AS next_display_id FROM programs WHERE user_id = ?",
                (normalized,),
            )
            display_id = int(display_id_row["next_display_id"] or 1) if display_id_row else 1
            cursor = await conn.execute(
                """
                INSERT INTO programs (user_id, display_id, name, active, temporary, parent_program_id, expires_at)
                VALUES (?, ?, ?, 1, ?, ?, ?)
                """,
                (normalized, display_id, program_name, int(temporary), parent_program_id, expires_at),
            )
            program_id = cursor.lastrowid

            for day in days:
                day_order = int(day.get("day_order", 0))
                day_name = day.get("name") or f"Day {day_order + 1}"
                day_cursor = await conn.execute(
                    """
                    INSERT INTO program_days (program_id, day_order, name, block, week, day_number, is_rest_day)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        program_id,
                        day_order,
                        day_name,
                        day.get("block"),
                        day.get("week"),
                        day.get("day_number"),
                        int(bool(day.get("is_rest_day"))),
                    ),
                )
                day_id = day_cursor.lastrowid
                exercises = day.get("exercises") or []
                for idx, ex in enumerate(exercises):
                    await conn.execute(
                        """
                        INSERT INTO exercises (
                            program_day_id, name, display_order, sets, rep_range_low,
                            rep_range_high, category, equipment_type, superset_group, technique,
                            warmup_sets_low, warmup_sets_high, early_rpe_low, early_rpe_high,
                            last_rpe_low, last_rpe_high, rest_low, rest_high, sub1, sub2,
                            notes, muscle_groups
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            day_id,
                            ex.get("name") or f"Exercise {idx + 1}",
                            int(ex.get("display_order", idx)),
                            int(ex.get("sets", 1)),
                            ex.get("rep_range_low"),
                            ex.get("rep_range_high"),
                            ex.get("category") or "cable_machine",
                            ex.get("equipment_type") or self._infer_equipment_type(
                                str(ex.get("name") or ""),
                                str(ex.get("category") or ""),
                            ),
                            ex.get("superset_group"),
                            ex.get("technique"),
                            ex.get("warmup_sets_low"),
                            ex.get("warmup_sets_high"),
                            ex.get("early_rpe_low"),
                            ex.get("early_rpe_high"),
                            ex.get("last_rpe_low"),
                            ex.get("last_rpe_high"),
                            ex.get("rest_low"),
                            ex.get("rest_high"),
                            ex.get("sub1"),
                            ex.get("sub2"),
                            ex.get("notes") or "",
                            ex.get("muscle_groups") or "",
                        ),
                    )

            await conn.execute(
                """
                UPDATE user_state
                SET current_program_id = ?,
                    current_day_index = 0,
                    current_block = NULL,
                    current_week = NULL,
                    current_day_number = 1
                WHERE user_id = ?
                """,
                (program_id, normalized),
            )
            await conn.commit()
        await self.set_current_day_index(0, user_id=normalized)
        return int(program_id)

    async def get_program_by_id(self, program_id: int) -> Optional[dict[str, Any]]:
        async with self.connect() as conn:
            row = await self._fetchone(
                conn,
                "SELECT * FROM programs WHERE id = ? LIMIT 1",
                (program_id,),
            )
        return dict(row) if row else None

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
                "SELECT * FROM program_days WHERE program_id = ? AND name != ? ORDER BY day_order",
                (program_id, ARCHIVE_DAY_NAME),
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

    async def find_day_index_by_week_day(
        self,
        *,
        week: int,
        day_number: int,
        user_id: Optional[str | int] = None,
        block: Optional[str] = None,
    ) -> Optional[int]:
        program = await self.get_active_program(user_id)
        if not program:
            return None
        days = await self.get_program_days(int(program["id"]))
        if not days:
            return None

        preferred_block = (block or "").strip()
        if not preferred_block:
            state = await self.get_user_state(user_id)
            preferred_block = str(state.get("current_block") or "").strip()

        matches = [
            day for day in days
            if int(day.get("week") or -1) == int(week)
            and int(day.get("day_number") or -1) == int(day_number)
        ]
        if not matches:
            return None
        if preferred_block:
            for day in matches:
                if str(day.get("block") or "").strip().lower() == preferred_block.lower():
                    return int(day["day_order"])
        return int(matches[0]["day_order"])

    async def advance_day_index(self, user_id: Optional[str | int] = None, *, skip_rest_days: bool = False) -> int:
        program = await self.get_active_program(user_id)
        if not program:
            return 0
        days = await self.get_program_days(int(program["id"]))
        if not days:
            return 0

        current = await self.get_current_day_index(user_id)
        nxt = (current + 1) % len(days)
        if skip_rest_days:
            start_idx = nxt
            while bool(days[nxt].get("is_rest_day")):
                nxt = (nxt + 1) % len(days)
                if nxt == start_idx:
                    break
        await self.set_current_day_index(nxt, user_id=user_id)
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
                SELECT wl.*,
                       COALESCE(wl.performed_exercise_name, e.name) AS logged_exercise_name,
                       COALESCE(wl.performed_category, e.category) AS logged_category,
                       COALESCE(wl.performed_equipment_type, e.equipment_type) AS logged_equipment_type
                FROM workout_logs wl
                JOIN exercises e ON e.id = wl.exercise_id
                WHERE wl.exercise_id = ?
                  AND wl.user_id = ?
                  AND LOWER(COALESCE(wl.performed_exercise_name, e.name)) = LOWER(e.name)
                ORDER BY wl.date DESC, wl.set_number DESC
                LIMIT ?
                """,
                (exercise_id, normalized, limit),
            )
        return [dict(r) for r in rows]

    async def get_last_logs_for_named_exercise(
        self,
        exercise_name: str,
        *,
        limit: int = 6,
        user_id: Optional[str | int] = None,
    ) -> list[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            rows = await self._fetchall(
                conn,
                """
                SELECT wl.*,
                       COALESCE(wl.performed_exercise_name, e.name) AS logged_exercise_name,
                       COALESCE(wl.performed_category, e.category) AS logged_category,
                       COALESCE(wl.performed_equipment_type, e.equipment_type) AS logged_equipment_type
                FROM workout_logs wl
                JOIN exercises e ON e.id = wl.exercise_id
                WHERE LOWER(COALESCE(wl.performed_exercise_name, e.name)) = LOWER(?)
                  AND wl.user_id = ?
                ORDER BY wl.date DESC, wl.set_number DESC
                LIMIT ?
                """,
                (exercise_name, normalized, limit),
            )
        return [dict(r) for r in rows]

    async def get_recent_sessions_for_named_exercise(
        self,
        exercise_name: str,
        *,
        limit: int = 5,
        user_id: Optional[str | int] = None,
    ) -> list[dict[str, Any]]:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            rows = await self._fetchall(
                conn,
                """
                SELECT wl.date,
                       GROUP_CONCAT(
                           CASE
                               WHEN COALESCE(wl.performed_category, e.category) = 'bodyweight'
                                   THEN CASE
                                       WHEN TRIM(COALESCE(wl.notes, '')) = '' THEN 'bw x ' || wl.reps
                                       ELSE COALESCE(wl.notes, 'bodyweight') || ' x ' || wl.reps
                                   END
                               ELSE printf('%g %s x %d', wl.weight, wl.unit, wl.reps)
                           END,
                           ', '
                       ) AS sets_summary
                FROM workout_logs wl
                JOIN exercises e ON e.id = wl.exercise_id
                WHERE LOWER(COALESCE(wl.performed_exercise_name, e.name)) = LOWER(?)
                  AND wl.user_id = ?
                GROUP BY wl.date
                ORDER BY wl.date DESC
                LIMIT ?
                """,
                (exercise_name, normalized, limit),
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
        performed_exercise_name: Optional[str] = None,
        performed_category: Optional[str] = None,
        performed_equipment_type: Optional[str] = None,
    ) -> int:
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            cursor = await conn.execute(
                """
                INSERT INTO workout_logs (
                    user_id, exercise_id, performed_exercise_name, performed_category, performed_equipment_type,
                    date, set_number, weight, reps, unit, rir, notes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized,
                    exercise_id,
                    performed_exercise_name,
                    performed_category,
                    performed_equipment_type,
                    workout_date.isoformat(),
                    set_number,
                    weight,
                    reps,
                    unit,
                    rir,
                    notes,
                ),
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
                SELECT COALESCE(wl.performed_exercise_name, e.name) AS exercise_name, wl.*
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
                WHERE LOWER(COALESCE(wl.performed_exercise_name, e.name)) = LOWER(?)
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
                           COALESCE(wl.performed_exercise_name, e.name) AS exercise,
                           wl.set_number,
                           wl.weight,
                           wl.unit,
                           wl.reps,
                           wl.rir,
                           (wl.weight * (1 + wl.reps / 30.0)) AS e1rm,
                           wl.notes
                    FROM workout_logs wl
                    JOIN exercises e ON e.id = wl.exercise_id
                    WHERE LOWER(COALESCE(wl.performed_exercise_name, e.name)) = LOWER(?)
                      AND wl.user_id = ?
                    ORDER BY wl.date, e.name, wl.set_number
                    """,
                    (target, normalized),
                )
            else:
                rows = await self._fetchall(conn, 
                    """
                    SELECT wl.date,
                           COALESCE(wl.performed_exercise_name, e.name) AS exercise,
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
                SELECT COALESCE(wl.performed_exercise_name, e.name) AS name, COUNT(*) AS cnt
                FROM workout_logs wl
                JOIN exercises e ON e.id = wl.exercise_id
                WHERE LOWER(COALESCE(wl.performed_exercise_name, e.name)) = LOWER(?)
                  AND wl.user_id = ?
                GROUP BY COALESCE(wl.performed_exercise_name, e.name)
                ORDER BY cnt DESC, LENGTH(name) ASC
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
                SELECT COALESCE(wl.performed_exercise_name, e.name) AS name, COUNT(*) AS cnt
                FROM workout_logs wl
                JOIN exercises e ON e.id = wl.exercise_id
                WHERE LOWER(COALESCE(wl.performed_exercise_name, e.name)) LIKE ?
                  AND wl.user_id = ?
                GROUP BY COALESCE(wl.performed_exercise_name, e.name)
                ORDER BY cnt DESC, LENGTH(name) ASC
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

    async def get_exercise_by_reference(
        self,
        day_number: int,
        exercise_number: int,
        *,
        user_id: Optional[str | int] = None,
    ) -> Optional[dict[str, Any]]:
        program = await self.get_active_program(user_id)
        if not program:
            return None
        days = await self.get_program_days(int(program["id"]))
        if day_number < 1 or day_number > len(days):
            return None
        day = days[day_number - 1]
        exercises = await self.get_exercises_for_day(int(day["id"]))
        if exercise_number < 1 or exercise_number > len(exercises):
            return None
        exercise = dict(exercises[exercise_number - 1])
        exercise["day_name"] = str(day["name"])
        exercise["day_number"] = day_number
        exercise["exercise_number"] = exercise_number
        exercise["program_id"] = int(program["id"])
        return exercise

    async def update_exercise_scheme_by_id(
        self,
        exercise_id: int,
        *,
        user_id: Optional[str | int] = None,
        sets: int,
        rep_low: Optional[int],
        rep_high: Optional[int],
    ) -> Optional[dict[str, Any]]:
        program = await self.get_active_program(user_id)
        if not program:
            return None
        async with self.connect() as conn:
            row = await self._fetchone(
                conn,
                """
                SELECT e.id, e.name, e.sets, e.rep_range_low, e.rep_range_high, d.name AS day_name
                FROM exercises e
                JOIN program_days d ON d.id = e.program_day_id
                WHERE e.id = ?
                  AND d.program_id = ?
                LIMIT 1
                """,
                (exercise_id, int(program["id"])),
            )
            if not row:
                return None
            await conn.execute(
                """
                UPDATE exercises
                SET sets = ?, rep_range_low = ?, rep_range_high = ?
                WHERE id = ?
                """,
                (max(1, int(sets)), rep_low, rep_high, exercise_id),
            )
            await conn.commit()
        return {
            "exercise_id": int(row["id"]),
            "exercise_name": str(row["name"]),
            "day_name": str(row["day_name"]),
            "old_sets": int(row["sets"] or 1),
            "old_rep_low": None if row["rep_range_low"] is None else int(row["rep_range_low"]),
            "old_rep_high": None if row["rep_range_high"] is None else int(row["rep_range_high"]),
            "new_sets": max(1, int(sets)),
            "new_rep_low": rep_low,
            "new_rep_high": rep_high,
        }

    async def update_exercise_category_by_id(
        self,
        exercise_id: int,
        new_category: str,
        *,
        user_id: Optional[str | int] = None,
    ) -> Optional[dict[str, Any]]:
        program = await self.get_active_program(user_id)
        if not program:
            return None
        async with self.connect() as conn:
            row = await self._fetchone(
                conn,
                """
                SELECT e.id, e.name, e.category, e.equipment_type, d.name AS day_name
                FROM exercises e
                JOIN program_days d ON d.id = e.program_day_id
                WHERE e.id = ?
                  AND d.program_id = ?
                LIMIT 1
                """,
                (exercise_id, int(program["id"])),
            )
            if not row:
                return None

            old_category = self._category_label_from_storage(str(row["category"] or ""), str(row["equipment_type"] or ""))
            db_category, equipment_type = self._storage_category_and_equipment(
                str(row["name"]),
                new_category,
                current_category=str(row["category"] or ""),
            )
            await conn.execute(
                "UPDATE exercises SET category = ?, equipment_type = ? WHERE id = ?",
                (db_category, equipment_type, exercise_id),
            )
            await conn.commit()
        return {
            "exercise_id": int(row["id"]),
            "exercise_name": str(row["name"]),
            "day_name": str(row["day_name"]),
            "old_category": old_category,
            "new_category": self._category_label_from_storage(db_category, equipment_type),
            "updated_rows": 1,
        }

    async def update_exercise_category(
        self,
        exercise_name: str,
        new_category: str,
        *,
        user_id: Optional[str | int] = None,
    ) -> Optional[dict[str, Any]]:
        program = await self.get_active_program(user_id)
        if not program:
            return None

        async with self.connect() as conn:
            rows = await self._fetchall(
                conn,
                """
                SELECT e.id, e.name, e.category, e.equipment_type
                FROM exercises e
                JOIN program_days d ON d.id = e.program_day_id
                WHERE d.program_id = ?
                  AND LOWER(e.name) = LOWER(?)
                ORDER BY d.day_order, e.display_order
                """,
                (int(program["id"]), exercise_name),
            )
            if not rows:
                rows = await self._fetchall(
                    conn,
                    """
                    SELECT e.id, e.name, e.category, e.equipment_type
                    FROM exercises e
                    JOIN program_days d ON d.id = e.program_day_id
                    WHERE d.program_id = ?
                      AND LOWER(e.name) LIKE ?
                    ORDER BY LENGTH(e.name), d.day_order, e.display_order
                    """,
                    (int(program["id"]), f"%{exercise_name.strip().lower()}%"),
                )
            if not rows:
                return None

            target_name = str(rows[0]["name"])
            old_category = self._category_label_from_storage(str(rows[0]["category"] or ""), str(rows[0]["equipment_type"] or ""))
            db_category, equipment_type = self._storage_category_and_equipment(
                target_name,
                new_category,
                current_category=str(rows[0]["category"] or ""),
            )
            cursor = await conn.execute(
                """
                UPDATE exercises
                SET category = ?, equipment_type = ?
                WHERE id IN (
                    SELECT e.id
                    FROM exercises e
                    JOIN program_days d ON d.id = e.program_day_id
                    WHERE d.program_id = ?
                      AND LOWER(e.name) = LOWER(?)
                )
                """,
                (db_category, equipment_type, int(program["id"]), target_name),
            )
            await conn.commit()
        return {
            "exercise_name": target_name,
            "old_category": old_category,
            "new_category": self._category_label_from_storage(db_category, equipment_type),
            "updated_rows": int(cursor.rowcount or 0),
        }

    async def update_exercise_name_in_active_program(
        self,
        old_name: str,
        new_name: str,
        user_id: Optional[str | int] = None,
    ) -> int:
        program = await self.get_active_program(user_id)
        if not program:
            return 0
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            row = await self._fetchone(
                conn,
                """
                SELECT e.id AS exercise_id, e.name AS exercise_name, e.category AS category, e.equipment_type AS equipment_type
                FROM exercises e
                JOIN program_days d ON d.id = e.program_day_id
                WHERE d.program_id = ? AND LOWER(e.name) = LOWER(?)
                ORDER BY d.day_order, e.display_order
                LIMIT 1
                """,
                (int(program["id"]), old_name),
            )
            if row:
                await self._snapshot_exercise_logs(
                    conn,
                    exercise_id=int(row["exercise_id"]),
                    user_id=normalized,
                    exercise_name=str(row["exercise_name"]),
                    category=str(row["category"] or ""),
                    equipment_type=str(row["equipment_type"] or ""),
                )
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
        new_equipment_type: Optional[str] = None,
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
                     , e.equipment_type AS equipment_type
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
            await self._snapshot_exercise_logs(
                conn,
                exercise_id=ex_id,
                user_id=self._normalize_user_id(user_id),
                exercise_name=str(row["exercise_name"]),
                category=str(row["category"] or ""),
                equipment_type=str(row["equipment_type"] or ""),
            )
            next_category = str(new_category or row["category"] or "cable_machine")
            next_equipment_type = str(
                new_equipment_type or row["equipment_type"] or self._infer_equipment_type(new_name, next_category)
            )
            await conn.execute(
                "UPDATE exercises SET name = ?, category = ?, equipment_type = ? WHERE id = ?",
                (new_name, next_category, next_equipment_type, ex_id),
            )
            await conn.commit()
            return {
                "exercise_id": ex_id,
                "old_name": str(row["exercise_name"]),
                "new_name": new_name,
                "day_name": str(row["day_name"]),
                "category": next_category,
                "equipment_type": next_equipment_type,
            }

    async def replace_exercise_in_active_program_by_id(
        self,
        exercise_id: int,
        *,
        user_id: Optional[str | int] = None,
        new_name: str,
        new_category: str,
        new_equipment_type: str,
        new_sets: Optional[int] = None,
        new_rep_low: Optional[int] = None,
        new_rep_high: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        program = await self.get_active_program(user_id)
        if not program:
            return None
        normalized = self._normalize_user_id(user_id)
        async with self.connect() as conn:
            row = await self._fetchone(
                conn,
                """
                SELECT e.id AS exercise_id,
                       e.name AS exercise_name,
                       e.category AS category,
                       e.equipment_type AS equipment_type,
                       e.sets AS sets,
                       e.rep_range_low AS rep_range_low,
                       e.rep_range_high AS rep_range_high,
                       d.name AS day_name
                FROM exercises e
                JOIN program_days d ON d.id = e.program_day_id
                WHERE e.id = ?
                  AND d.program_id = ?
                LIMIT 1
                """,
                (exercise_id, int(program["id"])),
            )
            if not row:
                return None

            await self._snapshot_exercise_logs(
                conn,
                exercise_id=int(row["exercise_id"]),
                user_id=normalized,
                exercise_name=str(row["exercise_name"]),
                category=str(row["category"] or ""),
                equipment_type=str(row["equipment_type"] or ""),
            )
            sets = int(new_sets) if new_sets is not None else int(row["sets"] or 1)
            rep_low = new_rep_low if new_rep_low is not None else row["rep_range_low"]
            rep_high = new_rep_high if new_rep_high is not None else row["rep_range_high"]
            await conn.execute(
                """
                UPDATE exercises
                SET name = ?, category = ?, equipment_type = ?, sets = ?, rep_range_low = ?, rep_range_high = ?
                WHERE id = ?
                """,
                (new_name, new_category, new_equipment_type, sets, rep_low, rep_high, exercise_id),
            )
            await conn.commit()
        return {
            "exercise_id": int(row["exercise_id"]),
            "old_name": str(row["exercise_name"]),
            "new_name": new_name,
            "day_name": str(row["day_name"]),
            "old_sets": int(row["sets"] or 1),
            "old_rep_low": None if row["rep_range_low"] is None else int(row["rep_range_low"]),
            "old_rep_high": None if row["rep_range_high"] is None else int(row["rep_range_high"]),
            "new_sets": sets,
            "new_rep_low": None if rep_low is None else int(rep_low),
            "new_rep_high": None if rep_high is None else int(rep_high),
            "category": new_category,
            "equipment_type": new_equipment_type,
        }

    async def remove_exercise_from_active_program_by_id(
        self,
        exercise_id: int,
        *,
        user_id: Optional[str | int] = None,
    ) -> Optional[dict[str, Any]]:
        program = await self.get_active_program(user_id)
        if not program:
            return None
        async with self.connect() as conn:
            row = await self._fetchone(
                conn,
                """
                SELECT e.id AS exercise_id,
                       e.name AS exercise_name,
                       e.display_order AS display_order,
                       e.program_day_id AS program_day_id,
                       d.name AS day_name
                FROM exercises e
                JOIN program_days d ON d.id = e.program_day_id
                WHERE e.id = ?
                  AND d.program_id = ?
                LIMIT 1
                """,
                (exercise_id, int(program["id"])),
            )
            if not row:
                return None

            source_day_id = int(row["program_day_id"])
            archive_day_id = await self._get_or_create_archive_day(conn, program_id=int(program["id"]))
            archive_order_row = await self._fetchone(
                conn,
                "SELECT COALESCE(MAX(display_order), -1) AS max_display_order FROM exercises WHERE program_day_id = ?",
                (archive_day_id,),
            )
            archive_order = int(archive_order_row["max_display_order"] or -1) + 1 if archive_order_row else 0
            await conn.execute(
                "UPDATE exercises SET program_day_id = ?, display_order = ? WHERE id = ?",
                (archive_day_id, archive_order, exercise_id),
            )

            remaining = await self._fetchall(
                conn,
                """
                SELECT id
                FROM exercises
                WHERE program_day_id = ?
                ORDER BY display_order, id
                """,
                (source_day_id,),
            )
            for idx, exercise_row in enumerate(remaining):
                await conn.execute(
                    "UPDATE exercises SET display_order = ? WHERE id = ?",
                    (idx, int(exercise_row["id"])),
                )
            await conn.commit()
        return {
            "exercise_id": int(row["exercise_id"]),
            "exercise_name": str(row["exercise_name"]),
            "day_name": str(row["day_name"]),
        }

    async def add_exercise_to_program_day(
        self,
        day_id: int,
        *,
        user_id: Optional[str | int] = None,
        name: str,
        sets: int,
        rep_low: Optional[int],
        rep_high: Optional[int],
        category: str,
        equipment_type: str,
        notes: str = "",
        muscle_groups: str = "",
    ) -> Optional[dict[str, Any]]:
        program = await self.get_active_program(user_id)
        if not program:
            return None
        async with self.connect() as conn:
            day = await self._fetchone(
                conn,
                """
                SELECT id, name
                FROM program_days
                WHERE id = ?
                  AND program_id = ?
                LIMIT 1
                """,
                (day_id, int(program["id"])),
            )
            if not day or str(day["name"]) == ARCHIVE_DAY_NAME:
                return None
            order_row = await self._fetchone(
                conn,
                "SELECT COALESCE(MAX(display_order), -1) AS max_display_order FROM exercises WHERE program_day_id = ?",
                (day_id,),
            )
            next_order = int(order_row["max_display_order"] or -1) + 1 if order_row else 0
            cursor = await conn.execute(
                """
                INSERT INTO exercises (
                    program_day_id, name, display_order, sets, rep_range_low, rep_range_high,
                    category, equipment_type, superset_group, notes, muscle_groups
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                """,
                (day_id, name, next_order, max(1, int(sets)), rep_low, rep_high, category, equipment_type, notes, muscle_groups),
            )
            await conn.commit()
        return {
            "exercise_id": int(cursor.lastrowid),
            "exercise_name": name,
            "day_name": str(day["name"]),
            "display_order": next_order,
            "sets": max(1, int(sets)),
            "rep_range_low": rep_low,
            "rep_range_high": rep_high,
            "category": category,
            "equipment_type": equipment_type,
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
