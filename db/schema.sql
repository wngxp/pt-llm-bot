PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS programs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    display_id INTEGER,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    active BOOLEAN DEFAULT 1,
    temporary BOOLEAN DEFAULT 0,
    parent_program_id INTEGER,
    expires_at DATE,
    FOREIGN KEY (parent_program_id) REFERENCES programs(id)
);

CREATE TABLE IF NOT EXISTS program_days (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    program_id INTEGER NOT NULL,
    day_order INTEGER NOT NULL,
    name TEXT NOT NULL,
    block TEXT,
    week INTEGER,
    day_number INTEGER,
    is_rest_day BOOLEAN DEFAULT 0,
    FOREIGN KEY (program_id) REFERENCES programs(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_program_days_program_day_order
ON program_days(program_id, day_order);

CREATE TABLE IF NOT EXISTS exercises (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    program_day_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    display_order INTEGER NOT NULL,
    sets INTEGER NOT NULL,
    rep_range_low INTEGER,
    rep_range_high INTEGER,
    category TEXT DEFAULT 'cable_machine',
    equipment_type TEXT DEFAULT 'unknown',
    superset_group INTEGER,
    technique TEXT,
    warmup_sets_low INTEGER,
    warmup_sets_high INTEGER,
    early_rpe_low INTEGER,
    early_rpe_high INTEGER,
    last_rpe_low INTEGER,
    last_rpe_high INTEGER,
    rest_low INTEGER,
    rest_high INTEGER,
    sub1 TEXT,
    sub2 TEXT,
    notes TEXT,
    muscle_groups TEXT,
    FOREIGN KEY (program_day_id) REFERENCES program_days(id)
);

CREATE INDEX IF NOT EXISTS idx_exercises_program_day_display
ON exercises(program_day_id, display_order);

CREATE TABLE IF NOT EXISTS workout_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    exercise_id INTEGER NOT NULL,
    performed_exercise_name TEXT,
    performed_category TEXT,
    performed_equipment_type TEXT,
    date DATE NOT NULL,
    set_number INTEGER NOT NULL,
    weight REAL NOT NULL,
    reps INTEGER NOT NULL,
    unit TEXT DEFAULT 'lbs',
    rir INTEGER,
    notes TEXT,
    FOREIGN KEY (exercise_id) REFERENCES exercises(id)
);

CREATE INDEX IF NOT EXISTS idx_workout_logs_exercise_date
ON workout_logs(exercise_id, date);

CREATE TABLE IF NOT EXISTS activity_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    date DATE NOT NULL,
    activity_type TEXT NOT NULL,
    description TEXT,
    intensity TEXT,
    muscle_groups TEXT
);

CREATE TABLE IF NOT EXISTS injuries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    date DATE NOT NULL,
    description TEXT NOT NULL,
    muscle_groups TEXT,
    severity TEXT DEFAULT 'moderate',
    active BOOLEAN DEFAULT 1,
    resolved_date DATE
);

CREATE INDEX IF NOT EXISTS idx_injuries_active
ON injuries(active, date);

CREATE TABLE IF NOT EXISTS personal_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    exercise_name TEXT NOT NULL,
    weight REAL NOT NULL,
    reps INTEGER NOT NULL,
    unit TEXT DEFAULT 'lbs',
    estimated_1rm REAL,
    date DATE NOT NULL,
    workout_log_id INTEGER,
    FOREIGN KEY (workout_log_id) REFERENCES workout_logs(id)
);

CREATE INDEX IF NOT EXISTS idx_personal_records_exercise
ON personal_records(exercise_name);

CREATE TABLE IF NOT EXISTS exercise_cues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    exercise_name TEXT NOT NULL,
    cue TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL UNIQUE,
    current_program_id INTEGER,
    current_day_index INTEGER DEFAULT 0,
    phase TEXT DEFAULT 'maintain',
    default_unit TEXT DEFAULT 'lbs',
    timezone TEXT DEFAULT 'UTC',
    current_block TEXT,
    current_week INTEGER,
    current_day_number INTEGER,
    program_start_date TEXT,
    readiness INTEGER DEFAULT 7,
    weeks_since_deload INTEGER DEFAULT 0,
    current_streak INTEGER DEFAULT 0,
    longest_streak INTEGER DEFAULT 0,
    last_workout_date DATE,
    last_checkin_date DATE,
    FOREIGN KEY (current_program_id) REFERENCES programs(id)
);

CREATE TABLE IF NOT EXISTS workout_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    session_date DATE NOT NULL,
    channel_id INTEGER NOT NULL,
    day_id INTEGER NOT NULL,
    completed BOOLEAN DEFAULT 0,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    FOREIGN KEY (day_id) REFERENCES program_days(id)
);

CREATE INDEX IF NOT EXISTS idx_workout_sessions_date_channel
ON workout_sessions(session_date, channel_id);
