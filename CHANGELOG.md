## [0.6.4] - 2026-03-17
### Fixed
- Increased the `#coach` response cap so longer coaching replies are less likely to be cut off by the LLM token limit before Discord message splitting runs.

## [0.6.3] - 2026-03-17
### Fixed
- `#programme` active-program messages now route through a structured LLM action layer instead of the old regex-only handlers, which fixes set/rep edits like `change 2.1 to 5x10`.
- Mixed type-correction messages such as `2.1, 2.2 are cable. 2.6 is dumbbell` now preserve per-reference assignments instead of leaking the first type across every exercise.
- `!program` and natural-language `show` requests in `#programme` now render the current program with day/exercise indices, current-day context, and visible equipment labels.

### Added
- `#programme` now supports structured active-program actions for set/rep updates, swaps, removals, additions, and conversation replies through the LLM router.
- Post-import swaps now snapshot existing exercise logs before renaming rows, so old workout history stays tied to the previous exercise name while the replacement starts fresh.

## [0.6.2] - 2026-03-16
### Fixed
- Discord long-form replies now split on paragraph and sentence boundaries with a 1900-character safety buffer, preventing silent truncation in `#coach`, `#check-in`, `#programme`, and other LLM-driven channels.
- `#programme` now recognizes `show/list/current program` requests and prints the active program from the database instead of falling back to the import prompt.
- Users can now correct imported exercise categories in `#programme` with natural language like `leg raises is bodyweight` or `smith deadlift is smith machine`, and those fixes flow through to later workout behavior.
- PR announcements now require true `heavy_barbell` SBD patterns plus non-Smith barbell equipment, which blocks false PR callouts for Smith variants and other non-qualifying lifts.
- Workout logging now accepts shorthand after the first set (`100 8`, `100 8 2`) and `same` variants (`same`, `same 6`, `same -5`) without breaking unit context.
- Rest timer updates now preview the next exercise on the final set so users can set up equipment during the rest period.

### Added
- Session-only workout overrides for equipment switching (`switch to dumbbell`, `use smith machine`) and mid-session exercise reordering (`do lateral raises next`).
- Inline workout queries for remaining exercises, current-exercise history, and PR lookups, followed by a reminder of the current set context.
- A repo-root `COMMANDS.md` user manual and a new embed-based `!help [command]` / `!commands` flow.

## [0.6.1] - 2026-03-14
### Fixed
- `#programme` import/save flow now waits properly between `save` -> name -> start-day steps, with 60-second defaults instead of auto-advancing through multiple prompts at once.
- `#programme` preview/edit flow now uses the LLM to parse pasted programs and apply requested edits while preserving the pasted structure and user-confirmed exercise types.
- Imported exercise types are now stored on the exercise row and reused during workouts, preventing lifts like `Dumbbell Bench Press` from being re-labeled from name/category heuristics later.
- PR announcements are now restricted to SBD lifts and direct barbell or Smith variations, while all exercises still continue writing e1RM history to `personal_records`.
- First benchmark and PR callouts now include the e1RM consistently in both workout-channel and `#prs` announcements.
- Discord text message splitting now enforces the real 2000-character limit globally before every send instead of using the old 4000-character threshold.

### Added
- Per-user program display IDs are now persisted in the database and shown in import/update/revert confirmations.
- Startup changelog announcements now post the latest release section with a `latest changes` header, so version bumps are surfaced clearly in the changelog channel.
- Regression coverage for stored equipment types, SBD-only PR announcement filtering, and Discord message splitting.

## [0.6.0] - 2026-03-13
### Fixed
- `#programme` now runs as a passive import flow by default: it echoes the pasted program by day, classifies exercise types, and asks for explicit `save`/edit confirmation.
- Removed unsolicited rewrite/swap behavior in `#programme`; pending program edits now apply only explicit user commands (`swap`, `remove`, `add`, global smith conversion).
- Program preview output is sent day-by-day and chunked at smaller size to avoid Discord cutoffs in long templates.
- `save` flow now asks for program name first, imports, then asks start day; pending flow state is closed cleanly after import/day selection.
- Travel/vacation flow now generates a temporary draft, supports follow-up equipment context, and saves as a temporary program with expiry and parent linkage.
- Added expiry-revert notification in `#programme` when an expired temporary program is auto-reverted.
- PR baseline bug fixed: PR comparison now uses `personal_records` only (no workout-log fallback), so first benchmark sets are correctly written and announced.
- PR chain logging expanded with explicit comparison logs (`exercise`, `new_e1rm`, `existing_best`, `is_new_pr`) and insert success logs.
- PR announcement scope updated for compounds: `heavy_barbell`, `light_barbell`, `smith_machine`, and bodyweight compounds only.
- Startup now warns when no PR channel ID is configured.

### Added
- New `#coach` channel support (`COACH_CHANNEL_ID`) with a dedicated coaching persona and per-user/channel memory.
- `#coach` supports `import this` handoff into the `#programme` save flow.
- Config alias support for `PR_CHANNEL_ID` in addition to `PRS_CHANNEL_ID`.

## [0.5.0] - 2026-03-12
### Fixed
- Travel/vacation edits in `#programme` now follow discuss -> confirm -> save and persist to the database as temporary programs (`temporary=1`, `parent_program_id`, `expires_at`).
- Travel reversion flow now works both automatically on expiry and immediately via `"im back"` in `#programme`.
- PR diagnostics were expanded: startup now logs configured PR channel/cog state, per-set PR checks log full decision details, and PR channel delivery now logs resolution results.
- PR channel resolution is now more reliable by falling back to `fetch_channel(...)` when cache lookup misses.
- PR posts in `#prs` now mention the actual user (`<@user_id>`) for both first benchmarks and improvements.
- `!travel` now uses the same pending programme review flow as message-based travel requests, preventing unsaved or inconsistent travel edits.
- Changelog startup posts now render real newlines instead of literal `\\n`.

### Added
- Multi-user database support across state, programs, logs, activities, injuries, PRs, cues, and sessions via `user_id`.
- Multi-user migration helpers to backfill existing data to a legacy user and initialize per-user state automatically.
- `Database.list_user_ids()` and per-user weekly check-in auto-posting so proactive summaries are generated independently per user.

## [0.4.1] - 2026-03-12
### Fixed
- `#programme` review output is now sent as per-day messages, preventing long swap reviews from being cut off mid-sentence.
- Programme flow now guards against stale post-save LLM replies by serializing per-user handling, tracking flow IDs, and canceling pending review tasks when flow state closes.
- Post-import day selection now supports implicit replies like `3` and `legs` in addition to `start on day 3`, and writes `current_day_index` with explicit info logging.
- First-ever compound/bodyweight benchmarks now announce in workout flow and publish to `#prs` as benchmark entries.
- Bodyweight exercise validation now rejects numeric load entries (for example `225 x 10`) and accepts reps-only input (`10`) as `bw x 10`.
- `!setday` and `!skipday` now end active workout sessions cleanly before changing day index.
- `!reset` now clears in-memory conversation/programme flow state across cogs to prevent stale context after a data wipe.
- Activity muscle-group assignment now uses a deterministic lookup map for common activities (for example climbing/boxing/soccer) instead of unreliable free-form guessing.
- Activity detection now recognizes common activity keywords even without duration/intensity and logs with sane defaults, prompting for details afterward.

### Added
- Workout message-edit support: edited set messages now update the original `workout_logs` row, re-check PRs, and confirm with an `✏️ Updated` message.
- In-memory mapping from Discord `message_id` to logged set metadata for edit-time correction.
- Startup fresh-DB runtime-state cleanup hook to avoid carrying stale in-memory context when no active program exists.

## [0.3.0] - 2026-03-12
### Fixed
- Relaxed prompt guardrails so fitness-adjacent requests (equipment swaps, nutrition/recovery topics) are answered normally while still declining prompt-injection attempts.
- `#programme` no longer auto-imports pasted programs; it now stages draft analysis, suggests swaps, and imports only after user confirmation (`save`).
- `#programme` now clears pending/import flow state correctly after save + day selection to prevent duplicate re-import loops.
- `#programme` day selection handling is now separated from edit flow so `start on legs` sets `current_day_index` instead of mutating program structure.
- Duplicate import guard added: same program name imported within 5 minutes is blocked.
- Pre-import review now includes extracted exercise categories and applies stricter Smith-machine swap guidance (barbell categories only).
- Parser now handles complex schemes like `3x(8, 5, 12)` and `3x(5+15)` so those exercises are not silently dropped.
- Increased `#ask` response budget (`max_tokens=300`) to avoid cut-off replies.
- `#ask` gating tightened: bot only answers mentions/replies or fitness-related questions.
- Added `#ask` output safety filter to block off-topic recipe/code-style outputs from being sent.
- Fixed early-exit `move on` loop by normalizing input, accepting broader move-on variants, and correcting decision-branch handling.
- Added early-exit timeout handling (60s) that auto-resumes instead of leaving users stuck.
- Improved early-exit copy for zero-completion sessions (`No exercises completed...`) and cleaned up confirmation flow.
- Added first-benchmark announcement for first-ever compound/bodyweight logs (for example: first Squat benchmark message).
- Activity impact now surfaces at session start and influences overlapping exercise suggestions in the next 72 hours.
- Readiness score now directly affects suggestions and messaging (high/normal/low/very low paths).
- `#activity` no longer logs every message; it now logs only true activity reports and handles injury reports separately.
- Injury reports are now stored as injuries and surfaced as skip/substitute warnings during workouts.
- Fixed changelog startup post newline rendering (`\\n` literals no longer shown).
- Message splitting is now enforced globally: all text sends route through splitter utility.

### Added
- Startup changelog auto-post support with `CHANGELOG_CHANNEL_ID` and version-change dedupe via local state file.
- New `restart.sh` script that initializes Conda correctly in non-interactive shells before launching the bot.
- Admin/debug commands: `!reset`, `!deleteprogram`, `!setday`, `!debug` (owner/admin role restricted).
- Workout day jump commands: `!skipday` and `!goto`.

## [0.2.0] - 2026-03-11
### Fixed
- Input parser now accepts natural language ("i hit 225 x 3")
- Unit memory persists within a session per exercise
- Program parsing validates day count and rep ranges
- Session no longer auto-advances to next day
- Warm-up suggestions now display for barbell compounds
- PR announcements suppressed for first-ever sets
- Natural language messages forwarded to LLM during workouts
- Double message race condition fixed with async lock
- Rest timer now fires consistently after every set
- Program naming extracted from user context

### Added
- Bodyweight input support (bw x 10, bw+25 x 8, bw-40 x 10)
- Exercise category fallback lookup table

## [0.2.1] - 2026-03-11
### Fixed
- Workout start is now enforced to the current weekday channel based on user timezone
- Early session stop flow now asks for confirmation and supports `resume` or `move on`

### Added
- `user_state.timezone` support with migration-safe initialization
- `!timezone` command to view/set timezone (IANA format)

## [0.2.2] - 2026-03-12
### Fixed
- Strict set-count enforcement now prevents extra sets beyond programmed amount and advances correctly
- Exercise transitions now require full exercise presentation before accepting set logs
- Set parsing now accepts trailing text (for example: `225 x 4 im tired`) and logs fatigue cues
- Workout message priority now parses sets before LLM routing to avoid unsolicited long coaching replies
- PR announcements in workout flow are now short inline messages and only announced for compound/bodyweight categories
- Parser now preserves distinct exercise variants (for example `Pause Squat`) and repairs duplicate-name collisions
- `#programme` now responds to post-import follow-ups including start-day selection and basic edit intents
- `#activity` now reports log date and recovery impact guidance for upcoming sessions
- `#check-in` now responds to conversational readiness signals and adjusts readiness

### Added
- Post-import `#programme` context window (~5 minutes) for follow-up commands like `start on Legs`
- Dedicated utility command support with `SETTINGS_CHANNEL_ID` and tips when commands are run outside `#settings`
- `!help`, `!version`, and `!startday` commands

## [0.2.3] - 2026-03-12
### Fixed
- Added anti-injection guardrails as the first line in all core LLM system prompts and guarded programme-edit/travel prompts
- `#ask` is now concise (2-3 sentences, `max_tokens=150`) and only responds to mentions, questions, or replies to the bot
- `#ask` and `#programme` now keep short per-user per-channel conversation memory (TTL 30 minutes)
- `#programme` now handles follow-up conversation reliably instead of dropping non-import messages
- Added safe Discord message splitting to avoid 4000-character API errors on long LLM output
- Workout set logging now rejects unrealistic inputs (`>1500 lbs` / `>700 kg` / `>200 reps`)
- Input parser now strips commas (`10,000 x 10`) and extracts set patterns while preserving trailing notes
- Bodyweight entries are now blocked for non-bodyweight exercises with a clear correction message
- `bw+N` bodyweight logging now stores `weight=N` for proper e1RM/PR calculations; plain `bw` remains baseline with no e1RM
- `!plates` now works globally and defaults to the user’s configured unit when unit is omitted
- `!e1rm` now supports case-insensitive and partial matching through canonical exercise resolution
- CSV export now rounds numeric fields to 1 decimal place and filters unrealistic outlier weights
- Early-exit flow no longer repeats motivational text, and `move on` now confirms the exact next day
- Workout suggestions now account for overlapping high-intensity activities in the last 72h (10% suggestion reduction)
- PR checks now include debug tracing and fallback comparison against workout history when PR table baseline is missing
- Program parser now sends required exercise constraints to the LLM, validates against extracted names, and aggressively repairs/falls back on mismatch
- Added parser regression test to ensure `Pause Squat` stays distinct from `Squat` with the correct `2x5` scheme
- `#check-in` now has stronger channel-ID fallback handling, conversational replies with richer context, and explicit `!summary`

### Added
- New utilities: `utils/discord_messages.py`, `utils/conversation_memory.py`, `utils/numbers.py`
- New parser regression test: `tests/test_program_parser.py`

### Docs
- Documented that utility commands (`!timezone`, `!volume`, `!e1rm`, `!export`, `!cue`, `!plates`, `!help`) are global
- Clarified that workout interactions remain restricted to workout day channels
