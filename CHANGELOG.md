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
