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
