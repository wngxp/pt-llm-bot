# PT-LLM User Manual

## Hard Commands

| Command | Syntax | Description |
| --- | --- | --- |
| `!help` / `!commands` | `!help [command]` | Show the command list or detailed help for one command. |
| `!version` | `!version` | Show the current bot version. |
| `!ask` | `!ask <question>` | Ask a fitness question in command form. |
| `!start` | `!start` | Start today's workout session in the current weekday workout channel. |
| `!done` | `!done` | Finish the current workout session immediately. |
| `!skipday` | `!skipday <day name or number>` | Skip ahead to another program day. |
| `!goto` | `!goto <day name or number>` | Jump to another program day. |
| `!plates` | `!plates <weight> [kg|lbs]` | Show a plate breakdown for a target weight. |
| `!e1rm` | `!e1rm <exercise>` | Show estimated 1RM history for an exercise. |
| `!volume` | `!volume` | Show weekly volume by muscle group. |
| `!cue` | `!cue <exercise> <text>` | Save a personal form cue for an exercise. |
| `!export` | `!export [exercise]` | Export workout logs as CSV. |
| `!activity` | `!activity <description>` | Log an activity with recovery impact tracking. |
| `!readiness` | `!readiness <1-10>` | Set your readiness score. |
| `!phase` | `!phase <cut|bulk|maintain>` | Set your current training phase. |
| `!timezone` | `!timezone <tz>` | Set your timezone, for example `Asia/Shanghai`. |
| `!import` | `!import <program text>` | Start a programme import preview in `#programme`. |
| `!program` | `!program` | Show your active program with full day-by-day exercise details. |
| `!startday` | `!startday <day name or number>` | Set which day to start from after importing or editing a program. |
| `!travel` | `!travel <constraints>` | Draft a temporary travel version of your active program. |
| `!backfill` | `!backfill week <W> day <D>` or `!backfill yesterday` | Backfill a missed training day into the current program timeline. |
| `!startdate` | `!startdate <YYYY-MM-DD>` | Set the program start date used for week/day backfill mapping. |
| `!checkin` | `!checkin` | Generate a weekly check-in summary in `#check-in`. |
| `!summary` | `!summary` | Generate the weekly summary in `#check-in`. |
| `!prs` | `!prs [days]` | Show recently recorded PR entries. |
| `!debug` | `!debug` | Show current bot state. Admin only. |
| `!setday` | `!setday <n>` | Force the current day index. Admin only. |
| `!reset` | `!reset confirm` | Reset workout data after confirmation. Admin only. |
| `!deleteprogram` | `!deleteprogram confirm` | Delete the active program after confirmation. Admin only. |

## Soft Commands

### `#programme`
- `show my program`, `list program`, `current program`: display the active program from the database.
- Paste a full program: preview it exactly as parsed before importing.
- `save`: confirm the pending import.
- `swap X with Y on pull day`: replace one exercise in the active program.
- `change X to Y`: rename a day or exercise in the active program.
- `leg raises is bodyweight`, `change leg raises to bodyweight`: fix an exercise category/classification.
- `i'm going on vacation for 1 week`, `I only have dumbbells`: create a temporary travel program.

### Daily workout channels (`#mon` to `#sun`)
- `ready`: start today's workout.
- `225 x 5`, `100x8`, `bw x 10`, `bw+25 x 8`: log a set.
- `100 8`, `100 8 2`: shorthand set logging after the first set establishes context.
- `same`, `same 6`, `same -5`: repeat the last set, change reps, or drop the weight.
- `switch to dumbbell`, `use smith machine`, `do this on cable`: temporary equipment switch for the current exercise only.
- `skip`: skip the current exercise and move on.
- `skip rest`: skip the active rest timer.
- `do lateral raises next`, `skip to triceps`: reorder the remaining exercise queue.
- `what exercises do I have left?`: list the remaining exercises.
- `history`, `show history`: show the last 5 sessions for the current exercise.
- `what's my PR for bench press?`: show the best logged PR for an exercise inline.
- `done`, `end workout`, `i'm done`: trigger the early-end workout flow.

### `#coach`
- Ask for training advice, substitutions, periodization changes, or full program design.
- `import this`, `use this`: hand a coach-built program off to `#programme`.

### `#check-in`
- Weekly check-in summaries and context-aware feedback.
- `@PT-LLM generate weekly summary`: explicit summary request.

### `#activity`
- Describe an activity in plain language to log it with recovery impact.

### `#ask`
- Ask fitness questions naturally when the message is a question or you mention the bot.

## Notes
- Long bot replies are automatically split into multiple Discord messages before hitting the character limit.
- PR announcements are limited to qualifying heavy barbell squat, bench, and deadlift variations.
- Equipment switches and exercise reordering during a workout are session-only. They do not rewrite the saved program.
- `!backfill` and `!startdate` work in workout channels as well as `#commands`, `#settings`, and `#programme`.
