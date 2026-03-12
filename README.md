# PT-LLM Bot

Personal trainer Discord bot with SQLite + Ollama.

## Features

- Program import from `#programme` with LLM parsing
- Sequential day tracking independent of weekday channels
- Guided workout flow in `#mon`-`#sun`
- Set logging (`weight x reps`), rest timers, and superset handling
- Progressive suggestions, warm-up suggestions, and saved cues
- PR detection + announcement routing to `#prs`
- Activity logging in `#activity`
- Weekly check-in summaries in `#check-in`
- Free coaching Q&A in `#ask`
- Volume, e1RM, plate calculator, streak tracking, CSV export

## Project Layout

```text
bot.py
cogs/
  programme.py
  workout.py
  activity.py
  checkin.py
  ask.py
  prs.py
db/
  database.py
  models.py
  schema.sql
llm/
  client.py
  prompts.py
  parser.py
utils/
  progression.py
  warmup.py
  input_parser.py
  plates.py
  e1rm.py
  volume.py
  streaks.py
  export.py
  formatters.py
config.py
requirements.txt
```

## Setup

1. Create and activate a Python 3.11 environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Install Ollama and pull a model:

```bash
ollama pull mistral
```

4. Create `.env`:

```env
DISCORD_TOKEN=your_bot_token
GUILD_ID=optional_guild_id
DATABASE_PATH=db/pt_llm_bot.sqlite3
OLLAMA_BASE_URL=http://127.0.0.1:11434
OLLAMA_MODEL=mistral
COMMAND_PREFIX=!

PROGRAMME_CHANNEL_ID=
ACTIVITY_CHANNEL_ID=
CHECKIN_CHANNEL_ID=
ASK_CHANNEL_ID=
PRS_CHANNEL_ID=
SETTINGS_CHANNEL_ID=
CHANGELOG_CHANNEL_ID=
ADMIN_ROLE_ID=
WORKOUT_CHANNEL_IDS=111,222,333,444,555,666,777
```

If channel IDs are not set, the bot falls back to channel names (`programme`, `activity`, `check-in`, `ask`, `prs`, `settings`, and weekday names `mon..sun`).

5. Run:

```bash
python3 bot.py
```

## Commands

- `!import <program text>` (in `#programme`, now stages suggestions; reply `save` to import)
- `!program`
- `!startday <day name or day number>`
- `!travel <description and optional duration>`
- `!start`, `!done` (workout channels)
- `!skipday <n|day name>`, `!goto <day name>` (workout channels)
- `!plates <weight> [lbs|kg]` (global; unit optional)
- `!e1rm <exercise>` (global; case-insensitive partial match supported)
- `!volume` (global)
- `!cue <exercise> <cue text>` (global)
- `!export [exercise]` (global)
- `!activity <description>`
- `!readiness <1-10>`
- `!phase <cut|bulk|maintain>`
- `!timezone [IANA tz name]` (global, example: `America/New_York`)
- `!checkin`, `!summary` (check-in channel)
- `!ask <question>`
- `!prs [days]`
- Admin: `!debug`, `!setday <n>`, `!reset`, `!deleteprogram`
- `!help`, `!version`

## Notes

- Program parsing and coaching rely on Ollama availability.
- Core logging math (progression, e1RM, warm-up, plates) runs without LLM.
- The database is initialized automatically on startup.
- Utility commands work in any channel; workout interactions (`ready`, set logging, `skip rest`) stay restricted to workout channels.
- On startup, the bot can post the latest changelog section to `#changelog` when a new version is detected.
