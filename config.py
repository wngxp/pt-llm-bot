from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


load_dotenv()


@dataclass(slots=True)
class Settings:
    discord_token: str
    guild_id: Optional[int]
    database_path: Path
    ollama_base_url: str
    ollama_model: str
    programme_channel_id: Optional[int]
    activity_channel_id: Optional[int]
    checkin_channel_id: Optional[int]
    ask_channel_id: Optional[int]
    prs_channel_id: Optional[int]
    settings_channel_id: Optional[int]
    changelog_channel_id: Optional[int]
    admin_role_id: Optional[int]
    workout_channel_ids: set[int]
    command_prefix: str



def _opt_int(name: str) -> Optional[int]:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    return int(value)



def _int_set(name: str) -> set[int]:
    raw = os.getenv(name, "")
    values = set()
    for piece in raw.split(","):
        piece = piece.strip()
        if not piece:
            continue
        values.add(int(piece))
    return values



def load_settings() -> Settings:
    token = os.getenv("DISCORD_TOKEN", "")
    if not token:
        raise RuntimeError("DISCORD_TOKEN is required")

    return Settings(
        discord_token=token,
        guild_id=_opt_int("GUILD_ID"),
        database_path=Path(os.getenv("DATABASE_PATH", "db/pt_llm_bot.sqlite3")),
        ollama_base_url=os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434"),
        ollama_model=os.getenv("OLLAMA_MODEL", "mistral"),
        programme_channel_id=_opt_int("PROGRAMME_CHANNEL_ID"),
        activity_channel_id=_opt_int("ACTIVITY_CHANNEL_ID"),
        checkin_channel_id=_opt_int("CHECKIN_CHANNEL_ID"),
        ask_channel_id=_opt_int("ASK_CHANNEL_ID"),
        prs_channel_id=_opt_int("PRS_CHANNEL_ID"),
        settings_channel_id=_opt_int("SETTINGS_CHANNEL_ID"),
        changelog_channel_id=_opt_int("CHANGELOG_CHANNEL_ID"),
        admin_role_id=_opt_int("ADMIN_ROLE_ID"),
        workout_channel_ids=_int_set("WORKOUT_CHANNEL_IDS"),
        command_prefix=os.getenv("COMMAND_PREFIX", "!"),
    )
