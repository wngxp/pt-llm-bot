from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(slots=True)
class _MessageItem:
    role: str
    content: str
    created_at: datetime


class ConversationMemory:
    def __init__(self, *, max_messages: int = 10, ttl_minutes: int = 30) -> None:
        self.max_messages = max_messages
        self.ttl = timedelta(minutes=ttl_minutes)
        self._store: dict[tuple[int, int], deque[_MessageItem]] = {}

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _cleanup_key(self, key: tuple[int, int]) -> None:
        queue = self._store.get(key)
        if not queue:
            return
        cutoff = self._now() - self.ttl
        while queue and queue[0].created_at < cutoff:
            queue.popleft()
        if not queue:
            self._store.pop(key, None)

    def append(self, *, user_id: int, channel_id: int, role: str, content: str) -> None:
        key = (int(user_id), int(channel_id))
        self._cleanup_key(key)
        queue = self._store.get(key)
        if queue is None:
            queue = deque(maxlen=self.max_messages)
            self._store[key] = queue
        queue.append(_MessageItem(role=role, content=content, created_at=self._now()))

    def get(self, *, user_id: int, channel_id: int) -> list[dict[str, Any]]:
        key = (int(user_id), int(channel_id))
        self._cleanup_key(key)
        queue = self._store.get(key)
        if not queue:
            return []
        return [{"role": item.role, "content": item.content} for item in queue]

    def clear(self, *, user_id: int, channel_id: int) -> None:
        self._store.pop((int(user_id), int(channel_id)), None)

    def clear_all(self) -> None:
        self._store.clear()
