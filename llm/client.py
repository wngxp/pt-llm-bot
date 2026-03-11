from __future__ import annotations

import json
from typing import Any, Optional

import httpx


class OllamaClient:
    def __init__(self, base_url: str, model: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=5.0))

    async def close(self) -> None:
        await self._client.aclose()

    async def chat(
        self,
        *,
        system: str,
        user: str,
        temperature: float = 0.2,
        response_format: Optional[dict[str, Any]] = None,
    ) -> str:
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "stream": False,
            "options": {
                "temperature": temperature,
            },
        }
        if response_format:
            payload["format"] = response_format

        response = await self._client.post(f"{self.base_url}/api/chat", json=payload)
        response.raise_for_status()
        data = response.json()
        message = data.get("message") or {}
        return str(message.get("content", "")).strip()

    async def chat_json(
        self,
        *,
        system: str,
        user: str,
        temperature: float = 0.1,
    ) -> Any:
        text = await self.chat(system=system, user=user, temperature=temperature)
        cleaned = self._extract_json_block(text)
        return json.loads(cleaned)

    @staticmethod
    def _extract_json_block(text: str) -> str:
        stripped = text.strip()
        if stripped.startswith("```"):
            stripped = stripped.strip("`")
            if stripped.startswith("json"):
                stripped = stripped[4:]
            stripped = stripped.strip()

        start = stripped.find("{")
        end = stripped.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("LLM response did not contain valid JSON object")
        return stripped[start : end + 1]
