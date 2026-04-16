import os
from typing import Any

import httpx


class SlackError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(f"Slack API error {status_code}: {message}")


class SlackClient:
    def __init__(self) -> None:
        token = os.environ.get("SLACK_BOT_TOKEN", "")
        if not token:
            raise RuntimeError("SLACK_BOT_TOKEN environment variable is not set")

        self._client = httpx.AsyncClient(
            base_url="https://slack.com/api",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = await self._client.request(method, path, **kwargs)
        if not response.is_success:
            raise SlackError(response.status_code, response.text)
        data = response.json()
        if not data.get("ok"):
            raise SlackError(response.status_code, data.get("error", "unknown error"))
        return data

    async def send_message(self, channel: str, text: str) -> Any:
        return await self._request(
            "POST",
            "/chat.postMessage",
            json={"channel": channel, "text": text},
        )

    async def send_formatted_message(
        self, channel: str, blocks: list[dict[str, Any]], text: str
    ) -> Any:
        return await self._request(
            "POST",
            "/chat.postMessage",
            json={"channel": channel, "blocks": blocks, "text": text},
        )

    async def aclose(self) -> None:
        await self._client.aclose()
