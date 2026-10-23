import asyncio
from typing import Any, Protocol

import httpx


class WebhookDeliveryError(Exception):
    """The receiver did not confirm delivery within the request deadline."""


class WebhookClient(Protocol):
    async def send(self, url: str, payload: dict[str, Any]) -> None: ...


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient, timeout_seconds: float) -> None:
        self._client = client
        self._timeout = timeout_seconds

    async def send(self, url: str, payload: dict[str, Any]) -> None:
        try:
            async with asyncio.timeout(self._timeout):
                response = await self._client.post(
                    url,
                    json=payload,
                    headers={"X-Webhook-ID": str(payload["payment_id"])},
                )
                response.raise_for_status()
        except (httpx.HTTPError, TimeoutError) as exc:
            raise WebhookDeliveryError(str(exc) or "total webhook timeout exceeded") from exc
