import asyncio

import httpx


class WebhookDeliveryError(Exception):
    """Raised when webhook delivery fails."""


class WebhookSender:
    def __init__(self, timeout_seconds: float) -> None:
        self._timeout = timeout_seconds

    async def send(self, url: str, payload: dict) -> None:
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await asyncio.wait_for(
                    client.post(
                        url,
                        json=payload,
                        headers={"X-Webhook-ID": str(payload["payment_id"])},
                    ),
                    timeout=self._timeout,
                )
                response.raise_for_status()
        except (TimeoutError, httpx.HTTPError) as exc:
            raise WebhookDeliveryError(str(exc) or "total webhook timeout exceeded") from exc
