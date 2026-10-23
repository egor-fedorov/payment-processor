import asyncio

import httpx

from app.contracts import PaymentWebhookPayload
from app.ports import WebhookDeliveryError


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient, timeout_seconds: float) -> None:
        self._client = client
        self._timeout = timeout_seconds

    async def send(self, url: str, payload: PaymentWebhookPayload) -> None:
        try:
            async with asyncio.timeout(self._timeout):
                response = await self._client.post(
                    url,
                    json=payload.model_dump(mode="json"),
                    headers={"X-Webhook-ID": str(payload.payment_id)},
                )
                response.raise_for_status()
        except (httpx.HTTPError, TimeoutError) as exc:
            raise WebhookDeliveryError(str(exc) or "total webhook timeout exceeded") from exc
