import asyncio

import httpx
import pytest

from app.services.webhook import WebhookDeliveryError, WebhookSender


async def test_webhook_has_stable_deduplication_header(monkeypatch):
    requests = []

    async def handler(request):
        requests.append(request)
        return httpx.Response(200)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    monkeypatch.setattr("app.services.webhook.httpx.AsyncClient", lambda **_: client)
    await WebhookSender(1).send("https://example.com/hook", {"payment_id": "payment-123"})
    assert requests[0].headers["X-Webhook-ID"] == "payment-123"


async def test_total_timeout_bounds_slow_http_operation(monkeypatch):
    async def handler(request):
        await asyncio.sleep(10)
        return httpx.Response(200)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    monkeypatch.setattr("app.services.webhook.httpx.AsyncClient", lambda **_: client)
    with pytest.raises(WebhookDeliveryError, match="total webhook timeout"):
        await asyncio.wait_for(
            WebhookSender(0.01).send(
                "https://example.com/hook",
                {"payment_id": "payment-123"},
            ),
            timeout=1,
        )
