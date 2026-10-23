import asyncio
import json
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest

from app.contracts import PaymentWebhookPayload
from app.ports import WebhookDeliveryError
from app.services.webhook import WebhookSender


@pytest.fixture
def payload():
    return PaymentWebhookPayload(
        payment_id=uuid4(),
        status="succeeded",
        amount="10.00",
        currency="RUB",
        description="order",
        metadata={"order_id": 123},
        processed_at=datetime.now(UTC),
    )


async def test_webhook_has_stable_deduplication_header_and_json_payload(payload):
    requests = []

    async def handler(request):
        requests.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sender = WebhookSender(client, 1)
        await sender.send("https://example.com/hook", payload)
        await sender.send("https://example.com/hook", payload)
    assert all(request.headers["X-Webhook-ID"] == str(payload.payment_id) for request in requests)
    assert json.loads(requests[0].content) == payload.model_dump(mode="json")
    assert requests[0].content == requests[1].content


async def test_total_timeout_bounds_slow_http_operation(payload):
    async def handler(request):
        await asyncio.sleep(10)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(WebhookDeliveryError, match="total webhook timeout"):
            await asyncio.wait_for(
                WebhookSender(client, 0.01).send("https://example.com/hook", payload),
                timeout=1,
            )


@pytest.mark.parametrize("failure", [httpx.ConnectError("offline"), httpx.Response(503)])
async def test_http_failures_follow_webhook_error_contract(payload, failure):
    def handler(request):
        if isinstance(failure, Exception):
            raise failure
        return failure

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(WebhookDeliveryError):
            await WebhookSender(client, 1).send("https://example.com/hook", payload)


async def test_webhook_preserves_cancellation(payload):
    entered = asyncio.Event()

    async def handler(request):
        entered.set()
        await asyncio.Event().wait()

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        task = asyncio.create_task(
            WebhookSender(client, 1).send("https://example.com/hook", payload)
        )
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
