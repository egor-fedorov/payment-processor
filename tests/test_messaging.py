import asyncio
import json
from time import monotonic
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.api.schemas import PaymentCreateRequest
from app.consumer import Consumer
from app.contracts import PaymentRequestedEvent
from app.db.models import OutboxMessage, Payment
from app.messaging.broker import create_broker
from app.messaging.dispatcher import OutboxDispatcher
from app.messaging.topology import DLQ_QUEUE, PAYMENTS_QUEUE
from app.services.payments import PaymentProcessingService, PaymentService
from app.services.webhook import WebhookDeliveryError

pytestmark = pytest.mark.integration


async def eventually(check, deadline_seconds=15):
    deadline = monotonic() + deadline_seconds
    while monotonic() < deadline:
        result = await check()
        if result:
            return result
        await asyncio.sleep(0.05)
    raise AssertionError("Condition did not become true before timeout")


async def payment_event(session_factory):
    async with session_factory() as session:
        await PaymentService(session).create_payment(
            PaymentCreateRequest(
                amount="1.00",
                currency="USD",
                description="test",
                webhook_url="http://example.com/hook",
            ).to_command(),
            str(uuid4()),
        )
        return PaymentRequestedEvent.model_validate(
            await session.scalar(select(OutboxMessage.payload))
        )


async def receive(broker, queue):
    declared = await broker.declare_queue(queue)
    return await declared.get(fail=False)


async def test_outbox_retains_unpublished_message_and_recovers(
    session_factory, rabbit, settings, clock
):
    event = await payment_event(session_factory)
    clock.advance(1)
    broken = AsyncMock()
    broken.publish.side_effect = ConnectionError("broker unavailable")
    assert (
        await OutboxDispatcher(session_factory, broken, settings, clock=clock).dispatch_batch() == 0
    )
    async with session_factory() as session:
        row = await session.scalar(select(OutboxMessage))
        assert row.published_at is None
        assert row.publish_attempts == 1
        assert row.last_error
    clock.advance(1)
    dispatcher = OutboxDispatcher(session_factory, rabbit, settings, clock=clock)
    assert await dispatcher.dispatch_batch() == 1
    message = await eventually(lambda: receive(rabbit, PAYMENTS_QUEUE))
    assert json.loads(message.body)["payment_id"] == str(event.payment_id)
    await message.ack()
    assert await dispatcher.dispatch_batch() == 0


@pytest.mark.parametrize("succeed", [True, False])
async def test_consumer_retries_then_delivers_or_dlq(
    session_factory, rabbit, settings, relay, succeed
):
    sender = AsyncMock()
    attempts = []

    async def send(*_):
        attempts.append(monotonic())
        if not succeed or len(attempts) < 3:
            raise WebhookDeliveryError("HTTP 503")

    sender.send.side_effect = send
    consumer = Consumer(
        create_broker(settings),
        PaymentProcessingService(session_factory, settings),
        sender,
        settings,
    )
    await consumer.broker.start()
    try:
        event = await payment_event(session_factory)

        async def terminal():
            async with session_factory() as session:
                row = await session.get(Payment, event.payment_id)
                return row.webhook_delivered_at if succeed else row.webhook_exhausted_at

        await eventually(terminal)
        if not succeed:
            dlq = await eventually(lambda: receive(rabbit, DLQ_QUEUE))
            assert json.loads(dlq.body)["delivery_attempt"] == 3
            await dlq.ack()
        assert len(attempts) == 3
        assert attempts[1] - attempts[0] >= 1
        assert attempts[2] - attempts[1] >= 2
        await consumer.process_event(event)
        assert len(attempts) == 3
    finally:
        await consumer.broker.stop()


async def test_transient_database_error_requeues_message(session_factory, rabbit, settings, relay):
    service = PaymentProcessingService(session_factory, settings)
    claim = service.claim_delivery
    calls = 0

    async def unavailable_once(*args):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("database temporarily unavailable")
        return await claim(*args)

    service.claim_delivery = unavailable_once
    sender = AsyncMock()
    consumer = Consumer(create_broker(settings), service, sender, settings)
    await consumer.broker.start()
    try:
        event = await payment_event(session_factory)

        async def delivered():
            async with session_factory() as session:
                return (await session.get(Payment, event.payment_id)).webhook_delivered_at

        await eventually(delivered)
        assert calls == 2
        sender.send.assert_awaited_once()
    finally:
        await consumer.broker.stop()


async def test_invalid_json_is_sent_to_dlq(rabbit, session_factory, settings, relay):
    consumer = Consumer(
        create_broker(settings),
        PaymentProcessingService(session_factory, settings),
        AsyncMock(),
        settings,
    )
    await consumer.broker.start()
    try:
        await rabbit.publish(
            b"not json", queue=PAYMENTS_QUEUE, content_type="application/json", persist=True
        )
        dlq = await eventually(lambda: receive(rabbit, DLQ_QUEUE))
        payload = json.loads(dlq.body)
        assert payload["original_body_base64"] == "bm90IGpzb24="
        assert "failure_reason" in payload
        await dlq.ack()
    finally:
        await consumer.broker.stop()


async def test_failure_recording_error_recovers_through_outbox(session_factory, rabbit, settings):
    settings = settings.model_copy(
        update={"webhook_lease_seconds": 1.5, "webhook_timeout_seconds": 0.1}
    )
    service = PaymentProcessingService(session_factory, settings)
    record = service.record_delivery_failure
    calls = 0

    async def unavailable_once(*args):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("database unavailable while recording failure")
        return await record(*args)

    service.record_delivery_failure = unavailable_once
    sender = AsyncMock()
    sender.send.side_effect = [WebhookDeliveryError("503"), None]
    consumer = Consumer(create_broker(settings), service, sender, settings)
    stop = asyncio.Event()
    await consumer.broker.start()
    task = asyncio.create_task(OutboxDispatcher(session_factory, rabbit, settings).run(stop))
    try:
        event = await payment_event(session_factory)

        async def delivered():
            async with session_factory() as session:
                return (await session.get(Payment, event.payment_id)).webhook_delivered_at

        await eventually(delivered)
        assert sender.send.await_count == 2
        async with session_factory() as session:
            assert (await session.get(Payment, event.payment_id)).webhook_attempts == 2
    finally:
        stop.set()
        await task
        await consumer.broker.stop()
