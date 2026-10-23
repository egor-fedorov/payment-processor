import asyncio
import json
from time import monotonic
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.api.schemas import PaymentCreateRequest
from app.core.config import get_settings
from app.db.models import OutboxMessage, Payment
from app.messaging.dispatcher import OutboxDispatcher
from app.messaging.schemas import PaymentRequestedEvent
from app.messaging.topology import DLQ_QUEUE, PAYMENTS_QUEUE, retry_queue
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
            ),
            str(uuid4()),
        )
        return PaymentRequestedEvent.model_validate(
            await session.scalar(select(OutboxMessage.payload))
        )


async def receive(broker, queue):
    declared = await broker.declare_queue(queue)
    return await declared.get(fail=False)


async def test_outbox_retains_unpublished_message_and_recovers(session_factory, rabbit):
    event = await payment_event(session_factory)
    broken = AsyncMock()
    broken.publish.side_effect = ConnectionError("broker unavailable")
    dispatcher = OutboxDispatcher(session_factory, broken, get_settings())
    with pytest.raises(ConnectionError):
        await dispatcher.dispatch_batch()
    async with session_factory() as session:
        assert (await session.scalar(select(OutboxMessage))).published_at is None
    dispatcher._broker = rabbit
    assert await dispatcher.dispatch_batch() == 1
    message = await eventually(lambda: receive(rabbit, PAYMENTS_QUEUE))
    assert json.loads(message.body)["payment_id"] == str(event.payment_id)
    await message.ack()
    assert await dispatcher.dispatch_batch() == 0


async def test_retry_queues_do_not_block_shorter_delay(rabbit):
    await rabbit.publish({"delay": 2}, queue=retry_queue(2, get_settings()), persist=True)
    await rabbit.publish({"delay": 1}, queue=retry_queue(1, get_settings()), persist=True)
    first = await eventually(lambda: receive(rabbit, PAYMENTS_QUEUE))
    assert json.loads(first.body)["delay"] == 1
    await first.ack()
    second = await eventually(lambda: receive(rabbit, PAYMENTS_QUEUE))
    assert json.loads(second.body)["delay"] == 2
    await second.ack()


async def test_consumer_retries_then_dlq_and_ignores_duplicate(
    session_factory, rabbit, monkeypatch
):
    from app.consumer import Consumer
    from app.messaging.broker import create_broker

    consumer = Consumer(
        create_broker(get_settings()),
        PaymentProcessingService(session_factory, get_settings()),
        AsyncMock(),
        get_settings(),
    )

    event = await payment_event(session_factory)
    attempts = []

    async def fail(*_):
        attempts.append(monotonic())
        raise WebhookDeliveryError("HTTP 503")

    monkeypatch.setattr(
        consumer, "processor", PaymentProcessingService(session_factory, get_settings())
    )
    monkeypatch.setattr(consumer.sender, "send", fail)
    await consumer.broker.start()
    try:
        await rabbit.publish(event.model_dump(mode="json"), queue=PAYMENTS_QUEUE, persist=True)
        dlq = await eventually(lambda: receive(rabbit, DLQ_QUEUE))
        assert json.loads(dlq.body)["delivery_attempt"] == 3
        await dlq.ack()
        assert len(attempts) == 3
        assert attempts[1] - attempts[0] >= 1
        assert attempts[2] - attempts[1] >= 2

        async def terminal():
            async with session_factory() as session:
                return (await session.get(Payment, event.payment_id)).webhook_dead_lettered_at

        await eventually(terminal)
        # An old outbox copy carries zero, but cannot reset the database budget.
        await rabbit.publish(event.model_dump(mode="json"), queue=PAYMENTS_QUEUE, persist=True)
        await asyncio.sleep(0.3)
        assert len(attempts) == 3
    finally:
        await consumer.broker.stop()


async def test_transient_database_error_requeues_message(session_factory, rabbit, monkeypatch):
    from app.consumer import Consumer
    from app.messaging.broker import create_broker

    consumer = Consumer(
        create_broker(get_settings()),
        PaymentProcessingService(session_factory, get_settings()),
        AsyncMock(),
        get_settings(),
    )

    event = await payment_event(session_factory)
    service = PaymentProcessingService(session_factory, get_settings())
    claim = service.claim_delivery
    calls = 0

    async def unavailable_once(payment_id):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("database temporarily unavailable")
        return await claim(payment_id)

    service.claim_delivery = unavailable_once
    monkeypatch.setattr(consumer, "processor", service)
    send = AsyncMock()
    monkeypatch.setattr(consumer.sender, "send", send)
    await consumer.broker.start()
    try:
        await rabbit.publish(event.model_dump(mode="json"), queue=PAYMENTS_QUEUE, persist=True)

        async def delivered():
            async with session_factory() as session:
                return (await session.get(Payment, event.payment_id)).webhook_delivered_at

        await eventually(delivered)
        assert calls == 2
        send.assert_awaited_once()
    finally:
        await consumer.broker.stop()


async def test_invalid_json_is_sent_to_dlq(rabbit, session_factory):
    from app.consumer import Consumer
    from app.messaging.broker import create_broker

    consumer = Consumer(
        create_broker(get_settings()),
        PaymentProcessingService(session_factory, get_settings()),
        AsyncMock(),
        get_settings(),
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


@pytest.mark.parametrize("destination", ["retry", "dlq"])
async def test_failed_republish_is_requeued_without_resetting_attempts(
    destination,
    session_factory,
    rabbit,
    monkeypatch,
):
    from app.consumer import Consumer
    from app.messaging.broker import create_broker

    consumer = Consumer(
        create_broker(get_settings()),
        PaymentProcessingService(session_factory, get_settings()),
        AsyncMock(),
        get_settings(),
    )

    event = await payment_event(session_factory)
    service = PaymentProcessingService(session_factory, get_settings())
    if destination == "dlq":
        service._settings = service._settings.model_copy(update={"retry_max_attempts": 1})
        monkeypatch.setattr(consumer, "settings", service._settings)
    monkeypatch.setattr(consumer, "processor", service)
    sender = AsyncMock(side_effect=WebhookDeliveryError("HTTP 503"))
    monkeypatch.setattr(consumer.sender, "send", sender)
    publish = consumer.broker.publish
    failures = 0

    async def unavailable_once(*args, **kwargs):
        nonlocal failures
        if failures == 0:
            failures += 1
            raise ConnectionError("publication failed")
        return await publish(*args, **kwargs)

    monkeypatch.setattr(consumer.broker, "publish", unavailable_once)
    await consumer.broker.start()
    try:
        await rabbit.publish(event.model_dump(mode="json"), queue=PAYMENTS_QUEUE, persist=True)
        dlq = await eventually(lambda: receive(rabbit, DLQ_QUEUE))
        assert json.loads(dlq.body)["delivery_attempt"] == (1 if destination == "dlq" else 3)
        assert sender.await_count == (1 if destination == "dlq" else 3)
        await dlq.ack()
    finally:
        await consumer.broker.stop()


async def test_failure_recording_error_keeps_source_message(session_factory, rabbit, monkeypatch):
    from app.consumer import Consumer
    from app.messaging.broker import create_broker

    consumer = Consumer(
        create_broker(get_settings()),
        PaymentProcessingService(session_factory, get_settings()),
        AsyncMock(),
        get_settings(),
    )

    event = await payment_event(session_factory)
    service = PaymentProcessingService(session_factory, get_settings())
    service._settings = service._settings.model_copy(
        update={
            "webhook_lease_seconds": 0.2,
            "webhook_timeout_seconds": 0.1,
        }
    )
    record = service.record_delivery_failure
    calls = 0

    async def unavailable_once(*args):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("database unavailable while recording failure")
        return await record(*args)

    service.record_delivery_failure = unavailable_once
    monkeypatch.setattr(consumer, "processor", service)
    sender = AsyncMock(side_effect=[WebhookDeliveryError("503"), None])
    monkeypatch.setattr(consumer.sender, "send", sender)
    await consumer.broker.start()
    try:
        await rabbit.publish(event.model_dump(mode="json"), queue=PAYMENTS_QUEUE, persist=True)

        async def delivered():
            async with session_factory() as session:
                return (await session.get(Payment, event.payment_id)).webhook_delivered_at

        await eventually(delivered)
        assert sender.await_count == 2
        async with session_factory() as session:
            assert (await session.get(Payment, event.payment_id)).webhook_attempts == 2
    finally:
        await consumer.broker.stop()
