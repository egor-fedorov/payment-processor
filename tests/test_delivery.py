import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select, update

from app.api.schemas import PaymentCreateRequest
from app.db.models import OutboxMessage, Payment
from app.services.payments import (
    DeliveryBusyError, DeliveryExhaustedError, DeliveryLeaseLostError,
    PaymentProcessingService, PaymentService,
)


pytestmark = pytest.mark.integration
PAYLOAD = PaymentCreateRequest(amount="10.00", currency="RUB", description="order", webhook_url="https://example.com/hook")


async def create(session_factory, key="key"):
    async with session_factory() as session:
        return await PaymentService(session).create_payment(PAYLOAD, key)


async def test_concurrent_creation_has_one_payment_and_outbox(session_factory):
    payments = await asyncio.gather(*(create(session_factory) for _ in range(20)))
    assert len({payment.id for payment in payments}) == 1
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 1


async def test_concurrent_claim_and_duplicate_after_success(session_factory):
    payment = await create(session_factory)
    service = PaymentProcessingService(session_factory)
    results = await asyncio.gather(
        service.claim_delivery(payment.id), service.claim_delivery(payment.id), return_exceptions=True,
    )
    assert sum(isinstance(result, DeliveryBusyError) for result in results) == 1
    delivery = next(result for result in results if not isinstance(result, Exception))
    assert delivery.attempt == 1
    await service.mark_webhook_delivered(delivery)
    assert await service.claim_delivery(payment.id) is None


async def test_three_attempts_then_terminal_state(session_factory):
    payment = await create(session_factory)
    service = PaymentProcessingService(session_factory)
    status = None
    processed_at = None
    for attempt in range(1, 4):
        delivery = await service.claim_delivery(payment.id)
        assert delivery.attempt == attempt
        if status is None:
            status, processed_at = delivery.payload.status, delivery.payload.processed_at
        assert (delivery.payload.status, delivery.payload.processed_at) == (status, processed_at)
        before = datetime.now(timezone.utc)
        await service.record_delivery_failure(delivery, "HTTP 503")
        if attempt < 3:
            with pytest.raises(DeliveryBusyError):
                await service.claim_delivery(payment.id)
            async with session_factory() as session:
                row = await session.get(Payment, payment.id)
                assert (row.webhook_next_attempt_at - before).total_seconds() >= 2 ** (attempt - 1)
                row.webhook_next_attempt_at = before - timedelta(seconds=1)
                await session.commit()
    with pytest.raises(DeliveryExhaustedError) as exc:
        await service.claim_delivery(payment.id)
    assert exc.value.attempts == 3
    await service.mark_dead_lettered(payment.id)
    assert await service.claim_delivery(payment.id) is None


async def test_crashed_worker_lease_recovers_and_fences_old_result(session_factory):
    payment = await create(session_factory)
    service = PaymentProcessingService(session_factory)
    old = await service.claim_delivery(payment.id)
    async with session_factory() as session:
        await session.execute(update(Payment).where(Payment.id == payment.id).values(
            webhook_lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        ))
        await session.commit()
    replacement = await service.claim_delivery(payment.id)
    assert replacement.attempt == 2
    assert replacement.payload == old.payload
    with pytest.raises(DeliveryLeaseLostError):
        await service.mark_webhook_delivered(old)
    await service.mark_webhook_delivered(replacement)


async def test_retry_publish_failure_keeps_budget_in_database(session_factory, monkeypatch):
    import app.consumer as consumer
    payment = await create(session_factory)
    async with session_factory() as session:
        event = await session.scalar(select(OutboxMessage.payload))
    monkeypatch.setattr(consumer, "processor", PaymentProcessingService(session_factory))
    monkeypatch.setattr(consumer.webhook_sender, "send", AsyncMock(side_effect=consumer.WebhookDeliveryError("503")))
    monkeypatch.setattr(consumer.broker, "publish", AsyncMock(side_effect=ConnectionError("broker down")))
    with pytest.raises(ConnectionError):
        await consumer.process_event(consumer.PaymentRequestedEvent.model_validate(event))
    async with session_factory() as session:
        row = await session.get(Payment, payment.id)
        assert row.webhook_attempts == 1
        assert row.webhook_next_attempt_at is not None
        assert row.webhook_lease_token is None
