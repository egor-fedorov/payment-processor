import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.api.schemas import PaymentCreateRequest
from app.db.models import OutboxMessage, Payment
from app.domain import PaymentStatus
from app.services.payments import (
    DeliveryLeaseLostError,
    PaymentNotFoundError,
    PaymentService,
)
from tests.factories import create_processor

pytestmark = pytest.mark.integration
PAYLOAD = PaymentCreateRequest(
    amount="10.00", currency="RUB", description="order", webhook_url="https://example.com/hook"
).to_command()


async def create(session_factory, key="key"):
    async with session_factory() as session:
        return await PaymentService(session).create_payment(PAYLOAD, key)


async def test_concurrent_creation_has_one_payment_and_outbox(session_factory):
    payments = await asyncio.gather(*(create(session_factory) for _ in range(20)))
    assert len({payment.id for payment in payments}) == 1
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 1


async def test_concurrent_claim_and_duplicate_after_success(session_factory, settings):
    payment = await create(session_factory)
    service = create_processor(session_factory, settings)
    results = await asyncio.gather(
        service.claim_delivery(payment.id), service.claim_delivery(payment.id)
    )
    assert results.count(None) == 1
    delivery = next(result for result in results if result is not None)
    assert delivery.attempt == 1
    await service.mark_webhook_delivered(delivery)
    assert await service.claim_delivery(payment.id) is None
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 1


async def test_three_attempts_schedule_backoff_then_one_dlq(session_factory, settings, clock):
    payment = await create(session_factory)
    service = create_processor(session_factory, settings, clock=clock)
    payload = None
    for attempt in range(1, 4):
        delivery = await service.claim_delivery(payment.id)
        assert delivery.attempt == attempt
        payload = payload or delivery.payload
        assert delivery.payload == payload
        await service.record_delivery_failure(delivery, "HTTP 503")
        assert await service.claim_delivery(payment.id) is None
        async with session_factory() as session:
            row = await session.get(Payment, payment.id)
            if attempt < 3:
                assert (row.webhook_next_attempt_at - clock.now()).total_seconds() == 2 ** (
                    attempt - 1
                )
                scheduled = await session.scalar(
                    select(OutboxMessage).where(
                        OutboxMessage.deduplication_key == f"retry:{payment.id}:{attempt}"
                    )
                )
                assert scheduled.available_at == row.webhook_next_attempt_at
            else:
                assert row.webhook_exhausted_at == clock.now()
        clock.advance(2 ** (attempt - 1))
    async with session_factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(OutboxMessage)
                .where(OutboxMessage.destination == "payments.dlq")
            )
            == 1
        )


async def test_crashed_worker_lease_recovers_and_fences_old_result(
    session_factory, settings, clock
):
    payment = await create(session_factory)
    service = create_processor(session_factory, settings, clock=clock)
    old = await service.claim_delivery(payment.id)
    async with session_factory() as session:
        recovery = await session.scalar(
            select(OutboxMessage).where(OutboxMessage.deduplication_key == f"recovery:{old.token}")
        )
        assert recovery.payload["recovery_token"] == str(old.token)
        assert (
            recovery.available_at - clock.now()
        ).total_seconds() == settings.webhook_lease_seconds
    clock.advance(settings.webhook_lease_seconds)
    replacement = await service.claim_delivery(payment.id, old.token)
    assert replacement.attempt == 2
    assert replacement.payload == old.payload
    assert await service.claim_delivery(payment.id, old.token) is None
    with pytest.raises(DeliveryLeaseLostError):
        await service.mark_webhook_delivered(old)
    await service.mark_webhook_delivered(replacement)


async def test_unknown_final_attempt_is_exhausted_after_crash(session_factory, settings, clock):
    payment = await create(session_factory)
    service = create_processor(
        session_factory, settings.model_copy(update={"retry_max_attempts": 1}), clock=clock
    )
    delivery = await service.claim_delivery(payment.id)
    clock.advance(settings.webhook_lease_seconds)
    with pytest.raises(DeliveryLeaseLostError):
        await service.record_delivery_failure(delivery, "late")
    assert await service.claim_delivery(payment.id, delivery.token) is None
    async with session_factory() as session:
        dlq = await session.scalar(
            select(OutboxMessage).where(OutboxMessage.destination == "payments.dlq")
        )
        assert dlq.payload["delivery_attempt"] == 1
        assert "outcome unknown" in dlq.payload["failure_reason"]


async def test_missing_payment(session_factory, settings):
    with pytest.raises(PaymentNotFoundError):
        await create_processor(session_factory, settings).claim_delivery(uuid4())


async def test_gateway_does_not_hold_database_connection(session_factory, settings):
    payment = await create(session_factory)
    entered = asyncio.Event()
    release = asyncio.Event()

    class Gateway:
        async def process(self, payment_id):
            entered.set()
            await release.wait()
            return PaymentStatus.SUCCEEDED

    service = create_processor(session_factory, settings, gateway=Gateway())
    task = asyncio.create_task(service.claim_delivery(payment.id))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        async with session_factory() as session:
            row = await asyncio.wait_for(session.get(Payment, payment.id, with_for_update=True), 1)
            assert row.status == PaymentStatus.PENDING
    finally:
        release.set()
        await task
