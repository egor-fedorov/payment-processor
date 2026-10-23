from dataclasses import replace

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.api.schemas import PaymentCreateRequest
from app.contracts import PaymentRequestedEvent
from app.db.models import OutboxMessage, Payment
from app.services.payments import PaymentService

pytestmark = pytest.mark.integration
PAYLOAD = PaymentCreateRequest(
    amount="10.00",
    currency="RUB",
    description="original",
    metadata={"order_id": 123},
    webhook_url="https://example.com/hook",
).to_command()


async def test_repeated_key_preserves_original_payment_and_event(session_factory):
    async with session_factory() as session:
        original = await PaymentService(session).create_payment(PAYLOAD, "same-key")
    async with session_factory() as session:
        repeated = await PaymentService(session).create_payment(
            replace(PAYLOAD, description="different", metadata={"order_id": 456}), "same-key"
        )
        assert repeated.id == original.id
        assert repeated.description == original.description
        assert repeated.metadata_json == original.metadata_json
        (event,) = list(await session.scalars(select(OutboxMessage)))
        parsed = PaymentRequestedEvent.model_validate(event.payload)
        assert parsed.payment_id == original.id
        assert parsed.metadata == PAYLOAD.metadata
        assert parsed.delivery_attempt == 0
        assert parsed.recovery_token is None
        assert event.headers == {"idempotency_key": "same-key"}


async def test_unrelated_database_constraint_is_not_hidden(session_factory):
    async with session_factory() as session:
        with pytest.raises(IntegrityError):
            await PaymentService(session).create_payment(replace(PAYLOAD, description=None), "key")
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0


async def test_outbox_failure_rolls_back_payment(session_factory, monkeypatch):
    async def fail_after_payment_insert(session, event):
        assert await session.get(Payment, event.payment_id) is not None
        raise ConnectionError("outbox insert interrupted")

    monkeypatch.setattr("app.services.payments.schedule", fail_after_payment_insert)
    async with session_factory() as session:
        with pytest.raises(ConnectionError, match="outbox insert"):
            await PaymentService(session).create_payment(PAYLOAD, "key")
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0
