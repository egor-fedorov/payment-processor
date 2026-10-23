from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy.exc import IntegrityError

from app.api.schemas import PaymentCreateRequest
from app.core.config import Settings
from app.db.session import get_session
from app.main import create_app
from app.services.payments import PaymentService

PAYLOAD = {
    "amount": "10.00",
    "currency": "RUB",
    "description": "order",
    "webhook_url": "https://example.com/webhook",
}


@pytest.mark.parametrize("key", ["", " ", "x" * 256])
async def test_invalid_idempotency_key(key, settings):
    app = create_app(settings)

    async def session():
        yield AsyncMock()

    app.dependency_overrides[get_session] = session
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                "/api/v1/payments",
                json=PAYLOAD,
                headers={"X-API-Key": settings.api_key, "Idempotency-Key": key},
            )
        assert response.status_code == 422
    finally:
        app.dependency_overrides.clear()


async def test_flush_conflict_returns_existing_payment():
    session = AsyncMock()
    session.add = lambda _: None
    cause = Exception("duplicate")
    cause.constraint_name = "uq_payments_idempotency_key"
    original = Exception("unique violation")
    original.__cause__ = cause
    session.flush.side_effect = IntegrityError("insert", {}, original)
    existing = SimpleNamespace(id=uuid4(), created_at=datetime.now(UTC))
    session.scalar.side_effect = [None, existing]
    result = await PaymentService(session).create_payment(
        PaymentCreateRequest(**PAYLOAD).to_command(), "key"
    )
    assert result is existing
    session.rollback.assert_awaited_once()
    session.commit.assert_not_awaited()


async def test_unrelated_integrity_error_is_not_hidden():
    session = AsyncMock()
    session.add = lambda _: None
    session.scalar.return_value = None
    session.flush.side_effect = IntegrityError("insert", {}, Exception("other constraint"))
    with pytest.raises(IntegrityError):
        await PaymentService(session).create_payment(
            PaymentCreateRequest(**PAYLOAD).to_command(), "key"
        )
    assert session.scalar.await_count == 1
    session.rollback.assert_awaited_once()


@pytest.mark.parametrize("path", ["/docs", "/redoc", "/openapi.json"])
async def test_documentation_is_disabled_by_default(path, settings):
    app = create_app(settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await client.get(path)).status_code == 404


@pytest.mark.parametrize(
    "values",
    [
        {"outbox_batch_size": 0},
        {"payment_success_rate": 1.1},
        {"retry_max_attempts": 0},
        {"retry_base_delay_seconds": 0},
        {"payment_delay_min_seconds": 5, "payment_delay_max_seconds": 2},
        {"webhook_timeout_seconds": 30, "webhook_lease_seconds": 5},
        {"outbox_poll_interval_seconds": float("inf")},
    ],
)
def test_invalid_settings_fail_at_startup(values):
    with pytest.raises(ValueError):
        Settings(_env_file=None, **values)
