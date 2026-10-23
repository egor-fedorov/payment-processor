import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy.exc import OperationalError

from app.consumer import create_worker
from app.dispatcher import run
from app.main import create_app

PAYLOAD = {
    "amount": "10.00",
    "currency": "RUB",
    "description": "order",
    "metadata": {"order_id": 1},
    "webhook_url": "https://example.com/hook",
}


@pytest.mark.integration
async def test_http_contract_and_readiness(migrated_database, settings):
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        assert (await client.get("/health/live")).status_code == 200
        assert (await client.get("/health/ready")).status_code == 422
        assert (
            await client.get("/health/ready", headers={"X-API-Key": "wrong"})
        ).status_code == 401
        client.headers["X-API-Key"] = settings.api_key
        assert (await client.get("/health/ready")).status_code == 200
        key = str(uuid4())
        response = await client.post(
            "/api/v1/payments", json=PAYLOAD, headers={"Idempotency-Key": key}
        )
        assert response.status_code == 202
        created = response.json()
        assert created["status"] == "pending"
        repeated = await client.post(
            "/api/v1/payments", json=PAYLOAD, headers={"Idempotency-Key": key}
        )
        assert repeated.json() == created
        detail = await client.get(f"/api/v1/payments/{created['payment_id']}")
        assert detail.status_code == 200
        assert detail.json()["metadata"] == PAYLOAD["metadata"]
        assert detail.json()["webhook_attempts"] == 0
        assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404


@pytest.mark.parametrize(
    "error", [TimeoutError(), OperationalError("select", {}, Exception("down"))]
)
async def test_readiness_reports_database_failure(settings, error):
    app = create_app(settings)
    database = AsyncMock()
    # sessions is a synchronous factory returning an asynchronous context manager.
    context = AsyncMock()
    context.__aenter__.return_value.execute.side_effect = error
    database.sessions = lambda: context
    app.state.database = database
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": settings.api_key},
    ) as client:
        assert (await client.get("/health/ready")).status_code == 503


async def test_applications_have_independent_settings(settings):
    left = create_app(settings)
    right = create_app(settings.model_copy(update={"api_key": "other-key", "docs_enabled": True}))
    assert left.state.settings.api_key != right.state.settings.api_key
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=right), base_url="http://test"
    ) as client:
        assert (await client.get("/openapi.json")).status_code == 200
        assert (
            await client.get("/health/ready", headers={"X-API-Key": settings.api_key})
        ).status_code == 401


@pytest.mark.integration
async def test_worker_factory_lifecycle(settings, migrated_database, rabbit):
    worker = create_worker(settings)
    await worker.start()
    await worker.stop()


@pytest.mark.integration
async def test_dispatcher_process_lifecycle(settings, migrated_database, rabbit):
    stop = asyncio.Event()
    stop.set()
    await run(settings, stop)
