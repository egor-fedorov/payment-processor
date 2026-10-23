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
    async with worker.lifespan_context():
        await worker.start()
        await worker.stop()


@pytest.mark.integration
async def test_dispatcher_process_lifecycle(settings, migrated_database, rabbit):
    stop = asyncio.Event()
    stop.set()
    await run(settings, stop)


@pytest.mark.parametrize("failure", ["connect", "topology", "cancel", "stop"])
async def test_worker_releases_resources_on_all_exit_paths(settings, monkeypatch, failure):
    from types import SimpleNamespace

    from app.messaging.broker import create_broker

    database = SimpleNamespace(sessions=AsyncMock(), close=AsyncMock())
    client = AsyncMock()
    broker = create_broker(settings)
    started = asyncio.Event()
    monkeypatch.setattr("app.consumer.create_database", lambda _: database)
    monkeypatch.setattr("app.consumer.create_broker", lambda _: broker)
    monkeypatch.setattr("app.consumer.httpx.AsyncClient", lambda **_: client)
    monkeypatch.setattr(
        broker,
        "connect",
        AsyncMock(side_effect=ConnectionError("connect") if failure == "connect" else None),
    )
    monkeypatch.setattr(
        "app.consumer.declare_topology",
        AsyncMock(side_effect=ConnectionError("topology") if failure == "topology" else None),
    )
    monkeypatch.setattr(broker, "start", AsyncMock(side_effect=lambda: started.set()))
    monkeypatch.setattr(broker, "stop", AsyncMock())
    worker = create_worker(settings)
    if failure in {"connect", "topology"}:
        with pytest.raises(ConnectionError, match=failure):
            await worker.run()
    else:
        task = asyncio.create_task(worker.run())
        await asyncio.wait_for(started.wait(), 1)
        if failure == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            worker.exit()
            await asyncio.wait_for(task, 1)
    client.aclose.assert_awaited_once()
    database.close.assert_awaited_once()
    broker.stop.assert_awaited()
