import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.db.models import OutboxMessage
from app.db.outbox import OutboxRepository, schedule
from tests.factories import create_dispatcher

pytestmark = pytest.mark.integration


async def enqueue(sessions, now, count=1):
    async with sessions.begin() as session:
        for _ in range(count):
            key = str(uuid4())
            await schedule(
                session, payment_id=uuid4(), key=key, payload={"key": key}, available_at=now
            )


async def test_replicas_claim_disjoint_batches_and_recover_expired_lease(
    session_factory, settings, clock
):
    settings = settings.model_copy(update={"outbox_publish_concurrency": 2})
    await enqueue(session_factory, clock.now(), 4)
    a, b = OutboxRepository(session_factory, settings), OutboxRepository(session_factory, settings)
    left, right = await asyncio.gather(a.claim(clock.now()), b.claim(clock.now()))
    assert len(left) == len(right) == 2
    assert {item.id for item in left}.isdisjoint(item.id for item in right)
    assert await a.claim(clock.now()) == []
    clock.advance(settings.outbox_lease_seconds)
    replacement = await b.claim(clock.now())
    assert len(replacement) == 2
    stale = next(item for item in left + right if item.id == replacement[0].id)
    await a.complete(stale, clock.now())
    async with session_factory() as session:
        row = await session.get(OutboxMessage, stale.id)
        assert row.published_at is None
        assert row.lease_token == replacement[0].token
    await b.complete(replacement[0], clock.now())
    async with session_factory() as session:
        assert (await session.get(OutboxMessage, stale.id)).published_at == clock.now()


async def test_scheduled_events_and_retention(session_factory, settings, clock):
    await enqueue(session_factory, clock.now() + timedelta(seconds=10))
    repository = OutboxRepository(session_factory, settings)
    assert await repository.claim(clock.now()) == []
    clock.advance(10)
    (item,) = await repository.claim(clock.now())
    await repository.complete(item, clock.now())
    await enqueue(session_factory, clock.now())
    clock.advance(settings.outbox_retention_hours * 3600 + 1)
    await repository.prune(clock.now())
    async with session_factory() as session:
        rows = list(await session.scalars(select(OutboxMessage)))
        assert len(rows) == 1
        assert rows[0].published_at is None


async def test_publish_timeout_keeps_event_for_retry(session_factory, settings, clock):
    await enqueue(session_factory, clock.now())
    broker = AsyncMock()
    broker.publish.side_effect = lambda *a, **kw: None

    async def stalled(*args, **kwargs):
        await asyncio.Event().wait()

    broker.publish.side_effect = stalled
    settings = settings.model_copy(update={"outbox_publish_timeout_seconds": 0.01})
    dispatcher = create_dispatcher(session_factory, broker, settings, clock=clock)
    assert await dispatcher.dispatch_batch() == 0
    async with session_factory() as session:
        row = await session.scalar(select(OutboxMessage))
        assert row.published_at is None
        assert row.lease_token is None
        assert "TimeoutError" in row.last_error
        assert row.available_at > clock.now()


async def test_broker_wait_does_not_hold_row_lock(session_factory, settings, clock):
    await enqueue(session_factory, clock.now())
    entered, release = asyncio.Event(), asyncio.Event()

    async def publish(*args, **kwargs):
        entered.set()
        await release.wait()

    broker = AsyncMock()
    broker.publish.side_effect = publish
    task = asyncio.create_task(
        create_dispatcher(session_factory, broker, settings, clock=clock).dispatch_batch()
    )
    try:
        await asyncio.wait_for(entered.wait(), 1)
        async with session_factory.begin() as session:
            row = await asyncio.wait_for(
                session.scalar(select(OutboxMessage).with_for_update(nowait=True)), 1
            )
            assert row.lease_token is not None
    finally:
        release.set()
        assert await task == 1
