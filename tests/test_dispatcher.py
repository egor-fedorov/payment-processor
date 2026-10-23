import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.contracts import Publication
from app.messaging.dispatcher import OutboxDispatcher


async def test_dispatcher_cancellation_is_not_swallowed(clock):
    dispatcher = OutboxDispatcher(
        AsyncMock(), AsyncMock(), clock=clock, poll_interval_seconds=0.01, publish_timeout_seconds=1
    )
    dispatcher.dispatch_batch = AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        await dispatcher.run(asyncio.Event())


@pytest.mark.parametrize("cancel", [False, True])
async def test_failed_or_cancelled_batch_joins_other_publications(clock, cancel):

    items = [
        Publication(uuid4(), uuid4(), uuid4(), "payments.new", {"index": i}, 1) for i in range(2)
    ]
    entered = asyncio.Event()
    finished = asyncio.Event()
    repository = AsyncMock()
    repository.claim.return_value = items

    async def complete(item, *_):
        if item == items[0] and not cancel:
            await entered.wait()
            raise ConnectionError("database completion failed")

    repository.complete.side_effect = complete
    publisher = AsyncMock()

    async def publish(item):
        if item.payload["index"] == 1:
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                finished.set()

    publisher.publish.side_effect = publish
    dispatcher = OutboxDispatcher(
        repository, publisher, clock=clock, poll_interval_seconds=0.01, publish_timeout_seconds=1
    )
    task = asyncio.create_task(dispatcher.dispatch_batch())
    await asyncio.wait_for(entered.wait(), 1)
    if cancel:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(ExceptionGroup) as caught:
            await task
        assert isinstance(caught.value.exceptions[0], ConnectionError)
    assert finished.is_set()
    # A subsequent batch starts only after the previous batch has stopped.
    repository.claim.return_value = []
    assert await dispatcher.dispatch_batch() == 0
