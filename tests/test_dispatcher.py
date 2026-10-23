import asyncio
from unittest.mock import AsyncMock

import pytest

from app.core.config import get_settings
from app.messaging.dispatcher import OutboxDispatcher


async def test_dispatcher_recovers_after_temporary_failure():
    dispatcher = OutboxDispatcher(AsyncMock(), AsyncMock(), get_settings())
    dispatcher._poll_interval = 0.001
    stop = asyncio.Event()

    async def recovered():
        assert dispatcher.last_error is not None
        stop.set()
        return 0

    calls = 0

    async def dispatch():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("temporarily unavailable")
        return await recovered()

    dispatcher.dispatch_batch = dispatch
    await asyncio.wait_for(dispatcher.run(stop), timeout=1)
    assert calls == 2
    assert dispatcher.last_error is None


async def test_dispatcher_cancellation_is_not_swallowed():
    dispatcher = OutboxDispatcher(AsyncMock(), AsyncMock(), get_settings())
    dispatcher.dispatch_batch = AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        await dispatcher.run(asyncio.Event())
