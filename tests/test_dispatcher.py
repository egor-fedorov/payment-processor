import asyncio
from unittest.mock import AsyncMock

import pytest

from app.messaging.dispatcher import OutboxDispatcher


async def test_dispatcher_cancellation_is_not_swallowed(settings):
    dispatcher = OutboxDispatcher(AsyncMock(), AsyncMock(), settings)
    dispatcher.dispatch_batch = AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        await dispatcher.run(asyncio.Event())
