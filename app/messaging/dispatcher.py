import asyncio
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core.config import get_settings
from app.db.models import OutboxMessage
from app.messaging.topology import PAYMENTS_QUEUE


class OutboxDispatcher:
    def __init__(self, session_factory: async_sessionmaker, broker) -> None:
        settings = get_settings()
        self._session_factory = session_factory
        self._broker = broker
        self._batch_size = settings.outbox_batch_size
        self._poll_interval = settings.outbox_poll_interval_seconds

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            dispatched = await self.dispatch_batch()
            if dispatched:
                continue

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._poll_interval)
            except asyncio.TimeoutError:
                continue

    async def dispatch_batch(self) -> int:
        async with self._session_factory() as session:
            ids = list(
                (
                    await session.scalars(
                        select(OutboxMessage.id)
                        .where(OutboxMessage.published_at.is_(None))
                        .order_by(OutboxMessage.created_at)
                        .limit(self._batch_size)
                    )
                ).all()
            )

        dispatched = 0
        for message_id in ids:
            published = await self._dispatch_one(message_id)
            if published:
                dispatched += 1

        return dispatched

    async def _dispatch_one(self, message_id) -> bool:
        async with self._session_factory() as session:
            message = await session.get(OutboxMessage, message_id, with_for_update=True)
            if message is None or message.published_at is not None:
                return False

            await self._broker.publish(
                message.payload,
                queue=PAYMENTS_QUEUE,
                headers=message.headers,
                correlation_id=str(message.aggregate_id),
                persist=True,
            )
            message.published_at = datetime.now(timezone.utc)
            await session.commit()
            return True
