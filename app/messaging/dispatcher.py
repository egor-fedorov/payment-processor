import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core.config import get_settings
from app.db.models import OutboxMessage
from app.messaging.topology import PAYMENTS_QUEUE

logger = logging.getLogger(__name__)


class OutboxDispatcher:
    def __init__(self, session_factory: async_sessionmaker, broker) -> None:
        settings = get_settings()
        self._session_factory = session_factory
        self._broker = broker
        self._batch_size = settings.outbox_batch_size
        self._poll_interval = settings.outbox_poll_interval_seconds
        self.last_error: str | None = None

    async def run(self, stop_event: asyncio.Event) -> None:
        retry_delay = min(self._poll_interval, 30.0)
        while not stop_event.is_set():
            try:
                dispatched = await self.dispatch_batch()
            except Exception as exc:
                self.last_error = repr(exc)
                logger.exception("Outbox dispatch failed; retrying in %.1fs", retry_delay)
                delay = retry_delay
                retry_delay = min(retry_delay * 2, 30.0)
            else:
                self.last_error = None
                retry_delay = min(self._poll_interval, 30.0)
                if dispatched:
                    continue
                delay = self._poll_interval

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay)
            except TimeoutError:
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
            message = await session.scalar(
                select(OutboxMessage)
                .where(OutboxMessage.id == message_id)
                .with_for_update(skip_locked=True)
            )
            if message is None or message.published_at is not None:
                return False

            await asyncio.wait_for(
                self._broker.publish(
                    message.payload,
                    queue=PAYMENTS_QUEUE,
                    headers=message.headers,
                    correlation_id=str(message.aggregate_id),
                    message_id=str(message.id),
                    persist=True,
                    mandatory=True,
                    timeout=10,
                ),
                timeout=10,
            )
            message.published_at = datetime.now(UTC)
            await session.commit()
            return True
