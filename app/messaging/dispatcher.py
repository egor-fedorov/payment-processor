import asyncio
import logging
from contextlib import suppress
from datetime import timedelta

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.db.outbox import OutboxRepository, Publication
from app.services.gateway import Clock, SystemClock

logger = logging.getLogger(__name__)


class OutboxDispatcher:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        settings: Settings,
        *,
        clock: Clock | None = None,
    ) -> None:
        self._repository = OutboxRepository(session_factory, settings)
        self._broker = broker
        self._settings = settings
        self._clock = clock or SystemClock()
        self.last_error: str | None = None

    async def run(self, stop_event: asyncio.Event) -> None:
        retry_delay = min(self._settings.outbox_poll_interval_seconds, 30.0)
        cleanup_at = self._clock.now()
        while not stop_event.is_set():
            try:
                dispatched = await self.dispatch_batch()
                if self._clock.now() >= cleanup_at:
                    await self._repository.prune(self._clock.now())
                    cleanup_at = self._clock.now() + timedelta(minutes=1)
            except Exception as exc:
                self.last_error = repr(exc)
                logger.exception("Outbox batch failed")
                delay = retry_delay
                retry_delay = min(retry_delay * 2, 30)
            else:
                retry_delay = min(self._settings.outbox_poll_interval_seconds, 30.0)
                if dispatched:
                    continue
                delay = self._settings.outbox_poll_interval_seconds
            with suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=delay)

    async def dispatch_batch(self) -> int:
        items = await self._repository.claim(self._clock.now())
        results = await asyncio.gather(*(self._publish(item) for item in items))
        if all(results):
            self.last_error = None
        return sum(results)

    async def _publish(self, item: Publication) -> bool:
        error = None
        try:
            async with asyncio.timeout(self._settings.outbox_publish_timeout_seconds):
                await self._broker.publish(
                    item.payload,
                    queue=item.destination,
                    correlation_id=str(item.aggregate_id),
                    message_id=str(item.id),
                    persist=True,
                    mandatory=True,
                    timeout=self._settings.outbox_publish_timeout_seconds,
                )
        except Exception as exc:
            error = repr(exc)
            self.last_error = error
            logger.exception("Outbox publication failed", extra={"event_id": str(item.id)})
        await self._repository.complete(item, self._clock.now(), error)
        return error is None
