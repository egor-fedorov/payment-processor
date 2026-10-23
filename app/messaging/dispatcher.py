import asyncio
import logging
from contextlib import suppress
from datetime import timedelta

from app.contracts import Publication
from app.ports import Clock, EventPublisher, OutboxStore

logger = logging.getLogger(__name__)


class OutboxDispatcher:
    def __init__(
        self,
        repository: OutboxStore,
        publisher: EventPublisher,
        *,
        clock: Clock,
        poll_interval_seconds: float,
        publish_timeout_seconds: float,
    ) -> None:
        self._repository = repository
        self._publisher = publisher
        self._poll_interval = poll_interval_seconds
        self._publish_timeout = publish_timeout_seconds
        self._clock = clock
        self.last_error: str | None = None

    async def run(self, stop_event: asyncio.Event) -> None:
        retry_delay = min(self._poll_interval, 30.0)
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
                retry_delay = min(self._poll_interval, 30.0)
                if dispatched:
                    continue
                delay = self._poll_interval
            with suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=delay)

    async def dispatch_batch(self) -> int:
        items = await self._repository.claim(self._clock.now())
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(self._publish(item)) for item in items]
        results = [task.result() for task in tasks]
        if all(results):
            self.last_error = None
        return sum(results)

    async def _publish(self, item: Publication) -> bool:
        error = None
        try:
            async with asyncio.timeout(self._publish_timeout):
                await self._publisher.publish(item)
        except Exception as exc:
            error = repr(exc)
            self.last_error = error
            logger.exception("Outbox publication failed", extra={"event_id": str(item.id)})
        await self._repository.complete(item, self._clock.now(), error)
        if error is None:
            logger.info(
                "Outbox event published",
                extra={
                    "event_id": str(item.id),
                    "payment_id": str(item.aggregate_id),
                    "attempt": item.attempt,
                },
            )
        return error is None
