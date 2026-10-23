import asyncio
import signal

from app.core.clock import SystemClock
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.db.outbox import OutboxRepository
from app.db.session import create_database
from app.messaging.broker import create_broker
from app.messaging.dispatcher import OutboxDispatcher
from app.messaging.publisher import RabbitEventPublisher
from app.messaging.topology import declare_topology


async def run(settings: Settings, stop: asyncio.Event) -> None:
    database = create_database(settings)
    broker = create_broker(settings)
    try:
        await broker.start()
        await declare_topology(broker)
        await OutboxDispatcher(
            OutboxRepository(database.sessions, settings),
            RabbitEventPublisher(broker, settings.outbox_publish_timeout_seconds),
            clock=SystemClock(),
            poll_interval_seconds=settings.outbox_poll_interval_seconds,
            publish_timeout_seconds=settings.outbox_publish_timeout_seconds,
        ).run(stop)
    finally:
        try:
            await broker.stop()
        finally:
            await database.close()


async def main() -> None:
    configure_logging()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await run(get_settings(), stop)


if __name__ == "__main__":
    asyncio.run(main())
