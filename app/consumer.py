import asyncio
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

import httpx
from faststream import FastStream

from app.core.clock import SystemClock
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.db.session import create_database
from app.messaging.broker import create_broker
from app.messaging.consumer import Consumer
from app.messaging.topology import declare_topology, subscribe_payments
from app.services.gateway import SimulatedGateway
from app.services.payments import PaymentProcessingService
from app.services.rejections import RejectedMessageService
from app.services.webhook import WebhookSender


def create_worker(settings: Settings) -> FastStream:
    database = create_database(settings)
    broker = create_broker(settings)
    client = httpx.AsyncClient(
        timeout=settings.webhook_timeout_seconds,
        limits=httpx.Limits(
            max_connections=settings.consumer_concurrency,
            max_keepalive_connections=settings.consumer_concurrency,
        ),
    )
    clock = SystemClock()
    consumer = Consumer(
        PaymentProcessingService(
            database.sessions, settings, gateway=SimulatedGateway(settings), clock=clock
        ),
        WebhookSender(client, settings.webhook_timeout_seconds),
        RejectedMessageService(database.sessions, clock),
    )
    subscribe_payments(broker, consumer.consume_payment)

    @asynccontextmanager
    async def lifespan() -> AsyncIterator[None]:
        async with AsyncExitStack() as resources:
            resources.push_async_callback(database.close)
            resources.push_async_callback(client.aclose)
            # FastStream stops the broker normally; this also covers partial startup.
            resources.push_async_callback(broker.stop)
            yield

    worker = FastStream(broker, lifespan=lifespan)

    @worker.on_startup
    async def startup() -> None:
        await broker.connect()
        await declare_topology(broker)

    return worker


async def main() -> None:
    configure_logging()
    await create_worker(get_settings()).run()


if __name__ == "__main__":
    asyncio.run(main())
