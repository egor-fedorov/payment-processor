import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from time import perf_counter

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker
from pydantic import ValidationError

from app.contracts import PaymentRequestedEvent
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.db.session import create_database
from app.messaging.broker import create_broker
from app.messaging.topology import PAYMENTS_QUEUE, declare_topology
from app.services.payments import (
    PaymentNotFoundError,
    PaymentProcessingService,
)
from app.services.webhook import WebhookClient, WebhookDeliveryError, WebhookSender

logger = logging.getLogger(__name__)


class Consumer:
    def __init__(
        self,
        broker: RabbitBroker,
        processor: PaymentProcessingService,
        sender: WebhookClient,
        settings: Settings,
    ) -> None:
        self.broker = broker
        self.processor = processor
        self.sender = sender
        self.settings = settings
        broker.subscriber(
            PAYMENTS_QUEUE, ack_policy=AckPolicy.NACK_ON_ERROR, decoder=lambda message: message.body
        )(self.consume_payment)

    async def consume_payment(self, body: bytes) -> None:
        try:
            try:
                event = PaymentRequestedEvent.model_validate_json(body)
            except ValidationError as exc:
                await self.processor.record_invalid(body, str(exc))
                return
            await self.process_event(event)
        except Exception:
            logger.exception("Payment processing interrupted; requeueing source message")
            await asyncio.sleep(1)
            raise

    async def process_event(self, event: PaymentRequestedEvent) -> None:
        try:
            delivery = await self.processor.claim_delivery(event.payment_id, event.recovery_token)
        except PaymentNotFoundError as exc:
            await self.processor.record_invalid(event.model_dump_json().encode(), str(exc))
            return
        if delivery is None:
            return

        started = perf_counter()
        try:
            await self.sender.send(delivery.url, delivery.payload.model_dump(mode="json"))
        except WebhookDeliveryError as exc:
            await self.processor.record_delivery_failure(delivery, str(exc))
            logger.info(
                "Webhook delivery failed",
                extra={
                    "payment_id": str(delivery.payment_id),
                    "attempt": delivery.attempt,
                    "status": "retry_or_exhausted",
                    "duration_ms": round((perf_counter() - started) * 1000, 3),
                },
            )
        else:
            await self.processor.mark_webhook_delivered(delivery)
            logger.info(
                "Webhook delivered",
                extra={
                    "payment_id": str(delivery.payment_id),
                    "attempt": delivery.attempt,
                    "status": delivery.payload.status,
                    "duration_ms": round((perf_counter() - started) * 1000, 3),
                },
            )


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
    Consumer(
        broker,
        PaymentProcessingService(database.sessions, settings),
        WebhookSender(client, settings.webhook_timeout_seconds),
        settings,
    )

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
