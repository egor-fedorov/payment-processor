import asyncio
import base64
import logging
from datetime import UTC, datetime

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker
from pydantic import ValidationError

from app.contracts import DeadLetterEvent, PaymentRequestedEvent
from app.core.config import Settings, get_settings
from app.db.session import create_database
from app.messaging.broker import create_broker
from app.messaging.topology import DLQ_QUEUE, PAYMENTS_QUEUE, declare_topology, retry_queue
from app.services.payments import (
    DeliveryBusyError,
    DeliveryExhaustedError,
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
                await self.broker.publish(
                    {
                        "failure_reason": str(exc),
                        "failed_at": datetime.now(UTC).isoformat(),
                        "original_body_base64": base64.b64encode(body).decode("ascii"),
                    },
                    queue=DLQ_QUEUE,
                    persist=True,
                    mandatory=True,
                    timeout=10,
                )
                return
            await self.process_event(event)
        except DeliveryBusyError:
            # Avoid a hot requeue loop while another worker owns the lease or a
            # retry is not due. An exception keeps the source message recoverable.
            await asyncio.sleep(1)
            raise
        except Exception:
            logger.exception("Payment processing interrupted; requeueing source message")
            await asyncio.sleep(1)
            raise

    async def process_event(self, event: PaymentRequestedEvent) -> None:
        try:
            delivery = await self.processor.claim_delivery(event.payment_id)
        except PaymentNotFoundError as exc:
            await self.publish_dead_letter(event, 0, str(exc))
            return
        except DeliveryExhaustedError as exc:
            await self.publish_dead_letter(event, exc.attempts, str(exc))
            await self.processor.mark_dead_lettered(event.payment_id)
            return
        if delivery is None:
            return

        try:
            await self.sender.send(delivery.url, delivery.payload.model_dump(mode="json"))
        except WebhookDeliveryError as exc:
            await self.processor.record_delivery_failure(delivery, str(exc))
            await self.retry_or_dead_letter(event, delivery.attempt, str(exc))
        else:
            await self.processor.mark_webhook_delivered(delivery)

    async def publish_dead_letter(
        self, event: PaymentRequestedEvent, attempt: int, reason: str
    ) -> None:
        await self.broker.publish(
            DeadLetterEvent(
                payment_id=event.payment_id,
                delivery_attempt=attempt,
                failure_reason=reason,
                failed_at=datetime.now(UTC),
                original_event=event,
            ).model_dump(mode="json"),
            queue=DLQ_QUEUE,
            correlation_id=str(event.payment_id),
            message_id=f"{event.payment_id}:dlq",
            persist=True,
            mandatory=True,
            timeout=10,
        )

    async def retry_or_dead_letter(
        self, event: PaymentRequestedEvent, attempt: int, reason: str
    ) -> None:
        if attempt < self.settings.retry_max_attempts:
            retry_event = event.model_copy(update={"delivery_attempt": attempt})
            await self.broker.publish(
                retry_event.model_dump(mode="json"),
                queue=retry_queue(attempt, self.settings),
                correlation_id=str(event.payment_id),
                message_id=f"{event.payment_id}:retry:{attempt}",
                persist=True,
                mandatory=True,
                timeout=10,
            )
        else:
            await self.publish_dead_letter(event, attempt, reason)
            await self.processor.mark_dead_lettered(event.payment_id)


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
    worker = FastStream(broker)

    @worker.on_startup
    async def startup() -> None:
        await broker.connect()
        await declare_topology(broker, settings)

    @worker.after_shutdown
    async def shutdown() -> None:
        try:
            await client.aclose()
        finally:
            await database.close()

    return worker


async def main() -> None:
    await create_worker(get_settings()).run()


if __name__ == "__main__":
    asyncio.run(main())
