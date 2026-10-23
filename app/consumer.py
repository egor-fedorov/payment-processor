import asyncio
import base64
import logging
from datetime import datetime, timezone

from faststream import AckPolicy, FastStream
from pydantic import ValidationError

from app.core.config import get_settings
from app.db.session import SessionFactory, dispose_engine
from app.messaging.broker import create_broker
from app.messaging.schemas import DeadLetterEvent, PaymentRequestedEvent
from app.messaging.topology import DLQ_QUEUE, PAYMENTS_QUEUE, declare_topology, retry_queue
from app.services.payments import (
    DeliveryBusyError,
    DeliveryExhaustedError,
    PaymentNotFoundError,
    PaymentProcessingService,
)
from app.services.webhook import WebhookDeliveryError, WebhookSender


logger = logging.getLogger(__name__)
settings = get_settings()
broker = create_broker()
stream_app = FastStream(broker)
processor = PaymentProcessingService(SessionFactory)
webhook_sender = WebhookSender(settings.webhook_timeout_seconds)


# Keep decoding inside the handler so invalid JSON also reaches the DLQ.
@broker.subscriber(
    PAYMENTS_QUEUE, ack_policy=AckPolicy.NACK_ON_ERROR,
    decoder=lambda message: message.body,
)
async def consume_payment(body: bytes) -> None:
    try:
        try:
            event = PaymentRequestedEvent.model_validate_json(body)
        except ValidationError as exc:
            await broker.publish(
                {
                    "failure_reason": str(exc),
                    "failed_at": datetime.now(timezone.utc).isoformat(),
                    "original_body_base64": base64.b64encode(body).decode("ascii"),
                },
                queue=DLQ_QUEUE, persist=True, mandatory=True, timeout=10,
            )
            return
        await process_event(event)
    except DeliveryBusyError:
        # Avoid a hot requeue loop while another worker owns the lease or a
        # retry is not due. An exception keeps the source message recoverable.
        await asyncio.sleep(1)
        raise
    except Exception:
        logger.exception("Payment processing interrupted; requeueing source message")
        await asyncio.sleep(1)
        raise


async def process_event(event: PaymentRequestedEvent) -> None:
    try:
        delivery = await processor.claim_delivery(event.payment_id)
    except PaymentNotFoundError as exc:
        await publish_dead_letter(event, 0, str(exc))
        return
    except DeliveryExhaustedError as exc:
        await publish_dead_letter(event, exc.attempts, str(exc))
        await processor.mark_dead_lettered(event.payment_id)
        return
    if delivery is None:
        return

    try:
        await webhook_sender.send(delivery.url, delivery.payload.model_dump(mode="json"))
    except WebhookDeliveryError as exc:
        await processor.record_delivery_failure(delivery, str(exc))
        await retry_or_dead_letter(event, delivery.attempt, str(exc))
    else:
        await processor.mark_webhook_delivered(delivery)


@stream_app.on_startup
async def startup() -> None:
    await broker.connect()
    await declare_topology(broker)


@stream_app.on_shutdown
async def shutdown() -> None:
    await dispose_engine()


async def publish_dead_letter(event: PaymentRequestedEvent, attempt: int, reason: str) -> None:
    await broker.publish(
        DeadLetterEvent(
            payment_id=event.payment_id,
            delivery_attempt=attempt,
            failure_reason=reason,
            failed_at=datetime.now(timezone.utc),
            original_event=event,
        ).model_dump(mode="json"),
        queue=DLQ_QUEUE,
        correlation_id=str(event.payment_id),
        message_id=f"{event.payment_id}:dlq",
        persist=True, mandatory=True, timeout=10,
    )


async def retry_or_dead_letter(event: PaymentRequestedEvent, attempt: int, reason: str) -> None:
    if attempt < settings.retry_max_attempts:
        retry_event = event.model_copy(update={"delivery_attempt": attempt})
        await broker.publish(
            retry_event.model_dump(mode="json"),
            queue=retry_queue(attempt),
            correlation_id=str(event.payment_id),
            message_id=f"{event.payment_id}:retry:{attempt}",
            persist=True, mandatory=True, timeout=10,
        )
    else:
        await publish_dead_letter(event, attempt, reason)
        await processor.mark_dead_lettered(event.payment_id)


async def main() -> None:
    await stream_app.run()


if __name__ == "__main__":
    asyncio.run(main())
