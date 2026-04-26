import asyncio
from datetime import datetime, timezone

from faststream import FastStream

from app.core.config import get_settings
from app.db.session import SessionFactory, dispose_engine
from app.messaging.broker import create_broker
from app.messaging.schemas import DeadLetterEvent, PaymentRequestedEvent
from app.messaging.topology import DLQ_QUEUE, PAYMENTS_QUEUE, RETRY_QUEUE, declare_topology
from app.services.payments import PaymentNotFoundError, PaymentProcessingService
from app.services.webhook import WebhookDeliveryError, WebhookSender


settings = get_settings()
broker = create_broker()
stream_app = FastStream(broker)
processor = PaymentProcessingService(SessionFactory)
webhook_sender = WebhookSender(settings.webhook_timeout_seconds)


@broker.subscriber(PAYMENTS_QUEUE)
async def consume_payment(event: PaymentRequestedEvent) -> None:
    try:
        payload = await processor.prepare_webhook_payload(event.payment_id)
        if payload is None:
            return

        await webhook_sender.send(
            str(event.webhook_url),
            payload.model_dump(mode="json"),
        )
        await processor.mark_webhook_delivered(event.payment_id)
    except (WebhookDeliveryError, PaymentNotFoundError) as exc:
        await processor.record_delivery_failure(event.payment_id, str(exc))
        await retry_or_dead_letter(event, str(exc))
    except Exception as exc:  # pragma: no cover - defensive branch
        await processor.record_delivery_failure(event.payment_id, repr(exc))
        await retry_or_dead_letter(event, repr(exc))


@stream_app.after_startup
async def startup() -> None:
    await declare_topology(broker)


@stream_app.on_shutdown
async def shutdown() -> None:
    await dispose_engine()


async def retry_or_dead_letter(event: PaymentRequestedEvent, reason: str) -> None:
    current_attempt = event.delivery_attempt + 1
    if current_attempt < settings.retry_max_attempts:
        retry_event = event.model_copy(update={"delivery_attempt": current_attempt})
        delay_seconds = settings.retry_base_delay_seconds * (2 ** (current_attempt - 1))
        await broker.publish(
            retry_event.model_dump(mode="json"),
            queue=RETRY_QUEUE,
            correlation_id=str(event.payment_id),
            persist=True,
            expiration=delay_seconds,
        )
        return

    await broker.publish(
        DeadLetterEvent(
            payment_id=event.payment_id,
            delivery_attempt=current_attempt,
            failure_reason=reason,
            failed_at=datetime.now(timezone.utc),
            original_event=event,
        ).model_dump(mode="json"),
        queue=DLQ_QUEUE,
        correlation_id=str(event.payment_id),
        persist=True,
    )


async def main() -> None:
    await stream_app.run()


if __name__ == "__main__":
    asyncio.run(main())
