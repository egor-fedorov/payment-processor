import asyncio
import logging
from time import perf_counter

from pydantic import ValidationError

from app.contracts import PaymentRequestedEvent
from app.ports import WebhookClient, WebhookDeliveryError
from app.services.payments import PaymentNotFoundError, PaymentProcessingService
from app.services.rejections import RejectedMessageService

logger = logging.getLogger(__name__)


class Consumer:
    def __init__(
        self,
        processor: PaymentProcessingService,
        sender: WebhookClient,
        rejections: RejectedMessageService,
    ) -> None:
        self.processor = processor
        self.sender = sender
        self.rejections = rejections

    async def consume_payment(self, body: bytes) -> None:
        try:
            try:
                event = PaymentRequestedEvent.model_validate_json(body)
            except ValidationError as exc:
                await self.rejections.record_invalid(body, str(exc))
                return
            await self.process_event(event)
        except Exception:
            logger.exception("Payment processing interrupted; requeueing source message")
            await asyncio.sleep(1)
            raise

    async def process_event(self, event: PaymentRequestedEvent) -> None:
        try:
            delivery = await self.processor.prepare_delivery(event.payment_id, event.recovery_token)
        except PaymentNotFoundError as exc:
            await self.rejections.record_invalid(event.model_dump_json().encode(), str(exc))
            return
        if delivery is None:
            return

        started = perf_counter()
        try:
            await self.sender.send(delivery.url, delivery.payload)
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
