import asyncio
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.schemas import PaymentCreateRequest
from app.core.config import Settings
from app.db.models import OutboxMessage, Payment
from app.domain import PaymentStatus
from app.messaging.schemas import PaymentRequestedEvent, PaymentWebhookPayload


class PaymentNotFoundError(Exception):
    """Raised when a payment does not exist."""


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_payment(
        self,
        payload: PaymentCreateRequest,
        idempotency_key: str,
    ) -> Payment:
        existing = await self.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            return existing

        payment = Payment(
            amount=payload.amount,
            currency=payload.currency,
            description=payload.description,
            metadata_json=payload.metadata,
            webhook_url=str(payload.webhook_url),
            idempotency_key=idempotency_key,
        )
        try:
            self._session.add(payment)
            await self._session.flush()
            event = PaymentRequestedEvent(
                payment_id=payment.id,
                amount=payload.amount,
                currency=payload.currency,
                description=payload.description,
                metadata=payload.metadata,
                webhook_url=payload.webhook_url,
                created_at=payment.created_at,
            )
            self._session.add(
                OutboxMessage(
                    aggregate_type="payment",
                    aggregate_id=payment.id,
                    event_type="payments.requested",
                    payload=event.model_dump(mode="json"),
                    headers={"idempotency_key": idempotency_key},
                )
            )
            await self._session.commit()
        except IntegrityError as exc:
            await self._session.rollback()
            # asyncpg keeps the PostgreSQL constraint name on the cause;
            # psycopg exposes it through diag. Do not hide other constraints.
            original = exc.orig
            cause = getattr(original, "__cause__", None)
            constraint = getattr(cause, "constraint_name", None) or getattr(
                getattr(original, "diag", None), "constraint_name", None
            )
            if constraint != "uq_payments_idempotency_key":
                raise
            existing = await self.get_by_idempotency_key(idempotency_key)
            if existing is None:
                raise
            return existing

        await self._session.refresh(payment)
        return payment

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        return await self._session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )

    async def get_payment(self, payment_id: UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)


class DeliveryBusyError(Exception):
    """Another worker owns the lease, or the retry delay has not elapsed."""


class DeliveryExhaustedError(Exception):
    def __init__(self, attempts: int, reason: str) -> None:
        super().__init__(reason)
        self.attempts = attempts


class DeliveryLeaseLostError(Exception):
    """The result belongs to an attempt whose lease has been replaced."""


@dataclass(frozen=True)
class WebhookDelivery:
    payment_id: UUID
    token: UUID
    attempt: int
    url: str
    payload: PaymentWebhookPayload


class PaymentProcessingService:
    def __init__(
        self, session_factory: async_sessionmaker[AsyncSession], settings: Settings
    ) -> None:
        self._session_factory = session_factory
        self._settings = settings

    async def claim_delivery(self, payment_id: UUID) -> WebhookDelivery | None:
        async with self._session_factory() as session:
            payment = await session.get(Payment, payment_id, with_for_update=True)
            if payment is None:
                raise PaymentNotFoundError(str(payment_id))
            if payment.webhook_delivered_at or payment.webhook_dead_lettered_at:
                return None

            now = datetime.now(UTC)
            if payment.webhook_lease_expires_at and payment.webhook_lease_expires_at > now:
                raise DeliveryBusyError("webhook delivery is already in progress")
            if (
                payment.webhook_exhausted_at
                or payment.webhook_attempts >= self._settings.retry_max_attempts
            ):
                payment.webhook_exhausted_at = payment.webhook_exhausted_at or now
                payment.last_error = payment.last_error or "delivery lease expired; outcome unknown"
                payment.webhook_lease_token = None
                payment.webhook_lease_expires_at = None
                await session.commit()
                raise DeliveryExhaustedError(payment.webhook_attempts, payment.last_error)
            if payment.webhook_next_attempt_at and payment.webhook_next_attempt_at > now:
                raise DeliveryBusyError("webhook retry delay has not elapsed")

            if payment.status == PaymentStatus.PENDING:
                await asyncio.sleep(
                    random.uniform(
                        self._settings.payment_delay_min_seconds,
                        self._settings.payment_delay_max_seconds,
                    )
                )
                payment.status = (
                    PaymentStatus.SUCCEEDED
                    if random.random() < self._settings.payment_success_rate
                    else PaymentStatus.FAILED
                )
                payment.processed_at = datetime.now(UTC)

            # Reserve and count before HTTP. A crash uses this attempt's budget;
            # its original unacked RabbitMQ message can recover after expiry.
            token = uuid4()
            payment.webhook_attempts += 1
            payment.webhook_lease_token = token
            payment.webhook_lease_expires_at = datetime.now(UTC) + timedelta(
                seconds=self._settings.webhook_lease_seconds
            )
            payment.webhook_next_attempt_at = None
            payload = PaymentWebhookPayload(
                payment_id=payment.id,
                status=payment.status,
                amount=Decimal(payment.amount),
                currency=payment.currency,
                description=payment.description,
                metadata=payment.metadata_json,
                processed_at=payment.processed_at,
            )
            await session.commit()
            return WebhookDelivery(
                payment.id, token, payment.webhook_attempts, payment.webhook_url, payload
            )

    @staticmethod
    def _check_lease(payment: Payment | None, delivery: WebhookDelivery) -> Payment:
        if payment is None:
            raise PaymentNotFoundError(str(delivery.payment_id))
        if payment.webhook_lease_token != delivery.token:
            raise DeliveryLeaseLostError(str(delivery.payment_id))
        return payment

    async def mark_webhook_delivered(self, delivery: WebhookDelivery) -> None:
        async with self._session_factory() as session:
            payment = self._check_lease(
                await session.get(Payment, delivery.payment_id, with_for_update=True), delivery
            )
            payment.webhook_delivered_at = datetime.now(UTC)
            payment.webhook_lease_token = None
            payment.webhook_lease_expires_at = None
            payment.last_error = None
            await session.commit()

    async def record_delivery_failure(self, delivery: WebhookDelivery, error_message: str) -> None:
        async with self._session_factory() as session:
            payment = self._check_lease(
                await session.get(Payment, delivery.payment_id, with_for_update=True), delivery
            )
            now = datetime.now(UTC)
            payment.webhook_lease_token = None
            payment.webhook_lease_expires_at = None
            payment.last_error = error_message
            if payment.webhook_attempts >= self._settings.retry_max_attempts:
                payment.webhook_exhausted_at = now
            else:
                payment.webhook_next_attempt_at = now + timedelta(
                    seconds=self._settings.retry_base_delay_seconds
                    * 2 ** (payment.webhook_attempts - 1)
                )
            await session.commit()

    async def mark_dead_lettered(self, payment_id: UUID) -> None:
        async with self._session_factory() as session:
            payment = await session.get(Payment, payment_id, with_for_update=True)
            if payment is None:
                raise PaymentNotFoundError(str(payment_id))
            payment.webhook_dead_lettered_at = datetime.now(UTC)
            await session.commit()
