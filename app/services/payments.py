import base64
import hashlib
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

from pydantic import AnyHttpUrl
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.contracts import DeadLetterEvent, PaymentRequestedEvent, PaymentWebhookPayload
from app.core.config import Settings
from app.db.models import OutboxMessage, Payment
from app.db.outbox import cancel_recovery, schedule
from app.db.payments import PaymentRepository
from app.domain import CreatePayment, PaymentStatus
from app.services.gateway import Clock, PaymentGateway, SimulatedGateway, SystemClock


class PaymentNotFoundError(Exception):
    """Raised when a payment does not exist."""


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_payment(
        self,
        payload: CreatePayment,
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
                webhook_url=AnyHttpUrl(payload.webhook_url),
                created_at=payment.created_at,
            )
            self._session.add(
                OutboxMessage(
                    aggregate_type="payment",
                    deduplication_key=f"created:{payment.id}",
                    destination="payments.new",
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
        return await PaymentRepository(self._session).by_key(idempotency_key)

    async def get_payment(self, payment_id: UUID) -> Payment | None:
        return await PaymentRepository(self._session).get(payment_id)


class DeliveryLeaseLostError(Exception):
    """An expired or replaced worker must not commit a result."""


@dataclass(frozen=True)
class WebhookDelivery:
    payment_id: UUID
    token: UUID
    attempt: int
    url: str
    payload: PaymentWebhookPayload


def payment_event(payment: Payment, recovery_token: UUID | None = None) -> dict[str, Any]:
    return PaymentRequestedEvent(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_json,
        webhook_url=AnyHttpUrl(payment.webhook_url),
        created_at=payment.created_at,
        delivery_attempt=payment.webhook_attempts,
        recovery_token=recovery_token,
    ).model_dump(mode="json")


class PaymentProcessingService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        *,
        gateway: PaymentGateway | None = None,
        clock: Clock | None = None,
    ) -> None:
        self._sessions = session_factory
        self._settings = settings
        self._gateway = gateway or SimulatedGateway(settings)
        self._clock = clock or SystemClock()

    async def claim_delivery(
        self, payment_id: UUID, recovery_token: UUID | None = None
    ) -> WebhookDelivery | None:
        async with self._sessions.begin() as session:
            payment = await PaymentRepository(session).get(payment_id, lock=True)
            if payment is None:
                raise PaymentNotFoundError(str(payment_id))
            if payment.webhook_delivered_at or payment.webhook_exhausted_at:
                return None
            now = self._clock.now()
            if recovery_token is not None and payment.webhook_lease_token != recovery_token:
                return None
            # It is safe to ack duplicates: the owner created a durable recovery
            # event in the same transaction as its reservation.
            if payment.webhook_lease_expires_at and payment.webhook_lease_expires_at > now:
                return None
            if payment.webhook_next_attempt_at and payment.webhook_next_attempt_at > now:
                return None
            if payment.webhook_attempts >= self._settings.retry_max_attempts:
                await self._exhaust(session, payment, "delivery lease expired; outcome unknown")
                return None
            if payment.webhook_lease_token:
                await cancel_recovery(session, payment.webhook_lease_token)
            token = uuid4()
            expires = now + timedelta(seconds=self._settings.webhook_lease_seconds)
            payment.webhook_lease_token = token
            payment.webhook_lease_expires_at = expires
            pending = payment.status == PaymentStatus.PENDING
            await schedule(
                session,
                payment_id=payment.id,
                key=f"recovery:{token}",
                payload=payment_event(payment, token),
                available_at=expires,
            )

        outcome = await self._gateway.process(payment_id) if pending else None
        async with self._sessions.begin() as session:
            payment = await self._owned(session, payment_id, token)
            if outcome is not None:
                payment.status = outcome
                payment.processed_at = self._clock.now()
            payment.webhook_attempts += 1
            payment.webhook_next_attempt_at = None
            assert payment.processed_at is not None
            payload = PaymentWebhookPayload(
                payment_id=payment.id,
                status=payment.status,
                amount=payment.amount,
                currency=payment.currency,
                description=payment.description,
                metadata=payment.metadata_json,
                processed_at=payment.processed_at,
            )
            return WebhookDelivery(
                payment.id, token, payment.webhook_attempts, payment.webhook_url, payload
            )

    async def _owned(self, session: AsyncSession, payment_id: UUID, token: UUID) -> Payment:
        payment = await PaymentRepository(session).get(payment_id, lock=True)
        if (
            payment is None
            or payment.webhook_lease_token != token
            or payment.webhook_lease_expires_at is None
            or payment.webhook_lease_expires_at <= self._clock.now()
        ):
            raise DeliveryLeaseLostError(str(payment_id))
        return payment

    async def mark_webhook_delivered(self, delivery: WebhookDelivery) -> None:
        async with self._sessions.begin() as session:
            payment = await self._owned(session, delivery.payment_id, delivery.token)
            payment.webhook_delivered_at = self._clock.now()
            payment.last_error = None
            payment.webhook_lease_token = None
            payment.webhook_lease_expires_at = None
            await cancel_recovery(session, delivery.token)

    async def record_delivery_failure(self, delivery: WebhookDelivery, reason: str) -> None:
        async with self._sessions.begin() as session:
            payment = await self._owned(session, delivery.payment_id, delivery.token)
            await cancel_recovery(session, delivery.token)
            payment.webhook_lease_token = None
            payment.webhook_lease_expires_at = None
            payment.last_error = reason
            if payment.webhook_attempts >= self._settings.retry_max_attempts:
                await self._exhaust(session, payment, reason)
            else:
                due = self._clock.now() + timedelta(
                    seconds=self._settings.retry_base_delay_seconds
                    * 2 ** (payment.webhook_attempts - 1)
                )
                payment.webhook_next_attempt_at = due
                await schedule(
                    session,
                    payment_id=payment.id,
                    key=f"retry:{payment.id}:{payment.webhook_attempts}",
                    payload=payment_event(payment),
                    available_at=due,
                )

    async def _exhaust(self, session: AsyncSession, payment: Payment, reason: str) -> None:
        payment.webhook_exhausted_at = self._clock.now()
        payment.last_error = reason
        if payment.webhook_lease_token:
            await cancel_recovery(session, payment.webhook_lease_token)
        payment.webhook_lease_token = None
        payment.webhook_lease_expires_at = None
        event = DeadLetterEvent(
            payment_id=payment.id,
            delivery_attempt=payment.webhook_attempts,
            failure_reason=reason,
            failed_at=self._clock.now(),
            original_event=PaymentRequestedEvent.model_validate(payment_event(payment)),
        )
        await schedule(
            session,
            payment_id=payment.id,
            key=f"dlq:{payment.id}",
            payload=event.model_dump(mode="json"),
            available_at=self._clock.now(),
            destination="payments.dlq",
            event_type="payments.dead_lettered",
        )

    async def record_invalid(self, body: bytes, reason: str) -> None:
        async with self._sessions.begin() as session:
            await schedule(
                session,
                payment_id=uuid4(),
                key=f"invalid:{hashlib.sha256(body).hexdigest()}",
                payload={
                    "failure_reason": reason,
                    "failed_at": self._clock.now().isoformat(),
                    "original_body_base64": base64.b64encode(body).decode("ascii"),
                },
                available_at=self._clock.now(),
                destination="payments.dlq",
                event_type="payments.invalid",
            )
