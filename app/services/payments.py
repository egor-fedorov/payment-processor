from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID, uuid4

from pydantic import AnyHttpUrl
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.contracts import (
    DEAD_LETTER_QUEUE_NAME,
    PAYMENT_DEAD_LETTERED,
    DeadLetterEvent,
    PaymentRequestedEvent,
    PaymentWebhookPayload,
    ScheduledEvent,
)
from app.db.models import Payment
from app.db.outbox import cancel_recovery, schedule
from app.db.payments import PaymentRepository
from app.domain import CreatePayment, DeliveryPolicy, PaymentOutcome, PaymentStatus
from app.ports import Clock, PaymentGateway


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
        async with self._session.begin():
            repository = PaymentRepository(self._session)
            payment = await repository.insert_if_absent(payload, idempotency_key)
            if payment is None:
                existing = await repository.by_key(idempotency_key)
                if existing is None:
                    raise PaymentStateError("Idempotency conflict without an existing payment")
                return existing
            await schedule(
                self._session,
                ScheduledEvent(
                    payment_id=payment.id,
                    key=f"created:{payment.id}",
                    payload=payment_event(payment).model_dump(mode="json"),
                    available_at=payment.created_at,
                    headers={"idempotency_key": idempotency_key},
                ),
            )
        return payment

    async def get_payment(self, payment_id: UUID) -> Payment | None:
        return await PaymentRepository(self._session).get(payment_id)


class PaymentStateError(Exception):
    """Persisted state or a gateway result violates a payment invariant."""


class DeliveryLeaseLostError(Exception):
    """An expired or replaced worker must not commit a result."""


@dataclass(frozen=True)
class WebhookDelivery:
    payment_id: UUID
    token: UUID
    attempt: int
    url: str
    payload: PaymentWebhookPayload


def payment_event(payment: Payment, recovery_token: UUID | None = None) -> PaymentRequestedEvent:
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
    )


@dataclass(frozen=True)
class _DeliveryReservation:
    token: UUID
    needs_processing: bool


class PaymentProcessingService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        policy: DeliveryPolicy,
        *,
        gateway: PaymentGateway,
        clock: Clock,
    ) -> None:
        self._sessions = session_factory
        self._policy = policy
        self._gateway = gateway
        self._clock = clock

    async def prepare_delivery(
        self, payment_id: UUID, recovery_token: UUID | None = None
    ) -> WebhookDelivery | None:
        reservation = await self._reserve_delivery(payment_id, recovery_token)
        if reservation is None:
            return None
        # The reservation is committed before any external I/O.
        outcome = None
        if reservation.needs_processing:
            outcome = await self._gateway.process(payment_id)
            if outcome not in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED):
                raise PaymentStateError(f"Gateway returned a nonterminal outcome: {outcome!r}")
        return await self._prepare_webhook(payment_id, reservation.token, outcome)

    async def _reserve_delivery(
        self, payment_id: UUID, recovery_token: UUID | None
    ) -> _DeliveryReservation | None:
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
            if payment.webhook_attempts >= self._policy.max_attempts:
                await self._exhaust(session, payment, "delivery lease expired; outcome unknown")
                return None
            if payment.webhook_lease_token:
                await cancel_recovery(session, payment.webhook_lease_token)
            token = uuid4()
            expires = now + timedelta(seconds=self._policy.lease_seconds)
            payment.webhook_lease_token = token
            payment.webhook_lease_expires_at = expires
            pending = payment.status == PaymentStatus.PENDING
            await schedule(
                session,
                ScheduledEvent(
                    payment_id=payment.id,
                    key=f"recovery:{token}",
                    payload=payment_event(payment, token).model_dump(mode="json"),
                    available_at=expires,
                ),
            )

        return _DeliveryReservation(token, pending)

    async def _prepare_webhook(
        self, payment_id: UUID, token: UUID, outcome: PaymentOutcome | None
    ) -> WebhookDelivery:
        async with self._sessions.begin() as session:
            payment = await self._owned(session, payment_id, token)
            if outcome is not None:
                payment.status = outcome
                payment.processed_at = self._clock.now()
            payment.webhook_attempts += 1
            payment.webhook_next_attempt_at = None
            if (
                payment.status not in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)
                or payment.processed_at is None
            ):
                raise PaymentStateError(f"Payment {payment.id} has no terminal processing result")
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
            if payment.webhook_attempts >= self._policy.max_attempts:
                await self._exhaust(session, payment, reason)
            else:
                due = self._clock.now() + timedelta(
                    seconds=self._policy.base_delay_seconds * 2 ** (payment.webhook_attempts - 1)
                )
                payment.webhook_next_attempt_at = due
                await schedule(
                    session,
                    ScheduledEvent(
                        payment_id=payment.id,
                        key=f"retry:{payment.id}:{payment.webhook_attempts}",
                        payload=payment_event(payment).model_dump(mode="json"),
                        available_at=due,
                    ),
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
            original_event=payment_event(payment),
        )
        await schedule(
            session,
            ScheduledEvent(
                payment_id=payment.id,
                key=f"dlq:{payment.id}",
                payload=event.model_dump(mode="json"),
                available_at=self._clock.now(),
                destination=DEAD_LETTER_QUEUE_NAME,
                event_type=PAYMENT_DEAD_LETTERED,
            ),
        )
