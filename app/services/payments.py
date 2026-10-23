import asyncio
import random
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.schemas import PaymentCreateRequest
from app.core.config import get_settings
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
            constraint = (
                getattr(cause, "constraint_name", None)
                or getattr(getattr(original, "diag", None), "constraint_name", None)
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


class PaymentProcessingService:
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._session_factory = session_factory
        self._settings = get_settings()

    async def prepare_webhook_payload(self, payment_id: UUID) -> PaymentWebhookPayload | None:
        async with self._session_factory() as session:
            payment = await session.scalar(
                select(Payment).where(Payment.id == payment_id).with_for_update()
            )
            if payment is None:
                raise PaymentNotFoundError(str(payment_id))

            if payment.webhook_delivered_at is not None:
                return None

            if payment.status == PaymentStatus.PENDING:
                await asyncio.sleep(
                    random.uniform(
                        self._settings.payment_delay_min_seconds,
                        self._settings.payment_delay_max_seconds,
                    )
                )
                payment.status = (
                    PaymentStatus.SUCCEEDED
                    if random.random() <= self._settings.payment_success_rate
                    else PaymentStatus.FAILED
                )
                payment.processed_at = datetime.now(timezone.utc)

            await session.commit()

            if payment.processed_at is None:
                payment.processed_at = datetime.now(timezone.utc)

            return PaymentWebhookPayload(
                payment_id=payment.id,
                status=payment.status,
                amount=Decimal(payment.amount),
                currency=payment.currency,
                description=payment.description,
                metadata=payment.metadata_json,
                processed_at=payment.processed_at,
            )

    async def mark_webhook_delivered(self, payment_id: UUID) -> None:
        async with self._session_factory() as session:
            payment = await session.get(Payment, payment_id, with_for_update=True)
            if payment is None:
                raise PaymentNotFoundError(str(payment_id))

            if payment.webhook_delivered_at is None:
                payment.webhook_delivered_at = datetime.now(timezone.utc)
            payment.last_error = None
            await session.commit()

    async def record_delivery_failure(self, payment_id: UUID, error_message: str) -> None:
        async with self._session_factory() as session:
            payment = await session.get(Payment, payment_id, with_for_update=True)
            if payment is None:
                return

            payment.webhook_attempts += 1
            payment.last_error = error_message
            await session.commit()
