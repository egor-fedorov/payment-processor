from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Payment
from app.domain import CreatePayment


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, payment_id: UUID, *, lock: bool = False) -> Payment | None:
        return await self.session.get(Payment, payment_id, with_for_update=lock)

    async def by_key(self, key: str) -> Payment | None:
        return await self.session.scalar(select(Payment).where(Payment.idempotency_key == key))

    async def insert_if_absent(self, payload: CreatePayment, key: str) -> Payment | None:
        return await self.session.scalar(
            insert(Payment)
            .values(
                amount=payload.amount,
                currency=payload.currency,
                description=payload.description,
                metadata_json=payload.metadata,
                webhook_url=payload.webhook_url,
                idempotency_key=key,
            )
            .on_conflict_do_nothing(constraint="uq_payments_idempotency_key")
            .returning(Payment)
        )
