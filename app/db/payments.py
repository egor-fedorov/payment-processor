from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Payment


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, payment_id: UUID, *, lock: bool = False) -> Payment | None:
        return await self.session.get(Payment, payment_id, with_for_update=lock)

    async def by_key(self, key: str) -> Payment | None:
        return await self.session.scalar(select(Payment).where(Payment.idempotency_key == key))
