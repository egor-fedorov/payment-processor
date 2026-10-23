from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import delete, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.contracts import Publication
from app.core.config import Settings
from app.db.models import OutboxMessage


async def schedule(
    session: AsyncSession,
    *,
    payment_id: UUID,
    key: str,
    payload: dict[str, Any],
    available_at: datetime,
    destination: str = "payments.new",
    event_type: str = "payments.requested",
) -> None:
    await session.execute(
        insert(OutboxMessage)
        .values(
            id=uuid4(),
            aggregate_type="payment",
            aggregate_id=payment_id,
            event_type=event_type,
            payload=payload,
            headers={},
            destination=destination,
            deduplication_key=key,
            available_at=available_at,
        )
        .on_conflict_do_nothing(index_elements=[OutboxMessage.deduplication_key])
    )


async def cancel_recovery(session: AsyncSession, token: UUID) -> None:
    await session.execute(
        delete(OutboxMessage).where(
            OutboxMessage.deduplication_key == f"recovery:{token}",
            OutboxMessage.published_at.is_(None),
        )
    )


class OutboxRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession], settings: Settings) -> None:
        self._sessions = sessions
        self._settings = settings

    async def claim(self, now: datetime) -> list[Publication]:
        async with self._sessions.begin() as session:
            rows = await session.scalars(
                select(OutboxMessage)
                .where(
                    OutboxMessage.published_at.is_(None),
                    OutboxMessage.available_at <= now,
                    or_(OutboxMessage.lease_until.is_(None), OutboxMessage.lease_until <= now),
                )
                .order_by(OutboxMessage.available_at, OutboxMessage.id)
                .limit(
                    min(self._settings.outbox_batch_size, self._settings.outbox_publish_concurrency)
                )
                .with_for_update(skip_locked=True)
            )
            result = []
            for row in rows:
                token = uuid4()
                row.lease_token = token
                row.lease_until = now + timedelta(seconds=self._settings.outbox_lease_seconds)
                row.publish_attempts += 1
                result.append(
                    Publication(
                        row.id,
                        token,
                        row.aggregate_id,
                        row.destination,
                        row.payload,
                        row.publish_attempts,
                    )
                )
            return result

    async def complete(self, item: Publication, now: datetime, error: str | None = None) -> None:
        values: dict[str, Any] = {"lease_token": None, "lease_until": None, "last_error": error}
        if error is None:
            values["published_at"] = now
        else:
            values["available_at"] = now + timedelta(
                seconds=min(0.25 * 2 ** min(item.attempt - 1, 7), 30)
            )
        async with self._sessions.begin() as session:
            await session.execute(
                update(OutboxMessage)
                .where(
                    OutboxMessage.id == item.id,
                    OutboxMessage.lease_token == item.token,
                )
                .values(**values)
            )

    async def prune(self, now: datetime) -> None:
        cutoff = now - timedelta(hours=self._settings.outbox_retention_hours)
        async with self._sessions.begin() as session:
            expired = (
                select(OutboxMessage.id)
                .where(OutboxMessage.published_at < cutoff)
                .limit(1000)
                .with_for_update(skip_locked=True)
            )
            await session.execute(delete(OutboxMessage).where(OutboxMessage.id.in_(expired)))
