import base64
import hashlib
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.outbox import schedule
from app.ports import Clock


class RejectedMessageService:
    def __init__(self, sessions: async_sessionmaker[AsyncSession], clock: Clock) -> None:
        self._sessions = sessions
        self._clock = clock

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
