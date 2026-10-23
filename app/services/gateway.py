import asyncio
import hashlib
import random
from uuid import UUID

from app.core.config import Settings
from app.domain import PaymentOutcome, PaymentStatus


class SimulatedGateway:
    """Stable outcome per payment, including replay after a worker crash."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def process(self, payment_id: UUID) -> PaymentOutcome:
        await asyncio.sleep(
            random.uniform(
                self._settings.payment_delay_min_seconds,
                self._settings.payment_delay_max_seconds,
            )
        )
        sample = int.from_bytes(hashlib.sha256(payment_id.bytes).digest()[:8]) / 2**64
        return (
            PaymentStatus.SUCCEEDED
            if sample < self._settings.payment_success_rate
            else PaymentStatus.FAILED
        )
