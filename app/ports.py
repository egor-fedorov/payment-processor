from datetime import datetime
from typing import Protocol
from uuid import UUID

from app.contracts import PaymentWebhookPayload, Publication
from app.domain import PaymentOutcome


class Clock(Protocol):
    def now(self) -> datetime: ...


class PaymentGateway(Protocol):
    """Return a terminal, stable outcome for repeated calls with the same payment ID."""

    async def process(self, payment_id: UUID) -> PaymentOutcome: ...


class WebhookDeliveryError(Exception):
    """The receiver did not confirm delivery within the request deadline."""


class WebhookClient(Protocol):
    """Confirm delivery or raise WebhookDeliveryError; preserve task cancellation."""

    async def send(self, url: str, payload: PaymentWebhookPayload) -> None: ...


class EventPublisher(Protocol):
    """Return after broker confirmation; raise on failure or uncertain outcome."""

    async def publish(self, item: Publication) -> None: ...


class OutboxStore(Protocol):
    async def claim(self, now: datetime) -> list[Publication]: ...

    async def complete(
        self, item: Publication, now: datetime, error: str | None = None
    ) -> None: ...

    async def prune(self, now: datetime) -> None: ...
