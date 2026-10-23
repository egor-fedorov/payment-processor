from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, Field

from app.domain import Currency, PaymentOutcome

PAYMENTS_QUEUE_NAME = "payments.new"
DEAD_LETTER_QUEUE_NAME = "payments.dlq"
PAYMENT_REQUESTED = "payments.requested"
PAYMENT_DEAD_LETTERED = "payments.dead_lettered"
PAYMENT_INVALID = "payments.invalid"


class PaymentRequestedEvent(BaseModel):
    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: AnyHttpUrl
    created_at: datetime
    delivery_attempt: int = Field(default=0, ge=0)
    recovery_token: UUID | None = None


class PaymentWebhookPayload(BaseModel):
    payment_id: UUID
    status: PaymentOutcome
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    processed_at: datetime


class DeadLetterEvent(BaseModel):
    payment_id: UUID
    delivery_attempt: int
    failure_reason: str
    failed_at: datetime
    original_event: PaymentRequestedEvent


@dataclass(frozen=True)
class Publication:
    id: UUID
    token: UUID
    aggregate_id: UUID
    destination: str
    payload: dict[str, Any]
    attempt: int


@dataclass(frozen=True, kw_only=True)
class ScheduledEvent:
    payment_id: UUID
    key: str
    payload: dict[str, Any]
    available_at: datetime
    destination: str = PAYMENTS_QUEUE_NAME
    event_type: str = PAYMENT_REQUESTED
    headers: dict[str, Any] = field(default_factory=dict)
