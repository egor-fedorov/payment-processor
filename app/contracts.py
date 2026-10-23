from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, Field

from app.domain import Currency, PaymentStatus


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
    status: PaymentStatus
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
