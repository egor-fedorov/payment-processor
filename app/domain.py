from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


type PaymentOutcome = Literal[PaymentStatus.SUCCEEDED, PaymentStatus.FAILED]


@dataclass(frozen=True)
class DeliveryPolicy:
    max_attempts: int
    base_delay_seconds: float
    lease_seconds: float


@dataclass(frozen=True)
class CreatePayment:
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str
