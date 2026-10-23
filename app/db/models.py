import uuid
from datetime import datetime
from decimal import Decimal
from enum import Enum as PythonEnum
from typing import Any

from sqlalchemy import DateTime, Enum, Index, Numeric, String, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.domain import Currency, PaymentStatus


def enum_values(enum_cls: type[PythonEnum]) -> list[str]:
    return [member.value for member in enum_cls]


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[Currency] = mapped_column(
        Enum(Currency, name="currency_enum", values_callable=enum_values),
        nullable=False,
    )
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    status: Mapped[PaymentStatus] = mapped_column(
        Enum(PaymentStatus, name="payment_status_enum", values_callable=enum_values),
        nullable=False,
        default=PaymentStatus.PENDING,
        server_default=text("'pending'"),
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    webhook_url: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_lease_token: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    webhook_lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_exhausted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_attempts: Mapped[int] = mapped_column(
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    last_error: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class OutboxMessage(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_available",
            "available_at",
            "id",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    aggregate_type: Mapped[str] = mapped_column(String(50), nullable=False)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    headers: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    destination: Mapped[str] = mapped_column(
        String(100), nullable=False, default="payments.new", server_default="payments.new"
    )
    deduplication_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    lease_token: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(
        nullable=False, default=0, server_default=text("0")
    )
    last_error: Mapped[str | None] = mapped_column(Text)
