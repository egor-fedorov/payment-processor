"""Persist webhook delivery leases and terminal state.

Revision ID: 20261023_0002
Revises: 20260422_0001
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20261023_0002"
down_revision = "20260422_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("payments", sa.Column("webhook_lease_token", postgresql.UUID(as_uuid=True)))
    for name in (
        "webhook_lease_expires_at",
        "webhook_next_attempt_at",
        "webhook_exhausted_at",
        "webhook_dead_lettered_at",
    ):
        op.add_column("payments", sa.Column(name, sa.DateTime(timezone=True)))
    # Previously this field counted failures, not all attempts. Include the
    # successful delivery for historical records; unknown interrupted attempts
    # cannot be reconstructed. Undelivered records retain their failure budget.
    op.execute(
        "UPDATE payments SET webhook_attempts = webhook_attempts + 1 "
        "WHERE webhook_delivered_at IS NOT NULL"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE payments SET webhook_attempts = GREATEST(webhook_attempts - 1, 0) "
        "WHERE webhook_delivered_at IS NOT NULL"
    )
    for name in (
        "webhook_dead_lettered_at",
        "webhook_exhausted_at",
        "webhook_next_attempt_at",
        "webhook_lease_expires_at",
        "webhook_lease_token",
    ):
        op.drop_column("payments", name)
