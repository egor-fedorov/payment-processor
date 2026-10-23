"""Scheduled outbox publications and reclaimable publication leases."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20261023_0003"
down_revision = "20261023_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "outbox",
        sa.Column("destination", sa.String(100), nullable=False, server_default="payments.new"),
    )
    op.add_column("outbox", sa.Column("deduplication_key", sa.String(255)))
    op.execute("UPDATE outbox SET deduplication_key = 'legacy:' || id::text")
    op.alter_column("outbox", "deduplication_key", nullable=False)
    op.create_unique_constraint("uq_outbox_deduplication_key", "outbox", ["deduplication_key"])
    op.add_column(
        "outbox",
        sa.Column(
            "available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
    )
    op.add_column("outbox", sa.Column("lease_token", postgresql.UUID(as_uuid=True)))
    op.add_column("outbox", sa.Column("lease_until", sa.DateTime(timezone=True)))
    op.add_column(
        "outbox", sa.Column("publish_attempts", sa.Integer(), nullable=False, server_default="0")
    )
    op.add_column("outbox", sa.Column("last_error", sa.Text()))
    op.drop_index("ix_outbox_published_at_created_at", "outbox")
    op.create_index(
        "ix_outbox_available",
        "outbox",
        ["available_at", "id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_column("payments", "webhook_dead_lettered_at")


def downgrade() -> None:
    op.add_column("payments", sa.Column("webhook_dead_lettered_at", sa.DateTime(timezone=True)))
    op.drop_index("ix_outbox_available", "outbox")
    op.create_index("ix_outbox_published_at_created_at", "outbox", ["published_at", "created_at"])
    op.drop_constraint("uq_outbox_deduplication_key", "outbox", type_="unique")
    for name in (
        "last_error",
        "publish_attempts",
        "lease_until",
        "lease_token",
        "available_at",
        "deduplication_key",
        "destination",
    ):
        op.drop_column("outbox", name)
