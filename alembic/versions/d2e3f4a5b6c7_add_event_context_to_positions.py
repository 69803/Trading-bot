"""Add event_context column to positions.

Stores the event-risk context at trade-open time so the analytics service
can classify trades as normal / reduced_size_due_to_event without having
to re-run the event-risk check retroactively.

Revision ID: d2e3f4a5b6c7
Revises: c4d5e6f7a8b9
Create Date: 2026-03-23 12:00:00.000000
"""
import sqlalchemy as sa
from alembic import op

revision = "d2e3f4a5b6c7"
down_revision = "c4d5e6f7a8b9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS event_context VARCHAR(40)")


def downgrade() -> None:
    op.drop_column("positions", "event_context")
