"""add is_paper to positions

Revision ID: f2a3b4c5d6e7
Revises: e2f3a4b5c6d7
Create Date: 2026-03-23 00:00:00.000000

Adds ``is_paper`` (Boolean, nullable) to the ``positions`` table so paper
and live trades can be filtered independently in analytics endpoints.

NULL rows (created before this migration) are treated as paper for
backward compatibility — the system has only ever run paper trades up to
this point.
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "f2a3b4c5d6e7"
down_revision = "e2f3a4b5c6d7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS is_paper BOOLEAN")


def downgrade() -> None:
    op.drop_column("positions", "is_paper")
