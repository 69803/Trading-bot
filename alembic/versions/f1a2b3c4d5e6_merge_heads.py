"""Merge decision_logs and historical_tables heads.

Revision ID: f1a2b3c4d5e6
Revises: 730f6cb52035, d1e2f3a4b5c6
Create Date: 2026-03-22 12:00:00.000000
"""
from alembic import op

# revision identifiers
revision = "f1a2b3c4d5e6"
down_revision = ("730f6cb52035", "d1e2f3a4b5c6")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass   # merge only — no schema changes


def downgrade() -> None:
    pass
