"""Add observability columns to decision_logs.

Adds:
  - tech_composite_score  (Integer)  raw composite score from technical engine
  - tech_adx              (Float)    ADX value at decision time
  - tech_score_breakdown  (Text)     JSON array of per-indicator score factors
  - tech_hold_reason      (Text)     why signal was HOLD (if applicable)
  - decision_summary      (Text)     one-liner explanation of the full decision

Revision ID: a2b3c4d5e6f7
Revises: f1a2b3c4d5e6
Create Date: 2026-03-22 12:01:00.000000
"""
import sqlalchemy as sa
from alembic import op

revision      = "a2b3c4d5e6f7"
down_revision = "f1a2b3c4d5e6"
branch_labels = None
depends_on    = None


def upgrade() -> None:
    op.execute("ALTER TABLE decision_logs ADD COLUMN IF NOT EXISTS tech_composite_score INTEGER")
    op.execute("ALTER TABLE decision_logs ADD COLUMN IF NOT EXISTS tech_adx FLOAT")
    op.execute("ALTER TABLE decision_logs ADD COLUMN IF NOT EXISTS tech_score_breakdown TEXT")
    op.execute("ALTER TABLE decision_logs ADD COLUMN IF NOT EXISTS tech_hold_reason TEXT")
    op.execute("ALTER TABLE decision_logs ADD COLUMN IF NOT EXISTS decision_summary TEXT")


def downgrade() -> None:
    op.drop_column("decision_logs", "decision_summary")
    op.drop_column("decision_logs", "tech_hold_reason")
    op.drop_column("decision_logs", "tech_score_breakdown")
    op.drop_column("decision_logs", "tech_adx")
    op.drop_column("decision_logs", "tech_composite_score")
