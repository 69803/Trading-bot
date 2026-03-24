"""add investment_amount to orders and positions

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-03-20 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('orders', sa.Column('investment_amount', sa.Numeric(precision=18, scale=8), nullable=True))
    op.add_column('positions', sa.Column('investment_amount', sa.Numeric(precision=18, scale=8), nullable=True))


def downgrade() -> None:
    op.drop_column('positions', 'investment_amount')
    op.drop_column('orders', 'investment_amount')
