"""Add current_plan_periods table

Revision ID: 0004
Revises: 0003
Create Date: 2026-04-07
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: Union[str, Sequence[str], None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "current_plan_periods",
        sa.Column("id", sa.Integer(), nullable=False, autoincrement=True),
        sa.Column("plan_id", sa.Integer(), nullable=False),
        sa.Column("active_from", sa.Date(), nullable=False),
        sa.Column("active_to", sa.Date(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["plan_id"], ["tariff_plans.id"]),
    )
    op.create_index(
        "current_plan_periods_active_from_idx",
        "current_plan_periods",
        ["active_from"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("current_plan_periods_active_from_idx", "current_plan_periods")
    op.drop_table("current_plan_periods")
