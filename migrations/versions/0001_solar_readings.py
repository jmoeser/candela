"""Create solar_readings table

Revision ID: 0001
Revises:
Create Date: 2026-04-05

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "solar_readings",
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("solar_w", sa.Integer(), nullable=False),
        sa.Column("grid_w", sa.Integer(), nullable=False),
        sa.Column("load_w", sa.Integer(), nullable=False),
        sa.Column("daily_yield_kwh", sa.Float(), nullable=True),
        sa.Column("total_yield_kwh", sa.Float(), nullable=True),
        sa.Column("inverter_temp_c", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("ts"),
    )
    op.create_index(
        "solar_readings_ts_idx",
        "solar_readings",
        [sa.text("ts DESC")],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("solar_readings_ts_idx", table_name="solar_readings")
    op.drop_table("solar_readings")
