"""Create load_events table

Revision ID: 0003
Revises: 0002
Create Date: 2026-04-05
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: Union[str, Sequence[str], None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "load_events",
        sa.Column("id", sa.Integer(), nullable=False, autoincrement=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("load_name", sa.Text(), nullable=False),
        # load_name: 'ev_charging' | 'hot_water_heatpump' | 'hot_water_boost' | 'unknown'
        sa.Column("avg_watts", sa.Integer(), nullable=True),
        sa.Column("kwh", sa.Numeric(8, 3), nullable=True),
        sa.Column("confidence", sa.Numeric(4, 3), nullable=True),  # 0.000–1.000
        sa.Column("source", sa.Text(), nullable=False),
        # source: 'inferred' | 'tesla_api' | 'manual'
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("load_events_started_at_idx", "load_events", ["started_at"])
    op.create_index("load_events_load_name_idx", "load_events", ["load_name"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("load_events_load_name_idx", "load_events")
    op.drop_index("load_events_started_at_idx", "load_events")
    op.drop_table("load_events")
