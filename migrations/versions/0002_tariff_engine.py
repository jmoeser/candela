"""Create tariff_plans, tariff_rates, aemo_trading_prices tables

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-05

Notes
-----
``days_of_week`` and ``months`` in ``tariff_rates`` are stored as JSON TEXT
rather than integer arrays, for SQLite compatibility.  Application code is
responsible for serialising/deserialising with ``json.loads`` / ``json.dumps``.

``window_start``, ``window_end``, ``demand_window_start``, and
``demand_window_end`` are stored as ``sa.Time()`` — TEXT on SQLite,
TIME on PostgreSQL.

``aemo_trading_prices`` is included here rather than Phase 1 because the
engine is the primary consumer.  The collector's ``aemo.py`` upserts into
this table regardless of which migration created it.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: Union[str, Sequence[str], None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "tariff_plans",
        sa.Column("id", sa.Integer(), nullable=False, autoincrement=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("retailer", sa.Text(), nullable=True),
        sa.Column("plan_type", sa.Text(), nullable=False),
        sa.Column("supply_charge_daily_cents", sa.Numeric(8, 4), nullable=False),
        sa.Column("feed_in_tariff_cents", sa.Numeric(8, 4), nullable=True),
        sa.Column("valid_from", sa.Date(), nullable=False),
        sa.Column("valid_to", sa.Date(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "tariff_rates",
        sa.Column("id", sa.Integer(), nullable=False, autoincrement=True),
        sa.Column("plan_id", sa.Integer(), nullable=False),
        sa.Column("rate_type", sa.Text(), nullable=False),
        sa.Column("cents_per_kwh", sa.Numeric(8, 4), nullable=True),
        sa.Column("cents_per_kw", sa.Numeric(8, 4), nullable=True),
        sa.Column("window_start", sa.Time(), nullable=True),
        sa.Column("window_end", sa.Time(), nullable=True),
        # JSON-encoded list[int]; e.g. "[0,1,2,3,4]" for Mon–Fri (0=Mon,6=Sun)
        sa.Column("days_of_week", sa.Text(), nullable=True),
        # JSON-encoded list[int]; e.g. "[1,2,3]" for Jan–Mar
        sa.Column("months", sa.Text(), nullable=True),
        sa.Column("demand_window_start", sa.Time(), nullable=True),
        sa.Column("demand_window_end", sa.Time(), nullable=True),
        sa.ForeignKeyConstraint(["plan_id"], ["tariff_plans.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "aemo_trading_prices",
        sa.Column("interval_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("interval_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("rrp_per_mwh", sa.Numeric(10, 4), nullable=False),
        sa.Column("region", sa.Text(), nullable=False, server_default="QLD1"),
        sa.PrimaryKeyConstraint("interval_start", "region"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("aemo_trading_prices")
    op.drop_table("tariff_rates")
    op.drop_table("tariff_plans")
