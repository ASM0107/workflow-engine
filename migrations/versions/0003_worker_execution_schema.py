"""0003_worker_execution_schema

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-20
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add lease_owner to node_runs
    op.add_column(
        "node_runs",
        sa.Column("lease_owner", sa.Text(), nullable=True),
    )

    # 2. Add worker_id and output to node_attempts
    op.add_column(
        "node_attempts",
        sa.Column("worker_id", sa.Text(), nullable=True),
    )
    op.add_column(
        "node_attempts",
        sa.Column(
            "output",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("node_attempts", "output")
    op.drop_column("node_attempts", "worker_id")
    op.drop_column("node_runs", "lease_owner")
