"""0002_runtime_definition_composite_fks

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-20
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Unique constraint on runs (id, workflow_version_id)
    op.create_unique_constraint(
        "uq_runs_id_workflow_version_id",
        "runs",
        ["id", "workflow_version_id"],
    )

    # 2. Add workflow_version_id to node_runs
    op.add_column(
        "node_runs",
        sa.Column("workflow_version_id", sa.Text(), nullable=True),
    )
    op.execute("""
        UPDATE node_runs
        SET workflow_version_id = runs.workflow_version_id
        FROM runs
        WHERE node_runs.run_id = runs.id;
    """)
    op.alter_column("node_runs", "workflow_version_id", nullable=False)

    op.drop_constraint("node_runs_run_id_fkey", "node_runs", type_="foreignkey")
    op.create_foreign_key(
        "fk_node_runs_run_version",
        "node_runs",
        "runs",
        ["run_id", "workflow_version_id"],
        ["id", "workflow_version_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_node_runs_version_node",
        "node_runs",
        "workflow_nodes",
        ["workflow_version_id", "node_key"],
        ["workflow_version_id", "node_key"],
        ondelete="RESTRICT",
    )

    # 3. Add workflow_version_id to edge_runs
    op.add_column(
        "edge_runs",
        sa.Column("workflow_version_id", sa.Text(), nullable=True),
    )
    op.execute("""
        UPDATE edge_runs
        SET workflow_version_id = runs.workflow_version_id
        FROM runs
        WHERE edge_runs.run_id = runs.id;
    """)
    op.alter_column("edge_runs", "workflow_version_id", nullable=False)

    op.drop_constraint("edge_runs_run_id_fkey", "edge_runs", type_="foreignkey")
    op.create_foreign_key(
        "fk_edge_runs_run_version",
        "edge_runs",
        "runs",
        ["run_id", "workflow_version_id"],
        ["id", "workflow_version_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_edge_runs_version_edge",
        "edge_runs",
        "workflow_edges",
        ["workflow_version_id", "edge_key"],
        ["workflow_version_id", "edge_key"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    # 1. Revert edge_runs constraints
    op.drop_constraint("fk_edge_runs_version_edge", "edge_runs", type_="foreignkey")
    op.drop_constraint("fk_edge_runs_run_version", "edge_runs", type_="foreignkey")
    op.create_foreign_key(
        "edge_runs_run_id_fkey",
        "edge_runs",
        "runs",
        ["run_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.drop_column("edge_runs", "workflow_version_id")

    # 2. Revert node_runs constraints
    op.drop_constraint("fk_node_runs_version_node", "node_runs", type_="foreignkey")
    op.drop_constraint("fk_node_runs_run_version", "node_runs", type_="foreignkey")
    op.create_foreign_key(
        "node_runs_run_id_fkey",
        "node_runs",
        "runs",
        ["run_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.drop_column("node_runs", "workflow_version_id")

    # 3. Revert unique constraint on runs
    op.drop_constraint("uq_runs_id_workflow_version_id", "runs", type_="unique")
