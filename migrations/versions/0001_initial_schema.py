"""0001_initial_schema

Revision ID: 0001
Revises:
Create Date: 2026-11-20
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '0001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'workflows',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('name', sa.Text(), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('clock_timestamp()'),
            nullable=False,
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('clock_timestamp()'),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint('id'),
    )

    op.create_table(
        'workflow_versions',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('workflow_id', sa.Text(), nullable=False),
        sa.Column('version', sa.Integer(), nullable=False),
        sa.Column('definition', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('clock_timestamp()'),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(['workflow_id'], ['workflows.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'workflow_id',
            'version',
            name='uq_workflow_versions_workflow_id_version',
        ),
    )

    op.create_table(
        'workflow_nodes',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('workflow_version_id', sa.Text(), nullable=False),
        sa.Column('node_key', sa.Text(), nullable=False),
        sa.Column('type', sa.Text(), nullable=False),
        sa.Column('name', sa.Text(), nullable=False),
        sa.Column(
            'config',
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column('retry_limit', sa.Integer(), server_default='0', nullable=False),
        sa.ForeignKeyConstraint(
            ['workflow_version_id'],
            ['workflow_versions.id'],
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'workflow_version_id',
            'node_key',
            name='uq_workflow_nodes_version_node_key',
        ),
    )

    op.create_table(
        'workflow_edges',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('workflow_version_id', sa.Text(), nullable=False),
        sa.Column('edge_key', sa.Text(), nullable=False),
        sa.Column('from_node_key', sa.Text(), nullable=False),
        sa.Column('to_node_key', sa.Text(), nullable=False),
        sa.Column('condition_match', sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ['workflow_version_id'],
            ['workflow_versions.id'],
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'workflow_version_id',
            'edge_key',
            name='uq_workflow_edges_version_edge_key',
        ),
    )

    op.create_table(
        'runs',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('workflow_id', sa.Text(), nullable=False),
        sa.Column('workflow_version_id', sa.Text(), nullable=False),
        sa.Column('status', sa.Text(), server_default='QUEUED', nullable=False),
        sa.Column('last_claimed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('clock_timestamp()'),
            nullable=False,
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('clock_timestamp()'),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')",
            name='ck_runs_status',
        ),
        sa.ForeignKeyConstraint(['workflow_id'], ['workflows.id'], ondelete='RESTRICT'),
        sa.ForeignKeyConstraint(
            ['workflow_version_id'],
            ['workflow_versions.id'],
            ondelete='RESTRICT',
        ),
        sa.PrimaryKeyConstraint('id'),
    )

    op.create_table(
        'node_runs',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('run_id', sa.Text(), nullable=False),
        sa.Column('node_key', sa.Text(), nullable=False),
        sa.Column('status', sa.Text(), server_default='PENDING', nullable=False),
        sa.Column('output', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('error', sa.Text(), nullable=True),
        sa.Column('run_after', sa.DateTime(timezone=True), nullable=True),
        sa.Column('lease_generation', sa.BigInteger(), server_default='0', nullable=False),
        sa.Column('lease_expires_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('clock_timestamp()'),
            nullable=False,
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('clock_timestamp()'),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'READY', 'RUNNING', 'SUCCEEDED', "
            "'FAILED', 'SKIPPED', 'CANCELLED')",
            name='ck_node_runs_status',
        ),
        sa.ForeignKeyConstraint(['run_id'], ['runs.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('run_id', 'node_key', name='uq_node_runs_run_id_node_key'),
    )

    op.create_table(
        'edge_runs',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('run_id', sa.Text(), nullable=False),
        sa.Column('edge_key', sa.Text(), nullable=False),
        sa.Column('from_node_key', sa.Text(), nullable=False),
        sa.Column('to_node_key', sa.Text(), nullable=False),
        sa.Column('status', sa.Text(), server_default='PENDING', nullable=False),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('clock_timestamp()'),
            nullable=False,
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('clock_timestamp()'),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'TAKEN', 'DEAD')",
            name='ck_edge_runs_status',
        ),
        sa.ForeignKeyConstraint(['run_id'], ['runs.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('run_id', 'edge_key', name='uq_edge_runs_run_id_edge_key'),
    )

    op.create_table(
        'node_attempts',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('node_run_id', sa.Text(), nullable=False),
        sa.Column('attempt_number', sa.Integer(), nullable=False),
        sa.Column('lease_generation', sa.BigInteger(), nullable=False),
        sa.Column('status', sa.Text(), nullable=False),
        sa.Column('error', sa.Text(), nullable=True),
        sa.Column(
            'started_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('clock_timestamp()'),
            nullable=False,
        ),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['node_run_id'], ['node_runs.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'node_run_id',
            'attempt_number',
            name='uq_node_attempts_node_run_attempt',
        ),
    )

    op.create_table(
        'run_events',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('run_id', sa.Text(), nullable=False),
        sa.Column('event_type', sa.Text(), nullable=False),
        sa.Column(
            'payload',
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('clock_timestamp()'),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(['run_id'], ['runs.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )

    op.create_index(
        'idx_node_runs_ready',
        'node_runs',
        ['run_id', 'run_after'],
        unique=False,
        postgresql_where=sa.text("status = 'READY'"),
    )

    op.create_index(
        'idx_node_runs_running_lease',
        'node_runs',
        ['run_id', 'lease_expires_at'],
        unique=False,
        postgresql_where=sa.text("status = 'RUNNING'"),
    )

    op.create_index(
        'idx_runs_claimable',
        'runs',
        [sa.text('last_claimed_at ASC NULLS FIRST'), sa.text('created_at ASC')],
        unique=False,
        postgresql_where=sa.text("status IN ('QUEUED', 'RUNNING')"),
    )


def downgrade() -> None:
    op.drop_index('idx_runs_claimable', table_name='runs')
    op.drop_index('idx_node_runs_running_lease', table_name='node_runs')
    op.drop_index('idx_node_runs_ready', table_name='node_runs')
    op.drop_table('run_events')
    op.drop_table('node_attempts')
    op.drop_table('edge_runs')
    op.drop_table('node_runs')
    op.drop_table('runs')
    op.drop_table('workflow_edges')
    op.drop_table('workflow_nodes')
    op.drop_table('workflow_versions')
    op.drop_table('workflows')
