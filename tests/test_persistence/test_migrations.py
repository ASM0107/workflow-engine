import os

from alembic import command
from alembic.config import Config
from workflow_engine.persistence.connection import get_connection


def test_alembic_upgrade_and_downgrade_cycle() -> None:
    alembic_ini_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "alembic.ini")
    )
    alembic_cfg = Config(alembic_ini_path)

    command.downgrade(alembic_cfg, "base")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name != 'alembic_version';
            """)
            tables = cur.fetchall()
            assert len(tables) == 0

    command.upgrade(alembic_cfg, "head")

    expected_tables = {
        "workflows",
        "workflow_versions",
        "workflow_nodes",
        "workflow_edges",
        "runs",
        "node_runs",
        "edge_runs",
        "node_attempts",
        "run_events",
    }

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name != 'alembic_version';
            """)
            created_tables = {row["table_name"] for row in cur.fetchall()}
            assert expected_tables.issubset(created_tables)
