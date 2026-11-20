import uuid

import pytest
from psycopg.errors import CheckViolation, ForeignKeyViolation, UniqueViolation
from workflow_engine.persistence.connection import get_connection


def test_foreign_key_constraint_on_workflow_version() -> None:
    query = """
        INSERT INTO workflow_versions (id, workflow_id, version, definition)
        VALUES (%s, %s, 1, '{}'::jsonb);
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            with pytest.raises(ForeignKeyViolation):
                cur.execute(query, (uuid.uuid4().hex, "non_existent_wf_id"))
        conn.rollback()


def test_unique_constraint_on_workflow_version() -> None:
    wf_id = uuid.uuid4().hex
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO workflows (id, name) VALUES (%s, 'Test WF');", (wf_id,))
            cur.execute(
                """
                INSERT INTO workflow_versions (id, workflow_id, version, definition)
                VALUES (%s, %s, 1, '{}'::jsonb);
                """,
                (uuid.uuid4().hex, wf_id),
            )
            with pytest.raises(UniqueViolation):
                cur.execute(
                    """
                    INSERT INTO workflow_versions (id, workflow_id, version, definition)
                    VALUES (%s, %s, 1, '{}'::jsonb);
                    """,
                    (uuid.uuid4().hex, wf_id),
                )
        conn.rollback()


def test_check_constraint_on_run_status() -> None:
    wf_id = uuid.uuid4().hex
    ver_id = uuid.uuid4().hex
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO workflows (id, name) VALUES (%s, 'Test WF');", (wf_id,))
            cur.execute(
                """
                INSERT INTO workflow_versions (id, workflow_id, version, definition)
                VALUES (%s, %s, 1, '{}'::jsonb);
                """,
                (ver_id, wf_id),
            )
            with pytest.raises(CheckViolation):
                cur.execute(
                    """
                    INSERT INTO runs (id, workflow_id, workflow_version_id, status)
                    VALUES (%s, %s, %s, 'INVALID_STATUS');
                    """,
                    (uuid.uuid4().hex, wf_id, ver_id),
                )
        conn.rollback()


def test_check_constraint_on_node_run_status() -> None:
    wf_id = uuid.uuid4().hex
    ver_id = uuid.uuid4().hex
    run_id = uuid.uuid4().hex
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO workflows (id, name) VALUES (%s, 'Test WF');", (wf_id,))
            cur.execute(
                """
                INSERT INTO workflow_versions (id, workflow_id, version, definition)
                VALUES (%s, %s, 1, '{}'::jsonb);
                """,
                (ver_id, wf_id),
            )
            cur.execute(
                """
                INSERT INTO runs (id, workflow_id, workflow_version_id, status)
                VALUES (%s, %s, %s, 'QUEUED');
                """,
                (run_id, wf_id, ver_id),
            )
            with pytest.raises(CheckViolation):
                cur.execute(
                    """
                    INSERT INTO node_runs (id, run_id, node_key, status)
                    VALUES (%s, %s, 'n1', 'BAD_NODE_STATUS');
                    """,
                    (uuid.uuid4().hex, run_id),
                )
        conn.rollback()


def test_check_constraint_on_edge_run_status() -> None:
    wf_id = uuid.uuid4().hex
    ver_id = uuid.uuid4().hex
    run_id = uuid.uuid4().hex
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO workflows (id, name) VALUES (%s, 'Test WF');", (wf_id,))
            cur.execute(
                """
                INSERT INTO workflow_versions (id, workflow_id, version, definition)
                VALUES (%s, %s, 1, '{}'::jsonb);
                """,
                (ver_id, wf_id),
            )
            cur.execute(
                """
                INSERT INTO runs (id, workflow_id, workflow_version_id, status)
                VALUES (%s, %s, %s, 'QUEUED');
                """,
                (run_id, wf_id, ver_id),
            )
            with pytest.raises(CheckViolation):
                cur.execute(
                    """
                    INSERT INTO edge_runs (
                        id, run_id, edge_key, from_node_key, to_node_key, status
                    )
                    VALUES (%s, %s, 'e1', 'a', 'b', 'BAD_EDGE_STATUS');
                    """,
                    (uuid.uuid4().hex, run_id),
                )
        conn.rollback()


def test_unique_constraint_on_node_runs_per_run() -> None:
    wf_id = uuid.uuid4().hex
    ver_id = uuid.uuid4().hex
    run_id = uuid.uuid4().hex
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO workflows (id, name) VALUES (%s, 'Test WF');", (wf_id,))
            cur.execute(
                """
                INSERT INTO workflow_versions (id, workflow_id, version, definition)
                VALUES (%s, %s, 1, '{}'::jsonb);
                """,
                (ver_id, wf_id),
            )
            cur.execute(
                """
                INSERT INTO runs (id, workflow_id, workflow_version_id, status)
                VALUES (%s, %s, %s, 'QUEUED');
                """,
                (run_id, wf_id, ver_id),
            )
            cur.execute(
                """
                INSERT INTO node_runs (id, run_id, node_key, status)
                VALUES (%s, %s, 'node_x', 'READY');
                """,
                (uuid.uuid4().hex, run_id),
            )
            with pytest.raises(UniqueViolation):
                cur.execute(
                    """
                    INSERT INTO node_runs (id, run_id, node_key, status)
                    VALUES (%s, %s, 'node_x', 'PENDING');
                    """,
                    (uuid.uuid4().hex, run_id),
                )
        conn.rollback()
