import pytest
from workflow_engine.domain.entities import WorkflowDefinition
from workflow_engine.domain.errors import RunNotFoundError
from workflow_engine.persistence.connection import get_connection
from workflow_engine.persistence.repositories import RunRepository, WorkflowRepository


def test_atomic_run_creation_initial_states(
    workflow_repo: WorkflowRepository,
    run_repo: RunRepository,
    sample_definition: WorkflowDefinition,
) -> None:
    wf_id = workflow_repo.create_workflow(name="Order Flow")
    workflow_repo.publish_version(wf_id, 1, sample_definition)

    run_id = run_repo.create_run(wf_id, 1)
    assert run_id is not None

    run_record = run_repo.get_run(run_id)
    assert run_record["status"] == "QUEUED"
    assert run_record["workflow_id"] == wf_id

    node_runs = run_repo.get_node_runs(run_id)
    assert len(node_runs) == 5

    assert node_runs["trigger"]["status"] == "READY"
    assert node_runs["trigger"]["lease_generation"] == 0

    assert node_runs["cond"]["status"] == "PENDING"
    assert node_runs["path_a"]["status"] == "PENDING"
    assert node_runs["path_b"]["status"] == "PENDING"
    assert node_runs["join"]["status"] == "PENDING"

    edge_runs = run_repo.get_edge_runs(run_id)
    assert len(edge_runs) == 5
    for edge_run in edge_runs.values():
        assert edge_run["status"] == "PENDING"

    events = run_repo.get_run_events(run_id)
    assert len(events) == 1
    assert events[0]["event_type"] == "RUN_CREATED"
    assert events[0]["payload"]["workflow_id"] == wf_id
    assert events[0]["payload"]["version"] == 1


def test_atomic_rollback_on_transaction_failure(
    workflow_repo: WorkflowRepository,
    sample_definition: WorkflowDefinition,
) -> None:
    wf_id = workflow_repo.create_workflow(name="Failing Flow")
    workflow_repo.publish_version(wf_id, 1, sample_definition)

    attempted_run_id = "fail_run_123"

    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO runs (id, workflow_id, workflow_version_id, status)
                    VALUES (%s, %s, %s, 'QUEUED');
                    """,
                    (attempted_run_id, wf_id, "bad_version_id"),
                )
                conn.commit()
            except Exception:
                conn.rollback()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) as count FROM runs WHERE id = %s;",
                (attempted_run_id,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row["count"] == 0


def test_get_run_raises_run_not_found_error(run_repo: RunRepository) -> None:
    with pytest.raises(RunNotFoundError):
        run_repo.get_run("non_existent_run_id")
