from typing import Callable

import pytest
from workflow_engine.domain.entities import WorkflowDefinition
from workflow_engine.domain.errors import FencingTokenError
from workflow_engine.persistence.connection import get_connection
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.completion import CompletionEngine


def test_fenced_completion_success(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_repo.create_run(wf_id, version=1)

    claimed = claim_engine.discover_and_claim_node(worker_id="worker-1")
    assert claimed is not None

    completion_engine.complete_success(claimed, output={"result": "ok"})

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, output, lease_owner, lease_expires_at
                FROM node_runs
                WHERE id = %s;
                """,
                (claimed.node_run_id,),
            )
            node_row = cur.fetchone()
            assert node_row is not None
            assert node_row["status"] == "SUCCEEDED"
            assert node_row["output"] == {"result": "ok"}
            assert node_row["lease_owner"] is None
            assert node_row["lease_expires_at"] is None

            cur.execute(
                "SELECT status, output FROM node_attempts WHERE node_run_id = %s;",
                (claimed.node_run_id,),
            )
            attempt_row = cur.fetchone()
            assert attempt_row is not None
            assert attempt_row["status"] == "SUCCEEDED"
            assert attempt_row["output"] == {"result": "ok"}


def test_stale_completion_rejected_by_fencing_token(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_repo.create_run(wf_id, version=1)

    claimed = claim_engine.discover_and_claim_node(worker_id="worker-1")
    assert claimed is not None

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE node_runs SET lease_generation = lease_generation + 1 WHERE id = %s;",
                (claimed.node_run_id,),
            )
        conn.commit()

    with pytest.raises(FencingTokenError):
        completion_engine.complete_success(claimed, output={"result": "stale"})

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM node_runs WHERE id = %s;",
                (claimed.node_run_id,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row["status"] == "RUNNING"


def test_failure_retry_budget_scheduling_and_backoff(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_repo.create_run(wf_id, version=1)

    claimed = claim_engine.discover_and_claim_node(worker_id="worker-1")
    assert claimed is not None
    assert claimed.attempt_number == 1
    assert claimed.retry_limit == 2

    retried = completion_engine.complete_failure(
        claimed, error_message="Network glitch", backoff_base_seconds=0.1
    )
    assert retried is True

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, lease_generation, run_after, error FROM node_runs WHERE id = %s;",
                (claimed.node_run_id,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row["status"] == "READY"
            assert row["lease_generation"] == 2
            assert row["run_after"] is not None
            assert row["error"] == "Network glitch"


def test_failure_retry_budget_exhaustion_fails_run(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_repo.create_run(wf_id, version=1)

    claimed1 = claim_engine.discover_and_claim_node(worker_id="worker-1")
    assert claimed1 is not None
    completion_engine.complete_failure(claimed1, "Error 1", backoff_base_seconds=0.01)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE node_runs SET run_after = clock_timestamp() - INTERVAL '1 second';")
        conn.commit()

    claimed2 = claim_engine.discover_and_claim_node(worker_id="worker-2")
    assert claimed2 is not None
    assert claimed2.attempt_number == 2
    completion_engine.complete_failure(claimed2, "Error 2", backoff_base_seconds=0.01)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE node_runs SET run_after = clock_timestamp() - INTERVAL '1 second';")
        conn.commit()

    claimed3 = claim_engine.discover_and_claim_node(worker_id="worker-3")
    assert claimed3 is not None
    assert claimed3.attempt_number == 3
    retried = completion_engine.complete_failure(claimed3, "Fatal Error")
    assert retried is False

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error FROM node_runs WHERE id = %s;",
                (claimed3.node_run_id,),
            )
            node_row = cur.fetchone()
            assert node_row is not None
            assert node_row["status"] == "FAILED"
            assert node_row["error"] == "Fatal Error"

            cur.execute("SELECT status FROM runs WHERE id = %s;", (claimed3.run_id,))
            run_row = cur.fetchone()
            assert run_row is not None
            assert run_row["status"] == "FAILED"

            cur.execute(
                "SELECT status FROM node_runs WHERE run_id = %s AND node_key = 'next';",
                (claimed3.run_id,),
            )
            next_node = cur.fetchone()
            assert next_node is not None
            assert next_node["status"] == "CANCELLED"
