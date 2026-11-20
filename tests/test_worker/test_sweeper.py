from typing import Callable

from workflow_engine.domain.entities import WorkflowDefinition
from workflow_engine.persistence.connection import get_connection
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.sweeper import LeaseRecoverySweeper


def test_sweeper_recovers_expired_lease(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    sweeper: LeaseRecoverySweeper,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_repo.create_run(wf_id, version=1)

    claimed = claim_engine.discover_and_claim_node(worker_id="worker-1", lease_seconds=1.0)
    assert claimed is not None

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE node_runs
                SET lease_expires_at = clock_timestamp() - INTERVAL '5 seconds'
                WHERE id = %s;
                """,
                (claimed.node_run_id,),
            )
        conn.commit()

    recovered = sweeper.recover_expired_leases(backoff_base_seconds=0.01)
    assert recovered == 1

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, lease_owner, lease_generation, run_after
                FROM node_runs
                WHERE id = %s;
                """,
                (claimed.node_run_id,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row["status"] == "READY"
            assert row["lease_owner"] is None
            assert row["lease_generation"] == 2

            cur.execute(
                "SELECT status, error FROM node_attempts WHERE node_run_id = %s;",
                (claimed.node_run_id,),
            )
            attempt = cur.fetchone()
            assert attempt is not None
            assert attempt["status"] == "TIMED_OUT"


def test_sweeper_ignores_active_leases(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    sweeper: LeaseRecoverySweeper,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_repo.create_run(wf_id, version=1)

    claimed = claim_engine.discover_and_claim_node(worker_id="worker-1", lease_seconds=60.0)
    assert claimed is not None

    recovered = sweeper.recover_expired_leases()
    assert recovered == 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM node_runs WHERE id = %s;", (claimed.node_run_id,))
            row = cur.fetchone()
            assert row is not None
            assert row["status"] == "RUNNING"
