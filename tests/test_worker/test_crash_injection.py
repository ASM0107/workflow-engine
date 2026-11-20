import time
from typing import Any, Callable

import pytest
from workflow_engine.domain.entities import WorkflowDefinition
from workflow_engine.domain.errors import FencingTokenError, LeaseLostError
from workflow_engine.persistence.connection import get_connection
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.completion import CompletionEngine
from workflow_engine.worker.sweeper import LeaseRecoverySweeper
from workflow_engine.worker.worker import CancellationToken, WorkflowWorker


def test_crash_injection_worker_death_recovery_and_stale_completion_rejection(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
    sweeper: LeaseRecoverySweeper,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_repo.create_run(wf_id, version=1)

    worker1_id = "worker-crash-1"
    claimed_w1 = claim_engine.discover_and_claim_node(worker_id=worker1_id, lease_seconds=1.0)
    assert claimed_w1 is not None
    assert claimed_w1.lease_generation == 1
    assert claimed_w1.attempt_number == 1

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE node_runs
                SET lease_expires_at = clock_timestamp() - INTERVAL '5 seconds'
                WHERE id = %s;
                """,
                (claimed_w1.node_run_id,),
            )
        conn.commit()

    recovered = sweeper.recover_expired_leases(backoff_base_seconds=0.01)
    assert recovered == 1

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE node_runs SET run_after = clock_timestamp() - INTERVAL '1 second';")
        conn.commit()

    worker2_id = "worker-healthy-2"
    claimed_w2 = claim_engine.discover_and_claim_node(worker_id=worker2_id, lease_seconds=30.0)
    assert claimed_w2 is not None
    assert claimed_w2.lease_generation == 3
    assert claimed_w2.attempt_number == 2
    assert claimed_w2.worker_id == worker2_id

    with pytest.raises(FencingTokenError):
        completion_engine.complete_success(claimed_w1, output={"zombie": True})

    completion_engine.complete_success(claimed_w2, output={"healthy": True})

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, output, lease_generation FROM node_runs WHERE id = %s;",
                (claimed_w1.node_run_id,),
            )
            node_row = cur.fetchone()
            assert node_row is not None
            assert node_row["status"] == "SUCCEEDED"
            assert node_row["output"] == {"healthy": True}
            assert node_row["lease_generation"] == 3

            cur.execute(
                """
                SELECT attempt_number, worker_id, status, error, output
                FROM node_attempts
                WHERE node_run_id = %s
                ORDER BY attempt_number ASC;
                """,
                (claimed_w1.node_run_id,),
            )
            attempts = cur.fetchall()
            assert len(attempts) == 2
            assert attempts[0]["attempt_number"] == 1
            assert attempts[0]["worker_id"] == worker1_id
            assert attempts[0]["status"] == "TIMED_OUT"
            assert attempts[1]["attempt_number"] == 2
            assert attempts[1]["worker_id"] == worker2_id
            assert attempts[1]["status"] == "SUCCEEDED"
            assert attempts[1]["output"] == {"healthy": True}


def test_worker_aborts_local_execution_on_lost_heartbeat(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_repo.create_run(wf_id, version=1)

    worker = WorkflowWorker(
        worker_id="worker-aborted",
        lease_duration_seconds=0.2,
        heartbeat_interval_seconds=0.05,
    )

    def slow_task_executor(claimed_node: Any) -> dict[str, str]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE node_runs SET lease_generation = 999 WHERE id = %s;",
                    (claimed_node.node_run_id,),
                )
            conn.commit()

        time.sleep(0.15)
        return {"done": "too late"}

    with pytest.raises((FencingTokenError, LeaseLostError)):
        worker.execute_one(slow_task_executor)


def test_long_running_executor_observes_cooperative_cancellation_before_side_effects(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_repo.create_run(wf_id, version=1)

    worker = WorkflowWorker(
        worker_id="worker-coop",
        lease_duration_seconds=0.2,
        heartbeat_interval_seconds=0.05,
    )

    step1_executed = False
    step2_executed = False

    def multistep_executor(
        claimed_node: Any, cancellation_token: CancellationToken
    ) -> dict[str, Any]:
        nonlocal step1_executed, step2_executed

        step1_executed = True

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE node_runs SET lease_generation = 888 WHERE id = %s;",
                    (claimed_node.node_run_id,),
                )
            conn.commit()

        deadline = time.time() + 2.0
        while time.time() < deadline and not cancellation_token.is_cancelled:
            time.sleep(0.02)

        cancellation_token.check_cancelled()

        step2_executed = True
        return {"step1": True, "step2": True}

    with pytest.raises(LeaseLostError):
        worker.execute_one(multistep_executor)

    assert step1_executed is True
    assert step2_executed is False
