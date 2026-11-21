from typing import Callable

import pytest
from workflow_engine.domain.entities import Node, WorkflowDefinition
from workflow_engine.domain.errors import FencingTokenError
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.connection import get_connection
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.completion import CompletionEngine
from workflow_engine.worker.heartbeat import HeartbeatService
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


def test_sweeper_fail_fast_cancels_running_siblings(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
    sweeper: LeaseRecoverySweeper,
    heartbeat_service: HeartbeatService,
) -> None:
    sibling_workflow = WorkflowDefinition(
        id="wf_sweeper_siblings",
        version=1,
        nodes={
            "task_x": Node(id="task_x", type=NodeType.MANUAL_TRIGGER, retry_limit=0),
            "task_y": Node(id="task_y", type=NodeType.MANUAL_TRIGGER, retry_limit=0),
        },
        edges={},
    )
    wf_id = create_and_publish(sibling_workflow)
    run_id = run_repo.create_run(wf_id, version=1)

    claimed_x = claim_engine.discover_and_claim_node(worker_id="worker-x", lease_seconds=30.0)
    claimed_y = claim_engine.discover_and_claim_node(worker_id="worker-y", lease_seconds=30.0)
    assert claimed_x is not None
    assert claimed_y is not None

    expiring_claim = claimed_x if claimed_x.node_key == "task_x" else claimed_y
    running_claim = claimed_y if claimed_x.node_key == "task_x" else claimed_x

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE node_runs
                SET lease_expires_at = clock_timestamp() - INTERVAL '10 seconds'
                WHERE id = %s;
                """,
                (expiring_claim.node_run_id,),
            )
        conn.commit()

    sweeper.recover_expired_leases()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM runs WHERE id = %s;", (run_id,))
            run_row = cur.fetchone()
            assert run_row is not None
            assert run_row["status"] == "FAILED"

            cur.execute(
                """
                SELECT status, lease_owner, lease_expires_at, lease_generation, error
                FROM node_runs
                WHERE id = %s;
                """,
                (running_claim.node_run_id,),
            )
            sibling_row = cur.fetchone()
            assert sibling_row is not None
            assert sibling_row["status"] == "CANCELLED"
            assert sibling_row["lease_owner"] is None
            assert sibling_row["lease_expires_at"] is None
            assert sibling_row["lease_generation"] == running_claim.lease_generation + 1
            assert sibling_row["error"] == "Cancelled due to sibling node failure"

            cur.execute(
                "SELECT status, error FROM node_attempts WHERE node_run_id = %s;",
                (running_claim.node_run_id,),
            )
            attempt_row = cur.fetchone()
            assert attempt_row is not None
            assert attempt_row["status"] == "CANCELLED"

    with pytest.raises(FencingTokenError):
        completion_engine.complete_success(running_claim, output={"late": True})

    with pytest.raises(FencingTokenError):
        heartbeat_service.send_heartbeat(running_claim, extension_seconds=30.0)


def test_sweeper_versus_heartbeat_interleaving(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    sweeper: LeaseRecoverySweeper,
    heartbeat_service: HeartbeatService,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_repo.create_run(wf_id, version=1)

    claimed = claim_engine.discover_and_claim_node(worker_id="worker-race", lease_seconds=1.0)
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

    with pytest.raises(FencingTokenError):
        heartbeat_service.send_heartbeat(claimed, extension_seconds=20.0)


def test_sweeper_fail_fast_prevents_recovery_of_cancelled_sibling_exhausted_first(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    sweeper: LeaseRecoverySweeper,
) -> None:
    wf = WorkflowDefinition(
        id="wf_sweeper_fail_fast_exhausted_first",
        version=1,
        nodes={
            "node_a": Node(id="node_a", type=NodeType.MANUAL_TRIGGER, retry_limit=0),
            "node_b": Node(id="node_b", type=NodeType.MANUAL_TRIGGER, retry_limit=2),
        },
        edges={},
    )
    wf_id = create_and_publish(wf)
    run_id = run_repo.create_run(wf_id, version=1)

    claimed_1 = claim_engine.discover_and_claim_node(worker_id="worker-1", lease_seconds=30.0)
    claimed_2 = claim_engine.discover_and_claim_node(worker_id="worker-2", lease_seconds=30.0)
    assert claimed_1 is not None
    assert claimed_2 is not None

    claim_a = claimed_1 if claimed_1.node_key == "node_a" else claimed_2
    claim_b = claimed_2 if claimed_1.node_key == "node_a" else claimed_1

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE node_runs
                SET lease_expires_at = clock_timestamp() - INTERVAL '10 seconds',
                    created_at = clock_timestamp() - INTERVAL '20 seconds'
                WHERE id = %s;
                """,
                (claim_a.node_run_id,),
            )
            cur.execute(
                """
                UPDATE node_runs
                SET lease_expires_at = clock_timestamp() - INTERVAL '10 seconds',
                    created_at = clock_timestamp() - INTERVAL '10 seconds'
                WHERE id = %s;
                """,
                (claim_b.node_run_id,),
            )
        conn.commit()

    sweeper.recover_expired_leases()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM runs WHERE id = %s;", (run_id,))
            run_row = cur.fetchone()
            assert run_row is not None
            assert run_row["status"] == "FAILED"

            cur.execute("SELECT status FROM node_runs WHERE id = %s;", (claim_a.node_run_id,))
            node_a_row = cur.fetchone()
            assert node_a_row is not None
            assert node_a_row["status"] == "FAILED"

            cur.execute("SELECT status FROM node_runs WHERE id = %s;", (claim_b.node_run_id,))
            node_b_row = cur.fetchone()
            assert node_b_row is not None
            assert node_b_row["status"] == "CANCELLED"

            cur.execute(
                "SELECT status FROM node_attempts WHERE node_run_id = %s;",
                (claim_b.node_run_id,),
            )
            attempt_b_row = cur.fetchone()
            assert attempt_b_row is not None
            assert attempt_b_row["status"] == "CANCELLED"

            cur.execute(
                """
                SELECT COUNT(*) AS active_nodes
                FROM node_runs
                WHERE run_id = %s AND status IN ('READY', 'RUNNING');
                """,
                (run_id,),
            )
            active_count = cur.fetchone()
            assert active_count is not None
            assert active_count["active_nodes"] == 0

            cur.execute(
                """
                SELECT COUNT(*) AS active_attempts
                FROM node_attempts na
                JOIN node_runs nr ON na.node_run_id = nr.id
                WHERE nr.run_id = %s AND na.status = 'RUNNING';
                """,
                (run_id,),
            )
            active_attempts = cur.fetchone()
            assert active_attempts is not None
            assert active_attempts["active_attempts"] == 0

    next_claim = claim_engine.discover_and_claim_node(worker_id="worker-subsequent")
    assert next_claim is None


def test_sweeper_fail_fast_prevents_recovery_of_cancelled_sibling_retryable_first(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    sweeper: LeaseRecoverySweeper,
) -> None:
    wf = WorkflowDefinition(
        id="wf_sweeper_fail_fast_retryable_first",
        version=1,
        nodes={
            "node_a": Node(id="node_a", type=NodeType.MANUAL_TRIGGER, retry_limit=0),
            "node_b": Node(id="node_b", type=NodeType.MANUAL_TRIGGER, retry_limit=2),
        },
        edges={},
    )
    wf_id = create_and_publish(wf)
    run_id = run_repo.create_run(wf_id, version=1)

    claimed_1 = claim_engine.discover_and_claim_node(worker_id="worker-1", lease_seconds=30.0)
    claimed_2 = claim_engine.discover_and_claim_node(worker_id="worker-2", lease_seconds=30.0)
    assert claimed_1 is not None
    assert claimed_2 is not None

    claim_a = claimed_1 if claimed_1.node_key == "node_a" else claimed_2
    claim_b = claimed_2 if claimed_1.node_key == "node_a" else claimed_1

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE node_runs
                SET lease_expires_at = clock_timestamp() - INTERVAL '10 seconds',
                    created_at = clock_timestamp() - INTERVAL '20 seconds'
                WHERE id = %s;
                """,
                (claim_b.node_run_id,),
            )
            cur.execute(
                """
                UPDATE node_runs
                SET lease_expires_at = clock_timestamp() - INTERVAL '10 seconds',
                    created_at = clock_timestamp() - INTERVAL '10 seconds'
                WHERE id = %s;
                """,
                (claim_a.node_run_id,),
            )
        conn.commit()

    sweeper.recover_expired_leases()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM runs WHERE id = %s;", (run_id,))
            run_row = cur.fetchone()
            assert run_row is not None
            assert run_row["status"] == "FAILED"

            cur.execute("SELECT status FROM node_runs WHERE id = %s;", (claim_a.node_run_id,))
            node_a_row = cur.fetchone()
            assert node_a_row is not None
            assert node_a_row["status"] == "FAILED"

            cur.execute("SELECT status FROM node_runs WHERE id = %s;", (claim_b.node_run_id,))
            node_b_row = cur.fetchone()
            assert node_b_row is not None
            assert node_b_row["status"] == "CANCELLED"

            cur.execute(
                """
                SELECT COUNT(*) AS active_nodes
                FROM node_runs
                WHERE run_id = %s AND status IN ('READY', 'RUNNING');
                """,
                (run_id,),
            )
            active_count = cur.fetchone()
            assert active_count is not None
            assert active_count["active_nodes"] == 0

            cur.execute(
                """
                SELECT COUNT(*) AS active_attempts
                FROM node_attempts na
                JOIN node_runs nr ON na.node_run_id = nr.id
                WHERE nr.run_id = %s AND na.status = 'RUNNING';
                """,
                (run_id,),
            )
            active_attempts = cur.fetchone()
            assert active_attempts is not None
            assert active_attempts["active_attempts"] == 0

    next_claim = claim_engine.discover_and_claim_node(worker_id="worker-subsequent")
    assert next_claim is None
