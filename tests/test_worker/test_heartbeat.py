import time
from datetime import datetime
from typing import Callable

import pytest
from workflow_engine.domain.entities import WorkflowDefinition
from workflow_engine.domain.errors import FencingTokenError, LeaseLostError
from workflow_engine.persistence.connection import get_connection
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.heartbeat import HeartbeatMonitor, HeartbeatService


def test_fenced_heartbeat_extends_lease(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    heartbeat_service: HeartbeatService,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_repo.create_run(wf_id, version=1)

    claimed = claim_engine.discover_and_claim_node(worker_id="worker-1", lease_seconds=10.0)
    assert claimed is not None
    initial_expiry = claimed.lease_expires_at

    time.sleep(0.05)
    new_expiry = heartbeat_service.send_heartbeat(claimed, extension_seconds=20.0)
    assert new_expiry > initial_expiry


def test_fenced_heartbeat_rejects_stale_generation(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    heartbeat_service: HeartbeatService,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_repo.create_run(wf_id, version=1)

    claimed = claim_engine.discover_and_claim_node(worker_id="worker-1", lease_seconds=10.0)
    assert claimed is not None

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE node_runs SET lease_generation = lease_generation + 1 WHERE id = %s;",
                (claimed.node_run_id,),
            )
        conn.commit()

    with pytest.raises(FencingTokenError):
        heartbeat_service.send_heartbeat(claimed, extension_seconds=20.0)


def test_heartbeat_monitor_aborts_on_lease_loss(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    heartbeat_service: HeartbeatService,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_repo.create_run(wf_id, version=1)

    claimed = claim_engine.discover_and_claim_node(worker_id="worker-1", lease_seconds=10.0)
    assert claimed is not None

    aborted = False

    def on_lost() -> None:
        nonlocal aborted
        aborted = True

    monitor = HeartbeatMonitor(
        service=heartbeat_service,
        claimed_node=claimed,
        interval_seconds=0.05,
        extension_seconds=10.0,
        on_lease_lost=on_lost,
    )
    monitor.start()

    time.sleep(0.08)
    assert not monitor.is_lease_lost

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE node_runs SET lease_generation = 99 WHERE id = %s;",
                (claimed.node_run_id,),
            )
        conn.commit()

    deadline = time.time() + 2.0
    while time.time() < deadline and not monitor.is_lease_lost:
        time.sleep(0.02)

    assert monitor.is_lease_lost
    assert aborted
    with pytest.raises(LeaseLostError):
        monitor.check_lease_validity()
    monitor.stop()


def test_expired_heartbeat_rejected_and_never_resurrected(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    heartbeat_service: HeartbeatService,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_repo.create_run(wf_id, version=1)

    claimed = claim_engine.discover_and_claim_node(worker_id="worker-1", lease_seconds=10.0)
    assert claimed is not None

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE node_runs
                SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
                WHERE id = %s;
                """,
                (claimed.node_run_id,),
            )
        conn.commit()

    with pytest.raises(FencingTokenError):
        heartbeat_service.send_heartbeat(claimed, extension_seconds=20.0)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, lease_expires_at FROM node_runs WHERE id = %s;",
                (claimed.node_run_id,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row["status"] == "RUNNING"
            assert row["lease_expires_at"] < datetime.now().astimezone()


def test_heartbeat_rejected_when_parent_run_not_running(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    heartbeat_service: HeartbeatService,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_id = run_repo.create_run(wf_id, version=1)

    claimed = claim_engine.discover_and_claim_node(worker_id="worker-1", lease_seconds=10.0)
    assert claimed is not None

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE runs SET status = 'CANCELLED' WHERE id = %s;",
                (run_id,),
            )
        conn.commit()

    with pytest.raises(FencingTokenError):
        heartbeat_service.send_heartbeat(claimed, extension_seconds=20.0)
