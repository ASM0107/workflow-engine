import time
from typing import Callable

from workflow_engine.domain.entities import Node, WorkflowDefinition
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.connection import get_connection
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.claim import ClaimEngine


def test_atomic_queued_to_running_transition_on_claim(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_id = run_repo.create_run(wf_id, version=1)

    initial_run = run_repo.get_run(run_id)
    assert initial_run["status"] == "QUEUED"

    claimed = claim_engine.discover_and_claim_node(worker_id="worker-1", lease_seconds=10.0)
    assert claimed is not None
    assert claimed.run_id == run_id
    assert claimed.node_key == "start"
    assert claimed.worker_id == "worker-1"
    assert claimed.lease_generation == 1
    assert claimed.attempt_number == 1
    assert claimed.retry_limit == 2

    updated_run = run_repo.get_run(run_id)
    assert updated_run["status"] == "RUNNING"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, lease_owner, lease_generation FROM node_runs WHERE id = %s;",
                (claimed.node_run_id,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row["status"] == "RUNNING"
            assert row["lease_owner"] == "worker-1"
            assert row["lease_generation"] == 1


def test_fair_candidate_discovery_across_multiple_runs(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
) -> None:
    wf1 = WorkflowDefinition(
        id="wf_1",
        version=1,
        nodes={"n1": Node(id="n1", type=NodeType.MANUAL_TRIGGER)},
        edges={},
    )
    wf2 = WorkflowDefinition(
        id="wf_2",
        version=1,
        nodes={"n2": Node(id="n2", type=NodeType.MANUAL_TRIGGER)},
        edges={},
    )
    wf1_id = create_and_publish(wf1)
    wf2_id = create_and_publish(wf2)

    run1_id = run_repo.create_run(wf1_id, version=1)
    time.sleep(0.01)
    run2_id = run_repo.create_run(wf2_id, version=1)

    claimed_1 = claim_engine.discover_and_claim_node(worker_id="worker-a")
    assert claimed_1 is not None
    assert claimed_1.run_id == run1_id

    claimed_2 = claim_engine.discover_and_claim_node(worker_id="worker-b")
    assert claimed_2 is not None
    assert claimed_2.run_id == run2_id


def test_claim_returns_none_when_no_ready_nodes(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    two_node_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(two_node_workflow)
    run_repo.create_run(wf_id, version=1)

    claimed = claim_engine.discover_and_claim_node(worker_id="worker-1")
    assert claimed is not None

    claimed_second = claim_engine.discover_and_claim_node(worker_id="worker-2")
    assert claimed_second is None
