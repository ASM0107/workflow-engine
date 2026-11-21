import threading
from typing import Callable

from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.completion import CompletionEngine
from workflow_engine.worker.worker import WorkflowWorker


def test_concurrent_workers_on_fan_out_fan_in_diamond(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    diamond_def = WorkflowDefinition(
        id="wf_concurrent_diamond",
        version=1,
        nodes={
            "start": Node(id="start", type=NodeType.MANUAL_TRIGGER),
            "branch_a": Node(id="branch_a", type=NodeType.HTTP),
            "branch_b": Node(id="branch_b", type=NodeType.HTTP),
            "join": Node(id="join", type=NodeType.JSON_TRANSFORM),
        },
        edges={
            "e_start_a": Edge(id="e_start_a", from_node_id="start", to_node_id="branch_a"),
            "e_start_b": Edge(id="e_start_b", from_node_id="start", to_node_id="branch_b"),
            "e_a_join": Edge(id="e_a_join", from_node_id="branch_a", to_node_id="join"),
            "e_b_join": Edge(id="e_b_join", from_node_id="branch_b", to_node_id="join"),
        },
    )

    wf_id = create_and_publish(diamond_def)
    run_id = run_repo.create_run(wf_id, 1)

    node_start = claim_engine.discover_and_claim_node("worker-start")
    assert node_start is not None
    completion_engine.complete_success(node_start, output={})

    nodes_mid = run_repo.get_node_runs(run_id)
    assert nodes_mid["branch_a"]["status"] == "READY"
    assert nodes_mid["branch_b"]["status"] == "READY"

    errors: list[Exception] = []

    def worker_loop(w_id: str) -> None:
        try:
            worker = WorkflowWorker(worker_id=w_id)
            for _ in range(5):
                claimed = worker.execute_one(lambda n: {"worker": w_id})
                if not claimed:
                    break
        except Exception as exc:
            errors.append(exc)

    t1 = threading.Thread(target=worker_loop, args=("worker-t1",))
    t2 = threading.Thread(target=worker_loop, args=("worker-t2",))

    t1.start()
    t2.start()
    t1.join(timeout=10.0)
    t2.join(timeout=10.0)

    assert not errors

    nodes = run_repo.get_node_runs(run_id)
    assert nodes["branch_a"]["status"] == "SUCCEEDED"
    assert nodes["branch_b"]["status"] == "SUCCEEDED"

    if nodes["join"]["status"] == "READY":
        final_worker = WorkflowWorker(worker_id="worker-final")
        final_worker.execute_one(lambda n: {"completed": True})

    final_run = run_repo.get_run(run_id)
    assert final_run["status"] == "SUCCEEDED"


def test_concurrent_workers_on_multi_root_fan_in(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    multi_root_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(multi_root_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    errors: list[Exception] = []

    def worker_loop(w_id: str) -> None:
        try:
            worker = WorkflowWorker(worker_id=w_id)
            for _ in range(5):
                claimed = worker.execute_one(lambda n: {"w": w_id})
                if not claimed:
                    break
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker_loop, args=(f"worker-mr-{i}",)) for i in range(3)]

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert not errors

    final_run = run_repo.get_run(run_id)
    assert final_run["status"] == "SUCCEEDED"


def test_no_duplicate_promotions_under_high_concurrency(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    four_way_fan_in = WorkflowDefinition(
        id="wf_four_way_fan_in",
        version=1,
        nodes={
            "root": Node(id="root", type=NodeType.MANUAL_TRIGGER),
            "b1": Node(id="b1", type=NodeType.HTTP),
            "b2": Node(id="b2", type=NodeType.HTTP),
            "b3": Node(id="b3", type=NodeType.HTTP),
            "b4": Node(id="b4", type=NodeType.HTTP),
            "join": Node(id="join", type=NodeType.JSON_TRANSFORM),
        },
        edges={
            "e_root_1": Edge(id="e_root_1", from_node_id="root", to_node_id="b1"),
            "e_root_2": Edge(id="e_root_2", from_node_id="root", to_node_id="b2"),
            "e_root_3": Edge(id="e_root_3", from_node_id="root", to_node_id="b3"),
            "e_root_4": Edge(id="e_root_4", from_node_id="root", to_node_id="b4"),
            "e_1_join": Edge(id="e_1_join", from_node_id="b1", to_node_id="join"),
            "e_2_join": Edge(id="e_2_join", from_node_id="b2", to_node_id="join"),
            "e_3_join": Edge(id="e_3_join", from_node_id="b3", to_node_id="join"),
            "e_4_join": Edge(id="e_4_join", from_node_id="b4", to_node_id="join"),
        },
    )

    wf_id = create_and_publish(four_way_fan_in)
    run_id = run_repo.create_run(wf_id, 1)

    node_root = claim_engine.discover_and_claim_node("worker-root")
    assert node_root is not None
    completion_engine.complete_success(node_root, output={})

    errors: list[Exception] = []

    def worker_loop(w_id: str) -> None:
        try:
            worker = WorkflowWorker(worker_id=w_id)
            for _ in range(6):
                claimed = worker.execute_one(lambda n: {"processed_by": w_id})
                if not claimed:
                    break
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker_loop, args=(f"worker-fan-{i}",)) for i in range(4)]

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15.0)

    assert not errors

    events = run_repo.get_run_events(run_id)
    join_promoted_events = [
        e
        for e in events
        if e["event_type"] == "NODE_PROMOTED" and e["payload"].get("node_key") == "join"
    ]
    assert len(join_promoted_events) == 1

    final_run = run_repo.get_run(run_id)
    assert final_run["status"] == "SUCCEEDED"
