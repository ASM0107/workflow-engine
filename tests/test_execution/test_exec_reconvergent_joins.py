from typing import Callable

from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.completion import CompletionEngine


def test_inclusive_or_join_promoted_when_one_incoming_taken_and_one_dead(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    diamond_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    wf_id = create_and_publish(diamond_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    node_trigger = claim_engine.discover_and_claim_node("worker-1")
    assert node_trigger is not None
    completion_engine.complete_success(node_trigger, output={})

    node_cond = claim_engine.discover_and_claim_node("worker-1")
    assert node_cond is not None
    completion_engine.complete_success(node_cond, output={"result": True})

    nodes_after_cond = run_repo.get_node_runs(run_id)
    edges_after_cond = run_repo.get_edge_runs(run_id)

    assert nodes_after_cond["branch_true"]["status"] == "READY"
    assert nodes_after_cond["branch_false"]["status"] == "SKIPPED"
    assert nodes_after_cond["join"]["status"] == "PENDING"
    assert edges_after_cond["e_false_join"]["status"] == "DEAD"
    assert edges_after_cond["e_true_join"]["status"] == "PENDING"

    node_true = claim_engine.discover_and_claim_node("worker-1")
    assert node_true is not None
    assert node_true.node_key == "branch_true"
    completion_engine.complete_success(node_true, output={"data": 42})

    nodes_after_true = run_repo.get_node_runs(run_id)
    edges_after_true = run_repo.get_edge_runs(run_id)

    assert nodes_after_true["join"]["status"] == "READY"
    assert edges_after_true["e_true_join"]["status"] == "TAKEN"

    events = run_repo.get_run_events(run_id)
    promoted = [e for e in events if e["event_type"] == "NODE_PROMOTED"]
    assert any(p["payload"]["node_key"] == "join" for p in promoted)

    node_join = claim_engine.discover_and_claim_node("worker-1")
    assert node_join is not None
    assert node_join.node_key == "join"


def test_inclusive_or_join_waits_for_all_incoming_paths_when_multiple_taken(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    fan_out_join_def = WorkflowDefinition(
        id="wf_fan_out_join",
        version=1,
        nodes={
            "root": Node(id="root", type=NodeType.MANUAL_TRIGGER),
            "branch_a": Node(id="branch_a", type=NodeType.HTTP),
            "branch_b": Node(id="branch_b", type=NodeType.HTTP),
            "join": Node(id="join", type=NodeType.JSON_TRANSFORM),
        },
        edges={
            "e_root_a": Edge(id="e_root_a", from_node_id="root", to_node_id="branch_a"),
            "e_root_b": Edge(id="e_root_b", from_node_id="root", to_node_id="branch_b"),
            "e_a_join": Edge(id="e_a_join", from_node_id="branch_a", to_node_id="join"),
            "e_b_join": Edge(id="e_b_join", from_node_id="branch_b", to_node_id="join"),
        },
    )

    wf_id = create_and_publish(fan_out_join_def)
    run_id = run_repo.create_run(wf_id, 1)

    node_root = claim_engine.discover_and_claim_node("worker-1")
    assert node_root is not None
    completion_engine.complete_success(node_root, output={})

    nodes_mid = run_repo.get_node_runs(run_id)
    assert nodes_mid["branch_a"]["status"] == "READY"
    assert nodes_mid["branch_b"]["status"] == "READY"
    assert nodes_mid["join"]["status"] == "PENDING"

    node_a = claim_engine.discover_and_claim_node("worker-1")
    assert node_a is not None
    completion_engine.complete_success(node_a, output={})

    nodes_after_a = run_repo.get_node_runs(run_id)
    assert nodes_after_a["join"]["status"] == "PENDING"

    node_b = claim_engine.discover_and_claim_node("worker-2")
    assert node_b is not None
    completion_engine.complete_success(node_b, output={})

    nodes_after_b = run_repo.get_node_runs(run_id)
    assert nodes_after_b["join"]["status"] == "READY"


def test_three_way_reconvergent_join_with_mixed_paths(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    three_way_def = WorkflowDefinition(
        id="wf_three_way_join",
        version=1,
        nodes={
            "root": Node(id="root", type=NodeType.CONDITION),
            "path_a": Node(id="path_a", type=NodeType.HTTP),
            "path_b": Node(id="path_b", type=NodeType.HTTP),
            "path_c": Node(id="path_c", type=NodeType.HTTP),
            "join": Node(id="join", type=NodeType.JSON_TRANSFORM),
        },
        edges={
            "e_root_a": Edge(
                id="e_root_a",
                from_node_id="root",
                to_node_id="path_a",
                condition_match="a",
            ),
            "e_root_b": Edge(
                id="e_root_b",
                from_node_id="root",
                to_node_id="path_b",
                condition_match="b",
            ),
            "e_root_c": Edge(
                id="e_root_c",
                from_node_id="root",
                to_node_id="path_c",
                condition_match="c",
            ),
            "e_a_join": Edge(id="e_a_join", from_node_id="path_a", to_node_id="join"),
            "e_b_join": Edge(id="e_b_join", from_node_id="path_b", to_node_id="join"),
            "e_c_join": Edge(id="e_c_join", from_node_id="path_c", to_node_id="join"),
        },
    )

    wf_id = create_and_publish(three_way_def)
    run_id = run_repo.create_run(wf_id, 1)

    node_root = claim_engine.discover_and_claim_node("worker-1")
    assert node_root is not None
    completion_engine.complete_success(node_root, output={"branch": "a"})

    nodes_after_root = run_repo.get_node_runs(run_id)
    edges_after_root = run_repo.get_edge_runs(run_id)

    assert nodes_after_root["path_a"]["status"] == "READY"
    assert nodes_after_root["path_b"]["status"] == "SKIPPED"
    assert nodes_after_root["path_c"]["status"] == "SKIPPED"
    assert nodes_after_root["join"]["status"] == "PENDING"
    assert edges_after_root["e_b_join"]["status"] == "DEAD"
    assert edges_after_root["e_c_join"]["status"] == "DEAD"
    assert edges_after_root["e_a_join"]["status"] == "PENDING"

    node_a = claim_engine.discover_and_claim_node("worker-1")
    assert node_a is not None
    assert node_a.node_key == "path_a"
    completion_engine.complete_success(node_a, output={})

    nodes_after_a = run_repo.get_node_runs(run_id)
    assert nodes_after_a["join"]["status"] == "READY"


def test_reconvergent_join_with_all_incoming_dead_is_skipped(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    diamond_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    wf_id = create_and_publish(diamond_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    node_trigger = claim_engine.discover_and_claim_node("worker-1")
    assert node_trigger is not None
    completion_engine.complete_success(node_trigger, output={})

    node_cond = claim_engine.discover_and_claim_node("worker-1")
    assert node_cond is not None
    completion_engine.complete_success(node_cond, output={"result": None, "branch": "unknown"})

    nodes = run_repo.get_node_runs(run_id)
    edges = run_repo.get_edge_runs(run_id)

    assert nodes["branch_true"]["status"] == "SKIPPED"
    assert nodes["branch_false"]["status"] == "SKIPPED"
    assert nodes["join"]["status"] == "SKIPPED"
    assert edges["e_cond_true"]["status"] == "DEAD"
    assert edges["e_cond_false"]["status"] == "DEAD"
    assert edges["e_true_join"]["status"] == "DEAD"
    assert edges["e_false_join"]["status"] == "DEAD"

    events = run_repo.get_run_events(run_id)
    skipped_nodes = [e["payload"]["node_key"] for e in events if e["event_type"] == "NODE_SKIPPED"]
    assert "branch_true" in skipped_nodes
    assert "branch_false" in skipped_nodes
    assert "join" in skipped_nodes
