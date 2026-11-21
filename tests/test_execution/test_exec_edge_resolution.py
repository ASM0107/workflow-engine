from typing import Callable

from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.completion import CompletionEngine


def test_linear_node_completion_resolves_edge_to_taken(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    linear_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    wf_id = create_and_publish(linear_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    initial_edges = run_repo.get_edge_runs(run_id)
    assert initial_edges["edge_1_2"]["status"] == "PENDING"
    assert initial_edges["edge_2_3"]["status"] == "PENDING"

    claimed_node = claim_engine.discover_and_claim_node("worker-1")
    assert claimed_node is not None
    assert claimed_node.node_key == "node_1"

    completion_engine.complete_success(claimed_node, output={"message": "done"})

    updated_edges = run_repo.get_edge_runs(run_id)
    assert updated_edges["edge_1_2"]["status"] == "TAKEN"
    assert updated_edges["edge_2_3"]["status"] == "PENDING"

    events = run_repo.get_run_events(run_id)
    taken_events = [e for e in events if e["event_type"] == "EDGE_TAKEN"]
    assert len(taken_events) == 1
    assert taken_events[0]["payload"]["edge_key"] == "edge_1_2"


def test_fan_out_standard_node_resolves_all_outgoing_edges_taken(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    fan_out_def = WorkflowDefinition(
        id="wf_fan_out",
        version=1,
        nodes={
            "root": Node(id="root", type=NodeType.MANUAL_TRIGGER),
            "worker_a": Node(id="worker_a", type=NodeType.HTTP),
            "worker_b": Node(id="worker_b", type=NodeType.HTTP),
            "worker_c": Node(id="worker_c", type=NodeType.HTTP),
        },
        edges={
            "e_root_a": Edge(id="e_root_a", from_node_id="root", to_node_id="worker_a"),
            "e_root_b": Edge(id="e_root_b", from_node_id="root", to_node_id="worker_b"),
            "e_root_c": Edge(id="e_root_c", from_node_id="root", to_node_id="worker_c"),
        },
    )

    wf_id = create_and_publish(fan_out_def)
    run_id = run_repo.create_run(wf_id, 1)

    claimed_root = claim_engine.discover_and_claim_node("worker-1")
    assert claimed_root is not None
    assert claimed_root.node_key == "root"

    completion_engine.complete_success(claimed_root, output={"batch_id": "123"})

    edges = run_repo.get_edge_runs(run_id)
    assert edges["e_root_a"]["status"] == "TAKEN"
    assert edges["e_root_b"]["status"] == "TAKEN"
    assert edges["e_root_c"]["status"] == "TAKEN"

    events = run_repo.get_run_events(run_id)
    taken_events = [e for e in events if e["event_type"] == "EDGE_TAKEN"]
    assert len(taken_events) == 3


def test_condition_node_evaluates_true_branch(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    diamond_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    wf_id = create_and_publish(diamond_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    claimed_trigger = claim_engine.discover_and_claim_node("worker-1")
    assert claimed_trigger is not None
    assert claimed_trigger.node_key == "trigger"
    completion_engine.complete_success(claimed_trigger, output={})

    claimed_condition = claim_engine.discover_and_claim_node("worker-1")
    assert claimed_condition is not None
    assert claimed_condition.node_key == "condition"

    completion_engine.complete_success(claimed_condition, output={"result": True})

    edges = run_repo.get_edge_runs(run_id)
    assert edges["e_cond_true"]["status"] == "TAKEN"
    assert edges["e_cond_false"]["status"] == "DEAD"

    events = run_repo.get_run_events(run_id)
    taken_keys = [e["payload"]["edge_key"] for e in events if e["event_type"] == "EDGE_TAKEN"]
    dead_keys = [e["payload"]["edge_key"] for e in events if e["event_type"] == "EDGE_DEAD"]
    assert "e_cond_true" in taken_keys
    assert "e_cond_false" in dead_keys


def test_condition_node_evaluates_false_branch(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    diamond_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    wf_id = create_and_publish(diamond_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    claimed_trigger = claim_engine.discover_and_claim_node("worker-1")
    assert claimed_trigger is not None
    completion_engine.complete_success(claimed_trigger, output={})

    claimed_condition = claim_engine.discover_and_claim_node("worker-1")
    assert claimed_condition is not None
    completion_engine.complete_success(claimed_condition, output={"result": False})

    edges = run_repo.get_edge_runs(run_id)
    assert edges["e_cond_true"]["status"] == "DEAD"
    assert edges["e_cond_false"]["status"] == "TAKEN"


def test_condition_node_evaluates_named_branch(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    named_branch_def = WorkflowDefinition(
        id="wf_named_branch",
        version=1,
        nodes={
            "evaluator": Node(id="evaluator", type=NodeType.CONDITION),
            "approval_path": Node(id="approval_path", type=NodeType.HTTP),
            "rejection_path": Node(id="rejection_path", type=NodeType.HTTP),
        },
        edges={
            "e_approve": Edge(
                id="e_approve",
                from_node_id="evaluator",
                to_node_id="approval_path",
                condition_match="approved",
            ),
            "e_reject": Edge(
                id="e_reject",
                from_node_id="evaluator",
                to_node_id="rejection_path",
                condition_match="rejected",
            ),
        },
    )

    wf_id = create_and_publish(named_branch_def)
    run_id = run_repo.create_run(wf_id, 1)

    claimed_eval = claim_engine.discover_and_claim_node("worker-1")
    assert claimed_eval is not None
    completion_engine.complete_success(claimed_eval, output={"branch": "approved"})

    edges = run_repo.get_edge_runs(run_id)
    assert edges["e_approve"]["status"] == "TAKEN"
    assert edges["e_reject"]["status"] == "DEAD"


def test_condition_node_evaluates_default_fallback(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    default_def = WorkflowDefinition(
        id="wf_default_fallback",
        version=1,
        nodes={
            "evaluator": Node(id="evaluator", type=NodeType.CONDITION),
            "special_path": Node(id="special_path", type=NodeType.HTTP),
            "default_path": Node(id="default_path", type=NodeType.HTTP),
        },
        edges={
            "e_special": Edge(
                id="e_special",
                from_node_id="evaluator",
                to_node_id="special_path",
                condition_match="vip",
            ),
            "e_default": Edge(
                id="e_default",
                from_node_id="evaluator",
                to_node_id="default_path",
                condition_match="default",
            ),
        },
    )

    wf_id = create_and_publish(default_def)
    run_id = run_repo.create_run(wf_id, 1)

    claimed_eval = claim_engine.discover_and_claim_node("worker-1")
    assert claimed_eval is not None
    completion_engine.complete_success(claimed_eval, output={"branch": "guest"})

    edges = run_repo.get_edge_runs(run_id)
    assert edges["e_special"]["status"] == "DEAD"
    assert edges["e_default"]["status"] == "TAKEN"


def test_condition_node_no_match_no_default_marks_all_dead(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    strict_def = WorkflowDefinition(
        id="wf_strict_match",
        version=1,
        nodes={
            "evaluator": Node(id="evaluator", type=NodeType.CONDITION),
            "path_a": Node(id="path_a", type=NodeType.HTTP),
        },
        edges={
            "e_a": Edge(
                id="e_a",
                from_node_id="evaluator",
                to_node_id="path_a",
                condition_match="premium",
            ),
        },
    )

    wf_id = create_and_publish(strict_def)
    run_id = run_repo.create_run(wf_id, 1)

    claimed_eval = claim_engine.discover_and_claim_node("worker-1")
    assert claimed_eval is not None
    completion_engine.complete_success(claimed_eval, output={"branch": "standard"})

    edges = run_repo.get_edge_runs(run_id)
    assert edges["e_a"]["status"] == "DEAD"


def test_explicit_selected_edge_ids_overrides_automatic_evaluation(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    diamond_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    wf_id = create_and_publish(diamond_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    claimed_trigger = claim_engine.discover_and_claim_node("worker-1")
    assert claimed_trigger is not None
    completion_engine.complete_success(claimed_trigger, output={})

    claimed_condition = claim_engine.discover_and_claim_node("worker-1")
    assert claimed_condition is not None
    completion_engine.complete_success(
        claimed_condition,
        output={"result": True},
        selected_edge_ids=["e_cond_false"],
    )

    edges = run_repo.get_edge_runs(run_id)
    assert edges["e_cond_true"]["status"] == "DEAD"
    assert edges["e_cond_false"]["status"] == "TAKEN"
