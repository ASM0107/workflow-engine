from typing import Callable

from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.completion import CompletionEngine


def test_single_node_run_transitions_to_succeeded(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    single_def = WorkflowDefinition(
        id="wf_single_node",
        version=1,
        nodes={"trigger": Node(id="trigger", type=NodeType.MANUAL_TRIGGER)},
        edges={},
    )

    wf_id = create_and_publish(single_def)
    run_id = run_repo.create_run(wf_id, 1)

    initial_run = run_repo.get_run(run_id)
    assert initial_run["status"] == "QUEUED"

    claimed_node = claim_engine.discover_and_claim_node("worker-1")
    assert claimed_node is not None

    mid_run = run_repo.get_run(run_id)
    assert mid_run["status"] == "RUNNING"

    completion_engine.complete_success(claimed_node, output={"status": "ok"})

    final_run = run_repo.get_run(run_id)
    assert final_run["status"] == "SUCCEEDED"

    events = run_repo.get_run_events(run_id)
    assert any(e["event_type"] == "RUN_SUCCEEDED" for e in events)


def test_linear_three_node_run_transitions_to_succeeded(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    linear_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    wf_id = create_and_publish(linear_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    node_1 = claim_engine.discover_and_claim_node("worker-1")
    assert node_1 is not None
    completion_engine.complete_success(node_1, output={})
    assert run_repo.get_run(run_id)["status"] == "RUNNING"

    node_2 = claim_engine.discover_and_claim_node("worker-1")
    assert node_2 is not None
    completion_engine.complete_success(node_2, output={})
    assert run_repo.get_run(run_id)["status"] == "RUNNING"

    node_3 = claim_engine.discover_and_claim_node("worker-1")
    assert node_3 is not None
    completion_engine.complete_success(node_3, output={})

    assert run_repo.get_run(run_id)["status"] == "SUCCEEDED"


def test_partially_completed_run_remains_running(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    linear_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    wf_id = create_and_publish(linear_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    node_1 = claim_engine.discover_and_claim_node("worker-1")
    assert node_1 is not None
    completion_engine.complete_success(node_1, output={})

    run_record = run_repo.get_run(run_id)
    nodes = run_repo.get_node_runs(run_id)

    assert run_record["status"] == "RUNNING"
    assert nodes["node_1"]["status"] == "SUCCEEDED"
    assert nodes["node_2"]["status"] == "READY"
    assert nodes["node_3"]["status"] == "PENDING"


def test_run_with_all_nodes_skipped_transitions_to_succeeded(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    all_skip_def = WorkflowDefinition(
        id="wf_all_skip",
        version=1,
        nodes={
            "evaluator": Node(id="evaluator", type=NodeType.CONDITION),
            "branch_a": Node(id="branch_a", type=NodeType.HTTP),
            "branch_b": Node(id="branch_b", type=NodeType.HTTP),
            "join": Node(id="join", type=NodeType.JSON_TRANSFORM),
        },
        edges={
            "e_eval_a": Edge(
                id="e_eval_a",
                from_node_id="evaluator",
                to_node_id="branch_a",
                condition_match="a",
            ),
            "e_eval_b": Edge(
                id="e_eval_b",
                from_node_id="evaluator",
                to_node_id="branch_b",
                condition_match="b",
            ),
            "e_a_join": Edge(id="e_a_join", from_node_id="branch_a", to_node_id="join"),
            "e_b_join": Edge(id="e_b_join", from_node_id="branch_b", to_node_id="join"),
        },
    )

    wf_id = create_and_publish(all_skip_def)
    run_id = run_repo.create_run(wf_id, 1)

    node_eval = claim_engine.discover_and_claim_node("worker-1")
    assert node_eval is not None
    completion_engine.complete_success(node_eval, output={"branch": "none"})

    nodes = run_repo.get_node_runs(run_id)
    assert nodes["evaluator"]["status"] == "SUCCEEDED"
    assert nodes["branch_a"]["status"] == "SKIPPED"
    assert nodes["branch_b"]["status"] == "SKIPPED"
    assert nodes["join"]["status"] == "SKIPPED"

    run_record = run_repo.get_run(run_id)
    assert run_record["status"] == "SUCCEEDED"


def test_run_cancellation_cancels_all_non_terminal_nodes(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    linear_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
) -> None:
    wf_id = create_and_publish(linear_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    node_1 = claim_engine.discover_and_claim_node("worker-1")
    assert node_1 is not None

    run_repo.cancel_run(run_id)

    run_record = run_repo.get_run(run_id)
    nodes = run_repo.get_node_runs(run_id)

    assert run_record["status"] == "CANCELLED"
    assert nodes["node_1"]["status"] == "CANCELLED"
    assert nodes["node_2"]["status"] == "CANCELLED"
    assert nodes["node_3"]["status"] == "CANCELLED"

    events = run_repo.get_run_events(run_id)
    assert any(e["event_type"] == "RUN_CANCELLED" for e in events)


def test_run_cancellation_on_already_terminal_run_is_noop(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    single_def = WorkflowDefinition(
        id="wf_noop_cancel",
        version=1,
        nodes={"trigger": Node(id="trigger", type=NodeType.MANUAL_TRIGGER)},
        edges={},
    )

    wf_id = create_and_publish(single_def)
    run_id = run_repo.create_run(wf_id, 1)

    claimed = claim_engine.discover_and_claim_node("worker-1")
    assert claimed is not None
    completion_engine.complete_success(claimed, output={})

    assert run_repo.get_run(run_id)["status"] == "SUCCEEDED"

    run_repo.cancel_run(run_id)
    assert run_repo.get_run(run_id)["status"] == "SUCCEEDED"
