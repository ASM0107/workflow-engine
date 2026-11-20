import time
import uuid
from typing import Any, Callable

from workflow_engine.dag.evaluator import evaluate_outgoing_edges
from workflow_engine.dag.topology import get_incoming_edges, get_outgoing_edges
from workflow_engine.dag.validator import find_root_nodes, validate_workflow
from workflow_engine.domain.entities import (
    EdgeRunState,
    NodeRunState,
    WorkflowDefinition,
    WorkflowRun,
)
from workflow_engine.domain.errors import (
    InvalidEdgeStateTransitionError,
    InvalidNodeStateTransitionError,
    InvalidRunStateTransitionError,
    UnknownEdgeError,
    UnknownNodeError,
)
from workflow_engine.domain.types import EdgeState, NodeState, RunState

VALID_NODE_TRANSITIONS: dict[NodeState, set[NodeState]] = {
    NodeState.PENDING: {NodeState.READY, NodeState.SKIPPED, NodeState.CANCELLED},
    NodeState.READY: {NodeState.RUNNING, NodeState.CANCELLED},
    NodeState.RUNNING: {
        NodeState.SUCCEEDED,
        NodeState.READY,
        NodeState.FAILED,
        NodeState.CANCELLED,
    },
    NodeState.SUCCEEDED: set(),
    NodeState.FAILED: set(),
    NodeState.SKIPPED: set(),
    NodeState.CANCELLED: set(),
}

VALID_RUN_TRANSITIONS: dict[RunState, set[RunState]] = {
    RunState.QUEUED: {RunState.RUNNING, RunState.CANCELLED},
    RunState.RUNNING: {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED},
    RunState.SUCCEEDED: set(),
    RunState.FAILED: set(),
    RunState.CANCELLED: set(),
}

VALID_EDGE_TRANSITIONS: dict[EdgeState, set[EdgeState]] = {
    EdgeState.PENDING: {EdgeState.TAKEN, EdgeState.DEAD},
    EdgeState.TAKEN: set(),
    EdgeState.DEAD: set(),
}


def transition_node_state(
    run: WorkflowRun,
    node_id: str,
    target_state: NodeState,
) -> None:
    if node_id not in run.node_states:
        raise UnknownNodeError(f"Node '{node_id}' not found in run '{run.run_id}'")

    current_state = run.node_states[node_id].status
    if target_state == current_state:
        return

    allowed = VALID_NODE_TRANSITIONS.get(current_state, set())
    if target_state not in allowed:
        raise InvalidNodeStateTransitionError(node_id, current_state.value, target_state.value)

    run.node_states[node_id].status = target_state
    run.updated_at = time.time()


def transition_edge_state(
    run: WorkflowRun,
    edge_id: str,
    target_state: EdgeState,
) -> None:
    if edge_id not in run.edge_states:
        raise UnknownEdgeError(f"Edge '{edge_id}' not found in run '{run.run_id}'")

    current_state = run.edge_states[edge_id].status
    if target_state == current_state:
        return

    allowed = VALID_EDGE_TRANSITIONS.get(current_state, set())
    if target_state not in allowed:
        raise InvalidEdgeStateTransitionError(edge_id, current_state.value, target_state.value)

    run.edge_states[edge_id].status = target_state
    run.updated_at = time.time()


def transition_run_state(
    run: WorkflowRun,
    target_state: RunState,
) -> None:
    current_state = run.status
    if target_state == current_state:
        return

    allowed = VALID_RUN_TRANSITIONS.get(current_state, set())
    if target_state not in allowed:
        raise InvalidRunStateTransitionError(run.run_id, current_state.value, target_state.value)

    run.status = target_state
    run.updated_at = time.time()


def create_run(
    workflow: WorkflowDefinition,
    run_id: str | None = None,
) -> WorkflowRun:
    validate_workflow(workflow)

    assigned_run_id = run_id or uuid.uuid4().hex
    root_nodes = set(find_root_nodes(workflow))

    node_states: dict[str, NodeRunState] = {}
    for node_id in workflow.nodes:
        initial_status = NodeState.READY if node_id in root_nodes else NodeState.PENDING
        node_states[node_id] = NodeRunState(
            node_id=node_id,
            status=initial_status,
        )

    edge_states: dict[str, EdgeRunState] = {}
    for edge in workflow.edges.values():
        edge_states[edge.id] = EdgeRunState(
            edge_id=edge.id,
            from_node_id=edge.from_node_id,
            to_node_id=edge.to_node_id,
            status=EdgeState.PENDING,
        )

    return WorkflowRun(
        run_id=assigned_run_id,
        workflow_id=workflow.id,
        version=workflow.version,
        status=RunState.QUEUED,
        node_states=node_states,
        edge_states=edge_states,
        created_at=time.time(),
        updated_at=time.time(),
    )


def start_run(run: WorkflowRun) -> None:
    transition_run_state(run, RunState.RUNNING)


def propagate_multi_hop_skips(
    run: WorkflowRun,
    workflow: WorkflowDefinition,
) -> list[str]:
    root_node_ids = set(find_root_nodes(workflow))
    newly_skipped_node_ids: list[str] = []

    while True:
        iteration_skipped: list[str] = []

        for node_id, node_run in run.node_states.items():
            if node_run.status != NodeState.PENDING:
                continue
            if node_id in root_node_ids:
                continue

            incoming_edges = get_incoming_edges(workflow, node_id)
            if not incoming_edges:
                continue

            all_incoming_dead = all(
                run.edge_states[edge.id].status == EdgeState.DEAD for edge in incoming_edges
            )

            if all_incoming_dead:
                transition_node_state(run, node_id, NodeState.SKIPPED)
                iteration_skipped.append(node_id)
                newly_skipped_node_ids.append(node_id)

                for outgoing_edge in get_outgoing_edges(workflow, node_id):
                    if run.edge_states[outgoing_edge.id].status == EdgeState.PENDING:
                        transition_edge_state(run, outgoing_edge.id, EdgeState.DEAD)

        if not iteration_skipped:
            break

    for node_id, node_run in run.node_states.items():
        if node_run.status != NodeState.PENDING:
            continue
        if node_id in root_node_ids:
            continue

        incoming_edges = get_incoming_edges(workflow, node_id)
        if not incoming_edges:
            continue

        all_incoming_resolved = all(
            run.edge_states[edge.id].status in (EdgeState.TAKEN, EdgeState.DEAD)
            for edge in incoming_edges
        )

        has_at_least_one_taken = any(
            run.edge_states[edge.id].status == EdgeState.TAKEN for edge in incoming_edges
        )

        if all_incoming_resolved and has_at_least_one_taken:
            transition_node_state(run, node_id, NodeState.READY)

    return newly_skipped_node_ids


def claim_node(run: WorkflowRun, node_id: str) -> None:
    if node_id not in run.node_states:
        raise UnknownNodeError(f"Node '{node_id}' not found in run '{run.run_id}'")

    if run.status == RunState.QUEUED:
        transition_run_state(run, RunState.RUNNING)

    transition_node_state(run, node_id, NodeState.RUNNING)
    node_run = run.node_states[node_id]
    node_run.attempts += 1
    node_run.lease_generation += 1


def complete_node(
    run: WorkflowRun,
    workflow: WorkflowDefinition,
    node_id: str,
    output: dict[str, Any] | None = None,
    selected_edge_ids: list[str] | None = None,
) -> None:
    if node_id not in run.node_states:
        raise UnknownNodeError(f"Node '{node_id}' not found in run '{run.run_id}'")

    node_run = run.node_states[node_id]
    if node_run.status == NodeState.READY:
        claim_node(run, node_id)

    transition_node_state(run, node_id, NodeState.SUCCEEDED)
    node_run.output = output

    outgoing_edges = get_outgoing_edges(workflow, node_id)
    if outgoing_edges:
        if selected_edge_ids is not None:
            selected_set = set(selected_edge_ids)
            for edge in outgoing_edges:
                target_state = EdgeState.TAKEN if edge.id in selected_set else EdgeState.DEAD
                transition_edge_state(run, edge.id, target_state)
        else:
            node = workflow.nodes[node_id]
            eval_result = evaluate_outgoing_edges(node, outgoing_edges, output)
            for edge_id in eval_result.taken_edge_ids:
                transition_edge_state(run, edge_id, EdgeState.TAKEN)
            for edge_id in eval_result.dead_edge_ids:
                transition_edge_state(run, edge_id, EdgeState.DEAD)

    propagate_multi_hop_skips(run, workflow)
    check_run_completion(run)


def fail_node(
    run: WorkflowRun,
    workflow: WorkflowDefinition,
    node_id: str,
    error: str,
) -> None:
    if node_id not in run.node_states:
        raise UnknownNodeError(f"Node '{node_id}' not found in run '{run.run_id}'")

    node_run = run.node_states[node_id]
    if node_run.status == NodeState.READY:
        claim_node(run, node_id)

    transition_node_state(run, node_id, NodeState.FAILED)
    node_run.error = error

    transition_run_state(run, RunState.FAILED)

    for other_id, other_node in run.node_states.items():
        if other_id == node_id:
            continue
        if other_node.status in (NodeState.PENDING, NodeState.READY, NodeState.RUNNING):
            transition_node_state(run, other_id, NodeState.CANCELLED)


def retry_node(
    run: WorkflowRun,
    node_id: str,
    run_after: float | None = None,
) -> None:
    if node_id not in run.node_states:
        raise UnknownNodeError(f"Node '{node_id}' not found in run '{run.run_id}'")

    node_run = run.node_states[node_id]
    transition_node_state(run, node_id, NodeState.READY)
    node_run.run_after = run_after


def cancel_run(run: WorkflowRun) -> None:
    transition_run_state(run, RunState.CANCELLED)

    for node_id, node_run in run.node_states.items():
        if node_run.status in (NodeState.PENDING, NodeState.READY, NodeState.RUNNING):
            transition_node_state(run, node_id, NodeState.CANCELLED)


def check_run_completion(run: WorkflowRun) -> bool:
    if run.status in (RunState.FAILED, RunState.CANCELLED, RunState.SUCCEEDED):
        return True

    all_terminal = all(
        node_run.status in (NodeState.SUCCEEDED, NodeState.SKIPPED)
        for node_run in run.node_states.values()
    )

    if all_terminal:
        transition_run_state(run, RunState.SUCCEEDED)
        return True

    return False


def get_ready_nodes(run: WorkflowRun) -> list[str]:
    return sorted(
        [
            node_id
            for node_id, node_run in run.node_states.items()
            if node_run.status == NodeState.READY
        ]
    )


def execute_reference_workflow(
    workflow: WorkflowDefinition,
    executors: dict[str, Callable[[dict[str, Any] | None], dict[str, Any]]] | None = None,
    run_id: str | None = None,
) -> WorkflowRun:
    run = create_run(workflow, run_id=run_id)
    node_executors = executors or {}

    while True:
        ready_nodes = get_ready_nodes(run)
        if not ready_nodes:
            break

        for node_id in ready_nodes:
            claim_node(run, node_id)
            executor = node_executors.get(node_id)
            output: dict[str, Any] | None = None
            if executor:
                output = executor(run.node_states[node_id].output)
            complete_node(run, workflow, node_id, output=output)

    return run
