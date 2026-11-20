import pytest
from workflow_engine.dag.engine import (
    cancel_run,
    check_run_completion,
    claim_node,
    complete_node,
    create_run,
    fail_node,
    retry_node,
    start_run,
    transition_edge_state,
    transition_node_state,
    transition_run_state,
)
from workflow_engine.domain.entities import WorkflowDefinition
from workflow_engine.domain.errors import (
    InvalidEdgeStateTransitionError,
    InvalidNodeStateTransitionError,
    InvalidRunStateTransitionError,
    UnknownEdgeError,
    UnknownNodeError,
)
from workflow_engine.domain.types import EdgeState, NodeState, RunState


def test_run_succeeds_when_all_nodes_succeeded_or_skipped(
    diamond_workflow: WorkflowDefinition,
) -> None:
    run = create_run(diamond_workflow)
    assert run.status == RunState.QUEUED

    complete_node(run, diamond_workflow, "trigger")
    assert run.status == RunState.RUNNING

    complete_node(run, diamond_workflow, "condition", output={"result": True})
    assert run.status == RunState.RUNNING

    complete_node(run, diamond_workflow, "branch_true")
    assert run.status == RunState.RUNNING

    complete_node(run, diamond_workflow, "join")
    assert run.status == RunState.SUCCEEDED


def test_node_failure_aborts_run_and_cancels_pending_nodes(
    linear_workflow: WorkflowDefinition,
) -> None:
    run = create_run(linear_workflow)
    complete_node(run, linear_workflow, "node_1")

    fail_node(run, linear_workflow, "node_2", error="Network timeout")

    assert run.status == RunState.FAILED
    assert run.node_states["node_1"].status == NodeState.SUCCEEDED
    assert run.node_states["node_2"].status == NodeState.FAILED
    assert run.node_states["node_3"].status == NodeState.CANCELLED


def test_run_cancellation_cancels_all_active_nodes(
    linear_workflow: WorkflowDefinition,
) -> None:
    run = create_run(linear_workflow)
    start_run(run)

    cancel_run(run)

    assert run.status == RunState.CANCELLED
    assert run.node_states["node_1"].status == NodeState.CANCELLED
    assert run.node_states["node_2"].status == NodeState.CANCELLED
    assert run.node_states["node_3"].status == NodeState.CANCELLED


def test_retry_node_resets_running_to_ready_with_future_timestamp(
    linear_workflow: WorkflowDefinition,
) -> None:
    run = create_run(linear_workflow)
    claim_node(run, "node_1")
    assert run.node_states["node_1"].status == NodeState.RUNNING

    future_time = 1770000000.0
    retry_node(run, "node_1", run_after=future_time)

    assert run.node_states["node_1"].status == NodeState.READY
    assert run.node_states["node_1"].run_after == future_time
    assert run.status == RunState.RUNNING


def test_state_transitions_to_same_state_are_no_ops(
    linear_workflow: WorkflowDefinition,
) -> None:
    run = create_run(linear_workflow)
    transition_node_state(run, "node_1", NodeState.READY)
    assert run.node_states["node_1"].status == NodeState.READY

    transition_edge_state(run, "edge_1_2", EdgeState.PENDING)
    assert run.edge_states["edge_1_2"].status == EdgeState.PENDING

    transition_run_state(run, RunState.QUEUED)
    assert run.status == RunState.QUEUED


def test_unknown_node_raises_unknown_node_error(
    linear_workflow: WorkflowDefinition,
) -> None:
    run = create_run(linear_workflow)
    with pytest.raises(UnknownNodeError):
        transition_node_state(run, "missing_node", NodeState.READY)

    with pytest.raises(UnknownNodeError):
        claim_node(run, "missing_node")

    with pytest.raises(UnknownNodeError):
        complete_node(run, linear_workflow, "missing_node")

    with pytest.raises(UnknownNodeError):
        fail_node(run, linear_workflow, "missing_node", error="fail")

    with pytest.raises(UnknownNodeError):
        retry_node(run, "missing_node")


def test_unknown_edge_raises_unknown_edge_error(
    linear_workflow: WorkflowDefinition,
) -> None:
    run = create_run(linear_workflow)
    with pytest.raises(UnknownEdgeError):
        transition_edge_state(run, "missing_edge", EdgeState.TAKEN)


def test_check_run_completion_returns_true_for_terminal_runs(
    linear_workflow: WorkflowDefinition,
) -> None:
    run = create_run(linear_workflow)
    cancel_run(run)
    assert check_run_completion(run) is True


def test_illegal_node_transition_raises_domain_error(
    linear_workflow: WorkflowDefinition,
) -> None:
    run = create_run(linear_workflow)
    with pytest.raises(InvalidNodeStateTransitionError):
        transition_node_state(run, "node_2", NodeState.SUCCEEDED)


def test_illegal_run_transition_raises_domain_error(
    linear_workflow: WorkflowDefinition,
) -> None:
    run = create_run(linear_workflow)
    with pytest.raises(InvalidRunStateTransitionError):
        transition_run_state(run, RunState.SUCCEEDED)


def test_illegal_edge_transition_raises_domain_error(
    linear_workflow: WorkflowDefinition,
) -> None:
    run = create_run(linear_workflow)
    transition_edge_state(run, "edge_1_2", EdgeState.TAKEN)
    with pytest.raises(InvalidEdgeStateTransitionError):
        transition_edge_state(run, "edge_1_2", EdgeState.DEAD)
