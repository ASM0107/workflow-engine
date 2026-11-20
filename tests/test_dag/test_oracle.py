from collections.abc import Callable
from typing import Any

from workflow_engine.dag.engine import (
    claim_node,
    complete_node,
    create_run,
    execute_reference_workflow,
    get_ready_nodes,
)
from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.types import EdgeState, NodeState, NodeType, RunState


def test_oracle_linear_execution(linear_workflow: WorkflowDefinition) -> None:
    results: dict[str, str] = {}

    def exec_node_1(_: dict[str, Any] | None) -> dict[str, Any]:
        results["node_1"] = "done"
        return {"step": 1}

    def exec_node_2(_: dict[str, Any] | None) -> dict[str, Any]:
        results["node_2"] = "done"
        return {"step": 2}

    def exec_node_3(_: dict[str, Any] | None) -> dict[str, Any]:
        results["node_3"] = "done"
        return {"step": 3}

    run = execute_reference_workflow(
        linear_workflow,
        executors={
            "node_1": exec_node_1,
            "node_2": exec_node_2,
            "node_3": exec_node_3,
        },
    )

    assert run.status == RunState.SUCCEEDED
    assert results == {"node_1": "done", "node_2": "done", "node_3": "done"}
    assert run.node_states["node_1"].status == NodeState.SUCCEEDED
    assert run.node_states["node_2"].status == NodeState.SUCCEEDED
    assert run.node_states["node_3"].status == NodeState.SUCCEEDED


def test_oracle_diamond_true_branch(diamond_workflow: WorkflowDefinition) -> None:
    executed_nodes: list[str] = []

    def make_executor(
        name: str, payload: dict[str, Any]
    ) -> Callable[[dict[str, Any] | None], dict[str, Any]]:
        def _exec(_: dict[str, Any] | None) -> dict[str, Any]:
            executed_nodes.append(name)
            return payload

        return _exec

    run = execute_reference_workflow(
        diamond_workflow,
        executors={
            "trigger": make_executor("trigger", {"init": True}),
            "condition": make_executor("condition", {"result": True}),
            "branch_true": make_executor("branch_true", {"routed": "true"}),
            "branch_false": make_executor("branch_false", {"routed": "false"}),
            "join": make_executor("join", {"final": True}),
        },
    )

    assert run.status == RunState.SUCCEEDED
    assert executed_nodes == ["trigger", "condition", "branch_true", "join"]
    assert run.node_states["branch_true"].status == NodeState.SUCCEEDED
    assert run.node_states["branch_false"].status == NodeState.SKIPPED
    assert run.edge_states["e_cond_true"].status == EdgeState.TAKEN
    assert run.edge_states["e_cond_false"].status == EdgeState.DEAD


def test_oracle_diamond_false_branch(diamond_workflow: WorkflowDefinition) -> None:
    executed_nodes: list[str] = []

    def make_executor(
        name: str, payload: dict[str, Any]
    ) -> Callable[[dict[str, Any] | None], dict[str, Any]]:
        def _exec(_: dict[str, Any] | None) -> dict[str, Any]:
            executed_nodes.append(name)
            return payload

        return _exec

    run = execute_reference_workflow(
        diamond_workflow,
        executors={
            "trigger": make_executor("trigger", {"init": True}),
            "condition": make_executor("condition", {"result": False}),
            "branch_true": make_executor("branch_true", {"routed": "true"}),
            "branch_false": make_executor("branch_false", {"routed": "false"}),
            "join": make_executor("join", {"final": True}),
        },
    )

    assert run.status == RunState.SUCCEEDED
    assert executed_nodes == ["trigger", "condition", "branch_false", "join"]
    assert run.node_states["branch_true"].status == NodeState.SKIPPED
    assert run.node_states["branch_false"].status == NodeState.SUCCEEDED
    assert run.edge_states["e_cond_true"].status == EdgeState.DEAD
    assert run.edge_states["e_cond_false"].status == EdgeState.TAKEN


def test_oracle_multi_root_execution(multi_root_workflow: WorkflowDefinition) -> None:
    run = execute_reference_workflow(multi_root_workflow)

    assert run.status == RunState.SUCCEEDED
    assert run.node_states["root_a"].status == NodeState.SUCCEEDED
    assert run.node_states["root_b"].status == NodeState.SUCCEEDED
    assert run.node_states["merger"].status == NodeState.SUCCEEDED
    assert run.node_states["final"].status == NodeState.SUCCEEDED


def test_claim_mechanics_and_lease_generation(linear_workflow: WorkflowDefinition) -> None:
    run = create_run(linear_workflow)
    assert run.status == RunState.QUEUED

    node_1_state = run.node_states["node_1"]
    assert node_1_state.attempts == 0
    assert node_1_state.lease_generation == 0

    claim_node(run, "node_1")

    assert run.status == RunState.RUNNING
    assert node_1_state.status == NodeState.RUNNING
    assert node_1_state.attempts == 1
    assert node_1_state.lease_generation == 1


def test_oracle_wide_fan_out_and_reconverge() -> None:
    parallel_branch_count = 10
    nodes = {
        "root": Node(id="root", type=NodeType.MANUAL_TRIGGER),
        "sink": Node(id="sink", type=NodeType.JSON_TRANSFORM),
    }
    edges = {}

    for i in range(parallel_branch_count):
        node_id = f"branch_{i}"
        nodes[node_id] = Node(id=node_id, type=NodeType.HTTP)
        edges[f"e_root_{node_id}"] = Edge(
            id=f"e_root_{node_id}", from_node_id="root", to_node_id=node_id
        )
        edges[f"e_{node_id}_sink"] = Edge(
            id=f"e_{node_id}_sink", from_node_id=node_id, to_node_id="sink"
        )

    wf = WorkflowDefinition(id="wf_fan_out", version=1, nodes=nodes, edges=edges)
    run = execute_reference_workflow(wf)

    assert run.status == RunState.SUCCEEDED
    for i in range(parallel_branch_count):
        assert run.node_states[f"branch_{i}"].status == NodeState.SUCCEEDED
    assert run.node_states["sink"].status == NodeState.SUCCEEDED


def test_oracle_deep_linear_chain() -> None:
    chain_length = 25
    nodes = {
        f"node_{i}": Node(id=f"node_{i}", type=NodeType.JSON_TRANSFORM) for i in range(chain_length)
    }
    edges = {
        f"edge_{i}_{i + 1}": Edge(
            id=f"edge_{i}_{i + 1}", from_node_id=f"node_{i}", to_node_id=f"node_{i + 1}"
        )
        for i in range(chain_length - 1)
    }

    wf = WorkflowDefinition(id="wf_deep_chain", version=1, nodes=nodes, edges=edges)
    run = create_run(wf)

    for i in range(chain_length):
        ready = get_ready_nodes(run)
        assert ready == [f"node_{i}"]
        complete_node(run, wf, f"node_{i}", output={"step": i})

    assert run.status == RunState.SUCCEEDED
