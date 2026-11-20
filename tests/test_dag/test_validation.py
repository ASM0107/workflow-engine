import pytest
from workflow_engine.dag.validator import (
    detect_cycles,
    find_leaf_nodes,
    find_root_nodes,
    validate_workflow,
)
from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.errors import (
    CycleDetectedError,
    DanglingEdgeError,
    EmptyWorkflowError,
)
from workflow_engine.domain.types import NodeType


def test_empty_workflow_raises_empty_error() -> None:
    empty_wf = WorkflowDefinition(id="empty", version=1, nodes={}, edges={})
    with pytest.raises(EmptyWorkflowError):
        validate_workflow(empty_wf)


def test_dangling_from_node_raises_dangling_error() -> None:
    wf = WorkflowDefinition(
        id="dangling_from",
        version=1,
        nodes={"n1": Node(id="n1", type=NodeType.MANUAL_TRIGGER)},
        edges={"e1": Edge(id="e1", from_node_id="ghost", to_node_id="n1")},
    )
    with pytest.raises(DanglingEdgeError):
        validate_workflow(wf)


def test_dangling_to_node_raises_dangling_error() -> None:
    wf = WorkflowDefinition(
        id="dangling_to",
        version=1,
        nodes={"n1": Node(id="n1", type=NodeType.MANUAL_TRIGGER)},
        edges={"e1": Edge(id="e1", from_node_id="n1", to_node_id="ghost")},
    )
    with pytest.raises(DanglingEdgeError):
        validate_workflow(wf)


def test_self_loop_raises_cycle_error() -> None:
    wf = WorkflowDefinition(
        id="self_loop",
        version=1,
        nodes={"n1": Node(id="n1", type=NodeType.MANUAL_TRIGGER)},
        edges={"e1": Edge(id="e1", from_node_id="n1", to_node_id="n1")},
    )
    with pytest.raises(CycleDetectedError):
        validate_workflow(wf)


def test_two_node_cycle_raises_cycle_error() -> None:
    wf = WorkflowDefinition(
        id="two_cycle",
        version=1,
        nodes={
            "n1": Node(id="n1", type=NodeType.MANUAL_TRIGGER),
            "n2": Node(id="n2", type=NodeType.HTTP),
        },
        edges={
            "e1": Edge(id="e1", from_node_id="n1", to_node_id="n2"),
            "e2": Edge(id="e2", from_node_id="n2", to_node_id="n1"),
        },
    )
    with pytest.raises(CycleDetectedError):
        validate_workflow(wf)


def test_three_node_cycle_raises_cycle_error() -> None:
    wf = WorkflowDefinition(
        id="three_cycle",
        version=1,
        nodes={
            "n1": Node(id="n1", type=NodeType.MANUAL_TRIGGER),
            "n2": Node(id="n2", type=NodeType.HTTP),
            "n3": Node(id="n3", type=NodeType.DELAY),
        },
        edges={
            "e1": Edge(id="e1", from_node_id="n1", to_node_id="n2"),
            "e2": Edge(id="e2", from_node_id="n2", to_node_id="n3"),
            "e3": Edge(id="e3", from_node_id="n3", to_node_id="n1"),
        },
    )
    with pytest.raises(CycleDetectedError):
        validate_workflow(wf)


def test_subgraph_cycle_with_independent_root_raises_cycle_error() -> None:
    wf = WorkflowDefinition(
        id="subgraph_cycle",
        version=1,
        nodes={
            "root": Node(id="root", type=NodeType.MANUAL_TRIGGER),
            "a": Node(id="a", type=NodeType.JSON_TRANSFORM),
            "b": Node(id="b", type=NodeType.JSON_TRANSFORM),
            "c": Node(id="c", type=NodeType.JSON_TRANSFORM),
        },
        edges={
            "e_root_a": Edge(id="e_root_a", from_node_id="root", to_node_id="a"),
            "e_a_b": Edge(id="e_a_b", from_node_id="a", to_node_id="b"),
            "e_b_c": Edge(id="e_b_c", from_node_id="b", to_node_id="c"),
            "e_c_a": Edge(id="e_c_a", from_node_id="c", to_node_id="a"),
        },
    )
    with pytest.raises(CycleDetectedError):
        validate_workflow(wf)


def test_valid_linear_workflow_passes(linear_workflow: WorkflowDefinition) -> None:
    validate_workflow(linear_workflow)
    detect_cycles(linear_workflow)


def test_valid_diamond_workflow_passes(diamond_workflow: WorkflowDefinition) -> None:
    validate_workflow(diamond_workflow)
    detect_cycles(diamond_workflow)


def test_find_root_nodes_linear(linear_workflow: WorkflowDefinition) -> None:
    roots = find_root_nodes(linear_workflow)
    assert roots == ["node_1"]


def test_find_root_nodes_multi_root(multi_root_workflow: WorkflowDefinition) -> None:
    roots = find_root_nodes(multi_root_workflow)
    assert roots == ["root_a", "root_b"]


def test_find_leaf_nodes_linear(linear_workflow: WorkflowDefinition) -> None:
    leaves = find_leaf_nodes(linear_workflow)
    assert leaves == ["node_3"]


def test_find_leaf_nodes_diamond(diamond_workflow: WorkflowDefinition) -> None:
    leaves = find_leaf_nodes(diamond_workflow)
    assert leaves == ["join"]
