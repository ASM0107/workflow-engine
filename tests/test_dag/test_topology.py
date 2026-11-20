import pytest
from workflow_engine.dag.topology import (
    get_child_node_ids,
    get_incoming_edges,
    get_outgoing_edges,
    get_parent_node_ids,
    topological_sort,
)
from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.errors import CycleDetectedError
from workflow_engine.domain.types import NodeType


def test_topological_sort_linear(linear_workflow: WorkflowDefinition) -> None:
    order = topological_sort(linear_workflow)
    assert order == ["node_1", "node_2", "node_3"]


def test_topological_sort_diamond(diamond_workflow: WorkflowDefinition) -> None:
    order = topological_sort(diamond_workflow)
    assert order[0] == "trigger"
    assert order[1] == "condition"
    assert set(order[2:4]) == {"branch_true", "branch_false"}
    assert order[4] == "join"


def test_topological_sort_multi_root(multi_root_workflow: WorkflowDefinition) -> None:
    order = topological_sort(multi_root_workflow)
    assert set(order[0:2]) == {"root_a", "root_b"}
    assert order[2] == "merger"
    assert order[3] == "final"


def test_topological_sort_detects_cycle_when_called_directly() -> None:
    wf = WorkflowDefinition(
        id="direct_cycle",
        version=1,
        nodes={
            "a": Node(id="a", type=NodeType.MANUAL_TRIGGER),
            "b": Node(id="b", type=NodeType.HTTP),
        },
        edges={
            "e1": Edge(id="e1", from_node_id="a", to_node_id="b"),
            "e2": Edge(id="e2", from_node_id="b", to_node_id="a"),
        },
    )
    with pytest.raises(CycleDetectedError):
        topological_sort(wf)


def test_get_incoming_and_outgoing_edges(diamond_workflow: WorkflowDefinition) -> None:
    incoming_join = get_incoming_edges(diamond_workflow, "join")
    assert {e.id for e in incoming_join} == {"e_true_join", "e_false_join"}

    outgoing_cond = get_outgoing_edges(diamond_workflow, "condition")
    assert {e.id for e in outgoing_cond} == {"e_cond_true", "e_cond_false"}

    outgoing_join = get_outgoing_edges(diamond_workflow, "join")
    assert outgoing_join == []


def test_get_parent_and_child_node_ids(diamond_workflow: WorkflowDefinition) -> None:
    parents_join = get_parent_node_ids(diamond_workflow, "join")
    assert parents_join == ["branch_false", "branch_true"]

    children_cond = get_child_node_ids(diamond_workflow, "condition")
    assert children_cond == ["branch_false", "branch_true"]

    parents_root = get_parent_node_ids(diamond_workflow, "trigger")
    assert parents_root == []
