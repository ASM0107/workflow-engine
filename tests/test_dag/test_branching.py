from workflow_engine.dag.evaluator import evaluate_outgoing_edges
from workflow_engine.domain.entities import Edge, Node
from workflow_engine.domain.types import NodeType


def test_ordinary_node_marks_all_outgoing_edges_taken() -> None:
    node = Node(id="normal", type=NodeType.HTTP)
    edges = [
        Edge(id="e1", from_node_id="normal", to_node_id="target_a"),
        Edge(id="e2", from_node_id="normal", to_node_id="target_b"),
    ]
    result = evaluate_outgoing_edges(node, edges, output={"data": 123})
    assert result.taken_edge_ids == ["e1", "e2"]
    assert result.dead_edge_ids == []


def test_condition_node_evaluates_boolean_true() -> None:
    node = Node(id="cond", type=NodeType.CONDITION)
    edges = [
        Edge(id="e_true", from_node_id="cond", to_node_id="t", condition_match="true"),
        Edge(id="e_false", from_node_id="cond", to_node_id="f", condition_match="false"),
    ]
    result = evaluate_outgoing_edges(node, edges, output={"result": True})
    assert result.taken_edge_ids == ["e_true"]
    assert result.dead_edge_ids == ["e_false"]


def test_condition_node_evaluates_boolean_false() -> None:
    node = Node(id="cond", type=NodeType.CONDITION)
    edges = [
        Edge(id="e_true", from_node_id="cond", to_node_id="t", condition_match="true"),
        Edge(id="e_false", from_node_id="cond", to_node_id="f", condition_match="false"),
    ]
    result = evaluate_outgoing_edges(node, edges, output={"result": False})
    assert result.taken_edge_ids == ["e_false"]
    assert result.dead_edge_ids == ["e_true"]


def test_condition_node_evaluates_named_branch() -> None:
    node = Node(id="cond", type=NodeType.CONDITION)
    edges = [
        Edge(id="e_low", from_node_id="cond", to_node_id="low", condition_match="low_priority"),
        Edge(id="e_high", from_node_id="cond", to_node_id="high", condition_match="high_priority"),
        Edge(id="e_crit", from_node_id="cond", to_node_id="crit", condition_match="critical"),
    ]
    result = evaluate_outgoing_edges(node, edges, output={"branch": "high_priority"})
    assert result.taken_edge_ids == ["e_high"]
    assert set(result.dead_edge_ids) == {"e_low", "e_crit"}


def test_condition_node_falls_back_to_default_branch() -> None:
    node = Node(id="cond", type=NodeType.CONDITION)
    edges = [
        Edge(id="e_vip", from_node_id="cond", to_node_id="vip", condition_match="vip"),
        Edge(id="e_def", from_node_id="cond", to_node_id="def", condition_match="default"),
    ]
    result = evaluate_outgoing_edges(node, edges, output={"branch": "regular"})
    assert result.taken_edge_ids == ["e_def"]
    assert result.dead_edge_ids == ["e_vip"]


def test_condition_node_ignores_edges_without_condition_match() -> None:
    node = Node(id="cond", type=NodeType.CONDITION)
    edges = [
        Edge(
            id="e_unconditional",
            from_node_id="cond",
            to_node_id="unconditional",
            condition_match=None,
        ),
        Edge(id="e_true", from_node_id="cond", to_node_id="true_target", condition_match="true"),
    ]
    result = evaluate_outgoing_edges(node, edges, output={"result": True})
    assert result.taken_edge_ids == ["e_true"]
    assert result.dead_edge_ids == ["e_unconditional"]


def test_node_with_no_outgoing_edges_returns_empty() -> None:
    node = Node(id="terminal", type=NodeType.HTTP)
    result = evaluate_outgoing_edges(node, [], output={"ok": True})
    assert result.taken_edge_ids == []
    assert result.dead_edge_ids == []
