import pytest
from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.types import NodeType


@pytest.fixture
def linear_workflow() -> WorkflowDefinition:
    nodes = {
        "node_1": Node(id="node_1", type=NodeType.MANUAL_TRIGGER, name="Trigger"),
        "node_2": Node(id="node_2", type=NodeType.JSON_TRANSFORM, name="Transform"),
        "node_3": Node(id="node_3", type=NodeType.HTTP, name="HTTP Post"),
    }
    edges = {
        "edge_1_2": Edge(id="edge_1_2", from_node_id="node_1", to_node_id="node_2"),
        "edge_2_3": Edge(id="edge_2_3", from_node_id="node_2", to_node_id="node_3"),
    }
    return WorkflowDefinition(id="wf_linear", version=1, nodes=nodes, edges=edges)


@pytest.fixture
def diamond_workflow() -> WorkflowDefinition:
    nodes = {
        "trigger": Node(id="trigger", type=NodeType.MANUAL_TRIGGER),
        "condition": Node(id="condition", type=NodeType.CONDITION),
        "branch_true": Node(id="branch_true", type=NodeType.HTTP),
        "branch_false": Node(id="branch_false", type=NodeType.DELAY),
        "join": Node(id="join", type=NodeType.JSON_TRANSFORM),
    }
    edges = {
        "e_trig_cond": Edge(id="e_trig_cond", from_node_id="trigger", to_node_id="condition"),
        "e_cond_true": Edge(
            id="e_cond_true",
            from_node_id="condition",
            to_node_id="branch_true",
            condition_match="true",
        ),
        "e_cond_false": Edge(
            id="e_cond_false",
            from_node_id="condition",
            to_node_id="branch_false",
            condition_match="false",
        ),
        "e_true_join": Edge(id="e_true_join", from_node_id="branch_true", to_node_id="join"),
        "e_false_join": Edge(id="e_false_join", from_node_id="branch_false", to_node_id="join"),
    }
    return WorkflowDefinition(id="wf_diamond", version=1, nodes=nodes, edges=edges)


@pytest.fixture
def deep_cascade_workflow() -> WorkflowDefinition:
    nodes = {
        "root": Node(id="root", type=NodeType.MANUAL_TRIGGER),
        "cond": Node(id="cond", type=NodeType.CONDITION),
        "true_1": Node(id="true_1", type=NodeType.JSON_TRANSFORM),
        "true_2": Node(id="true_2", type=NodeType.JSON_TRANSFORM),
        "true_3": Node(id="true_3", type=NodeType.JSON_TRANSFORM),
        "false_1": Node(id="false_1", type=NodeType.JSON_TRANSFORM),
        "false_2": Node(id="false_2", type=NodeType.JSON_TRANSFORM),
        "false_3": Node(id="false_3", type=NodeType.JSON_TRANSFORM),
        "sink": Node(id="sink", type=NodeType.HTTP),
    }
    edges = {
        "e_root_cond": Edge(id="e_root_cond", from_node_id="root", to_node_id="cond"),
        "e_cond_true": Edge(
            id="e_cond_true", from_node_id="cond", to_node_id="true_1", condition_match="true"
        ),
        "e_true_1_2": Edge(id="e_true_1_2", from_node_id="true_1", to_node_id="true_2"),
        "e_true_2_3": Edge(id="e_true_2_3", from_node_id="true_2", to_node_id="true_3"),
        "e_true_3_sink": Edge(id="e_true_3_sink", from_node_id="true_3", to_node_id="sink"),
        "e_cond_false": Edge(
            id="e_cond_false", from_node_id="cond", to_node_id="false_1", condition_match="false"
        ),
        "e_false_1_2": Edge(id="e_false_1_2", from_node_id="false_1", to_node_id="false_2"),
        "e_false_2_3": Edge(id="e_false_2_3", from_node_id="false_2", to_node_id="false_3"),
        "e_false_3_sink": Edge(id="e_false_3_sink", from_node_id="false_3", to_node_id="sink"),
    }
    return WorkflowDefinition(id="wf_deep_cascade", version=1, nodes=nodes, edges=edges)


@pytest.fixture
def multi_root_workflow() -> WorkflowDefinition:
    nodes = {
        "root_a": Node(id="root_a", type=NodeType.MANUAL_TRIGGER),
        "root_b": Node(id="root_b", type=NodeType.MANUAL_TRIGGER),
        "merger": Node(id="merger", type=NodeType.JSON_TRANSFORM),
        "final": Node(id="final", type=NodeType.HTTP),
    }
    edges = {
        "e_a_merger": Edge(id="e_a_merger", from_node_id="root_a", to_node_id="merger"),
        "e_b_merger": Edge(id="e_b_merger", from_node_id="root_b", to_node_id="merger"),
        "e_merger_final": Edge(id="e_merger_final", from_node_id="merger", to_node_id="final"),
    }
    return WorkflowDefinition(id="wf_multi_root", version=1, nodes=nodes, edges=edges)
