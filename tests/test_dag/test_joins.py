from workflow_engine.dag.engine import (
    complete_node,
    create_run,
    transition_edge_state,
)
from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.types import EdgeState, NodeState, NodeType


def test_inclusive_or_join_promotes_to_ready(diamond_workflow: WorkflowDefinition) -> None:
    run = create_run(diamond_workflow)
    complete_node(run, diamond_workflow, "trigger")
    complete_node(run, diamond_workflow, "condition", output={"result": True})

    assert run.node_states["branch_true"].status == NodeState.READY
    assert run.node_states["branch_false"].status == NodeState.SKIPPED
    assert run.edge_states["e_false_join"].status == EdgeState.DEAD
    assert run.node_states["join"].status == NodeState.PENDING

    complete_node(run, diamond_workflow, "branch_true")

    assert run.edge_states["e_true_join"].status == EdgeState.TAKEN
    assert run.node_states["join"].status == NodeState.READY


def test_join_becomes_skipped_when_all_incoming_edges_are_dead(
    diamond_workflow: WorkflowDefinition,
) -> None:
    run = create_run(diamond_workflow)
    complete_node(run, diamond_workflow, "trigger")

    complete_node(
        run,
        diamond_workflow,
        "condition",
        selected_edge_ids=[],
    )

    assert run.edge_states["e_cond_true"].status == EdgeState.DEAD
    assert run.edge_states["e_cond_false"].status == EdgeState.DEAD
    assert run.node_states["branch_true"].status == NodeState.SKIPPED
    assert run.node_states["branch_false"].status == NodeState.SKIPPED
    assert run.edge_states["e_true_join"].status == EdgeState.DEAD
    assert run.edge_states["e_false_join"].status == EdgeState.DEAD

    assert run.node_states["join"].status == NodeState.SKIPPED


def test_join_waits_for_all_incoming_edges_before_promotion(
    diamond_workflow: WorkflowDefinition,
) -> None:
    wf = WorkflowDefinition(
        id="parallel_branches",
        version=1,
        nodes={
            "root": Node(id="root", type=NodeType.MANUAL_TRIGGER),
            "worker_1": Node(id="worker_1", type=NodeType.HTTP),
            "worker_2": Node(id="worker_2", type=NodeType.HTTP),
            "join": Node(id="join", type=NodeType.JSON_TRANSFORM),
        },
        edges={
            "e_root_w1": Edge(id="e_root_w1", from_node_id="root", to_node_id="worker_1"),
            "e_root_w2": Edge(id="e_root_w2", from_node_id="root", to_node_id="worker_2"),
            "e_w1_join": Edge(id="e_w1_join", from_node_id="worker_1", to_node_id="join"),
            "e_w2_join": Edge(id="e_w2_join", from_node_id="worker_2", to_node_id="join"),
        },
    )
    run = create_run(wf)
    complete_node(run, wf, "root")

    assert run.node_states["worker_1"].status == NodeState.READY
    assert run.node_states["worker_2"].status == NodeState.READY
    assert run.node_states["join"].status == NodeState.PENDING

    complete_node(run, wf, "worker_1")

    assert run.edge_states["e_w1_join"].status == EdgeState.TAKEN
    assert run.edge_states["e_w2_join"].status == EdgeState.PENDING
    assert run.node_states["join"].status == NodeState.PENDING

    complete_node(run, wf, "worker_2")

    assert run.edge_states["e_w2_join"].status == EdgeState.TAKEN
    assert run.node_states["join"].status == NodeState.READY


def test_three_way_join_with_two_dead_paths_promotes_when_taken_resolves() -> None:
    wf = WorkflowDefinition(
        id="three_way_join",
        version=1,
        nodes={
            "root": Node(id="root", type=NodeType.MANUAL_TRIGGER),
            "p1": Node(id="p1", type=NodeType.JSON_TRANSFORM),
            "p2": Node(id="p2", type=NodeType.JSON_TRANSFORM),
            "p3": Node(id="p3", type=NodeType.JSON_TRANSFORM),
            "join": Node(id="join", type=NodeType.HTTP),
        },
        edges={
            "e_r_p1": Edge(id="e_r_p1", from_node_id="root", to_node_id="p1"),
            "e_r_p2": Edge(id="e_r_p2", from_node_id="root", to_node_id="p2"),
            "e_r_p3": Edge(id="e_r_p3", from_node_id="root", to_node_id="p3"),
            "e_p1_j": Edge(id="e_p1_j", from_node_id="p1", to_node_id="join"),
            "e_p2_j": Edge(id="e_p2_j", from_node_id="p2", to_node_id="join"),
            "e_p3_j": Edge(id="e_p3_j", from_node_id="p3", to_node_id="join"),
        },
    )
    run = create_run(wf)
    complete_node(run, wf, "root")

    transition_edge_state(run, "e_p2_j", EdgeState.DEAD)
    transition_edge_state(run, "e_p3_j", EdgeState.DEAD)

    assert run.node_states["join"].status == NodeState.PENDING

    complete_node(run, wf, "p1")

    assert run.edge_states["e_p1_j"].status == EdgeState.TAKEN
    assert run.node_states["join"].status == NodeState.READY
