from workflow_engine.dag.engine import (
    complete_node,
    create_run,
    propagate_multi_hop_skips,
    transition_edge_state,
)
from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.types import EdgeState, NodeState, NodeType


def test_single_hop_skip(diamond_workflow: WorkflowDefinition) -> None:
    run = create_run(diamond_workflow)
    complete_node(run, diamond_workflow, "trigger")

    complete_node(
        run,
        diamond_workflow,
        "condition",
        output={"result": True},
    )

    assert run.edge_states["e_cond_true"].status == EdgeState.TAKEN
    assert run.edge_states["e_cond_false"].status == EdgeState.DEAD

    assert run.node_states["branch_true"].status == NodeState.READY
    assert run.node_states["branch_false"].status == NodeState.SKIPPED
    assert run.edge_states["e_false_join"].status == EdgeState.DEAD


def test_multi_hop_cascade_across_three_levels(
    deep_cascade_workflow: WorkflowDefinition,
) -> None:
    run = create_run(deep_cascade_workflow)
    complete_node(run, deep_cascade_workflow, "root")
    complete_node(
        run,
        deep_cascade_workflow,
        "cond",
        output={"result": True},
    )

    assert run.edge_states["e_cond_true"].status == EdgeState.TAKEN
    assert run.edge_states["e_cond_false"].status == EdgeState.DEAD

    assert run.node_states["true_1"].status == NodeState.READY
    assert run.node_states["true_2"].status == NodeState.PENDING
    assert run.node_states["true_3"].status == NodeState.PENDING

    assert run.node_states["false_1"].status == NodeState.SKIPPED
    assert run.node_states["false_2"].status == NodeState.SKIPPED
    assert run.node_states["false_3"].status == NodeState.SKIPPED

    assert run.edge_states["e_false_1_2"].status == EdgeState.DEAD
    assert run.edge_states["e_false_2_3"].status == EdgeState.DEAD
    assert run.edge_states["e_false_3_sink"].status == EdgeState.DEAD

    assert run.node_states["sink"].status == NodeState.PENDING
    assert run.edge_states["e_true_3_sink"].status == EdgeState.PENDING


def test_root_nodes_excluded_from_skip_propagation() -> None:
    wf = WorkflowDefinition(
        id="multi_root_independent",
        version=1,
        nodes={
            "root_1": Node(id="root_1", type=NodeType.MANUAL_TRIGGER),
            "root_2": Node(id="root_2", type=NodeType.MANUAL_TRIGGER),
            "child": Node(id="child", type=NodeType.HTTP),
        },
        edges={
            "e_root1_child": Edge(id="e_root1_child", from_node_id="root_1", to_node_id="child"),
        },
    )
    run = create_run(wf)
    assert run.node_states["root_1"].status == NodeState.READY
    assert run.node_states["root_2"].status == NodeState.READY

    transition_edge_state(run, "e_root1_child", EdgeState.DEAD)
    skipped = propagate_multi_hop_skips(run, wf)

    assert "child" in skipped
    assert run.node_states["child"].status == NodeState.SKIPPED
    assert run.node_states["root_1"].status == NodeState.READY
    assert run.node_states["root_2"].status == NodeState.READY


def test_propagation_terminates_at_fixed_point_without_extra_work(
    deep_cascade_workflow: WorkflowDefinition,
) -> None:
    run = create_run(deep_cascade_workflow)
    complete_node(run, deep_cascade_workflow, "root")
    complete_node(
        run,
        deep_cascade_workflow,
        "cond",
        output={"result": False},
    )

    first_skipped = propagate_multi_hop_skips(run, deep_cascade_workflow)
    assert first_skipped == []

    second_skipped = propagate_multi_hop_skips(run, deep_cascade_workflow)
    assert second_skipped == []


def test_skip_propagation_defensive_root_and_no_incoming_handling() -> None:
    wf = WorkflowDefinition(
        id="wf_defensive",
        version=1,
        nodes={
            "root": Node(id="root", type=NodeType.MANUAL_TRIGGER),
            "isolated": Node(id="isolated", type=NodeType.JSON_TRANSFORM),
            "child": Node(id="child", type=NodeType.HTTP),
        },
        edges={
            "e_root_child": Edge(id="e_root_child", from_node_id="root", to_node_id="child"),
        },
    )
    run = create_run(wf)
    run.node_states["root"].status = NodeState.PENDING
    run.node_states["isolated"].status = NodeState.PENDING

    skipped = propagate_multi_hop_skips(run, wf)

    assert skipped == []
    assert run.node_states["root"].status == NodeState.PENDING
    assert run.node_states["isolated"].status == NodeState.PENDING
