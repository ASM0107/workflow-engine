from typing import Callable

from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.completion import CompletionEngine


def test_multi_hop_skip_propagation_cascades_transitively(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    four_hop_def = WorkflowDefinition(
        id="wf_four_hop_skip",
        version=1,
        nodes={
            "root": Node(id="root", type=NodeType.CONDITION),
            "hop_1": Node(id="hop_1", type=NodeType.JSON_TRANSFORM),
            "hop_2": Node(id="hop_2", type=NodeType.JSON_TRANSFORM),
            "hop_3": Node(id="hop_3", type=NodeType.JSON_TRANSFORM),
            "hop_4": Node(id="hop_4", type=NodeType.HTTP),
        },
        edges={
            "e_root_1": Edge(
                id="e_root_1",
                from_node_id="root",
                to_node_id="hop_1",
                condition_match="true",
            ),
            "e_1_2": Edge(id="e_1_2", from_node_id="hop_1", to_node_id="hop_2"),
            "e_2_3": Edge(id="e_2_3", from_node_id="hop_2", to_node_id="hop_3"),
            "e_3_4": Edge(id="e_3_4", from_node_id="hop_3", to_node_id="hop_4"),
        },
    )

    wf_id = create_and_publish(four_hop_def)
    run_id = run_repo.create_run(wf_id, 1)

    node_root = claim_engine.discover_and_claim_node("worker-1")
    assert node_root is not None
    completion_engine.complete_success(node_root, output={"result": False})

    nodes = run_repo.get_node_runs(run_id)
    edges = run_repo.get_edge_runs(run_id)

    assert nodes["hop_1"]["status"] == "SKIPPED"
    assert nodes["hop_2"]["status"] == "SKIPPED"
    assert nodes["hop_3"]["status"] == "SKIPPED"
    assert nodes["hop_4"]["status"] == "SKIPPED"

    assert edges["e_root_1"]["status"] == "DEAD"
    assert edges["e_1_2"]["status"] == "DEAD"
    assert edges["e_2_3"]["status"] == "DEAD"
    assert edges["e_3_4"]["status"] == "DEAD"

    events = run_repo.get_run_events(run_id)
    skipped_keys = [e["payload"]["node_key"] for e in events if e["event_type"] == "NODE_SKIPPED"]
    assert skipped_keys == ["hop_1", "hop_2", "hop_3", "hop_4"]


def test_multi_hop_skip_stops_at_unresolved_join(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    partial_def = WorkflowDefinition(
        id="wf_partial_cascade",
        version=1,
        nodes={
            "cond": Node(id="cond", type=NodeType.CONDITION),
            "h1": Node(id="h1", type=NodeType.JSON_TRANSFORM),
            "h2": Node(id="h2", type=NodeType.JSON_TRANSFORM),
            "independent": Node(id="independent", type=NodeType.MANUAL_TRIGGER),
            "join": Node(id="join", type=NodeType.HTTP),
        },
        edges={
            "e_cond_h1": Edge(
                id="e_cond_h1",
                from_node_id="cond",
                to_node_id="h1",
                condition_match="go",
            ),
            "e_h1_h2": Edge(id="e_h1_h2", from_node_id="h1", to_node_id="h2"),
            "e_h2_join": Edge(id="e_h2_join", from_node_id="h2", to_node_id="join"),
            "e_indep_join": Edge(id="e_indep_join", from_node_id="independent", to_node_id="join"),
        },
    )

    wf_id = create_and_publish(partial_def)
    run_id = run_repo.create_run(wf_id, 1)

    node_cond = claim_engine.discover_and_claim_node("worker-1")
    assert node_cond is not None
    assert node_cond.node_key in ("cond", "independent")

    if node_cond.node_key != "cond":
        other_node = claim_engine.discover_and_claim_node("worker-2")
        assert other_node is not None
        assert other_node.node_key == "cond"
        cond_node_claimed = other_node
    else:
        cond_node_claimed = node_cond

    completion_engine.complete_success(cond_node_claimed, output={"branch": "stop"})

    nodes = run_repo.get_node_runs(run_id)
    edges = run_repo.get_edge_runs(run_id)

    assert nodes["h1"]["status"] == "SKIPPED"
    assert nodes["h2"]["status"] == "SKIPPED"
    assert nodes["join"]["status"] == "PENDING"
    assert edges["e_h2_join"]["status"] == "DEAD"
    assert edges["e_indep_join"]["status"] == "PENDING"


def test_deep_cascade_fixture_execution(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    deep_cascade_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    wf_id = create_and_publish(deep_cascade_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    node_root = claim_engine.discover_and_claim_node("worker-1")
    assert node_root is not None
    completion_engine.complete_success(node_root, output={})

    node_cond = claim_engine.discover_and_claim_node("worker-1")
    assert node_cond is not None
    completion_engine.complete_success(node_cond, output={"result": True})

    nodes_after_cond = run_repo.get_node_runs(run_id)
    edges_after_cond = run_repo.get_edge_runs(run_id)

    assert nodes_after_cond["false_1"]["status"] == "SKIPPED"
    assert nodes_after_cond["false_2"]["status"] == "SKIPPED"
    assert nodes_after_cond["false_3"]["status"] == "SKIPPED"
    assert edges_after_cond["e_false_3_sink"]["status"] == "DEAD"

    assert nodes_after_cond["true_1"]["status"] == "READY"
    assert nodes_after_cond["sink"]["status"] == "PENDING"

    node_t1 = claim_engine.discover_and_claim_node("worker-1")
    assert node_t1 is not None
    assert node_t1.node_key == "true_1"
    completion_engine.complete_success(node_t1, output={})

    node_t2 = claim_engine.discover_and_claim_node("worker-1")
    assert node_t2 is not None
    assert node_t2.node_key == "true_2"
    completion_engine.complete_success(node_t2, output={})

    node_t3 = claim_engine.discover_and_claim_node("worker-1")
    assert node_t3 is not None
    assert node_t3.node_key == "true_3"
    completion_engine.complete_success(node_t3, output={})

    nodes_after_t3 = run_repo.get_node_runs(run_id)
    assert nodes_after_t3["sink"]["status"] == "READY"

    node_sink = claim_engine.discover_and_claim_node("worker-1")
    assert node_sink is not None
    assert node_sink.node_key == "sink"
    completion_engine.complete_success(node_sink, output={})

    final_run = run_repo.get_run(run_id)
    assert final_run["status"] == "SUCCEEDED"
