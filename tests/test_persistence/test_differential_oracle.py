from workflow_engine.dag.engine import create_run as create_in_memory_run
from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.repositories import RunRepository, WorkflowRepository


def test_differential_agreement_on_diamond_workflow(
    workflow_repo: WorkflowRepository,
    run_repo: RunRepository,
    sample_definition: WorkflowDefinition,
) -> None:
    wf_id = workflow_repo.create_workflow(name="Differential Diamond")
    workflow_repo.publish_version(wf_id, 1, sample_definition)

    in_memory_run = create_in_memory_run(sample_definition)
    db_run_id = run_repo.create_run(wf_id, 1)

    db_run = run_repo.get_run(db_run_id)
    db_node_runs = run_repo.get_node_runs(db_run_id)
    db_edge_runs = run_repo.get_edge_runs(db_run_id)

    assert in_memory_run.status.value == db_run["status"]
    assert db_run["status"] == "QUEUED"

    assert set(in_memory_run.node_states.keys()) == set(db_node_runs.keys())
    for node_key, in_memory_node in in_memory_run.node_states.items():
        assert in_memory_node.status.value == db_node_runs[node_key]["status"]

    assert set(in_memory_run.edge_states.keys()) == set(db_edge_runs.keys())
    for edge_key, in_memory_edge in in_memory_run.edge_states.items():
        assert in_memory_edge.status.value == db_edge_runs[edge_key]["status"]


def test_differential_agreement_on_multi_root_workflow(
    workflow_repo: WorkflowRepository,
    run_repo: RunRepository,
) -> None:
    multi_root_def = WorkflowDefinition(
        id="wf_multi_root_diff",
        version=1,
        nodes={
            "root_alpha": Node(id="root_alpha", type=NodeType.MANUAL_TRIGGER),
            "root_beta": Node(id="root_beta", type=NodeType.MANUAL_TRIGGER),
            "aggregator": Node(id="aggregator", type=NodeType.JSON_TRANSFORM),
            "output": Node(id="output", type=NodeType.HTTP),
        },
        edges={
            "e_alpha_agg": Edge(
                id="e_alpha_agg", from_node_id="root_alpha", to_node_id="aggregator"
            ),
            "e_beta_agg": Edge(id="e_beta_agg", from_node_id="root_beta", to_node_id="aggregator"),
            "e_agg_out": Edge(id="e_agg_out", from_node_id="aggregator", to_node_id="output"),
        },
    )

    wf_id = workflow_repo.create_workflow(name="Differential Multi Root")
    workflow_repo.publish_version(wf_id, 1, multi_root_def)

    in_memory_run = create_in_memory_run(multi_root_def)
    db_run_id = run_repo.create_run(wf_id, 1)

    db_run = run_repo.get_run(db_run_id)
    db_node_runs = run_repo.get_node_runs(db_run_id)

    assert in_memory_run.status.value == db_run["status"]
    assert db_node_runs["root_alpha"]["status"] == "READY"
    assert db_node_runs["root_beta"]["status"] == "READY"
    assert db_node_runs["aggregator"]["status"] == "PENDING"
    assert db_node_runs["output"]["status"] == "PENDING"

    for node_key, in_memory_node in in_memory_run.node_states.items():
        assert in_memory_node.status.value == db_node_runs[node_key]["status"]
