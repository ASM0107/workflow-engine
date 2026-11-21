from typing import Any, Callable

from workflow_engine.dag.engine import execute_reference_workflow
from workflow_engine.domain.entities import ClaimedNode, Edge, Node, WorkflowDefinition
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.worker import WorkflowWorker


def execute_database_workflow(
    run_repo: RunRepository,
    run_id: str,
    executors: dict[str, Callable[[dict[str, Any] | None], dict[str, Any]]] | None = None,
    worker_id: str = "oracle-diff-worker",
) -> None:
    worker = WorkflowWorker(worker_id=worker_id)
    node_executors = executors or {}

    while True:

        def callback(node: ClaimedNode) -> dict[str, Any] | None:
            if node.node_key in node_executors:
                return node_executors[node.node_key](node.config)
            return {"executed_by": node.node_key}

        claimed = worker.execute_one(callback)
        if not claimed:
            break


def assert_differential_agreement(
    in_memory_run: Any,
    run_repo: RunRepository,
    db_run_id: str,
) -> None:
    db_run = run_repo.get_run(db_run_id)
    db_nodes = run_repo.get_node_runs(db_run_id)
    db_edges = run_repo.get_edge_runs(db_run_id)

    assert in_memory_run.status.value == db_run["status"]
    assert set(in_memory_run.node_states.keys()) == set(db_nodes.keys())

    for node_key, mem_node in in_memory_run.node_states.items():
        db_node = db_nodes[node_key]
        assert mem_node.status.value == db_node["status"], (
            f"Node '{node_key}' status mismatch: "
            f"in-memory={mem_node.status.value}, db={db_node['status']}"
        )

    assert set(in_memory_run.edge_states.keys()) == set(db_edges.keys())

    for edge_key, mem_edge in in_memory_run.edge_states.items():
        db_edge = db_edges[edge_key]
        assert mem_edge.status.value == db_edge["status"], (
            f"Edge '{edge_key}' status mismatch: "
            f"in-memory={mem_edge.status.value}, db={db_edge['status']}"
        )


def test_differential_linear_pipeline(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    linear_workflow: WorkflowDefinition,
) -> None:
    executors: dict[str, Callable[[dict[str, Any] | None], dict[str, Any]]] = {
        "node_1": lambda c: {"step": 1},
        "node_2": lambda c: {"step": 2},
        "node_3": lambda c: {"step": 3},
    }

    in_memory_run = execute_reference_workflow(linear_workflow, executors=executors)

    wf_id = create_and_publish(linear_workflow)
    db_run_id = run_repo.create_run(wf_id, 1)
    execute_database_workflow(run_repo, db_run_id, executors=executors)

    assert_differential_agreement(in_memory_run, run_repo, db_run_id)


def test_differential_diamond_true_branch(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    diamond_workflow: WorkflowDefinition,
) -> None:
    executors: dict[str, Callable[[dict[str, Any] | None], dict[str, Any]]] = {
        "trigger": lambda c: {"start": True},
        "condition": lambda c: {"result": True},
        "branch_true": lambda c: {"path": "true"},
        "branch_false": lambda c: {"path": "false"},
        "join": lambda c: {"merged": True},
    }

    in_memory_run = execute_reference_workflow(diamond_workflow, executors=executors)

    wf_id = create_and_publish(diamond_workflow)
    db_run_id = run_repo.create_run(wf_id, 1)
    execute_database_workflow(run_repo, db_run_id, executors=executors)

    assert_differential_agreement(in_memory_run, run_repo, db_run_id)


def test_differential_diamond_false_branch(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    diamond_workflow: WorkflowDefinition,
) -> None:
    executors: dict[str, Callable[[dict[str, Any] | None], dict[str, Any]]] = {
        "trigger": lambda c: {"start": True},
        "condition": lambda c: {"result": False},
        "branch_true": lambda c: {"path": "true"},
        "branch_false": lambda c: {"path": "false"},
        "join": lambda c: {"merged": True},
    }

    in_memory_run = execute_reference_workflow(diamond_workflow, executors=executors)

    wf_id = create_and_publish(diamond_workflow)
    db_run_id = run_repo.create_run(wf_id, 1)
    execute_database_workflow(run_repo, db_run_id, executors=executors)

    assert_differential_agreement(in_memory_run, run_repo, db_run_id)


def test_differential_diamond_both_dead(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    diamond_workflow: WorkflowDefinition,
) -> None:
    executors: dict[str, Callable[[dict[str, Any] | None], dict[str, Any]]] = {
        "trigger": lambda c: {"start": True},
        "condition": lambda c: {"result": None, "branch": "unknown"},
        "branch_true": lambda c: {"path": "true"},
        "branch_false": lambda c: {"path": "false"},
        "join": lambda c: {"merged": True},
    }

    in_memory_run = execute_reference_workflow(diamond_workflow, executors=executors)

    wf_id = create_and_publish(diamond_workflow)
    db_run_id = run_repo.create_run(wf_id, 1)
    execute_database_workflow(run_repo, db_run_id, executors=executors)

    assert_differential_agreement(in_memory_run, run_repo, db_run_id)


def test_differential_deep_cascade(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    deep_cascade_workflow: WorkflowDefinition,
) -> None:
    executors: dict[str, Callable[[dict[str, Any] | None], dict[str, Any]]] = {
        "root": lambda c: {"start": 1},
        "cond": lambda c: {"result": True},
        "true_1": lambda c: {"t": 1},
        "true_2": lambda c: {"t": 2},
        "true_3": lambda c: {"t": 3},
        "false_1": lambda c: {"f": 1},
        "false_2": lambda c: {"f": 2},
        "false_3": lambda c: {"f": 3},
        "sink": lambda c: {"final": True},
    }

    in_memory_run = execute_reference_workflow(deep_cascade_workflow, executors=executors)

    wf_id = create_and_publish(deep_cascade_workflow)
    db_run_id = run_repo.create_run(wf_id, 1)
    execute_database_workflow(run_repo, db_run_id, executors=executors)

    assert_differential_agreement(in_memory_run, run_repo, db_run_id)


def test_differential_multi_root(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    multi_root_workflow: WorkflowDefinition,
) -> None:
    executors: dict[str, Callable[[dict[str, Any] | None], dict[str, Any]]] = {
        "root_a": lambda c: {"src": "a"},
        "root_b": lambda c: {"src": "b"},
        "merger": lambda c: {"merged": True},
        "final": lambda c: {"done": True},
    }

    in_memory_run = execute_reference_workflow(multi_root_workflow, executors=executors)

    wf_id = create_and_publish(multi_root_workflow)
    db_run_id = run_repo.create_run(wf_id, 1)
    execute_database_workflow(run_repo, db_run_id, executors=executors)

    assert_differential_agreement(in_memory_run, run_repo, db_run_id)


def test_differential_complex_reconvergent_graph(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
) -> None:
    complex_def = WorkflowDefinition(
        id="wf_complex_reconvergent",
        version=1,
        nodes={
            "init_a": Node(id="init_a", type=NodeType.MANUAL_TRIGGER),
            "init_b": Node(id="init_b", type=NodeType.MANUAL_TRIGGER),
            "eval_a": Node(id="eval_a", type=NodeType.CONDITION),
            "path_a1": Node(id="path_a1", type=NodeType.JSON_TRANSFORM),
            "path_a2": Node(id="path_a2", type=NodeType.JSON_TRANSFORM),
            "mid_join": Node(id="mid_join", type=NodeType.JSON_TRANSFORM),
            "end_sink": Node(id="end_sink", type=NodeType.HTTP),
        },
        edges={
            "e_ia_ea": Edge(id="e_ia_ea", from_node_id="init_a", to_node_id="eval_a"),
            "e_ea_a1": Edge(
                id="e_ea_a1", from_node_id="eval_a", to_node_id="path_a1", condition_match="yes"
            ),
            "e_ea_a2": Edge(
                id="e_ea_a2", from_node_id="eval_a", to_node_id="path_a2", condition_match="no"
            ),
            "e_a1_mid": Edge(id="e_a1_mid", from_node_id="path_a1", to_node_id="mid_join"),
            "e_a2_mid": Edge(id="e_a2_mid", from_node_id="path_a2", to_node_id="mid_join"),
            "e_ib_mid": Edge(id="e_ib_mid", from_node_id="init_b", to_node_id="mid_join"),
            "e_mid_end": Edge(id="e_mid_end", from_node_id="mid_join", to_node_id="end_sink"),
        },
    )

    executors: dict[str, Callable[[dict[str, Any] | None], dict[str, Any]]] = {
        "init_a": lambda c: {"ok": True},
        "init_b": lambda c: {"ok": True},
        "eval_a": lambda c: {"branch": "yes"},
        "path_a1": lambda c: {"data": "a1"},
        "path_a2": lambda c: {"data": "a2"},
        "mid_join": lambda c: {"data": "mid"},
        "end_sink": lambda c: {"data": "end"},
    }

    in_memory_run = execute_reference_workflow(complex_def, executors=executors)

    wf_id = create_and_publish(complex_def)
    db_run_id = run_repo.create_run(wf_id, 1)
    execute_database_workflow(run_repo, db_run_id, executors=executors)

    assert_differential_agreement(in_memory_run, run_repo, db_run_id)
