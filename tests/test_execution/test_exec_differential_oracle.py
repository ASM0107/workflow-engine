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
                return node_executors[node.node_key](None)
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
        mem_output = mem_node.output
        db_output = db_node["output"]
        assert mem_output == db_output, (
            f"Node '{node_key}' output mismatch: in-memory={mem_output}, db={db_output}"
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


def test_explicit_expected_results_boolean_true_branch(
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

    wf_id = create_and_publish(diamond_workflow)
    db_run_id = run_repo.create_run(wf_id, 1)
    execute_database_workflow(run_repo, db_run_id, executors=executors)

    db_run = run_repo.get_run(db_run_id)
    db_nodes = run_repo.get_node_runs(db_run_id)
    db_edges = run_repo.get_edge_runs(db_run_id)

    assert db_run["status"] == "SUCCEEDED"

    assert db_nodes["trigger"]["status"] == "SUCCEEDED"
    assert db_nodes["trigger"]["output"] == {"start": True}

    assert db_nodes["condition"]["status"] == "SUCCEEDED"
    assert db_nodes["condition"]["output"] == {"result": True}

    assert db_nodes["branch_true"]["status"] == "SUCCEEDED"
    assert db_nodes["branch_true"]["output"] == {"path": "true"}

    assert db_nodes["branch_false"]["status"] == "SKIPPED"
    assert db_nodes["branch_false"]["output"] is None

    assert db_nodes["join"]["status"] == "SUCCEEDED"
    assert db_nodes["join"]["output"] == {"merged": True}

    assert db_edges["e_trig_cond"]["status"] == "TAKEN"
    assert db_edges["e_cond_true"]["status"] == "TAKEN"
    assert db_edges["e_cond_false"]["status"] == "DEAD"
    assert db_edges["e_true_join"]["status"] == "TAKEN"
    assert db_edges["e_false_join"]["status"] == "DEAD"


def test_explicit_expected_results_boolean_false_branch(
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

    wf_id = create_and_publish(diamond_workflow)
    db_run_id = run_repo.create_run(wf_id, 1)
    execute_database_workflow(run_repo, db_run_id, executors=executors)

    db_run = run_repo.get_run(db_run_id)
    db_nodes = run_repo.get_node_runs(db_run_id)
    db_edges = run_repo.get_edge_runs(db_run_id)

    assert db_run["status"] == "SUCCEEDED"

    assert db_nodes["branch_true"]["status"] == "SKIPPED"
    assert db_nodes["branch_true"]["output"] is None

    assert db_nodes["branch_false"]["status"] == "SUCCEEDED"
    assert db_nodes["branch_false"]["output"] == {"path": "false"}

    assert db_nodes["join"]["status"] == "SUCCEEDED"
    assert db_nodes["join"]["output"] == {"merged": True}

    assert db_edges["e_cond_true"]["status"] == "DEAD"
    assert db_edges["e_cond_false"]["status"] == "TAKEN"
    assert db_edges["e_true_join"]["status"] == "DEAD"
    assert db_edges["e_false_join"]["status"] == "TAKEN"


def test_explicit_expected_results_named_branch(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
) -> None:
    named_def = WorkflowDefinition(
        id="wf_explicit_named",
        version=1,
        nodes={
            "evaluator": Node(id="evaluator", type=NodeType.CONDITION),
            "approval_path": Node(id="approval_path", type=NodeType.HTTP),
            "rejection_path": Node(id="rejection_path", type=NodeType.HTTP),
        },
        edges={
            "e_approve": Edge(
                id="e_approve",
                from_node_id="evaluator",
                to_node_id="approval_path",
                condition_match="approved",
            ),
            "e_reject": Edge(
                id="e_reject",
                from_node_id="evaluator",
                to_node_id="rejection_path",
                condition_match="rejected",
            ),
        },
    )

    executors: dict[str, Callable[[dict[str, Any] | None], dict[str, Any]]] = {
        "evaluator": lambda c: {"branch": "approved"},
        "approval_path": lambda c: {"decision": "accepted"},
        "rejection_path": lambda c: {"decision": "denied"},
    }

    wf_id = create_and_publish(named_def)
    db_run_id = run_repo.create_run(wf_id, 1)
    execute_database_workflow(run_repo, db_run_id, executors=executors)

    db_run = run_repo.get_run(db_run_id)
    db_nodes = run_repo.get_node_runs(db_run_id)
    db_edges = run_repo.get_edge_runs(db_run_id)

    assert db_run["status"] == "SUCCEEDED"
    assert db_nodes["approval_path"]["status"] == "SUCCEEDED"
    assert db_nodes["approval_path"]["output"] == {"decision": "accepted"}
    assert db_nodes["rejection_path"]["status"] == "SKIPPED"
    assert db_nodes["rejection_path"]["output"] is None
    assert db_edges["e_approve"]["status"] == "TAKEN"
    assert db_edges["e_reject"]["status"] == "DEAD"


def test_explicit_expected_results_default_fallback(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
) -> None:
    default_def = WorkflowDefinition(
        id="wf_explicit_default",
        version=1,
        nodes={
            "evaluator": Node(id="evaluator", type=NodeType.CONDITION),
            "special_path": Node(id="special_path", type=NodeType.HTTP),
            "default_path": Node(id="default_path", type=NodeType.HTTP),
        },
        edges={
            "e_special": Edge(
                id="e_special",
                from_node_id="evaluator",
                to_node_id="special_path",
                condition_match="vip",
            ),
            "e_default": Edge(
                id="e_default",
                from_node_id="evaluator",
                to_node_id="default_path",
                condition_match="default",
            ),
        },
    )

    executors: dict[str, Callable[[dict[str, Any] | None], dict[str, Any]]] = {
        "evaluator": lambda c: {"branch": "unknown_tier"},
        "special_path": lambda c: {"tier": "vip"},
        "default_path": lambda c: {"tier": "standard"},
    }

    wf_id = create_and_publish(default_def)
    db_run_id = run_repo.create_run(wf_id, 1)
    execute_database_workflow(run_repo, db_run_id, executors=executors)

    db_run = run_repo.get_run(db_run_id)
    db_nodes = run_repo.get_node_runs(db_run_id)
    db_edges = run_repo.get_edge_runs(db_run_id)

    assert db_run["status"] == "SUCCEEDED"
    assert db_nodes["special_path"]["status"] == "SKIPPED"
    assert db_nodes["special_path"]["output"] is None
    assert db_nodes["default_path"]["status"] == "SUCCEEDED"
    assert db_nodes["default_path"]["output"] == {"tier": "standard"}
    assert db_edges["e_special"]["status"] == "DEAD"
    assert db_edges["e_default"]["status"] == "TAKEN"


def test_explicit_expected_results_unmatched_branches(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
) -> None:
    unmatched_def = WorkflowDefinition(
        id="wf_explicit_unmatched",
        version=1,
        nodes={
            "evaluator": Node(id="evaluator", type=NodeType.CONDITION),
            "path_a": Node(id="path_a", type=NodeType.HTTP),
            "path_b": Node(id="path_b", type=NodeType.HTTP),
            "join": Node(id="join", type=NodeType.JSON_TRANSFORM),
        },
        edges={
            "e_a": Edge(
                id="e_a", from_node_id="evaluator", to_node_id="path_a", condition_match="opt_a"
            ),
            "e_b": Edge(
                id="e_b", from_node_id="evaluator", to_node_id="path_b", condition_match="opt_b"
            ),
            "e_a_join": Edge(id="e_a_join", from_node_id="path_a", to_node_id="join"),
            "e_b_join": Edge(id="e_b_join", from_node_id="path_b", to_node_id="join"),
        },
    )

    executors: dict[str, Callable[[dict[str, Any] | None], dict[str, Any]]] = {
        "evaluator": lambda c: {"branch": "opt_c"},
        "path_a": lambda c: {"a": 1},
        "path_b": lambda c: {"b": 2},
        "join": lambda c: {"j": 3},
    }

    wf_id = create_and_publish(unmatched_def)
    db_run_id = run_repo.create_run(wf_id, 1)
    execute_database_workflow(run_repo, db_run_id, executors=executors)

    db_run = run_repo.get_run(db_run_id)
    db_nodes = run_repo.get_node_runs(db_run_id)
    db_edges = run_repo.get_edge_runs(db_run_id)

    assert db_run["status"] == "SUCCEEDED"
    assert db_nodes["path_a"]["status"] == "SKIPPED"
    assert db_nodes["path_a"]["output"] is None
    assert db_nodes["path_b"]["status"] == "SKIPPED"
    assert db_nodes["path_b"]["output"] is None
    assert db_nodes["join"]["status"] == "SKIPPED"
    assert db_nodes["join"]["output"] is None
    assert db_edges["e_a"]["status"] == "DEAD"
    assert db_edges["e_b"]["status"] == "DEAD"
    assert db_edges["e_a_join"]["status"] == "DEAD"
    assert db_edges["e_b_join"]["status"] == "DEAD"


def test_explicit_expected_results_explicit_selected_edge_ids(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    diamond_workflow: WorkflowDefinition,
) -> None:
    wf_id = create_and_publish(diamond_workflow)
    db_run_id = run_repo.create_run(wf_id, 1)

    worker = WorkflowWorker(worker_id="override-worker")

    while True:

        def callback(node: ClaimedNode) -> Any:
            if node.node_key == "trigger":
                return {"started": True}
            if node.node_key == "condition":
                return ({"result": True}, ["e_cond_false"])
            if node.node_key == "branch_false":
                return {"path": "overridden_false"}
            if node.node_key == "join":
                return {"joined": True}
            return {}

        claimed = worker.execute_one(callback)
        if not claimed:
            break

    db_run = run_repo.get_run(db_run_id)
    db_nodes = run_repo.get_node_runs(db_run_id)
    db_edges = run_repo.get_edge_runs(db_run_id)

    assert db_run["status"] == "SUCCEEDED"
    assert db_nodes["branch_true"]["status"] == "SKIPPED"
    assert db_nodes["branch_true"]["output"] is None
    assert db_nodes["branch_false"]["status"] == "SUCCEEDED"
    assert db_nodes["branch_false"]["output"] == {"path": "overridden_false"}
    assert db_nodes["join"]["status"] == "SUCCEEDED"
    assert db_nodes["join"]["output"] == {"joined": True}
    assert db_edges["e_cond_true"]["status"] == "DEAD"
    assert db_edges["e_cond_false"]["status"] == "TAKEN"
    assert db_edges["e_true_join"]["status"] == "DEAD"
    assert db_edges["e_false_join"]["status"] == "TAKEN"
