import time
from typing import Callable
from unittest.mock import patch

import psycopg
import pytest
from workflow_engine.domain.entities import WorkflowDefinition
from workflow_engine.domain.errors import FencingTokenError
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.completion import CompletionEngine


def test_injected_database_failure_during_skip_propagation_rolls_back(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    diamond_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    wf_id = create_and_publish(diamond_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    node_trigger = claim_engine.discover_and_claim_node("worker-1")
    assert node_trigger is not None
    completion_engine.complete_success(node_trigger, output={})

    node_cond = claim_engine.discover_and_claim_node("worker-1")
    assert node_cond is not None

    with patch.object(
        completion_engine,
        "_propagate_multi_hop_skips",
        side_effect=psycopg.DatabaseError("Simulated DB failure during skip propagation"),
    ):
        with pytest.raises(psycopg.DatabaseError):
            completion_engine.complete_success(node_cond, output={"result": False})

    nodes = run_repo.get_node_runs(run_id)
    edges = run_repo.get_edge_runs(run_id)
    run_record = run_repo.get_run(run_id)

    assert run_record["status"] == "RUNNING"
    assert nodes["condition"]["status"] == "RUNNING"
    assert nodes["branch_true"]["status"] == "PENDING"
    assert nodes["branch_false"]["status"] == "PENDING"
    assert edges["e_cond_true"]["status"] == "PENDING"
    assert edges["e_cond_false"]["status"] == "PENDING"


def test_injected_database_failure_during_promotion_rolls_back(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    diamond_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    wf_id = create_and_publish(diamond_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    node_trigger = claim_engine.discover_and_claim_node("worker-1")
    assert node_trigger is not None

    with patch.object(
        completion_engine,
        "_promote_eligible_nodes",
        side_effect=RuntimeError("Simulated promotion crash"),
    ):
        with pytest.raises(RuntimeError):
            completion_engine.complete_success(node_trigger, output={})

    nodes = run_repo.get_node_runs(run_id)
    edges = run_repo.get_edge_runs(run_id)
    run_record = run_repo.get_run(run_id)

    assert run_record["status"] == "RUNNING"
    assert nodes["trigger"]["status"] == "RUNNING"
    assert nodes["condition"]["status"] == "PENDING"
    assert edges["e_trig_cond"]["status"] == "PENDING"


def test_injected_database_failure_during_edge_resolution_rolls_back(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    diamond_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    wf_id = create_and_publish(diamond_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    node_trigger = claim_engine.discover_and_claim_node("worker-1")
    assert node_trigger is not None

    with patch.object(
        completion_engine,
        "_resolve_outgoing_edges",
        side_effect=psycopg.OperationalError("Simulated connection dropped"),
    ):
        with pytest.raises(psycopg.OperationalError):
            completion_engine.complete_success(node_trigger, output={})

    nodes = run_repo.get_node_runs(run_id)
    edges = run_repo.get_edge_runs(run_id)

    assert nodes["trigger"]["status"] == "RUNNING"
    assert edges["e_trig_cond"]["status"] == "PENDING"


def test_fenced_lease_expiry_during_completion_rolls_back(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    linear_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    wf_id = create_and_publish(linear_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    claimed = claim_engine.discover_and_claim_node("worker-1", lease_seconds=0.2)
    assert claimed is not None

    time.sleep(0.35)

    with pytest.raises(FencingTokenError):
        completion_engine.complete_success(claimed, output={"result": "late"})

    nodes = run_repo.get_node_runs(run_id)
    edges = run_repo.get_edge_runs(run_id)

    assert nodes["node_1"]["status"] == "RUNNING"
    assert nodes["node_2"]["status"] == "PENDING"
    assert edges["edge_1_2"]["status"] == "PENDING"
