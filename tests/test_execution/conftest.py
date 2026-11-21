from collections.abc import Generator
from typing import Any, Callable

import pytest
from workflow_engine.domain.entities import ClaimedNode, WorkflowDefinition
from workflow_engine.persistence.connection import get_connection
from workflow_engine.persistence.repositories import RunRepository, WorkflowRepository
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.completion import CompletionEngine
from workflow_engine.worker.worker import WorkflowWorker


@pytest.fixture(autouse=True)
def clean_database() -> Generator[None, None, None]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                TRUNCATE TABLE
                    run_events,
                    node_attempts,
                    edge_runs,
                    node_runs,
                    runs,
                    workflow_edges,
                    workflow_nodes,
                    workflow_versions,
                    workflows
                CASCADE;
            """)
        conn.commit()
    yield


@pytest.fixture
def workflow_repo() -> WorkflowRepository:
    return WorkflowRepository()


@pytest.fixture
def run_repo(workflow_repo: WorkflowRepository) -> RunRepository:
    return RunRepository(workflow_repo=workflow_repo)


@pytest.fixture
def create_and_publish(
    workflow_repo: WorkflowRepository,
) -> Callable[[WorkflowDefinition], str]:
    def _publish(definition: WorkflowDefinition) -> str:
        wf_id = workflow_repo.create_workflow(name=definition.id, workflow_id=definition.id)
        workflow_repo.publish_version(wf_id, definition.version, definition)
        return wf_id

    return _publish


@pytest.fixture
def claim_engine() -> ClaimEngine:
    return ClaimEngine(default_lease_seconds=30.0)


@pytest.fixture
def completion_engine() -> CompletionEngine:
    return CompletionEngine()


@pytest.fixture
def run_workflow_to_completion() -> Callable[
    [
        RunRepository,
        str,
        dict[str, Callable[[ClaimedNode], dict[str, Any] | None]] | None,
    ],
    None,
]:
    def _run(
        run_repo: RunRepository,
        run_id: str,
        executors: dict[str, Callable[[ClaimedNode], dict[str, Any] | None]] | None = None,
    ) -> None:
        worker = WorkflowWorker(worker_id="test-executor")
        node_executors = executors or {}

        while True:

            def callback(node: ClaimedNode) -> dict[str, Any] | None:
                if node.node_key in node_executors:
                    return node_executors[node.node_key](node)
                return {"status": "ok"}

            claimed = worker.execute_one(callback)
            if not claimed:
                break

    return _run
