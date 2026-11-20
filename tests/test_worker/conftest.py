from collections.abc import Generator
from typing import Callable

import pytest
from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.connection import get_connection
from workflow_engine.persistence.repositories import RunRepository, WorkflowRepository
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.completion import CompletionEngine
from workflow_engine.worker.heartbeat import HeartbeatService
from workflow_engine.worker.sweeper import LeaseRecoverySweeper


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
def heartbeat_service() -> HeartbeatService:
    return HeartbeatService(default_extension_seconds=30.0)


@pytest.fixture
def sweeper() -> LeaseRecoverySweeper:
    return LeaseRecoverySweeper()


@pytest.fixture
def two_node_workflow() -> WorkflowDefinition:
    nodes = {
        "start": Node(
            id="start",
            type=NodeType.MANUAL_TRIGGER,
            name="Start Node",
            retry_limit=2,
        ),
        "next": Node(
            id="next",
            type=NodeType.HTTP,
            name="Next Node",
            retry_limit=1,
        ),
    }
    edges = {
        "e_start_next": Edge(
            id="e_start_next",
            from_node_id="start",
            to_node_id="next",
        ),
    }
    return WorkflowDefinition(id="wf_two_node", version=1, nodes=nodes, edges=edges)
