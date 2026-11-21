from collections.abc import Generator
from typing import Callable

import pytest
from workflow_engine.domain.entities import WorkflowDefinition
from workflow_engine.persistence.connection import get_connection
from workflow_engine.persistence.repositories import RunRepository, WorkflowRepository


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
