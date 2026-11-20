from collections.abc import Generator

import pytest
from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.types import NodeType
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
def sample_definition() -> WorkflowDefinition:
    nodes = {
        "trigger": Node(id="trigger", type=NodeType.MANUAL_TRIGGER, name="Start"),
        "cond": Node(id="cond", type=NodeType.CONDITION, name="Branch Decision"),
        "path_a": Node(id="path_a", type=NodeType.HTTP, name="Path A"),
        "path_b": Node(id="path_b", type=NodeType.DELAY, name="Path B"),
        "join": Node(id="join", type=NodeType.JSON_TRANSFORM, name="Join"),
    }
    edges = {
        "e_trig_cond": Edge(id="e_trig_cond", from_node_id="trigger", to_node_id="cond"),
        "e_cond_a": Edge(
            id="e_cond_a",
            from_node_id="cond",
            to_node_id="path_a",
            condition_match="true",
        ),
        "e_cond_b": Edge(
            id="e_cond_b",
            from_node_id="cond",
            to_node_id="path_b",
            condition_match="false",
        ),
        "e_a_join": Edge(id="e_a_join", from_node_id="path_a", to_node_id="join"),
        "e_b_join": Edge(id="e_b_join", from_node_id="path_b", to_node_id="join"),
    }
    return WorkflowDefinition(id="sample_wf", version=1, nodes=nodes, edges=edges)
