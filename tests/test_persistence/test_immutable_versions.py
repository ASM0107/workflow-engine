import pytest
from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.errors import (
    CycleDetectedError,
    DuplicateVersionError,
    VersionNotFoundError,
    WorkflowNotFoundError,
)
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.repositories import WorkflowRepository


def test_immutable_workflow_version_publishing(
    workflow_repo: WorkflowRepository,
    sample_definition: WorkflowDefinition,
) -> None:
    wf_id = workflow_repo.create_workflow(name="Order Processing", description="Handles orders")
    assert wf_id is not None

    v1_id = workflow_repo.publish_version(wf_id, 1, sample_definition)
    assert v1_id is not None

    v1_record = workflow_repo.get_version(wf_id, 1)
    assert v1_record["version"] == 1
    assert v1_record["id"] == v1_id

    with pytest.raises(DuplicateVersionError):
        workflow_repo.publish_version(wf_id, 1, sample_definition)

    v2_def = WorkflowDefinition(
        id=sample_definition.id,
        version=2,
        nodes={
            "root": Node(id="root", type=NodeType.MANUAL_TRIGGER),
            "step": Node(id="step", type=NodeType.JSON_TRANSFORM),
        },
        edges={
            "e_root_step": Edge(id="e_root_step", from_node_id="root", to_node_id="step"),
        },
    )
    v2_id = workflow_repo.publish_version(wf_id, 2, v2_def)
    assert v2_id != v1_id

    retrieved_v1 = workflow_repo.get_workflow_definition(wf_id, 1)
    retrieved_v2 = workflow_repo.get_workflow_definition(wf_id, 2)

    assert len(retrieved_v1.nodes) == 5
    assert len(retrieved_v2.nodes) == 2


def test_invalid_workflow_rejected_before_db_insertion(
    workflow_repo: WorkflowRepository,
) -> None:
    wf_id = workflow_repo.create_workflow(name="Cyclic Workflow")

    cyclic_def = WorkflowDefinition(
        id="cyclic",
        version=1,
        nodes={
            "a": Node(id="a", type=NodeType.MANUAL_TRIGGER),
            "b": Node(id="b", type=NodeType.HTTP),
        },
        edges={
            "e1": Edge(id="e1", from_node_id="a", to_node_id="b"),
            "e2": Edge(id="e2", from_node_id="b", to_node_id="a"),
        },
    )

    with pytest.raises(CycleDetectedError):
        workflow_repo.publish_version(wf_id, 1, cyclic_def)



def test_repository_get_methods_and_not_found_errors(
    workflow_repo: WorkflowRepository,
    sample_definition: WorkflowDefinition,
) -> None:
    wf_id = workflow_repo.create_workflow(name="Lookup Flow", description="Lookup Desc")
    workflow_repo.publish_version(wf_id, 1, sample_definition)

    wf_record = workflow_repo.get_workflow(wf_id)
    assert wf_record["name"] == "Lookup Flow"
    assert wf_record["description"] == "Lookup Desc"

    with pytest.raises(WorkflowNotFoundError):
        workflow_repo.get_workflow("non_existent_id")

    with pytest.raises(VersionNotFoundError):
        workflow_repo.get_version(wf_id, 999)
