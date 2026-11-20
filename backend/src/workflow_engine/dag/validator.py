from workflow_engine.domain.entities import WorkflowDefinition
from workflow_engine.domain.errors import (
    CycleDetectedError,
    DanglingEdgeError,
    EmptyWorkflowError,
)


def validate_workflow(workflow: WorkflowDefinition) -> None:
    if not workflow.nodes:
        raise EmptyWorkflowError("Workflow must contain at least one node")

    node_ids = set(workflow.nodes.keys())

    for edge in workflow.edges.values():
        if edge.from_node_id not in node_ids:
            raise DanglingEdgeError(
                f"Edge '{edge.id}' references non-existent source node '{edge.from_node_id}'"
            )
        if edge.to_node_id not in node_ids:
            raise DanglingEdgeError(
                f"Edge '{edge.id}' references non-existent target node '{edge.to_node_id}'"
            )
        if edge.from_node_id == edge.to_node_id:
            raise CycleDetectedError(
                f"Self-loop detected on node '{edge.from_node_id}' via edge '{edge.id}'"
            )

    detect_cycles(workflow)


def detect_cycles(workflow: WorkflowDefinition) -> None:
    in_degrees: dict[str, int] = {node_id: 0 for node_id in workflow.nodes}

    for edge in workflow.edges.values():
        in_degrees[edge.to_node_id] += 1

    queue = [node_id for node_id, deg in in_degrees.items() if deg == 0]
    visited_count = 0

    while queue:
        current_node_id = queue.pop(0)
        visited_count += 1

        for edge in workflow.edges.values():
            if edge.from_node_id == current_node_id:
                in_degrees[edge.to_node_id] -= 1
                if in_degrees[edge.to_node_id] == 0:
                    queue.append(edge.to_node_id)

    if visited_count != len(workflow.nodes):
        unresolved = [node_id for node_id, deg in in_degrees.items() if deg > 0]
        raise CycleDetectedError(f"Cycle detected involving nodes: {sorted(unresolved)}")


def find_root_nodes(workflow: WorkflowDefinition) -> list[str]:
    target_node_ids = {edge.to_node_id for edge in workflow.edges.values()}
    return sorted([node_id for node_id in workflow.nodes if node_id not in target_node_ids])


def find_leaf_nodes(workflow: WorkflowDefinition) -> list[str]:
    source_node_ids = {edge.from_node_id for edge in workflow.edges.values()}
    return sorted([node_id for node_id in workflow.nodes if node_id not in source_node_ids])
