from workflow_engine.domain.entities import Edge, WorkflowDefinition
from workflow_engine.domain.errors import CycleDetectedError


def topological_sort(workflow: WorkflowDefinition) -> list[str]:
    in_degrees: dict[str, int] = {node_id: 0 for node_id in workflow.nodes}

    for edge in workflow.edges.values():
        in_degrees[edge.to_node_id] += 1

    queue = sorted([node_id for node_id, deg in in_degrees.items() if deg == 0])
    sorted_order: list[str] = []

    while queue:
        current_node_id = queue.pop(0)
        sorted_order.append(current_node_id)

        next_candidates: list[str] = []
        for edge in workflow.edges.values():
            if edge.from_node_id == current_node_id:
                in_degrees[edge.to_node_id] -= 1
                if in_degrees[edge.to_node_id] == 0:
                    next_candidates.append(edge.to_node_id)

        for candidate in sorted(next_candidates):
            if candidate not in queue:
                queue.append(candidate)

    if len(sorted_order) != len(workflow.nodes):
        unresolved = [node_id for node_id, deg in in_degrees.items() if deg > 0]
        raise CycleDetectedError(f"Cycle detected during topological sort: {sorted(unresolved)}")

    return sorted_order


def get_incoming_edges(workflow: WorkflowDefinition, node_id: str) -> list[Edge]:
    return [edge for edge in workflow.edges.values() if edge.to_node_id == node_id]


def get_outgoing_edges(workflow: WorkflowDefinition, node_id: str) -> list[Edge]:
    return [edge for edge in workflow.edges.values() if edge.from_node_id == node_id]


def get_parent_node_ids(workflow: WorkflowDefinition, node_id: str) -> list[str]:
    return sorted(list({edge.from_node_id for edge in get_incoming_edges(workflow, node_id)}))


def get_child_node_ids(workflow: WorkflowDefinition, node_id: str) -> list[str]:
    return sorted(list({edge.to_node_id for edge in get_outgoing_edges(workflow, node_id)}))
