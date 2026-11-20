from typing import Any

from pydantic import BaseModel, Field

from workflow_engine.domain.entities import Edge, Node
from workflow_engine.domain.types import NodeType


class BranchEvaluationResult(BaseModel):
    taken_edge_ids: list[str] = Field(default_factory=list)
    dead_edge_ids: list[str] = Field(default_factory=list)


def evaluate_outgoing_edges(
    node: Node,
    outgoing_edges: list[Edge],
    output: dict[str, Any] | None = None,
) -> BranchEvaluationResult:
    if not outgoing_edges:
        return BranchEvaluationResult()

    if node.type != NodeType.CONDITION:
        return BranchEvaluationResult(
            taken_edge_ids=[edge.id for edge in outgoing_edges],
            dead_edge_ids=[],
        )

    output_payload = output or {}
    condition_result = output_payload.get("result")
    branch_target = output_payload.get("branch")

    matched_edge_ids: set[str] = set()

    for edge in outgoing_edges:
        match_condition = edge.condition_match
        if match_condition is None:
            continue

        normalized_condition = str(match_condition).strip().lower()

        if branch_target is not None and str(branch_target).strip().lower() == normalized_condition:
            matched_edge_ids.add(edge.id)
        elif isinstance(condition_result, bool):
            if condition_result and normalized_condition in ("true", "1", "yes"):
                matched_edge_ids.add(edge.id)
            elif not condition_result and normalized_condition in ("false", "0", "no"):
                matched_edge_ids.add(edge.id)

    if not matched_edge_ids:
        default_edges = [
            edge.id
            for edge in outgoing_edges
            if edge.condition_match is not None
            and str(edge.condition_match).strip().lower() in ("default", "else")
        ]
        matched_edge_ids.update(default_edges)

    taken_edge_ids = [edge.id for edge in outgoing_edges if edge.id in matched_edge_ids]
    dead_edge_ids = [edge.id for edge in outgoing_edges if edge.id not in matched_edge_ids]

    return BranchEvaluationResult(
        taken_edge_ids=taken_edge_ids,
        dead_edge_ids=dead_edge_ids,
    )
