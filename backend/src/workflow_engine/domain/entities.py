import time
from typing import Any

from pydantic import BaseModel, Field

from workflow_engine.domain.types import EdgeState, NodeState, NodeType, RunState


class Node(BaseModel):
    id: str
    type: NodeType
    name: str = ""
    config: dict[str, Any] = Field(default_factory=dict)
    retry_limit: int = 0


class Edge(BaseModel):
    id: str
    from_node_id: str
    to_node_id: str
    condition_match: str | None = None


class WorkflowDefinition(BaseModel):
    id: str
    version: int = 1
    nodes: dict[str, Node] = Field(default_factory=dict)
    edges: dict[str, Edge] = Field(default_factory=dict)


class NodeRunState(BaseModel):
    node_id: str
    status: NodeState = NodeState.PENDING
    output: dict[str, Any] | None = None
    error: str | None = None
    run_after: float | None = None
    attempts: int = 0
    lease_generation: int = 0


class EdgeRunState(BaseModel):
    edge_id: str
    from_node_id: str
    to_node_id: str
    status: EdgeState = EdgeState.PENDING


class WorkflowRun(BaseModel):
    run_id: str
    workflow_id: str
    version: int
    status: RunState = RunState.QUEUED
    node_states: dict[str, NodeRunState] = Field(default_factory=dict)
    edge_states: dict[str, EdgeRunState] = Field(default_factory=dict)
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)
