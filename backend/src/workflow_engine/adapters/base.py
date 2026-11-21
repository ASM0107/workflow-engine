from dataclasses import dataclass, field
from typing import Any, Protocol

from workflow_engine.domain.cancellation import CancellationToken
from workflow_engine.domain.types import NodeType


@dataclass(frozen=True)
class ExecutionContext:
    run_id: str
    node_key: str
    node_type: NodeType
    config: dict[str, Any]
    inputs: dict[str, Any]
    cancellation_token: CancellationToken
    attempt_number: int = 1


@dataclass(frozen=True)
class ExecutionResult:
    output: dict[str, Any] = field(default_factory=dict)
    selected_edge_ids: list[str] | None = None


class NodeExecutor(Protocol):
    def execute(self, context: ExecutionContext) -> ExecutionResult: ...
