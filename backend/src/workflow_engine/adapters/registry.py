from workflow_engine.adapters.base import ExecutionContext, ExecutionResult, NodeExecutor
from workflow_engine.domain.errors import UnregisteredExecutorError
from workflow_engine.domain.types import NodeType


class ExecutorRegistry:
    def __init__(self) -> None:
        self._executors: dict[NodeType, NodeExecutor] = {}

    def register(self, node_type: NodeType, executor: NodeExecutor) -> None:
        self._executors[node_type] = executor

    def unregister(self, node_type: NodeType) -> None:
        self._executors.pop(node_type, None)

    def has(self, node_type: NodeType) -> bool:
        return node_type in self._executors

    def get(self, node_type: NodeType) -> NodeExecutor:
        executor = self._executors.get(node_type)
        if executor is None:
            raise UnregisteredExecutorError(
                f"No executor registered for node type '{node_type.value}'"
            )
        return executor

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        executor = self.get(context.node_type)
        return executor.execute(context)
