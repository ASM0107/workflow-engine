import pytest
from workflow_engine.adapters.base import ExecutionContext, ExecutionResult
from workflow_engine.adapters.registry import ExecutorRegistry
from workflow_engine.domain.cancellation import CancellationToken
from workflow_engine.domain.errors import (
    ExecutionError,
    PermanentExecutionError,
    RetryableExecutionError,
    SSRFSecurityError,
    TimeoutExecutionError,
    TransformationError,
    UnregisteredExecutorError,
)
from workflow_engine.domain.types import NodeType


class DummyEchoExecutor:
    def execute(self, context: ExecutionContext) -> ExecutionResult:
        return ExecutionResult(output={"echo": context.config.get("val", 0) * 2})


class DummyFailingExecutor:
    def __init__(self, error: Exception) -> None:
        self._error = error

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        raise self._error


def test_registry_register_and_get() -> None:
    registry = ExecutorRegistry()
    executor = DummyEchoExecutor()
    registry.register(NodeType.MANUAL_TRIGGER, executor)

    assert registry.has(NodeType.MANUAL_TRIGGER)
    assert not registry.has(NodeType.HTTP)
    assert registry.get(NodeType.MANUAL_TRIGGER) is executor


def test_registry_get_unregistered_raises_error() -> None:
    registry = ExecutorRegistry()
    with pytest.raises(UnregisteredExecutorError, match="No executor registered"):
        registry.get(NodeType.HTTP)


def test_registry_execute_delegates_to_registered_executor() -> None:
    registry = ExecutorRegistry()
    registry.register(NodeType.MANUAL_TRIGGER, DummyEchoExecutor())

    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-1",
        node_key="step_1",
        node_type=NodeType.MANUAL_TRIGGER,
        config={"val": 21},
        inputs={},
        cancellation_token=token,
        attempt_number=1,
    )

    result = registry.execute(context)
    assert isinstance(result, ExecutionResult)
    assert result.output == {"echo": 42}
    assert result.selected_edge_ids is None


def test_registry_unregister() -> None:
    registry = ExecutorRegistry()
    executor = DummyEchoExecutor()
    registry.register(NodeType.MANUAL_TRIGGER, executor)
    assert registry.has(NodeType.MANUAL_TRIGGER)

    registry.unregister(NodeType.MANUAL_TRIGGER)
    assert not registry.has(NodeType.MANUAL_TRIGGER)
    with pytest.raises(UnregisteredExecutorError):
        registry.get(NodeType.MANUAL_TRIGGER)


def test_error_hierarchy_classification() -> None:
    perm_err = PermanentExecutionError("Permanent failure")
    assert isinstance(perm_err, ExecutionError)
    assert not isinstance(perm_err, RetryableExecutionError)

    retry_err = RetryableExecutionError("Transient network failure")
    assert isinstance(retry_err, ExecutionError)
    assert not isinstance(retry_err, PermanentExecutionError)

    ssrf_err = SSRFSecurityError("Blocked IP")
    assert isinstance(ssrf_err, PermanentExecutionError)

    transform_err = TransformationError("Syntax error")
    assert isinstance(transform_err, PermanentExecutionError)

    timeout_err = TimeoutExecutionError("Connection timed out")
    assert isinstance(timeout_err, RetryableExecutionError)


def test_failing_executor_raises_typed_error() -> None:
    registry = ExecutorRegistry()
    registry.register(
        NodeType.HTTP,
        DummyFailingExecutor(TimeoutExecutionError("Read timeout")),
    )

    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-err",
        node_key="http_node",
        node_type=NodeType.HTTP,
        config={},
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(TimeoutExecutionError, match="Read timeout"):
        registry.execute(context)
