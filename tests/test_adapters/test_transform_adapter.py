import pytest
from workflow_engine.adapters.base import ExecutionContext
from workflow_engine.adapters.transform_adapter import (
    ConditionNodeExecutor,
    DelayNodeExecutor,
    JsonTransformNodeExecutor,
    ManualTriggerNodeExecutor,
    SafeTemplateEngine,
)
from workflow_engine.domain.cancellation import CancellationToken
from workflow_engine.domain.errors import (
    LeaseLostError,
    PermanentExecutionError,
    TransformationError,
)
from workflow_engine.domain.types import NodeType


def test_jmespath_basic_and_nested_queries() -> None:
    engine = SafeTemplateEngine()
    data = {
        "steps": {
            "fetch_users": {
                "users": [
                    {"id": 1, "name": "Alice", "active": True},
                    {"id": 2, "name": "Bob", "active": False},
                    {"id": 3, "name": "Charlie", "active": True},
                ],
                "meta": {"total": 3},
            }
        }
    }

    assert engine.evaluate_expression("steps.fetch_users.meta.total", data) == 3
    assert engine.evaluate_expression("steps.fetch_users.users[0].name", data) == "Alice"
    assert engine.evaluate_expression("steps.fetch_users.users[*].id", data) == [1, 2, 3]
    active_users = engine.evaluate_expression(
        "steps.fetch_users.users[?active == `true`].name", data
    )
    assert active_users == ["Alice", "Charlie"]


def test_safe_template_engine_preserves_native_types() -> None:
    engine = SafeTemplateEngine()
    data = {"steps": {"step_a": {"num": 42, "flag": True, "obj": {"k": "v"}}}}

    assert engine.render_template("{{ steps.step_a.num }}", data) == 42
    assert engine.render_template("{{ steps.step_a.flag }}", data) is True
    assert engine.render_template("{{ steps.step_a.obj }}", data) == {"k": "v"}


def test_safe_template_engine_string_interpolation() -> None:
    engine = SafeTemplateEngine()
    data = {"steps": {"user": {"first": "John", "last": "Doe"}}}
    template = "Welcome, {{ steps.user.first }} {{ steps.user.last }}!"
    rendered = engine.render_template(template, data)
    assert rendered == "Welcome, John Doe!"


def test_safe_template_engine_nested_structure() -> None:
    engine = SafeTemplateEngine()
    data = {
        "steps": {
            "source": {
                "title": "Report",
                "count": 10,
                "tags": ["alpha", "beta"],
            }
        }
    }
    mapping = {
        "header": "Doc: {{ steps.source.title }}",
        "items_count": "{{ steps.source.count }}",
        "tags_list": "{{ steps.source.tags }}",
        "nested": {"first_tag": "{{ steps.source.tags[0] }}"},
    }

    result = engine.render_data_structure(mapping, data)
    assert result == {
        "header": "Doc: Report",
        "items_count": 10,
        "tags_list": ["alpha", "beta"],
        "nested": {"first_tag": "alpha"},
    }


def test_prohibited_expressions_rejected() -> None:
    engine = SafeTemplateEngine()
    data = {"foo": "bar"}

    with pytest.raises(TransformationError, match="Prohibited substring '__'"):
        engine.evaluate_expression("__class__.__base__", data)

    with pytest.raises(TransformationError, match="Prohibited substring 'import'"):
        engine.evaluate_expression("import os", data)

    with pytest.raises(TransformationError, match="Prohibited substring 'eval'"):
        engine.evaluate_expression("eval('1+1')", data)

    with pytest.raises(TransformationError, match="Empty transformation expression"):
        engine.evaluate_expression("   ", data)


def test_invalid_jmespath_syntax_raises_transformation_error() -> None:
    engine = SafeTemplateEngine()
    with pytest.raises(TransformationError, match="Invalid JMESPath expression"):
        engine.evaluate_expression("users[?active == ]", {"users": []})


def test_json_transform_node_executor_with_query() -> None:
    executor = JsonTransformNodeExecutor()
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-t1",
        node_key="transform_1",
        node_type=NodeType.JSON_TRANSFORM,
        config={"query": "steps.fetch.data[0].id"},
        inputs={"steps": {"fetch": {"data": [{"id": "item-99"}]}}},
        cancellation_token=token,
    )

    result = executor.execute(context)
    assert result.output["result"] == "item-99"


def test_json_transform_node_executor_with_mapping() -> None:
    executor = JsonTransformNodeExecutor()
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-t2",
        node_key="transform_2",
        node_type=NodeType.JSON_TRANSFORM,
        config={
            "mapping": {
                "user_id": "{{ steps.auth.user.id }}",
                "email": "{{ steps.auth.user.email }}",
            }
        },
        inputs={"steps": {"auth": {"user": {"id": 101, "email": "user@example.com"}}}},
        cancellation_token=token,
    )

    result = executor.execute(context)
    assert result.output["result"] == {"user_id": 101, "email": "user@example.com"}
    assert result.output["user_id"] == 101


def test_condition_node_executor_boolean_evaluation() -> None:
    executor = ConditionNodeExecutor()
    token = CancellationToken()

    true_ctx = ExecutionContext(
        run_id="run-c1",
        node_key="cond_true",
        node_type=NodeType.CONDITION,
        config={"query": "steps.check.count > `0`"},
        inputs={"steps": {"check": {"count": 5}}},
        cancellation_token=token,
    )
    true_res = executor.execute(true_ctx)
    assert true_res.output["result"] is True
    assert true_res.output["branch"] == "true"

    false_ctx = ExecutionContext(
        run_id="run-c2",
        node_key="cond_false",
        node_type=NodeType.CONDITION,
        config={"query": "steps.check.count > `10`"},
        inputs={"steps": {"check": {"count": 5}}},
        cancellation_token=token,
    )
    false_res = executor.execute(false_ctx)
    assert false_res.output["result"] is False
    assert false_res.output["branch"] == "false"


def test_manual_trigger_executor() -> None:
    executor = ManualTriggerNodeExecutor()
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-trig",
        node_key="trigger",
        node_type=NodeType.MANUAL_TRIGGER,
        config={"initial_data": {"user_id": 555}},
        inputs={},
        cancellation_token=token,
    )

    result = executor.execute(context)
    assert result.output == {"user_id": 555}


def test_delay_executor_and_cancellation() -> None:
    executor = DelayNodeExecutor()
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-delay",
        node_key="delay",
        node_type=NodeType.DELAY,
        config={"seconds": 0.05},
        inputs={},
        cancellation_token=token,
    )

    res = executor.execute(context)
    assert res.output["delayed_seconds"] == 0.05

    token.cancel()
    with pytest.raises(LeaseLostError):
        executor.execute(context)


def test_delay_invalid_duration_raises_permanent_error() -> None:
    executor = DelayNodeExecutor()
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-delay-inv",
        node_key="delay",
        node_type=NodeType.DELAY,
        config={"seconds": -5},
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(PermanentExecutionError, match="cannot be negative"):
        executor.execute(context)
