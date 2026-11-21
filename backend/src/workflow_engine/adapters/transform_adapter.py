import re
import time
from typing import Any

import jmespath
from jmespath.exceptions import JMESPathError

from workflow_engine.adapters.base import ExecutionContext, ExecutionResult
from workflow_engine.domain.errors import PermanentExecutionError, TransformationError

TEMPLATE_PATTERN = re.compile(r"\{\{\s*([a-zA-Z0-9_.\-\[\]]+)\s*\}\}")
FULL_TEMPLATE_PATTERN = re.compile(r"^\s*\{\{\s*([a-zA-Z0-9_.\-\[\]]+)\s*\}\}\s*$")
DISALLOWED_SUBSTRINGS = frozenset(["__", "import", "exec", "eval", "lambda"])


class SafeTemplateEngine:
    @staticmethod
    def evaluate_expression(expression: str, data: dict[str, Any]) -> Any:
        cleaned_expr = expression.strip()
        if not cleaned_expr:
            raise TransformationError("Empty transformation expression")

        for disallowed in DISALLOWED_SUBSTRINGS:
            if disallowed in cleaned_expr:
                raise TransformationError(
                    f"Prohibited substring '{disallowed}' in transformation expression"
                )

        try:
            compiled = jmespath.compile(cleaned_expr)
            return compiled.search(data)
        except JMESPathError as exc:
            raise TransformationError(
                f"Invalid JMESPath expression '{cleaned_expr}': {exc}"
            ) from exc
        except Exception as exc:
            raise TransformationError(
                f"Failed to evaluate expression '{cleaned_expr}': {exc}"
            ) from exc

    @classmethod
    def render_template(cls, template_str: str, data: dict[str, Any]) -> Any:
        full_match = FULL_TEMPLATE_PATTERN.match(template_str)
        if full_match:
            expr = full_match.group(1)
            return cls.evaluate_expression(expr, data)

        def replace_match(match: re.Match[str]) -> str:
            expr = match.group(1)
            val = cls.evaluate_expression(expr, data)
            if val is None:
                return ""
            return str(val)

        return TEMPLATE_PATTERN.sub(replace_match, template_str)

    @classmethod
    def render_data_structure(cls, structure: Any, data: dict[str, Any]) -> Any:
        if isinstance(structure, str):
            return cls.render_template(structure, data)
        if isinstance(structure, dict):
            return {str(k): cls.render_data_structure(v, data) for k, v in structure.items()}
        if isinstance(structure, list):
            return [cls.render_data_structure(item, data) for item in structure]
        return structure


class JsonTransformNodeExecutor:
    def __init__(self, template_engine: SafeTemplateEngine | None = None) -> None:
        self.engine = template_engine or SafeTemplateEngine()

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        config = context.config
        eval_data = self._build_evaluation_data(context)

        query = config.get("query") or config.get("expression") or config.get("jmespath")
        mapping = config.get("mapping") or config.get("template")

        if query:
            if not isinstance(query, str):
                raise PermanentExecutionError("Query in JSON transform must be a string")
            transformed = self.engine.evaluate_expression(query, eval_data)
            output: dict[str, Any]
            if isinstance(transformed, dict):
                output = {"result": transformed, **transformed}
            else:
                output = {"result": transformed}
            return ExecutionResult(output=output)

        if mapping is not None:
            rendered = self.engine.render_data_structure(mapping, eval_data)
            if isinstance(rendered, dict):
                output = {"result": rendered, **rendered}
            else:
                output = {"result": rendered}
            return ExecutionResult(output=output)

        output = {"result": eval_data}
        return ExecutionResult(output=output)

    def _build_evaluation_data(self, context: ExecutionContext) -> dict[str, Any]:
        inputs = context.inputs
        steps = inputs.get("steps") if isinstance(inputs.get("steps"), dict) else inputs
        return {
            "steps": steps,
            "inputs": inputs,
            "config": context.config,
            **inputs,
        }


class ConditionNodeExecutor:
    def __init__(self, template_engine: SafeTemplateEngine | None = None) -> None:
        self.engine = template_engine or SafeTemplateEngine()

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        config = context.config
        eval_data = self._build_evaluation_data(context)

        query = config.get("query") or config.get("expression")
        if query:
            if not isinstance(query, str):
                raise PermanentExecutionError("Query in condition node must be a string")
            val = self.engine.evaluate_expression(query, eval_data)
            bool_result = bool(val)
            branch = config.get("branch") or ("true" if bool_result else "false")
            return ExecutionResult(
                output={"result": bool_result, "branch": branch, "evaluated_value": val}
            )

        if "branch" in config:
            return ExecutionResult(output={"result": True, "branch": str(config["branch"])})

        if "result" in config:
            bool_result = bool(config["result"])
            return ExecutionResult(
                output={
                    "result": bool_result,
                    "branch": "true" if bool_result else "false",
                }
            )

        return ExecutionResult(output={"result": True, "branch": "default"})

    def _build_evaluation_data(self, context: ExecutionContext) -> dict[str, Any]:
        inputs = context.inputs
        steps = inputs.get("steps") if isinstance(inputs.get("steps"), dict) else inputs
        return {
            "steps": steps,
            "inputs": inputs,
            "config": context.config,
            **inputs,
        }


class ManualTriggerNodeExecutor:
    def execute(self, context: ExecutionContext) -> ExecutionResult:
        payload = context.config.get(
            "initial_data",
            context.config.get("payload", context.inputs),
        )
        if isinstance(payload, dict):
            return ExecutionResult(output=dict(payload))
        return ExecutionResult(output={"data": payload})


class DelayNodeExecutor:
    def execute(self, context: ExecutionContext) -> ExecutionResult:
        raw_seconds = context.config.get(
            "seconds",
            context.config.get("duration", 0),
        )
        try:
            seconds = float(raw_seconds)
        except (ValueError, TypeError) as exc:
            raise PermanentExecutionError(f"Invalid delay duration '{raw_seconds}': {exc}") from exc

        if seconds < 0:
            raise PermanentExecutionError("Delay duration cannot be negative")

        remaining = seconds
        step = 0.1
        while remaining > 0:
            context.cancellation_token.check_cancelled()
            sleep_time = min(step, remaining)
            time.sleep(sleep_time)
            remaining -= sleep_time

        context.cancellation_token.check_cancelled()
        return ExecutionResult(output={"delayed_seconds": seconds})
