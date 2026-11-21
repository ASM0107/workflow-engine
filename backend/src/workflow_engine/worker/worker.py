import inspect
import uuid
from typing import Any, Callable

from workflow_engine.adapters.base import ExecutionContext, ExecutionResult
from workflow_engine.adapters.registry import ExecutorRegistry, create_default_registry
from workflow_engine.domain.cancellation import CancellationToken
from workflow_engine.domain.entities import ClaimedNode
from workflow_engine.domain.errors import (
    FencingTokenError,
    LeaseLostError,
    PermanentExecutionError,
)
from workflow_engine.persistence.connection import get_connection
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.completion import CompletionEngine
from workflow_engine.worker.heartbeat import HeartbeatMonitor, HeartbeatService

__all__ = ["CancellationToken", "WorkflowWorker"]


class WorkflowWorker:
    def __init__(
        self,
        worker_id: str | None = None,
        lease_duration_seconds: float = 30.0,
        heartbeat_interval_seconds: float = 5.0,
        claim_engine: ClaimEngine | None = None,
        heartbeat_service: HeartbeatService | None = None,
        completion_engine: CompletionEngine | None = None,
        registry: ExecutorRegistry | None = None,
    ) -> None:
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.lease_duration_seconds = lease_duration_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.claim_engine = claim_engine or ClaimEngine(
            default_lease_seconds=lease_duration_seconds
        )
        self.heartbeat_service = heartbeat_service or HeartbeatService(
            default_extension_seconds=lease_duration_seconds
        )
        self.completion_engine = completion_engine or CompletionEngine()
        self.registry = registry or create_default_registry()

    def _fetch_run_inputs(self, run_id: str) -> dict[str, Any]:
        query = """
            SELECT node_key, output
            FROM node_runs
            WHERE run_id = %s
              AND status = 'SUCCEEDED'
              AND output IS NOT NULL;
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (run_id,))
                rows = cur.fetchall()
                steps = {row["node_key"]: row["output"] for row in rows}
                return {"steps": steps, **steps}

    def execute_one(
        self,
        executor_callback: Callable[..., Any] | None = None,
    ) -> bool:
        claimed_node: ClaimedNode | None = self.claim_engine.discover_and_claim_node(
            worker_id=self.worker_id,
            lease_seconds=self.lease_duration_seconds,
        )
        if not claimed_node:
            return False

        cancellation_token = CancellationToken()

        def handle_lease_lost() -> None:
            cancellation_token.cancel()

        monitor = HeartbeatMonitor(
            service=self.heartbeat_service,
            claimed_node=claimed_node,
            interval_seconds=self.heartbeat_interval_seconds,
            extension_seconds=self.lease_duration_seconds,
            on_lease_lost=handle_lease_lost,
        )
        monitor.start()

        try:
            result: Any
            if executor_callback is not None:
                callback_params = list(inspect.signature(executor_callback).parameters.values())
                if len(callback_params) >= 2:
                    result = executor_callback(claimed_node, cancellation_token)
                else:
                    result = executor_callback(claimed_node)
            else:
                inputs = self._fetch_run_inputs(claimed_node.run_id)
                context = ExecutionContext(
                    run_id=claimed_node.run_id,
                    node_key=claimed_node.node_key,
                    node_type=claimed_node.node_type,
                    config=claimed_node.config,
                    inputs=inputs,
                    cancellation_token=cancellation_token,
                    attempt_number=claimed_node.attempt_number,
                )
                result = self.registry.execute(context)

            cancellation_token.check_cancelled()
            monitor.check_lease_validity()
            monitor.stop()

            output: dict[str, Any] | None = None
            selected_edge_ids: list[str] | None = None
            if isinstance(result, ExecutionResult):
                output = result.output
                selected_edge_ids = result.selected_edge_ids
            elif isinstance(result, tuple) and len(result) == 2:
                output, selected_edge_ids = result
            elif isinstance(result, dict):
                output = result

            self.completion_engine.complete_success(
                claimed_node,
                output=output or {},
                selected_edge_ids=selected_edge_ids,
            )
            return True
        except (FencingTokenError, LeaseLostError):
            monitor.stop()
            raise
        except Exception as exc:
            monitor.stop()
            if monitor.is_lease_lost or cancellation_token.is_cancelled:
                raise LeaseLostError(
                    f"Execution failed due to lease loss on node '{claimed_node.node_key}'"
                ) from exc
            is_retryable = not isinstance(exc, PermanentExecutionError)
            self.completion_engine.complete_failure(claimed_node, str(exc), retryable=is_retryable)
            return True
