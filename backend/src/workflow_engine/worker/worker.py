import inspect
import uuid
from typing import Any, Callable

from workflow_engine.domain.cancellation import CancellationToken
from workflow_engine.domain.entities import ClaimedNode
from workflow_engine.domain.errors import FencingTokenError, LeaseLostError
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

    def execute_one(
        self,
        executor_callback: Callable[..., dict[str, Any] | None],
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
            callback_params = list(inspect.signature(executor_callback).parameters.values())
            if len(callback_params) >= 2:
                result = executor_callback(claimed_node, cancellation_token)
            else:
                result = executor_callback(claimed_node)

            cancellation_token.check_cancelled()
            monitor.check_lease_validity()
            monitor.stop()

            output: dict[str, Any] | None = None
            selected_edge_ids: list[str] | None = None
            if isinstance(result, tuple) and len(result) == 2:
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
            self.completion_engine.complete_failure(claimed_node, str(exc))
            return True
