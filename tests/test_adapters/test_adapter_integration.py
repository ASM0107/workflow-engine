import http.server
import json
import threading
import time
from typing import Any, Callable, Generator

import pytest
from workflow_engine.adapters.http_adapter import HttpSecurityPolicy
from workflow_engine.adapters.registry import create_default_registry
from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.connection import get_connection
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.worker import WorkflowWorker


class IntegrationMockServerHandler(http.server.BaseHTTPRequestHandler):
    fail_count = 0

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/user/42":
            self._send_json(
                200,
                {
                    "id": 42,
                    "name": "Alex",
                    "email": "alex@example.com",
                    "role": "admin",
                },
            )
        elif self.path == "/flaky":
            IntegrationMockServerHandler.fail_count += 1
            if IntegrationMockServerHandler.fail_count <= 1:
                self._send_json(500, {"error": "temporary server error"})
            else:
                self._send_json(200, {"recovered": True})
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        parsed_body = json.loads(body.decode("utf-8")) if body else {}
        self._send_json(
            201,
            {
                "received_path": self.path,
                "received_body": parsed_body,
                "received_role": self.headers.get("X-Role"),
            },
        )

    def _send_json(self, status: int, data: dict[str, Any]) -> None:
        payload = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


@pytest.fixture
def integration_server() -> Generator[str, None, None]:
    IntegrationMockServerHandler.fail_count = 0
    server = http.server.HTTPServer(("127.0.0.1", 0), IntegrationMockServerHandler)
    port = int(server.server_address[1])
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()
    server.server_close()


def test_end_to_end_workflow_with_native_adapters(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    integration_server: str,
) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[integration_server],
        allow_loopback_for_testing=True,
    )
    registry = create_default_registry(http_policy=policy)

    definition = WorkflowDefinition(
        id="wf_e2e_adapters",
        version=1,
        nodes={
            "start": Node(
                id="start",
                type=NodeType.MANUAL_TRIGGER,
                config={"initial_data": {"user_id": 42}},
            ),
            "fetch": Node(
                id="fetch",
                type=NodeType.HTTP,
                config={
                    "url": f"{integration_server}/user/42",
                    "method": "GET",
                },
            ),
            "transform": Node(
                id="transform",
                type=NodeType.JSON_TRANSFORM,
                config={
                    "mapping": {
                        "account_id": "{{ steps.fetch.body.id }}",
                        "display_name": "{{ steps.fetch.body.name }}",
                        "contact": "{{ steps.fetch.body.email }}",
                        "is_admin": "{{ steps.fetch.body.role }}",
                    }
                },
            ),
            "check_admin": Node(
                id="check_admin",
                type=NodeType.CONDITION,
                config={"query": "steps.transform.result.is_admin == 'admin'"},
            ),
        },
        edges={
            "e_start_fetch": Edge(id="e_start_fetch", from_node_id="start", to_node_id="fetch"),
            "e_fetch_transform": Edge(
                id="e_fetch_transform", from_node_id="fetch", to_node_id="transform"
            ),
            "e_transform_check": Edge(
                id="e_transform_check",
                from_node_id="transform",
                to_node_id="check_admin",
            ),
        },
    )

    wf_id = create_and_publish(definition)
    run_id = run_repo.create_run(wf_id, 1)

    worker = WorkflowWorker(worker_id="integration-worker", registry=registry)

    for _ in range(10):
        claimed = worker.execute_one()
        if not claimed:
            break

    run = run_repo.get_run(run_id)
    assert run["status"] == "SUCCEEDED"

    nodes = run_repo.get_node_runs(run_id)
    assert nodes["start"]["status"] == "SUCCEEDED"
    assert nodes["start"]["output"] == {"user_id": 42}

    assert nodes["fetch"]["status"] == "SUCCEEDED"
    assert nodes["fetch"]["output"]["body"]["email"] == "alex@example.com"

    assert nodes["transform"]["status"] == "SUCCEEDED"
    assert nodes["transform"]["output"]["display_name"] == "Alex"
    assert nodes["transform"]["output"]["account_id"] == 42

    assert nodes["check_admin"]["status"] == "SUCCEEDED"
    assert nodes["check_admin"]["output"]["result"] is True


def test_end_to_end_trigger_transform_http_post(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    integration_server: str,
) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[integration_server],
        allow_loopback_for_testing=True,
    )
    registry = create_default_registry(http_policy=policy)

    definition = WorkflowDefinition(
        id="wf_e2e_trigger_transform_post",
        version=1,
        nodes={
            "trigger": Node(
                id="trigger",
                type=NodeType.MANUAL_TRIGGER,
                config={"initial_data": {"user_id": 99, "action": "promote", "role": "admin"}},
            ),
            "transform": Node(
                id="transform",
                type=NodeType.JSON_TRANSFORM,
                config={
                    "mapping": {
                        "account_id": "{{ steps.trigger.user_id }}",
                        "new_role": "{{ steps.trigger.role }}",
                        "meta": {"operation": "{{ steps.trigger.action }}"},
                    }
                },
            ),
            "post_action": Node(
                id="post_action",
                type=NodeType.HTTP,
                config={
                    "url": (
                        f"{integration_server}/api/accounts/"
                        "{{ steps.transform.result.account_id }}"
                    ),
                    "method": "POST",
                    "headers": {
                        "X-Role": "{{ steps.transform.result.new_role }}",
                    },
                    "json": {
                        "account_id": "{{ steps.transform.result.account_id }}",
                        "role": "{{ steps.transform.result.new_role }}",
                        "operation": "{{ steps.transform.result.meta.operation }}",
                    },
                },
            ),
        },
        edges={
            "e_trigger_transform": Edge(
                id="e_trigger_transform", from_node_id="trigger", to_node_id="transform"
            ),
            "e_transform_post": Edge(
                id="e_transform_post", from_node_id="transform", to_node_id="post_action"
            ),
        },
    )

    wf_id = create_and_publish(definition)
    run_id = run_repo.create_run(wf_id, 1)

    worker = WorkflowWorker(worker_id="e2e-post-worker", registry=registry)
    for _ in range(10):
        claimed = worker.execute_one()
        if not claimed:
            break

    run = run_repo.get_run(run_id)
    assert run["status"] == "SUCCEEDED"

    nodes = run_repo.get_node_runs(run_id)
    assert nodes["trigger"]["status"] == "SUCCEEDED"
    assert nodes["transform"]["status"] == "SUCCEEDED"
    assert nodes["post_action"]["status"] == "SUCCEEDED"

    post_output = nodes["post_action"]["output"]
    assert post_output["status_code"] == 201
    assert post_output["body"]["received_path"] == "/api/accounts/99"
    assert post_output["body"]["received_role"] == "admin"
    assert post_output["body"]["received_body"] == {
        "account_id": 99,
        "role": "admin",
        "operation": "promote",
    }


def test_permanent_ssrf_failure_aborts_without_retrying(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=["http://10.0.0.1"],
        allow_loopback_for_testing=False,
    )
    registry = create_default_registry(http_policy=policy)

    definition = WorkflowDefinition(
        id="wf_ssrf_fail_fast",
        version=1,
        nodes={
            "forbidden_node": Node(
                id="forbidden_node",
                type=NodeType.HTTP,
                config={"url": "http://10.0.0.1/private", "method": "GET"},
                retry_limit=3,
            ),
            "unreachable_child": Node(
                id="unreachable_child",
                type=NodeType.MANUAL_TRIGGER,
            ),
        },
        edges={
            "e1": Edge(
                id="e1",
                from_node_id="forbidden_node",
                to_node_id="unreachable_child",
            )
        },
    )

    wf_id = create_and_publish(definition)
    run_id = run_repo.create_run(wf_id, 1)

    worker = WorkflowWorker(worker_id="ssrf-worker", registry=registry)
    claimed = worker.execute_one()
    assert claimed is True

    run = run_repo.get_run(run_id)
    assert run["status"] == "FAILED"

    nodes = run_repo.get_node_runs(run_id)
    assert nodes["forbidden_node"]["status"] == "FAILED"
    assert "SSRF security policy" in str(nodes["forbidden_node"]["error"])

    assert nodes["unreachable_child"]["status"] == "CANCELLED"


def test_retryable_http_failure_and_subsequent_recovery(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    integration_server: str,
) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[integration_server],
        allow_loopback_for_testing=True,
    )
    registry = create_default_registry(http_policy=policy)

    definition = WorkflowDefinition(
        id="wf_flaky_retry",
        version=1,
        nodes={
            "flaky_node": Node(
                id="flaky_node",
                type=NodeType.HTTP,
                config={
                    "url": f"{integration_server}/flaky",
                    "method": "GET",
                },
                retry_limit=2,
            ),
        },
        edges={},
    )

    wf_id = create_and_publish(definition)
    run_id = run_repo.create_run(wf_id, 1)

    worker = WorkflowWorker(worker_id="retry-worker", registry=registry)

    worker.execute_one()

    nodes_after_first = run_repo.get_node_runs(run_id)
    assert nodes_after_first["flaky_node"]["status"] == "READY"
    assert "status 500" in str(nodes_after_first["flaky_node"]["error"])

    run_after_first = run_repo.get_run(run_id)
    assert run_after_first["status"] == "RUNNING"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE node_runs
                SET run_after = clock_timestamp() - INTERVAL '1 second'
                WHERE run_id = %s;
                """,
                (run_id,),
            )
        conn.commit()

    worker.execute_one()

    nodes_after_second = run_repo.get_node_runs(run_id)
    assert nodes_after_second["flaky_node"]["status"] == "SUCCEEDED"
    assert nodes_after_second["flaky_node"]["output"]["body"]["recovered"] is True

    final_run = run_repo.get_run(run_id)
    assert final_run["status"] == "SUCCEEDED"


def test_cooperative_delay_cancellation(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
) -> None:
    registry = create_default_registry()

    definition = WorkflowDefinition(
        id="wf_cancel_delay",
        version=1,
        nodes={
            "long_delay": Node(
                id="long_delay",
                type=NodeType.DELAY,
                config={"seconds": 2.0},
            )
        },
        edges={},
    )

    wf_id = create_and_publish(definition)
    run_id = run_repo.create_run(wf_id, 1)

    worker = WorkflowWorker(
        worker_id="delay-worker",
        registry=registry,
        lease_duration_seconds=30.0,
        heartbeat_interval_seconds=1.0,
    )

    worker_error: list[Exception] = []

    def run_worker() -> None:
        try:
            worker.execute_one()
        except Exception as exc:
            worker_error.append(exc)

    t = threading.Thread(target=run_worker)
    t.start()

    time.sleep(0.3)
    run_repo.cancel_run(run_id)
    t.join(timeout=5.0)

    run = run_repo.get_run(run_id)
    assert run["status"] == "CANCELLED"
