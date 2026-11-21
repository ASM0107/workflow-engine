import http.server
import threading
import time
from typing import Any, Generator

import pytest
from workflow_engine.adapters.base import ExecutionContext
from workflow_engine.adapters.http_adapter import HttpNodeExecutor, HttpSecurityPolicy
from workflow_engine.domain.cancellation import CancellationToken
from workflow_engine.domain.errors import (
    LeaseLostError,
    PermanentExecutionError,
    ResponseSizeLimitExceededError,
    TimeoutExecutionError,
)
from workflow_engine.domain.types import NodeType


class LimitsServerHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/huge_content_length":
            self.send_response(200)
            self.send_header("Content-Length", "100000000")
            self.end_headers()
            self.wfile.write(b"a" * 100)
        elif self.path == "/streaming_overflow":
            self.send_response(200)
            self.end_headers()
            chunk = b"X" * 1024
            for _ in range(50):
                self.wfile.write(chunk)
                self.wfile.flush()
        elif self.path == "/slow_read":
            time.sleep(2.0)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"done")
        elif self.path == "/slow_streaming":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            for _ in range(30):
                self.wfile.write(b"tick\n")
                self.wfile.flush()
                time.sleep(0.1)
        elif self.path == "/redirect_loop":
            self.send_response(302)
            self.send_header("Location", "/redirect_loop")
            self.end_headers()
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")


@pytest.fixture
def limits_server() -> Generator[str, None, None]:
    server = http.server.HTTPServer(("127.0.0.1", 0), LimitsServerHandler)
    port = int(server.server_address[1])
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()
    server.server_close()


def test_content_length_exceeding_limit_aborts_immediately(limits_server: str) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[limits_server],
        allow_loopback_for_testing=True,
        max_response_bytes=1024,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-cl-limit",
        node_key="cl_limit_node",
        node_type=NodeType.HTTP,
        config={"url": f"{limits_server}/huge_content_length", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(ResponseSizeLimitExceededError, match="exceeds limit"):
        executor.execute(context)


def test_streaming_body_exceeding_limit_aborts(limits_server: str) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[limits_server],
        allow_loopback_for_testing=True,
        max_response_bytes=4096,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-stream-limit",
        node_key="stream_limit_node",
        node_type=NodeType.HTTP,
        config={"url": f"{limits_server}/streaming_overflow", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(ResponseSizeLimitExceededError, match="exceeded limit"):
        executor.execute(context)


def test_read_timeout_enforced(limits_server: str) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[limits_server],
        allow_loopback_for_testing=True,
        read_timeout_seconds=0.2,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-timeout",
        node_key="timeout_node",
        node_type=NodeType.HTTP,
        config={"url": f"{limits_server}/slow_read", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(TimeoutExecutionError):
        executor.execute(context)


def test_per_node_timeout_clamped_to_policy_ceiling(limits_server: str) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[limits_server],
        allow_loopback_for_testing=True,
        read_timeout_seconds=0.2,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-clamp-timeout",
        node_key="clamp_node",
        node_type=NodeType.HTTP,
        config={
            "url": f"{limits_server}/slow_read",
            "method": "GET",
            "read_timeout": 30.0,
        },
        inputs={},
        cancellation_token=token,
    )

    start = time.monotonic()
    with pytest.raises(TimeoutExecutionError):
        executor.execute(context)
    elapsed = time.monotonic() - start
    assert elapsed < 1.0


def test_per_node_max_bytes_clamped_to_policy_ceiling(limits_server: str) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[limits_server],
        allow_loopback_for_testing=True,
        max_response_bytes=1024,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-clamp-bytes",
        node_key="clamp_bytes_node",
        node_type=NodeType.HTTP,
        config={
            "url": f"{limits_server}/huge_content_length",
            "method": "GET",
            "max_response_bytes": 100000000,
        },
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(ResponseSizeLimitExceededError):
        executor.execute(context)


@pytest.mark.parametrize(
    ("bad_field", "bad_value"),
    [
        ("read_timeout", -1.0),
        ("read_timeout", 0.0),
        ("read_timeout", float("nan")),
        ("read_timeout", float("inf")),
        ("connect_timeout", -5.0),
        ("connect_timeout", 0.0),
        ("connect_timeout", float("nan")),
        ("connect_timeout", float("inf")),
        ("max_response_bytes", 0),
        ("max_response_bytes", -100),
    ],
)
def test_invalid_resource_limits_raise_permanent_error(
    limits_server: str, bad_field: str, bad_value: Any
) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[limits_server],
        allow_loopback_for_testing=True,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-invalid-limit",
        node_key="invalid_limit_node",
        node_type=NodeType.HTTP,
        config={
            "url": f"{limits_server}/",
            "method": "GET",
            bad_field: bad_value,
        },
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(PermanentExecutionError):
        executor.execute(context)


def test_in_flight_cancellation_aborts_slow_request_within_interval(limits_server: str) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[limits_server],
        allow_loopback_for_testing=True,
        read_timeout_seconds=5.0,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()

    context = ExecutionContext(
        run_id="run-inflight-cancel",
        node_key="inflight_node",
        node_type=NodeType.HTTP,
        config={"url": f"{limits_server}/slow_read", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    raised_error: list[Exception] = []
    cancel_time: list[float] = []
    abort_time: list[float] = []

    def execute_worker() -> None:
        try:
            executor.execute(context)
        except Exception as exc:
            abort_time.append(time.monotonic())
            raised_error.append(exc)

    thread = threading.Thread(target=execute_worker)
    thread.start()

    time.sleep(0.05)
    cancel_time.append(time.monotonic())
    token.cancel()

    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert len(raised_error) == 1
    assert isinstance(raised_error[0], LeaseLostError)

    elapsed_after_cancel = abort_time[0] - cancel_time[0]
    assert elapsed_after_cancel <= 0.15


def test_in_flight_cancellation_during_streaming_body(limits_server: str) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[limits_server],
        allow_loopback_for_testing=True,
        read_timeout_seconds=5.0,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()

    context = ExecutionContext(
        run_id="run-stream-cancel",
        node_key="stream_cancel_node",
        node_type=NodeType.HTTP,
        config={"url": f"{limits_server}/slow_streaming", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    raised_error: list[Exception] = []
    cancel_time: list[float] = []
    abort_time: list[float] = []

    def execute_worker() -> None:
        try:
            executor.execute(context)
        except Exception as exc:
            abort_time.append(time.monotonic())
            raised_error.append(exc)

    thread = threading.Thread(target=execute_worker)
    thread.start()

    time.sleep(0.05)
    cancel_time.append(time.monotonic())
    token.cancel()

    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert len(raised_error) == 1
    assert isinstance(raised_error[0], LeaseLostError)

    elapsed_after_cancel = abort_time[0] - cancel_time[0]
    assert elapsed_after_cancel <= 0.15


def test_max_redirects_exceeded_aborts(limits_server: str) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[limits_server],
        allow_loopback_for_testing=True,
        max_redirects=3,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-redir-loop",
        node_key="loop_node",
        node_type=NodeType.HTTP,
        config={"url": f"{limits_server}/redirect_loop", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(PermanentExecutionError, match="Maximum redirect hops exceeded"):
        executor.execute(context)


def test_cancellation_aborts_http_execution(limits_server: str) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[limits_server],
        allow_loopback_for_testing=True,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    token.cancel()

    context = ExecutionContext(
        run_id="run-cancel",
        node_key="cancel_node",
        node_type=NodeType.HTTP,
        config={"url": f"{limits_server}/", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(LeaseLostError, match="Execution cancelled"):
        executor.execute(context)
