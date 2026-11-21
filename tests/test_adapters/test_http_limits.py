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

    with pytest.raises(TimeoutExecutionError, match="timed out"):
        executor.execute(context)


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
