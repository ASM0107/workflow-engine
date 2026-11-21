import http.client
import http.server
import socket
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
            self.wfile.write(b"tick\n")
            self.wfile.flush()
            if hasattr(self.server, "streaming_chunk_sent"):
                self.server.streaming_chunk_sent.set()
            for _ in range(30):
                self.wfile.write(b"tick\n")
                self.wfile.flush()
                time.sleep(0.1)
        elif self.path == "/partial_headers":
            self.wfile.write(b"HTTP/1.1 200 OK\r\nX-Incomplete: true\r\n")
            self.wfile.flush()
            if hasattr(self.server, "partial_headers_sent"):
                self.server.partial_headers_sent.set()
            time.sleep(2.0)
            self.wfile.write(b"\r\n")
        elif self.path == "/persistent_keepalive_small":
            self.wfile.write(
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nOK"
            )
            self.wfile.flush()
            time.sleep(1.0)
        elif self.path == "/persistent_keepalive_large":
            body = b"Z" * 65536
            header = (
                f"HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\nConnection: keep-alive\r\n\r\n"
            ).encode()
            self.wfile.write(header + body)
            self.wfile.flush()
            time.sleep(1.0)
        elif self.path == "/oversized_headers":
            self.wfile.write(b"HTTP/1.1 200 OK\r\nX-Long-Header: " + (b"A" * 70000) + b"\r\n\r\nOK")
            self.wfile.flush()
        elif self.path == "/interim_100_then_200":
            self.wfile.write(
                b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"
            )
            self.wfile.flush()
        elif self.path == "/interim_103_then_200":
            self.wfile.write(
                b"HTTP/1.1 103 Early Hints\r\n"
                b"Link: </style.css>; rel=preload\r\n\r\n"
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Length: 2\r\n\r\n"
                b"OK"
            )
            self.wfile.flush()
        elif self.path == "/interim_100_stall":
            self.wfile.write(b"HTTP/1.1 100 Continue\r\n\r\n")
            self.wfile.flush()
            if hasattr(self.server, "interim_100_sent"):
                self.server.interim_100_sent.set()
            time.sleep(5.0)
        elif self.path == "/interim_103_stall":
            self.wfile.write(b"HTTP/1.1 103 Early Hints\r\nLink: </style.css>; rel=preload\r\n\r\n")
            self.wfile.flush()
            if hasattr(self.server, "interim_103_sent"):
                self.server.interim_103_sent.set()
            time.sleep(5.0)
        elif self.path == "/redirect_loop":
            self.send_response(302)
            self.send_header("Location", "/redirect_loop")
            self.end_headers()
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")


class LimitsHTTPServer(http.server.HTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        RequestHandlerClass: type[http.server.BaseHTTPRequestHandler],
    ) -> None:
        super().__init__(server_address, RequestHandlerClass)
        self.partial_headers_sent = threading.Event()
        self.streaming_chunk_sent = threading.Event()
        self.interim_100_sent = threading.Event()
        self.interim_103_sent = threading.Event()


class LimitsServer(str):
    partial_headers_sent: threading.Event
    streaming_chunk_sent: threading.Event
    interim_100_sent: threading.Event
    interim_103_sent: threading.Event
    server: LimitsHTTPServer

    @classmethod
    def create(cls, server: LimitsHTTPServer) -> "LimitsServer":
        port = int(server.server_address[1])
        inst = cls(f"http://127.0.0.1:{port}")
        inst.server = server
        inst.partial_headers_sent = server.partial_headers_sent
        inst.streaming_chunk_sent = server.streaming_chunk_sent
        inst.interim_100_sent = server.interim_100_sent
        inst.interim_103_sent = server.interim_103_sent
        return inst


@pytest.fixture
def limits_server() -> Generator[LimitsServer, None, None]:
    server = LimitsHTTPServer(("127.0.0.1", 0), LimitsServerHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    yield LimitsServer.create(server)
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
        ("max_response_bytes", True),
        ("max_response_bytes", False),
        ("max_response_bytes", 12.5),
        ("max_response_bytes", "12.5"),
        ("max_response_bytes", "1e5"),
        ("max_response_bytes", float("inf")),
        ("max_response_bytes", float("nan")),
        ("max_response_bytes", 0),
        ("max_response_bytes", -10),
        ("max_response_header_bytes", True),
        ("max_response_header_bytes", False),
        ("max_response_header_bytes", 12.5),
        ("max_response_header_bytes", "12.5"),
        ("max_response_header_bytes", "1e5"),
        ("max_response_header_bytes", float("inf")),
        ("max_response_header_bytes", float("nan")),
        ("max_response_header_bytes", 0),
        ("max_response_header_bytes", -10),
        ("read_timeout", True),
        ("read_timeout", False),
        ("read_timeout", -1.0),
        ("read_timeout", 0.0),
        ("read_timeout", float("nan")),
        ("read_timeout", float("inf")),
        ("connect_timeout", True),
        ("connect_timeout", False),
        ("connect_timeout", -5.0),
        ("connect_timeout", 0.0),
        ("connect_timeout", float("nan")),
        ("connect_timeout", float("inf")),
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


def test_in_flight_cancellation_during_partial_response_headers(
    limits_server: LimitsServer,
) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[str(limits_server)],
        allow_loopback_for_testing=True,
        read_timeout_seconds=5.0,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()

    context = ExecutionContext(
        run_id="run-partial-cancel",
        node_key="partial_cancel_node",
        node_type=NodeType.HTTP,
        config={"url": f"{limits_server}/partial_headers", "method": "GET"},
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

    assert limits_server.partial_headers_sent.wait(timeout=5.0)
    cancel_time.append(time.monotonic())
    token.cancel()

    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert len(raised_error) == 1
    assert isinstance(raised_error[0], LeaseLostError)

    elapsed_after_cancel = abort_time[0] - cancel_time[0]
    assert elapsed_after_cancel <= 0.15


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


def test_in_flight_cancellation_during_streaming_body(limits_server: LimitsServer) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[str(limits_server)],
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

    assert limits_server.streaming_chunk_sent.wait(timeout=5.0)
    cancel_time.append(time.monotonic())
    token.cancel()

    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert len(raised_error) == 1
    assert isinstance(raised_error[0], LeaseLostError)

    elapsed_after_cancel = abort_time[0] - cancel_time[0]
    assert elapsed_after_cancel <= 0.15


def test_persistent_connection_small_body_already_buffered(
    limits_server: LimitsServer,
) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[str(limits_server)],
        allow_loopback_for_testing=True,
        read_timeout_seconds=0.2,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-keepalive-small",
        node_key="keepalive_small_node",
        node_type=NodeType.HTTP,
        config={
            "url": f"{limits_server}/persistent_keepalive_small",
            "method": "GET",
        },
        inputs={},
        cancellation_token=token,
    )

    start_time = time.monotonic()
    result = executor.execute(context)
    elapsed = time.monotonic() - start_time

    assert result.output["status_code"] == 200
    assert result.output["body"] == "OK"
    assert elapsed < 0.15


def test_persistent_connection_large_body_already_buffered(
    limits_server: LimitsServer,
) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[str(limits_server)],
        allow_loopback_for_testing=True,
        read_timeout_seconds=0.2,
        max_response_bytes=100000,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-keepalive-large",
        node_key="keepalive_large_node",
        node_type=NodeType.HTTP,
        config={
            "url": f"{limits_server}/persistent_keepalive_large",
            "method": "GET",
            "max_response_bytes": 100000,
        },
        inputs={},
        cancellation_token=token,
    )

    start_time = time.monotonic()
    result = executor.execute(context)
    elapsed = time.monotonic() - start_time

    assert result.output["status_code"] == 200
    assert len(result.output["body"]) == 65536
    assert elapsed < 0.15


def test_oversized_response_headers_rejected(limits_server: LimitsServer) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[str(limits_server)],
        allow_loopback_for_testing=True,
        max_response_header_bytes=1024,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-oversized-headers",
        node_key="oversized_headers_node",
        node_type=NodeType.HTTP,
        config={
            "url": f"{limits_server}/oversized_headers",
            "method": "GET",
            "max_response_header_bytes": 1024,
        },
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(ResponseSizeLimitExceededError, match="Response headers exceeded limit"):
        executor.execute(context)


def test_interim_100_continue_handled_and_succeeds(limits_server: LimitsServer) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[str(limits_server)],
        allow_loopback_for_testing=True,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-interim-100",
        node_key="interim_100_node",
        node_type=NodeType.HTTP,
        config={"url": f"{limits_server}/interim_100_then_200", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    result = executor.execute(context)
    assert result.output["status_code"] == 200
    assert result.output["body"] == "OK"


def test_interim_103_early_hints_handled_and_succeeds(limits_server: LimitsServer) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[str(limits_server)],
        allow_loopback_for_testing=True,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-interim-103",
        node_key="interim_103_node",
        node_type=NodeType.HTTP,
        config={"url": f"{limits_server}/interim_103_then_200", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    result = executor.execute(context)
    assert result.output["status_code"] == 200
    assert result.output["body"] == "OK"


def test_cancellation_during_interim_100_stall(limits_server: LimitsServer) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[str(limits_server)],
        allow_loopback_for_testing=True,
        read_timeout_seconds=5.0,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-interim-100-stall",
        node_key="interim_100_stall_node",
        node_type=NodeType.HTTP,
        config={"url": f"{limits_server}/interim_100_stall", "method": "GET"},
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

    assert limits_server.interim_100_sent.wait(timeout=5.0)
    cancel_time.append(time.monotonic())
    token.cancel()

    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert len(raised_error) == 1
    assert isinstance(raised_error[0], LeaseLostError)

    elapsed_after_cancel = abort_time[0] - cancel_time[0]
    assert elapsed_after_cancel <= 0.15


def test_cancellation_during_interim_103_stall(limits_server: LimitsServer) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[str(limits_server)],
        allow_loopback_for_testing=True,
        read_timeout_seconds=5.0,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-interim-103-stall",
        node_key="interim_103_stall_node",
        node_type=NodeType.HTTP,
        config={"url": f"{limits_server}/interim_103_stall", "method": "GET"},
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

    assert limits_server.interim_103_sent.wait(timeout=5.0)
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


def test_cancellable_response_stream_unit_methods() -> None:
    from workflow_engine.adapters.http_adapter import CancellableResponseStream

    s = socket.socket()
    token = CancellationToken()
    stream = CancellableResponseStream(s, token, read_timeout=1.0)
    stream.buffer.extend(b"line1\nline2\nhello world")

    assert stream.has_buffered_data() is True
    assert stream.readline(3) == b"lin"
    assert stream.readline() == b"e1\n"
    assert stream.read(0) == b""
    assert stream.read(5) == b"line2"
    assert stream.read(1) == b"\n"

    b = bytearray(5)
    n = stream.readinto(b)
    assert b[:n] == b"hello"

    b2 = bytearray(6)
    n2 = stream.readinto1(b2)
    assert b2[:n2] == b" world"

    stream.flush()
    stream.close()
    s.close()


def test_direct_ip_http_connection_send_types() -> None:
    import io
    import unittest.mock

    from workflow_engine.adapters.http_adapter import DirectIPHTTPConnection

    conn = DirectIPHTTPConnection("127.0.0.1", 80, "127.0.0.1", 1.0, 1.0)
    conn.auto_open = False
    with pytest.raises(http.client.NotConnected):
        conn.send(b"test")

    mock_sock = unittest.mock.MagicMock()
    mock_sock.send.return_value = 100
    conn.sock = mock_sock
    with unittest.mock.patch("select.select", return_value=([], [mock_sock], [])):
        conn.send("string-data")
        conn.send([b"chunk1", "chunk2"])
        conn.send(io.BytesIO(b"file-like"))
