import http.server
import json
import threading
import urllib.parse
from typing import Any, Generator

import pytest
from workflow_engine.adapters.base import ExecutionContext
from workflow_engine.adapters.http_adapter import HttpNodeExecutor, HttpSecurityPolicy
from workflow_engine.domain.cancellation import CancellationToken
from workflow_engine.domain.errors import (
    PermanentExecutionError,
    RetryableExecutionError,
)
from workflow_engine.domain.types import NodeType


class MockServerHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        pass

    def do_GET(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path == "/echo_params":
            query_dict = dict(urllib.parse.parse_qsl(parsed.query))
            self._send_json(200, {"params": query_dict})
        elif parsed.path == "/redirect_to_target":
            self.send_response(302)
            self.send_header("Location", "/target")
            self.end_headers()
        elif parsed.path == "/target":
            auth_hdr = self.headers.get("Authorization")
            self._send_json(200, {"reached": True, "auth": auth_hdr})
        elif parsed.path == "/echo_headers":
            headers_dict = {k.lower(): v for k, v in self.headers.items()}
            self._send_json(200, {"headers": headers_dict})
        elif parsed.path == "/error_400":
            self._send_json(400, {"error": "bad request"})
        elif parsed.path == "/error_500":
            self._send_json(500, {"error": "internal error"})
        else:
            self._send_json(200, {"status": "ok"})

    def do_POST(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        parsed_body = json.loads(body.decode("utf-8")) if body else {}
        custom_header = self.headers.get("X-Custom-Header")
        self._send_json(
            201,
            {
                "received_path": parsed.path,
                "body": parsed_body,
                "custom_header": custom_header,
            },
        )

    def do_PUT(self) -> None:
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        self._send_json(200, {"method": "PUT", "data": body.decode("utf-8")})

    def do_DELETE(self) -> None:
        self._send_json(200, {"method": "DELETE", "deleted": True})

    def do_PATCH(self) -> None:
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        self._send_json(200, {"method": "PATCH", "data": body.decode("utf-8")})

    def _send_json(self, status: int, data: dict[str, Any]) -> None:
        payload = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


@pytest.fixture
def mock_server() -> Generator[tuple[str, int], None, None]:
    server = http.server.HTTPServer(("127.0.0.1", 0), MockServerHandler)
    port = int(server.server_address[1])
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    yield f"http://127.0.0.1:{port}", port
    server.shutdown()
    server.server_close()


@pytest.fixture
def secondary_server() -> Generator[tuple[str, int], None, None]:
    server = http.server.HTTPServer(("127.0.0.1", 0), MockServerHandler)
    port = int(server.server_address[1])
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    yield f"http://127.0.0.1:{port}", port
    server.shutdown()
    server.server_close()


def test_http_get_with_query_parameters(mock_server: tuple[str, int]) -> None:
    base_url, port = mock_server
    policy = HttpSecurityPolicy(
        allowed_origins=[base_url],
        allow_loopback_for_testing=True,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-get",
        node_key="http_get",
        node_type=NodeType.HTTP,
        config={
            "url": f"{base_url}/echo_params",
            "method": "GET",
            "params": {"query": "workflow", "page": "2"},
        },
        inputs={},
        cancellation_token=token,
    )

    result = executor.execute(context)
    assert result.output["status_code"] == 200
    assert result.output["body"]["params"] == {"query": "workflow", "page": "2"}


def test_http_post_with_json_and_headers(mock_server: tuple[str, int]) -> None:
    base_url, port = mock_server
    policy = HttpSecurityPolicy(
        allowed_origins=[base_url],
        allow_loopback_for_testing=True,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-post",
        node_key="http_post",
        node_type=NodeType.HTTP,
        config={
            "url": f"{base_url}/items",
            "method": "POST",
            "headers": {"X-Custom-Header": "SecretVal"},
            "json": {"item_id": 123, "name": "Task Item"},
        },
        inputs={},
        cancellation_token=token,
    )

    result = executor.execute(context)
    assert result.output["status_code"] == 201
    assert result.output["body"]["custom_header"] == "SecretVal"
    assert result.output["body"]["body"] == {"item_id": 123, "name": "Task Item"}


def test_http_put_delete_patch(mock_server: tuple[str, int]) -> None:
    base_url, port = mock_server
    policy = HttpSecurityPolicy(
        allowed_origins=[base_url],
        allow_loopback_for_testing=True,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()

    put_ctx = ExecutionContext(
        run_id="run-put",
        node_key="http_put",
        node_type=NodeType.HTTP,
        config={
            "url": f"{base_url}/update",
            "method": "PUT",
            "data": "updated_content",
        },
        inputs={},
        cancellation_token=token,
    )
    put_res = executor.execute(put_ctx)
    assert put_res.output["status_code"] == 200
    assert put_res.output["body"]["method"] == "PUT"

    delete_ctx = ExecutionContext(
        run_id="run-del",
        node_key="http_del",
        node_type=NodeType.HTTP,
        config={"url": f"{base_url}/remove", "method": "DELETE"},
        inputs={},
        cancellation_token=token,
    )
    del_res = executor.execute(delete_ctx)
    assert del_res.output["status_code"] == 200
    assert del_res.output["body"]["deleted"] is True

    patch_ctx = ExecutionContext(
        run_id="run-patch",
        node_key="http_patch",
        node_type=NodeType.HTTP,
        config={
            "url": f"{base_url}/partial",
            "method": "PATCH",
            "data": "patched_content",
        },
        inputs={},
        cancellation_token=token,
    )
    patch_res = executor.execute(patch_ctx)
    assert patch_res.output["status_code"] == 200
    assert patch_res.output["body"]["method"] == "PATCH"


def test_http_redirect_handled_correctly(mock_server: tuple[str, int]) -> None:
    base_url, port = mock_server
    policy = HttpSecurityPolicy(
        allowed_origins=[base_url],
        allow_loopback_for_testing=True,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-redir",
        node_key="http_redir",
        node_type=NodeType.HTTP,
        config={"url": f"{base_url}/redirect_to_target", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    result = executor.execute(context)
    assert result.output["status_code"] == 200
    assert result.output["body"]["reached"] is True


def test_cross_origin_redirect_strips_sensitive_headers_case_insensitively(
    mock_server: tuple[str, int],
    secondary_server: tuple[str, int],
) -> None:
    origin_a, _ = mock_server
    origin_b, _ = secondary_server

    class CrossOriginRedirectHandler(http.server.BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            pass

        def do_GET(self) -> None:
            if self.path == "/redirect_to_b":
                self.send_response(302)
                self.send_header("Location", f"{origin_b}/echo_headers")
                self.end_headers()

    server_a = http.server.HTTPServer(("127.0.0.1", 0), CrossOriginRedirectHandler)
    port_a = int(server_a.server_address[1])
    origin_custom = f"http://127.0.0.1:{port_a}"
    thread_a = threading.Thread(target=server_a.serve_forever, daemon=True)
    thread_a.start()

    policy = HttpSecurityPolicy(
        allowed_origins=[origin_custom, origin_b],
        allow_loopback_for_testing=True,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()

    headers = {
        "aUtHoRiZaTiOn": "Bearer token-123",
        "cOoKiE": "session=abc",
        "pRoXy-AuThOrIzAtIoN": "Basic xyz",
        "x-ApI-kEy": "secret-key-1",
        "X-Auth-Token": "auth-token-9",
        "Token": "simple-token",
        "Secret": "confidential",
        "X-Custom-Safe-Header": "safe-value",
    }

    context = ExecutionContext(
        run_id="run-strip-headers",
        node_key="strip_node",
        node_type=NodeType.HTTP,
        config={
            "url": f"{origin_custom}/redirect_to_b",
            "method": "GET",
            "headers": headers,
        },
        inputs={},
        cancellation_token=token,
    )

    try:
        result = executor.execute(context)
        assert result.output["status_code"] == 200
        received_headers = result.output["body"]["headers"]

        assert "authorization" not in received_headers
        assert "cookie" not in received_headers
        assert "proxy-authorization" not in received_headers
        assert "x-api-key" not in received_headers
        assert "x-auth-token" not in received_headers
        assert "token" not in received_headers
        assert "secret" not in received_headers

        assert received_headers.get("x-custom-safe-header") == "safe-value"
    finally:
        server_a.shutdown()
        server_a.server_close()


def test_dynamic_http_request_configuration_templating(mock_server: tuple[str, int]) -> None:
    base_url, port = mock_server
    policy = HttpSecurityPolicy(
        allowed_origins=[base_url],
        allow_loopback_for_testing=True,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()

    inputs = {
        "steps": {
            "trigger": {"endpoint": "items", "query_tag": "active"},
            "transform": {"user_id": 1001, "role": "admin", "tags": ["ops", "dev"]},
        }
    }

    context = ExecutionContext(
        run_id="run-template-http",
        node_key="template_http_node",
        node_type=NodeType.HTTP,
        config={
            "url": f"{base_url}/{{{{ steps.trigger.endpoint }}}}",
            "method": "POST",
            "headers": {
                "X-Custom-Header": "Role-{{ steps.transform.role }}",
            },
            "params": {
                "filter": "{{ steps.trigger.query_tag }}",
            },
            "json": {
                "account_id": "{{ steps.transform.user_id }}",
                "assigned_tags": "{{ steps.transform.tags }}",
            },
        },
        inputs=inputs,
        cancellation_token=token,
    )

    result = executor.execute(context)
    assert result.output["status_code"] == 201
    body = result.output["body"]
    assert body["received_path"] == "/items"
    assert body["custom_header"] == "Role-admin"
    assert body["body"]["account_id"] == 1001
    assert body["body"]["assigned_tags"] == ["ops", "dev"]


def test_http_error_status_discrimination(mock_server: tuple[str, int]) -> None:
    base_url, port = mock_server
    policy = HttpSecurityPolicy(
        allowed_origins=[base_url],
        allow_loopback_for_testing=True,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()

    ctx_400 = ExecutionContext(
        run_id="run-400",
        node_key="http_400",
        node_type=NodeType.HTTP,
        config={"url": f"{base_url}/error_400", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )
    with pytest.raises(PermanentExecutionError, match="status 400"):
        executor.execute(ctx_400)

    ctx_500 = ExecutionContext(
        run_id="run-500",
        node_key="http_500",
        node_type=NodeType.HTTP,
        config={"url": f"{base_url}/error_500", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )
    with pytest.raises(RetryableExecutionError, match="status 500"):
        executor.execute(ctx_500)

    ctx_no_fail = ExecutionContext(
        run_id="run-no-fail",
        node_key="http_no_fail",
        node_type=NodeType.HTTP,
        config={
            "url": f"{base_url}/error_400",
            "method": "GET",
            "fail_on_error_status": False,
        },
        inputs={},
        cancellation_token=token,
    )
    res_no_fail = executor.execute(ctx_no_fail)
    assert res_no_fail.output["status_code"] == 400


def test_http_configuration_validation() -> None:
    executor = HttpNodeExecutor(HttpSecurityPolicy(allowed_origins=["http://example.com"]))
    token = CancellationToken()

    missing_url_ctx = ExecutionContext(
        run_id="run-no-url",
        node_key="no_url",
        node_type=NodeType.HTTP,
        config={},
        inputs={},
        cancellation_token=token,
    )
    with pytest.raises(PermanentExecutionError, match="Missing 'url'"):
        executor.execute(missing_url_ctx)

    bad_method_ctx = ExecutionContext(
        run_id="run-bad-method",
        node_key="bad_method",
        node_type=NodeType.HTTP,
        config={"url": "http://example.com", "method": "OPTIONS"},
        inputs={},
        cancellation_token=token,
    )
    with pytest.raises(PermanentExecutionError, match="Unsupported HTTP method"):
        executor.execute(bad_method_ctx)


def test_http_data_body_formats(mock_server: tuple[str, int]) -> None:
    base_url, port = mock_server
    policy = HttpSecurityPolicy(
        allowed_origins=[base_url],
        allow_loopback_for_testing=True,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()

    dict_ctx = ExecutionContext(
        run_id="run-b-dict",
        node_key="b_dict",
        node_type=NodeType.HTTP,
        config={"url": f"{base_url}/items", "method": "POST", "data": {"key": "val"}},
        inputs={},
        cancellation_token=token,
    )
    res_dict = executor.execute(dict_ctx)
    assert res_dict.output["status_code"] == 201

    bytes_ctx = ExecutionContext(
        run_id="run-b-bytes",
        node_key="b_bytes",
        node_type=NodeType.HTTP,
        config={"url": f"{base_url}/items", "method": "POST", "data": b'{"raw": 1}'},
        inputs={},
        cancellation_token=token,
    )
    res_bytes = executor.execute(bytes_ctx)
    assert res_bytes.output["status_code"] == 201


def test_direct_ip_https_connection_connect() -> None:
    import unittest.mock

    from workflow_engine.adapters.http_adapter import DirectIPHTTPSConnection

    conn = DirectIPHTTPSConnection(
        host="example.com",
        port=443,
        target_ip="93.184.216.34",
        connect_timeout=2.0,
        read_timeout=3.0,
    )
    mock_sock = unittest.mock.MagicMock()
    with unittest.mock.patch("socket.create_connection", return_value=mock_sock):
        with unittest.mock.patch.object(conn.ssl_context, "wrap_socket", return_value=mock_sock):
            conn.connect()
            assert conn.sock is mock_sock
            mock_sock.settimeout.assert_called_with(3.0)
