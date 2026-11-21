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
