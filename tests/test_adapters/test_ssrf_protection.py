import http.server
import threading
import unittest.mock
from typing import Any, Generator

import pytest
from workflow_engine.adapters.base import ExecutionContext
from workflow_engine.adapters.http_adapter import HttpNodeExecutor, HttpSecurityPolicy
from workflow_engine.domain.cancellation import CancellationToken
from workflow_engine.domain.errors import (
    AllowlistViolationError,
    PermanentExecutionError,
    SSRFSecurityError,
)
from workflow_engine.domain.types import NodeType


class SSRFRedirectServerHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/redirect_to_metadata":
            self.send_response(302)
            self.send_header("Location", "http://169.254.169.254/latest/meta-data/")
            self.end_headers()
        elif self.path == "/redirect_to_private":
            self.send_response(302)
            self.send_header("Location", "http://10.0.0.1/internal")
            self.end_headers()
        elif self.path == "/redirect_to_unallowed_origin":
            self.send_response(302)
            self.send_header("Location", "http://unallowed.com/data")
            self.end_headers()
        else:
            self.send_response(200)
            self.end_headers()


@pytest.fixture
def ssrf_server() -> Generator[str, None, None]:
    server = http.server.HTTPServer(("127.0.0.1", 0), SSRFRedirectServerHandler)
    port = int(server.server_address[1])
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()
    server.server_close()


@pytest.mark.parametrize(
    "blocked_host",
    [
        "127.0.0.1",
        "127.0.0.2",
        "10.0.0.1",
        "10.255.255.254",
        "172.16.0.1",
        "172.31.255.254",
        "192.168.1.1",
        "169.254.169.254",
        "169.254.1.1",
        "0.0.0.0",
        "metadata.google.internal",
        "instance-data",
    ],
)
def test_direct_ssrf_destinations_blocked(blocked_host: str) -> None:
    origin = f"http://{blocked_host}"
    policy = HttpSecurityPolicy(
        allowed_origins=[origin],
        allow_loopback_for_testing=False,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-ssrf",
        node_key="ssrf_node",
        node_type=NodeType.HTTP,
        config={"url": f"{origin}/resource", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(SSRFSecurityError):
        executor.execute(context)


def test_dns_resolution_returning_private_ip_is_blocked() -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=["http://rebinding.test"],
        allow_loopback_for_testing=False,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-dns",
        node_key="dns_node",
        node_type=NodeType.HTTP,
        config={"url": "http://rebinding.test/api", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    with unittest.mock.patch("socket.getaddrinfo") as mock_getaddrinfo:
        mock_getaddrinfo.return_value = [(2, 1, 6, "", ("192.168.1.100", 80))]
        with pytest.raises(SSRFSecurityError, match="blocked by SSRF security policy"):
            executor.execute(context)


def test_dns_rebinding_multihomed_blocked_if_any_ip_private() -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=["http://multihomed.test"],
        allow_loopback_for_testing=False,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-multihomed",
        node_key="multi_node",
        node_type=NodeType.HTTP,
        config={"url": "http://multihomed.test/api", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    with unittest.mock.patch("socket.getaddrinfo") as mock_getaddrinfo:
        mock_getaddrinfo.return_value = [
            (2, 1, 6, "", ("93.184.216.34", 80)),
            (2, 1, 6, "", ("127.0.0.1", 80)),
        ]
        with pytest.raises(SSRFSecurityError, match="blocked by SSRF security policy"):
            executor.execute(context)


def test_redirect_to_cloud_metadata_is_blocked(ssrf_server: str) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[ssrf_server, "http://169.254.169.254"],
        allow_loopback_for_testing=True,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-redir-meta",
        node_key="redir_node",
        node_type=NodeType.HTTP,
        config={"url": f"{ssrf_server}/redirect_to_metadata", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(SSRFSecurityError, match="prohibited"):
        executor.execute(context)


def test_redirect_to_private_ip_is_blocked(ssrf_server: str) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[ssrf_server, "http://10.0.0.1"],
        allow_loopback_for_testing=True,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-redir-priv",
        node_key="redir_node",
        node_type=NodeType.HTTP,
        config={"url": f"{ssrf_server}/redirect_to_private", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(SSRFSecurityError, match="blocked by SSRF security policy"):
        executor.execute(context)


def test_empty_allowlist_blocks_all_requests() -> None:
    policy = HttpSecurityPolicy(allowed_origins=[])
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-empty",
        node_key="empty_node",
        node_type=NodeType.HTTP,
        config={"url": "https://example.com/api", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(AllowlistViolationError, match="no allowed origins configured"):
        executor.execute(context)


def test_unallowed_origin_rejected() -> None:
    policy = HttpSecurityPolicy(allowed_origins=["https://allowed.example.com"])
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-origin",
        node_key="origin_node",
        node_type=NodeType.HTTP,
        config={"url": "https://evil.example.com/api", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(AllowlistViolationError, match="not in configured allowlist"):
        executor.execute(context)


def test_unallowed_path_prefix_rejected() -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=["https://api.example.com"],
        allowed_path_prefixes=["/v1/public/"],
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-path",
        node_key="path_node",
        node_type=NodeType.HTTP,
        config={"url": "https://api.example.com/v1/admin/secret", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(AllowlistViolationError, match="does not match allowed path prefixes"):
        executor.execute(context)


def test_redirect_to_unallowed_origin_rejected(ssrf_server: str) -> None:
    policy = HttpSecurityPolicy(
        allowed_origins=[ssrf_server],
        allow_loopback_for_testing=True,
    )
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()
    context = ExecutionContext(
        run_id="run-redir-unallowed",
        node_key="redir_node",
        node_type=NodeType.HTTP,
        config={"url": f"{ssrf_server}/redirect_to_unallowed_origin", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )

    with pytest.raises(AllowlistViolationError, match="not in configured allowlist"):
        executor.execute(context)


def test_unsupported_url_schemes_rejected() -> None:
    policy = HttpSecurityPolicy(allowed_origins=["file://localhost", "ftp://ftp.example.com"])
    executor = HttpNodeExecutor(policy)
    token = CancellationToken()

    file_ctx = ExecutionContext(
        run_id="run-file",
        node_key="file_node",
        node_type=NodeType.HTTP,
        config={"url": "file:///etc/passwd", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )
    with pytest.raises(PermanentExecutionError, match="Unsupported URL scheme"):
        executor.execute(file_ctx)

    ftp_ctx = ExecutionContext(
        run_id="run-ftp",
        node_key="ftp_node",
        node_type=NodeType.HTTP,
        config={"url": "ftp://ftp.example.com/file", "method": "GET"},
        inputs={},
        cancellation_token=token,
    )
    with pytest.raises(PermanentExecutionError, match="Unsupported URL scheme"):
        executor.execute(ftp_ctx)
