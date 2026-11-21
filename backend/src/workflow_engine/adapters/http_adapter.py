import http.client
import ipaddress
import json
import socket
import ssl
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

from workflow_engine.adapters.base import ExecutionContext, ExecutionResult
from workflow_engine.domain.errors import (
    AllowlistViolationError,
    PermanentExecutionError,
    ResponseSizeLimitExceededError,
    RetryableExecutionError,
    SSRFSecurityError,
    TimeoutExecutionError,
)

METADATA_HOSTNAMES = frozenset(
    [
        "metadata.google.internal",
        "instance-data",
        "169.254.169.254",
        "fd00:ec2::254",
    ]
)


class DirectIPHTTPConnection(http.client.HTTPConnection):
    def __init__(
        self,
        host: str,
        port: int,
        target_ip: str,
        connect_timeout: float,
        read_timeout: float,
    ) -> None:
        super().__init__(host, port, timeout=read_timeout)
        self.target_ip = target_ip
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout

    def connect(self) -> None:
        self.sock = socket.create_connection(
            (self.target_ip, self.port),
            timeout=self.connect_timeout,
        )
        self.sock.settimeout(self.read_timeout)


class DirectIPHTTPSConnection(http.client.HTTPSConnection):
    def __init__(
        self,
        host: str,
        port: int,
        target_ip: str,
        connect_timeout: float,
        read_timeout: float,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        ctx = ssl_context or ssl.create_default_context()
        super().__init__(host, port, timeout=read_timeout, context=ctx)
        self.target_ip = target_ip
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.ssl_context = ctx

    def connect(self) -> None:
        self.sock = socket.create_connection(
            (self.target_ip, self.port),
            timeout=self.connect_timeout,
        )
        self.sock.settimeout(self.read_timeout)
        server_hostname = self.host
        self.sock = self.ssl_context.wrap_socket(self.sock, server_hostname=server_hostname)


@dataclass(frozen=True)
class HttpSecurityPolicy:
    allowed_origins: list[str] = field(default_factory=list)
    allowed_path_prefixes: list[str] = field(default_factory=lambda: ["/"])
    allow_loopback_for_testing: bool = False
    max_redirects: int = 5
    max_response_bytes: int = 10 * 1024 * 1024
    connect_timeout_seconds: float = 5.0
    read_timeout_seconds: float = 10.0


class HttpNodeExecutor:
    def __init__(self, policy: HttpSecurityPolicy | None = None) -> None:
        self.policy = policy or HttpSecurityPolicy()

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        config = context.config
        raw_url = config.get("url")
        if not raw_url:
            raise PermanentExecutionError("Missing 'url' in HTTP node configuration")

        method = str(config.get("method", "GET")).upper()
        if method not in ("GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"):
            raise PermanentExecutionError(f"Unsupported HTTP method: '{method}'")

        headers = {str(k): str(v) for k, v in config.get("headers", {}).items()}
        params = config.get("params")
        current_url = self._build_initial_url(raw_url, params)

        body_bytes = self._prepare_body(config, headers)

        policy = self.policy
        connect_timeout = float(config.get("connect_timeout", policy.connect_timeout_seconds))
        read_timeout = float(config.get("read_timeout", policy.read_timeout_seconds))
        max_bytes = int(config.get("max_response_bytes", policy.max_response_bytes))
        fail_on_error = bool(config.get("fail_on_error_status", True))

        redirects_remaining = policy.max_redirects
        current_method = method
        current_body: bytes | None = body_bytes
        initial_origin: str | None = None

        while True:
            context.cancellation_token.check_cancelled()
            parsed_url = urllib.parse.urlsplit(current_url)

            if parsed_url.scheme not in ("http", "https"):
                raise PermanentExecutionError(
                    f"Unsupported URL scheme '{parsed_url.scheme}'. Only http/https are allowed."
                )

            origin = self._extract_origin(parsed_url)
            if initial_origin is None:
                initial_origin = origin

            self._validate_origin_and_path(parsed_url, origin)

            hostname = parsed_url.hostname
            if not hostname:
                raise PermanentExecutionError("URL missing hostname")

            port = parsed_url.port or (443 if parsed_url.scheme == "https" else 80)
            target_ip = self._validate_and_resolve_destination(
                hostname, port, allow_loopback=policy.allow_loopback_for_testing
            )

            request_path = parsed_url.path or "/"
            if parsed_url.query:
                request_path = f"{request_path}?{parsed_url.query}"

            req_headers = dict(headers)
            req_headers.setdefault("User-Agent", "workflow-engine/0.1.0")
            req_headers.setdefault("Host", hostname if port in (80, 443) else f"{hostname}:{port}")

            if initial_origin and origin != initial_origin:
                req_headers.pop("Authorization", None)
                req_headers.pop("authorization", None)

            context.cancellation_token.check_cancelled()

            conn: http.client.HTTPConnection
            if parsed_url.scheme == "https":
                ssl_ctx = ssl.create_default_context()
                conn = DirectIPHTTPSConnection(
                    host=hostname,
                    port=port,
                    target_ip=target_ip,
                    connect_timeout=connect_timeout,
                    read_timeout=read_timeout,
                    ssl_context=ssl_ctx,
                )
            else:
                conn = DirectIPHTTPConnection(
                    host=hostname,
                    port=port,
                    target_ip=target_ip,
                    connect_timeout=connect_timeout,
                    read_timeout=read_timeout,
                )

            try:
                conn.connect()
                conn.request(
                    method=current_method,
                    url=request_path,
                    body=current_body if current_method not in ("GET", "HEAD") else None,
                    headers=req_headers,
                )
                response = conn.getresponse()

                if response.status in (301, 302, 303, 307, 308):
                    location = response.getheader("Location")
                    if location:
                        if redirects_remaining <= 0:
                            conn.close()
                            raise PermanentExecutionError("Maximum redirect hops exceeded")
                        redirects_remaining -= 1
                        current_url = urllib.parse.urljoin(current_url, location)
                        if response.status == 303:
                            current_method = "GET"
                            current_body = None
                        elif response.status in (301, 302) and current_method == "POST":
                            current_method = "GET"
                            current_body = None
                        conn.close()
                        continue

                response_headers = {k.lower(): v for k, v in response.getheaders()}

                raw_bytes = self._read_bounded_response(response, max_bytes, context)
                conn.close()

                text_content = raw_bytes.decode("utf-8", errors="replace")
                parsed_body = self._parse_response_body(
                    text_content, response_headers.get("content-type", "")
                )

                if fail_on_error:
                    if response.status >= 500 or response.status in (408, 429):
                        raise RetryableExecutionError(
                            f"HTTP failed with status {response.status}: {text_content[:200]}"
                        )
                    if 400 <= response.status < 500:
                        raise PermanentExecutionError(
                            f"HTTP client error with status {response.status}: {text_content[:200]}"
                        )

                return ExecutionResult(
                    output={
                        "status_code": response.status,
                        "headers": response_headers,
                        "body": parsed_body,
                        "text": text_content,
                    }
                )

            except (socket.timeout, TimeoutError) as exc:
                conn.close()
                raise TimeoutExecutionError(f"HTTP request timed out: {exc}") from exc
            except (ConnectionRefusedError, socket.gaierror) as exc:
                conn.close()
                raise RetryableExecutionError(f"HTTP network connection failure: {exc}") from exc
            except (
                AllowlistViolationError,
                SSRFSecurityError,
                ResponseSizeLimitExceededError,
                PermanentExecutionError,
                RetryableExecutionError,
            ):
                conn.close()
                raise
            except Exception as exc:
                conn.close()
                raise RetryableExecutionError(f"HTTP execution error: {exc}") from exc

    def _build_initial_url(self, raw_url: str, params: Any) -> str:
        if not params or not isinstance(params, dict):
            return raw_url
        parsed = urllib.parse.urlsplit(raw_url)
        existing_pairs = urllib.parse.parse_qsl(parsed.query)
        for k, v in params.items():
            existing_pairs.append((str(k), str(v)))
        encoded_query = urllib.parse.urlencode(existing_pairs)
        return urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, encoded_query, parsed.fragment)
        )

    def _prepare_body(self, config: dict[str, Any], headers: dict[str, str]) -> bytes | None:
        if "json" in config:
            body_bytes = json.dumps(config["json"]).encode("utf-8")
            headers.setdefault("Content-Type", "application/json")
            headers.setdefault("Content-Length", str(len(body_bytes)))
            return body_bytes
        if "data" in config or "body" in config:
            raw = config.get("data") if "data" in config else config.get("body")
            if isinstance(raw, str):
                body_bytes = raw.encode("utf-8")
            elif isinstance(raw, (bytes, bytearray)):
                body_bytes = bytes(raw)
            elif isinstance(raw, dict):
                body_bytes = json.dumps(raw).encode("utf-8")
                headers.setdefault("Content-Type", "application/json")
            else:
                body_bytes = b""
            headers.setdefault("Content-Length", str(len(body_bytes)))
            return body_bytes
        return None

    def _extract_origin(self, parsed: urllib.parse.SplitResult) -> str:
        scheme = parsed.scheme.lower()
        hostname = (parsed.hostname or "").lower()
        port = parsed.port
        if port is None or (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
            return f"{scheme}://{hostname}"
        return f"{scheme}://{hostname}:{port}"

    def _validate_origin_and_path(self, parsed: urllib.parse.SplitResult, origin: str) -> None:
        allowed = [o.lower().rstrip("/") for o in self.policy.allowed_origins]
        if not allowed:
            raise AllowlistViolationError(
                f"URL origin '{origin}' is not permitted (no allowed origins configured)"
            )
        if origin not in allowed:
            raise AllowlistViolationError(f"URL origin '{origin}' is not in configured allowlist")

        path = parsed.path or "/"
        matched_path = any(path.startswith(prefix) for prefix in self.policy.allowed_path_prefixes)
        if not matched_path:
            raise AllowlistViolationError(f"URL path '{path}' does not match allowed path prefixes")

    def _validate_and_resolve_destination(
        self, hostname: str, port: int, allow_loopback: bool
    ) -> str:
        lower_host = hostname.lower()
        if lower_host in METADATA_HOSTNAMES:
            raise SSRFSecurityError(
                f"Destination host '{hostname}' is a prohibited cloud metadata endpoint"
            )

        try:
            direct_ip = ipaddress.ip_address(hostname)
            self._check_ip_safety(direct_ip, allow_loopback)
            return str(direct_ip)
        except ValueError:
            pass

        try:
            addr_info = socket.getaddrinfo(hostname, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
        except socket.gaierror as exc:
            raise RetryableExecutionError(
                f"Failed to resolve hostname '{hostname}': {exc}"
            ) from exc

        if not addr_info:
            raise RetryableExecutionError(f"No address records returned for '{hostname}'")

        resolved_ips: list[str] = []
        for entry in addr_info:
            sockaddr = entry[4]
            ip_str = str(sockaddr[0])
            ip_obj = ipaddress.ip_address(ip_str)
            self._check_ip_safety(ip_obj, allow_loopback)
            resolved_ips.append(ip_str)

        return resolved_ips[0]

    def _check_ip_safety(
        self, ip: ipaddress.IPv4Address | ipaddress.IPv6Address, allow_loopback: bool
    ) -> None:
        if allow_loopback and ip.is_loopback:
            return

        if (
            ip.is_loopback
            or ip.is_private
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
        ):
            raise SSRFSecurityError(
                f"Access to IP address '{ip}' is blocked by SSRF security policy"
            )

        ip_str = str(ip)
        if ip_str in ("169.254.169.254", "fd00:ec2::254"):
            raise SSRFSecurityError(f"Access to cloud metadata IP '{ip_str}' is prohibited")

    def _read_bounded_response(
        self,
        response: http.client.HTTPResponse,
        max_bytes: int,
        context: ExecutionContext,
    ) -> bytes:
        content_length_str = response.getheader("Content-Length")
        if content_length_str:
            try:
                content_length = int(content_length_str)
                if content_length > max_bytes:
                    raise ResponseSizeLimitExceededError(
                        f"Response Content-Length {content_length} exceeds limit {max_bytes}"
                    )
            except ValueError:
                pass

        buffer = bytearray()
        chunk_size = 65536
        while True:
            context.cancellation_token.check_cancelled()
            chunk = response.read(chunk_size)
            if not chunk:
                break
            buffer.extend(chunk)
            if len(buffer) > max_bytes:
                raise ResponseSizeLimitExceededError(
                    f"Response body exceeded limit of {max_bytes} bytes"
                )

        return bytes(buffer)

    def _parse_response_body(self, text: str, content_type: str) -> Any:
        stripped = text.strip()
        if "application/json" in content_type.lower() or (
            stripped.startswith(("{", "[")) and stripped.endswith(("}", "]"))
        ):
            try:
                return json.loads(text)
            except Exception:
                return text
        return text
