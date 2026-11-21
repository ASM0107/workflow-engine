import http.client
import io
import ipaddress
import json
import math
import posixpath
import select
import socket
import ssl
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

from workflow_engine.adapters.base import ExecutionContext, ExecutionResult
from workflow_engine.adapters.transform_adapter import SafeTemplateEngine
from workflow_engine.domain.cancellation import CancellationToken
from workflow_engine.domain.errors import (
    AllowlistViolationError,
    LeaseLostError,
    PermanentExecutionError,
    ResponseSizeLimitExceededError,
    RetryableExecutionError,
    SSRFSecurityError,
    TimeoutExecutionError,
)

SHARED_ADDRESS_SPACE = ipaddress.ip_network("100.64.0.0/10")
ALIBABA_METADATA_IP = "100.100.100.200"
METADATA_IPS = frozenset(["169.254.169.254", "100.100.100.200", "fd00:ec2::254"])
METADATA_HOSTNAMES = frozenset(
    [
        "metadata.google.internal",
        "instance-data",
        "169.254.169.254",
        "100.100.100.200",
        "fd00:ec2::254",
    ]
)
SENSITIVE_HEADERS = frozenset(
    [
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "x-api-key",
        "api-key",
        "x-auth-token",
        "token",
        "secret",
    ]
)


def _resolve_bounded_timeout(val: Any, ceiling: float, name: str) -> float:
    if val is None:
        return ceiling
    if isinstance(val, bool):
        raise PermanentExecutionError(f"{name} cannot be a boolean")
    try:
        timeout = float(val)
    except (ValueError, TypeError, OverflowError) as exc:
        raise PermanentExecutionError(
            f"Invalid {name} '{val}': must be a finite positive number"
        ) from exc
    if math.isnan(timeout) or math.isinf(timeout) or timeout <= 0:
        raise PermanentExecutionError(f"{name} must be a positive finite number, got {val}")
    return min(timeout, ceiling)


def _resolve_bounded_max_bytes(val: Any, ceiling: int) -> int:
    if val is None:
        return ceiling
    if isinstance(val, bool):
        raise PermanentExecutionError("max_response_bytes cannot be a boolean")
    if isinstance(val, float):
        raise PermanentExecutionError("max_response_bytes must be an integer, not a float")
    try:
        if isinstance(val, str):
            if "." in val or "e" in val.lower():
                raise PermanentExecutionError(f"max_response_bytes must be an integer, got '{val}'")
        bytes_val = int(val)
    except (ValueError, TypeError, OverflowError) as exc:
        raise PermanentExecutionError(
            f"Invalid max_response_bytes '{val}': must be a positive integer"
        ) from exc
    if bytes_val <= 0:
        raise PermanentExecutionError(f"max_response_bytes must be positive, got {bytes_val}")
    return min(bytes_val, ceiling)


def _wait_for_readable(
    sock: socket.socket,
    timeout: float,
    token: CancellationToken,
    slice_interval: float = 0.05,
) -> None:
    start_time = time.monotonic()
    while True:
        token.check_cancelled()
        if isinstance(sock, ssl.SSLSocket):
            if sock.pending() > 0:
                return
        elapsed = time.monotonic() - start_time
        remaining = timeout - elapsed
        if remaining <= 0:
            raise TimeoutExecutionError("HTTP socket read timed out")
        wait_time = min(slice_interval, remaining)
        rlist, _, _ = select.select([sock], [], [], wait_time)
        if rlist:
            return


def _read_headers_cancellation_aware(
    sock: socket.socket,
    timeout: float,
    token: CancellationToken,
    slice_interval: float = 0.05,
) -> bytes:
    buffer = bytearray()
    start_time = time.monotonic()
    while True:
        token.check_cancelled()
        elapsed = time.monotonic() - start_time
        remaining = timeout - elapsed
        if remaining <= 0:
            raise TimeoutExecutionError("HTTP timeout waiting for response headers")

        if isinstance(sock, ssl.SSLSocket) and sock.pending() > 0:
            can_read = True
        else:
            wait_time = min(slice_interval, remaining)
            rlist, _, _ = select.select([sock], [], [], wait_time)
            can_read = bool(rlist)

        if can_read:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buffer.extend(chunk)
            if b"\r\n\r\n" in buffer or b"\n\n" in buffer:
                break

    token.check_cancelled()
    return bytes(buffer)


class CancellableHTTPResponse(http.client.HTTPResponse):
    def __init__(
        self,
        sock: socket.socket,
        prefetched_data: bytes,
        method: str | None = None,
    ) -> None:
        super().__init__(sock, method=method)
        underlying = self.fp
        buf = io.BytesIO(prefetched_data)
        self.buffered_io = buf
        self.prefetched_length = len(prefetched_data)

        class ChainedStream(io.RawIOBase):
            def readable(self) -> bool:
                return True

            def readinto(self, b: Any) -> int:
                n = buf.readinto(b)
                if n > 0:
                    return int(n)
                return int(underlying.readinto(b))

            def close(self) -> None:
                underlying.close()

        self.fp = io.BufferedReader(ChainedStream())

    def has_buffered_data(self) -> bool:
        return self.buffered_io.tell() < self.prefetched_length


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

        eval_data = self._build_evaluation_data(context)
        rendered_url = str(SafeTemplateEngine.render_template(str(raw_url), eval_data))

        method = str(config.get("method", "GET")).upper()
        if method not in ("GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"):
            raise PermanentExecutionError(f"Unsupported HTTP method: '{method}'")

        raw_headers = config.get("headers", {})
        rendered_headers = SafeTemplateEngine.render_data_structure(raw_headers, eval_data)
        if not isinstance(rendered_headers, dict):
            rendered_headers = {}
        headers = {str(k): str(v) for k, v in rendered_headers.items()}

        raw_params = config.get("params")
        rendered_params = (
            SafeTemplateEngine.render_data_structure(raw_params, eval_data)
            if raw_params is not None
            else None
        )
        current_url = self._build_initial_url(rendered_url, rendered_params)

        rendered_config = dict(config)
        if "json" in config:
            rendered_config["json"] = SafeTemplateEngine.render_data_structure(
                config["json"], eval_data
            )
        if "data" in config or "body" in config:
            data_key = "data" if "data" in config else "body"
            rendered_config[data_key] = SafeTemplateEngine.render_data_structure(
                config[data_key], eval_data
            )

        body_bytes = self._prepare_body(rendered_config, headers)

        policy = self.policy
        connect_timeout = _resolve_bounded_timeout(
            config.get("connect_timeout"),
            policy.connect_timeout_seconds,
            "connect_timeout",
        )
        read_timeout = _resolve_bounded_timeout(
            config.get("read_timeout"),
            policy.read_timeout_seconds,
            "read_timeout",
        )
        max_bytes = _resolve_bounded_max_bytes(
            config.get("max_response_bytes"),
            policy.max_response_bytes,
        )
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
                req_headers = {
                    k: v for k, v in req_headers.items() if k.lower() not in SENSITIVE_HEADERS
                }

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
                context.cancellation_token.check_cancelled()
                conn.connect()
                context.cancellation_token.check_cancelled()

                conn.request(
                    method=current_method,
                    url=request_path,
                    body=current_body if current_method not in ("GET", "HEAD") else None,
                    headers=req_headers,
                )
                context.cancellation_token.check_cancelled()

                if conn.sock is None:
                    raise RetryableExecutionError("Socket connection not available")

                prefetched = _read_headers_cancellation_aware(
                    conn.sock, read_timeout, context.cancellation_token
                )
                context.cancellation_token.check_cancelled()

                response = CancellableHTTPResponse(
                    conn.sock, prefetched_data=prefetched, method=current_method
                )
                response.begin()

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

                raw_bytes = self._read_bounded_response(
                    response, conn.sock, max_bytes, read_timeout, context
                )
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

            except LeaseLostError:
                conn.close()
                raise
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

    def _build_evaluation_data(self, context: ExecutionContext) -> dict[str, Any]:
        inputs = context.inputs
        steps = inputs.get("steps") if isinstance(inputs.get("steps"), dict) else inputs
        return {
            "steps": steps,
            "inputs": inputs,
            "config": context.config,
            **inputs,
        }

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
        net_host = f"[{hostname}]" if ":" in hostname else hostname
        port = parsed.port
        if port is None or (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
            return f"{scheme}://{net_host}"
        return f"{scheme}://{net_host}:{port}"

    def _validate_origin_and_path(self, parsed: urllib.parse.SplitResult, origin: str) -> None:
        allowed = [o.lower().rstrip("/") for o in self.policy.allowed_origins]
        if not allowed:
            raise AllowlistViolationError(
                f"URL origin '{origin}' is not permitted (no allowed origins configured)"
            )
        if origin not in allowed:
            raise AllowlistViolationError(f"URL origin '{origin}' is not in configured allowlist")

        raw_path = parsed.path or "/"

        if "\\" in raw_path:
            raise AllowlistViolationError(f"Backslash path traversal rejected: '{raw_path}'")

        lower_path = raw_path.lower()
        if "%25" in lower_path:
            raise AllowlistViolationError(f"Double encoding rejected: '{raw_path}'")
        if "%5c" in lower_path:
            raise AllowlistViolationError(f"Encoded backslash rejected: '{raw_path}'")
        if "%2e" in lower_path:
            raise AllowlistViolationError(f"Encoded dot rejected: '{raw_path}'")
        if "%2f" in lower_path:
            raise AllowlistViolationError(f"Encoded forward slash rejected: '{raw_path}'")

        raw_segments = raw_path.split("/")
        if ".." in raw_segments or "." in raw_segments:
            raise AllowlistViolationError(f"Dot segment path traversal rejected: '{raw_path}'")

        unquoted_path = urllib.parse.unquote(raw_path)
        if "\\" in unquoted_path:
            raise AllowlistViolationError(f"Unquoted backslash rejected: '{unquoted_path}'")

        unquoted_segments = unquoted_path.split("/")
        if ".." in unquoted_segments or "." in unquoted_segments:
            raise AllowlistViolationError(
                f"Unquoted traversal dot segment rejected: '{unquoted_path}'"
            )

        canonical_path = posixpath.normpath(unquoted_path)
        if canonical_path == ".." or canonical_path.startswith("../") or "/../" in canonical_path:
            raise AllowlistViolationError(f"Canonical path traversal rejected: '{canonical_path}'")

        matched_raw = any(
            self._matches_path_prefix(raw_path, prefix)
            for prefix in self.policy.allowed_path_prefixes
        )
        if not matched_raw:
            raise AllowlistViolationError(
                f"URL path '{raw_path}' does not match allowed path prefixes"
            )

        matched_canonical = any(
            self._matches_path_prefix(canonical_path, prefix)
            for prefix in self.policy.allowed_path_prefixes
        )
        if not matched_canonical:
            raise AllowlistViolationError(
                f"Canonical URL path '{canonical_path}' does not match allowed path prefixes"
            )

    @staticmethod
    def _matches_path_prefix(path: str, prefix: str) -> bool:
        if prefix in ("/", ""):
            return path.startswith("/")
        clean_prefix = prefix.rstrip("/")
        return path == clean_prefix or path.startswith(clean_prefix + "/")

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

        ip_str = str(ip)
        if ip_str in METADATA_IPS:
            raise SSRFSecurityError(f"Access to cloud metadata IP '{ip_str}' is prohibited")

        if isinstance(ip, ipaddress.IPv4Address) and ip in SHARED_ADDRESS_SPACE:
            raise SSRFSecurityError(
                f"Access to shared address space IP '{ip_str}' is blocked by SSRF policy"
            )

        if (
            not ip.is_global
            or ip.is_loopback
            or ip.is_private
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
        ):
            raise SSRFSecurityError(
                f"Access to IP address '{ip}' is blocked by SSRF security policy"
            )

    def _read_bounded_response(
        self,
        response: CancellableHTTPResponse,
        sock: socket.socket | None,
        max_bytes: int,
        read_timeout: float,
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
                if content_length == 0:
                    return b""
            except ValueError:
                pass

        buffer = bytearray()
        chunk_size = 65536
        while True:
            context.cancellation_token.check_cancelled()
            if sock is not None and not response.has_buffered_data():
                _wait_for_readable(sock, read_timeout, context.cancellation_token)
            chunk = response.read1(chunk_size)
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
