import threading
from datetime import datetime
from typing import Callable

from workflow_engine.domain.entities import ClaimedNode
from workflow_engine.domain.errors import FencingTokenError, LeaseLostError
from workflow_engine.persistence.connection import get_connection


class HeartbeatService:
    def __init__(self, default_extension_seconds: float = 30.0) -> None:
        self.default_extension_seconds = default_extension_seconds

    def send_heartbeat(
        self,
        claimed_node: ClaimedNode,
        extension_seconds: float | None = None,
    ) -> datetime:
        extension = (
            extension_seconds if extension_seconds is not None else self.default_extension_seconds
        )

        query_lock_run = """
            SELECT id, status
            FROM runs
            WHERE id = %s
            FOR UPDATE;
        """

        query_update_lease = """
            UPDATE node_runs
            SET lease_expires_at = clock_timestamp() + (%s * INTERVAL '1 second'),
                updated_at = clock_timestamp()
            WHERE id = %s
              AND lease_owner = %s
              AND lease_generation = %s
              AND status = 'RUNNING'
              AND lease_expires_at > clock_timestamp()
            RETURNING lease_expires_at;
        """

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query_lock_run, (claimed_node.run_id,))
                run_row = cur.fetchone()
                if not run_row or run_row["status"] != "RUNNING":
                    conn.rollback()
                    raise FencingTokenError(
                        f"Parent run '{claimed_node.run_id}' not RUNNING. Heartbeat rejected."
                    )

                cur.execute(
                    query_update_lease,
                    (
                        extension,
                        claimed_node.node_run_id,
                        claimed_node.worker_id,
                        claimed_node.lease_generation,
                    ),
                )
                row = cur.fetchone()
                if not row:
                    conn.rollback()
                    raise FencingTokenError(
                        f"Lease lost or expired for node '{claimed_node.node_key}' "
                        f"(generation {claimed_node.lease_generation}). Heartbeat rejected."
                    )
                conn.commit()
                new_expiry: datetime = row["lease_expires_at"]
                claimed_node.lease_expires_at = new_expiry
                return new_expiry


class HeartbeatMonitor:
    def __init__(
        self,
        service: HeartbeatService,
        claimed_node: ClaimedNode,
        interval_seconds: float = 5.0,
        extension_seconds: float = 30.0,
        on_lease_lost: Callable[[], None] | None = None,
    ) -> None:
        self.service = service
        self.claimed_node = claimed_node
        self.interval_seconds = interval_seconds
        self.extension_seconds = extension_seconds
        self.on_lease_lost = on_lease_lost
        self._stop_event = threading.Event()
        self._lease_lost_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    @property
    def is_lease_lost(self) -> bool:
        return self._lease_lost_event.is_set()

    def check_lease_validity(self) -> None:
        if self._lease_lost_event.is_set():
            raise LeaseLostError(
                f"Heartbeat ownership lost for node '{self.claimed_node.node_key}'"
            )

    def _run(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            try:
                self.service.send_heartbeat(
                    self.claimed_node,
                    extension_seconds=self.extension_seconds,
                )
            except FencingTokenError:
                self._lease_lost_event.set()
                if self.on_lease_lost:
                    self.on_lease_lost()
                break
            except Exception:
                self._lease_lost_event.set()
                if self.on_lease_lost:
                    self.on_lease_lost()
                break
