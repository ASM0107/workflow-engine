import json
import uuid
from typing import Any

from workflow_engine.domain.entities import ClaimedNode
from workflow_engine.domain.errors import FencingTokenError
from workflow_engine.persistence.connection import get_connection


class CompletionEngine:
    def complete_success(
        self,
        claimed_node: ClaimedNode,
        output: dict[str, Any] | None = None,
    ) -> None:
        query_lock_run = """
            SELECT id, status
            FROM runs
            WHERE id = %s
            FOR UPDATE;
        """

        query_fenced_node_success = """
            UPDATE node_runs
            SET status = 'SUCCEEDED',
                output = %s::jsonb,
                error = NULL,
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = clock_timestamp()
            WHERE id = %s
              AND lease_owner = %s
              AND lease_generation = %s
              AND status = 'RUNNING'
              AND lease_expires_at > clock_timestamp()
            RETURNING id;
        """

        query_update_attempt = """
            UPDATE node_attempts
            SET status = 'SUCCEEDED',
                output = %s::jsonb,
                completed_at = clock_timestamp()
            WHERE node_run_id = %s
              AND attempt_number = %s
              AND worker_id = %s
              AND status = 'RUNNING';
        """

        query_record_event = """
            INSERT INTO run_events (id, run_id, event_type, payload)
            VALUES (%s, %s, 'NODE_SUCCEEDED', %s);
        """

        payload_json = json.dumps(output or {})

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query_lock_run, (claimed_node.run_id,))
                run_row = cur.fetchone()
                if not run_row or run_row["status"] != "RUNNING":
                    conn.rollback()
                    raise FencingTokenError(
                        f"Run '{claimed_node.run_id}' is not in RUNNING state. Completion rejected."
                    )

                cur.execute(
                    query_fenced_node_success,
                    (
                        payload_json,
                        claimed_node.node_run_id,
                        claimed_node.worker_id,
                        claimed_node.lease_generation,
                    ),
                )
                updated_node = cur.fetchone()
                if not updated_node:
                    conn.rollback()
                    raise FencingTokenError(
                        f"Fencing token check failed for node '{claimed_node.node_key}' "
                        f"(generation {claimed_node.lease_generation}). Completion rejected."
                    )

                cur.execute(
                    query_update_attempt,
                    (
                        payload_json,
                        claimed_node.node_run_id,
                        claimed_node.attempt_number,
                        claimed_node.worker_id,
                    ),
                )
                if cur.rowcount != 1:
                    conn.rollback()
                    raise FencingTokenError(
                        f"Expected exactly 1 running attempt to update, found {cur.rowcount}"
                    )

                event_id = uuid.uuid4().hex
                cur.execute(
                    query_record_event,
                    (
                        event_id,
                        claimed_node.run_id,
                        json.dumps(
                            {
                                "node_key": claimed_node.node_key,
                                "worker_id": claimed_node.worker_id,
                                "attempt_number": claimed_node.attempt_number,
                            }
                        ),
                    ),
                )

                conn.commit()

    def complete_failure(
        self,
        claimed_node: ClaimedNode,
        error_message: str,
        backoff_base_seconds: float = 1.0,
    ) -> bool:
        query_lock_run = """
            SELECT id, status
            FROM runs
            WHERE id = %s
            FOR UPDATE;
        """

        query_fenced_check = """
            SELECT id
            FROM node_runs
            WHERE id = %s
              AND lease_owner = %s
              AND lease_generation = %s
              AND status = 'RUNNING'
              AND lease_expires_at > clock_timestamp()
            FOR UPDATE;
        """

        query_update_attempt = """
            UPDATE node_attempts
            SET status = 'FAILED',
                error = %s,
                completed_at = clock_timestamp()
            WHERE node_run_id = %s
              AND attempt_number = %s
              AND worker_id = %s
              AND status = 'RUNNING';
        """

        query_node_retry = """
            UPDATE node_runs
            SET status = 'READY',
                lease_owner = NULL,
                lease_expires_at = NULL,
                lease_generation = lease_generation + 1,
                run_after = clock_timestamp() + (%s * INTERVAL '1 second'),
                error = %s,
                updated_at = clock_timestamp()
            WHERE id = %s;
        """

        query_node_fail = """
            UPDATE node_runs
            SET status = 'FAILED',
                lease_owner = NULL,
                lease_expires_at = NULL,
                error = %s,
                updated_at = clock_timestamp()
            WHERE id = %s;
        """

        query_run_fail = """
            UPDATE runs
            SET status = 'FAILED',
                updated_at = clock_timestamp()
            WHERE id = %s;
        """

        query_cancel_running_siblings = """
            WITH cancelled_nodes AS (
                UPDATE node_runs
                SET status = 'CANCELLED',
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    lease_generation = lease_generation + 1,
                    error = 'Cancelled due to sibling node failure',
                    updated_at = clock_timestamp()
                WHERE run_id = %s
                  AND status = 'RUNNING'
                  AND id != %s
                RETURNING id
            )
            UPDATE node_attempts
            SET status = 'CANCELLED',
                error = 'Run failed fast due to sibling failure',
                completed_at = clock_timestamp()
            WHERE node_run_id IN (SELECT id FROM cancelled_nodes)
              AND status = 'RUNNING';
        """

        query_cancel_pending_and_ready = """
            UPDATE node_runs
            SET status = 'CANCELLED',
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = clock_timestamp()
            WHERE run_id = %s
              AND status IN ('PENDING', 'READY');
        """

        query_record_event = """
            INSERT INTO run_events (id, run_id, event_type, payload)
            VALUES (%s, %s, %s, %s);
        """

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query_lock_run, (claimed_node.run_id,))
                run_row = cur.fetchone()
                if not run_row or run_row["status"] != "RUNNING":
                    conn.rollback()
                    raise FencingTokenError(
                        f"Run '{claimed_node.run_id}' is not in RUNNING state. Failure rejected."
                    )

                cur.execute(
                    query_fenced_check,
                    (
                        claimed_node.node_run_id,
                        claimed_node.worker_id,
                        claimed_node.lease_generation,
                    ),
                )
                node_check = cur.fetchone()
                if not node_check:
                    conn.rollback()
                    raise FencingTokenError(
                        f"Fencing token check failed for node '{claimed_node.node_key}' "
                        f"(generation {claimed_node.lease_generation}). Failure rejected."
                    )

                cur.execute(
                    query_update_attempt,
                    (
                        error_message,
                        claimed_node.node_run_id,
                        claimed_node.attempt_number,
                        claimed_node.worker_id,
                    ),
                )
                if cur.rowcount != 1:
                    conn.rollback()
                    raise FencingTokenError(
                        f"Expected exactly 1 running attempt to update, found {cur.rowcount}"
                    )

                if claimed_node.attempt_number <= claimed_node.retry_limit:
                    delay = min(
                        60.0,
                        backoff_base_seconds * (2 ** (claimed_node.attempt_number - 1)),
                    )
                    cur.execute(
                        query_node_retry,
                        (delay, error_message, claimed_node.node_run_id),
                    )
                    event_id = uuid.uuid4().hex
                    cur.execute(
                        query_record_event,
                        (
                            event_id,
                            claimed_node.run_id,
                            "NODE_RETRY_SCHEDULED",
                            json.dumps(
                                {
                                    "node_key": claimed_node.node_key,
                                    "attempt_number": claimed_node.attempt_number,
                                    "retry_delay_seconds": delay,
                                    "error": error_message,
                                }
                            ),
                        ),
                    )
                    conn.commit()
                    return True
                else:
                    cur.execute(query_node_fail, (error_message, claimed_node.node_run_id))
                    cur.execute(query_run_fail, (claimed_node.run_id,))
                    cur.execute(
                        query_cancel_running_siblings,
                        (claimed_node.run_id, claimed_node.node_run_id),
                    )
                    cur.execute(query_cancel_pending_and_ready, (claimed_node.run_id,))

                    event_id1 = uuid.uuid4().hex
                    cur.execute(
                        query_record_event,
                        (
                            event_id1,
                            claimed_node.run_id,
                            "NODE_FAILED",
                            json.dumps(
                                {
                                    "node_key": claimed_node.node_key,
                                    "error": error_message,
                                    "attempts": claimed_node.attempt_number,
                                }
                            ),
                        ),
                    )
                    event_id2 = uuid.uuid4().hex
                    cur.execute(
                        query_record_event,
                        (
                            event_id2,
                            claimed_node.run_id,
                            "RUN_FAILED",
                            json.dumps(
                                {
                                    "failed_node_key": claimed_node.node_key,
                                    "error": error_message,
                                }
                            ),
                        ),
                    )
                    conn.commit()
                    return False
