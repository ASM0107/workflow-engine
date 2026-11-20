import json
import uuid

from workflow_engine.persistence.connection import get_connection


class LeaseRecoverySweeper:
    def recover_expired_leases(self, backoff_base_seconds: float = 1.0) -> int:
        query_find_candidate_runs = """
            SELECT DISTINCT run_id
            FROM node_runs
            WHERE status = 'RUNNING'
              AND lease_expires_at < statement_timestamp()
            LIMIT 20;
        """

        query_lock_run = """
            SELECT id, status
            FROM runs
            WHERE id = %s
            FOR UPDATE;
        """

        query_find_expired_nodes = """
            SELECT nr.id, nr.node_key, nr.lease_generation, nr.lease_owner,
                   wn.retry_limit
            FROM node_runs nr
            JOIN workflow_nodes wn
              ON nr.workflow_version_id = wn.workflow_version_id
             AND nr.node_key = wn.node_key
            WHERE nr.run_id = %s
              AND nr.status = 'RUNNING'
              AND nr.lease_expires_at < clock_timestamp()
            FOR UPDATE OF nr;
        """

        query_timeout_attempt = """
            UPDATE node_attempts
            SET status = 'TIMED_OUT',
                error = 'Lease expired without worker heartbeat',
                completed_at = clock_timestamp()
            WHERE node_run_id = %s AND status = 'RUNNING';
        """

        query_count_attempts = """
            SELECT COALESCE(MAX(attempt_number), 1) AS attempt_count
            FROM node_attempts
            WHERE node_run_id = %s;
        """

        query_recover_node = """
            UPDATE node_runs
            SET status = 'READY',
                lease_owner = NULL,
                lease_expires_at = NULL,
                lease_generation = lease_generation + 1,
                run_after = clock_timestamp() + (%s * INTERVAL '1 second'),
                error = 'Lease expired; recovered to READY',
                updated_at = clock_timestamp()
            WHERE id = %s;
        """

        query_fail_node = """
            UPDATE node_runs
            SET status = 'FAILED',
                lease_owner = NULL,
                lease_expires_at = NULL,
                error = 'Lease expired and retry limit exceeded',
                updated_at = clock_timestamp()
            WHERE id = %s;
        """

        query_fail_run = """
            UPDATE runs
            SET status = 'FAILED',
                updated_at = clock_timestamp()
            WHERE id = %s;
        """

        query_cancel_remaining = """
            UPDATE node_runs
            SET status = 'CANCELLED',
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = clock_timestamp()
            WHERE run_id = %s AND status IN ('PENDING', 'READY');
        """

        query_record_event = """
            INSERT INTO run_events (id, run_id, event_type, payload)
            VALUES (%s, %s, %s, %s);
        """

        recovered_count = 0

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query_find_candidate_runs)
                candidate_rows = cur.fetchall()
                candidate_run_ids = [row["run_id"] for row in candidate_rows]

        for run_id in candidate_run_ids:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query_lock_run, (run_id,))
                    run_row = cur.fetchone()
                    if not run_row or run_row["status"] not in ("QUEUED", "RUNNING"):
                        conn.rollback()
                        continue

                    cur.execute(query_find_expired_nodes, (run_id,))
                    expired_nodes = cur.fetchall()
                    if not expired_nodes:
                        conn.rollback()
                        continue

                    for node in expired_nodes:
                        node_id: str = node["id"]
                        node_key: str = node["node_key"]
                        retry_limit: int = node["retry_limit"]

                        cur.execute(query_timeout_attempt, (node_id,))
                        cur.execute(query_count_attempts, (node_id,))
                        count_row = cur.fetchone()
                        attempt_count: int = count_row["attempt_count"] if count_row else 1

                        if attempt_count <= retry_limit:
                            delay = min(
                                60.0,
                                backoff_base_seconds * (2 ** (attempt_count - 1)),
                            )
                            cur.execute(query_recover_node, (delay, node_id))
                            recovered_count += 1

                            event_id = uuid.uuid4().hex
                            cur.execute(
                                query_record_event,
                                (
                                    event_id,
                                    run_id,
                                    "LEASE_RECOVERED",
                                    json.dumps(
                                        {
                                            "node_key": node_key,
                                            "previous_worker": node["lease_owner"],
                                            "retry_delay_seconds": delay,
                                            "attempts": attempt_count,
                                        }
                                    ),
                                ),
                            )
                        else:
                            cur.execute(query_fail_node, (node_id,))
                            cur.execute(query_fail_run, (run_id,))
                            cur.execute(query_cancel_remaining, (run_id,))

                            event_id1 = uuid.uuid4().hex
                            cur.execute(
                                query_record_event,
                                (
                                    event_id1,
                                    run_id,
                                    "NODE_FAILED",
                                    json.dumps(
                                        {
                                            "node_key": node_key,
                                            "error": "Lease expired and retry limit exceeded",
                                            "attempts": attempt_count,
                                        }
                                    ),
                                ),
                            )
                            event_id2 = uuid.uuid4().hex
                            cur.execute(
                                query_record_event,
                                (
                                    event_id2,
                                    run_id,
                                    "RUN_FAILED",
                                    json.dumps(
                                        {
                                            "failed_node_key": node_key,
                                            "error": "Lease expired and retry limit exceeded",
                                        }
                                    ),
                                ),
                            )

                    conn.commit()

        return recovered_count
