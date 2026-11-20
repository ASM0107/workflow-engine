import json
import uuid

from workflow_engine.domain.entities import ClaimedNode
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.connection import get_connection


class ClaimEngine:
    def __init__(self, default_lease_seconds: float = 30.0) -> None:
        self.default_lease_seconds = default_lease_seconds

    def discover_and_claim_node(
        self,
        worker_id: str,
        lease_seconds: float | None = None,
    ) -> ClaimedNode | None:
        lease_duration = lease_seconds if lease_seconds is not None else self.default_lease_seconds

        query_find_run = """
            SELECT r.id, r.workflow_version_id, r.status
            FROM runs r
            WHERE r.status IN ('QUEUED', 'RUNNING')
              AND EXISTS (
                  SELECT 1
                  FROM node_runs nr
                  WHERE nr.run_id = r.id
                    AND nr.status = 'READY'
                    AND (nr.run_after IS NULL OR nr.run_after <= statement_timestamp())
              )
            ORDER BY r.last_claimed_at ASC NULLS FIRST, r.created_at ASC
            LIMIT 1
            FOR UPDATE OF r SKIP LOCKED;
        """

        query_find_ready_node = """
            SELECT nr.id, nr.node_key, nr.lease_generation
            FROM node_runs nr
            WHERE nr.run_id = %s
              AND nr.status = 'READY'
              AND (nr.run_after IS NULL OR nr.run_after <= clock_timestamp())
            ORDER BY nr.run_after ASC NULLS FIRST, nr.created_at ASC
            LIMIT 1
            FOR UPDATE OF nr;
        """

        query_transition_run = """
            UPDATE runs
            SET status = 'RUNNING',
                last_claimed_at = clock_timestamp(),
                updated_at = clock_timestamp()
            WHERE id = %s;
        """

        query_touch_run = """
            UPDATE runs
            SET last_claimed_at = clock_timestamp(),
                updated_at = clock_timestamp()
            WHERE id = %s;
        """

        query_claim_node = """
            UPDATE node_runs
            SET status = 'RUNNING',
                lease_owner = %s,
                lease_generation = lease_generation + 1,
                lease_expires_at = clock_timestamp() + (%s * INTERVAL '1 second'),
                updated_at = clock_timestamp()
            WHERE id = %s
            RETURNING id, node_key, lease_generation, lease_expires_at;
        """

        query_node_meta = """
            SELECT type, config, retry_limit
            FROM workflow_nodes
            WHERE workflow_version_id = %s AND node_key = %s;
        """

        query_next_attempt = """
            SELECT COALESCE(MAX(attempt_number), 0) + 1 AS next_attempt
            FROM node_attempts
            WHERE node_run_id = %s;
        """

        query_record_attempt = """
            INSERT INTO node_attempts (
                id, node_run_id, attempt_number, lease_generation, worker_id, status, started_at
            )
            VALUES (%s, %s, %s, %s, %s, 'RUNNING', clock_timestamp());
        """

        query_record_event = """
            INSERT INTO run_events (id, run_id, event_type, payload)
            VALUES (%s, %s, 'NODE_CLAIMED', %s);
        """

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query_find_run)
                run_row = cur.fetchone()
                if not run_row:
                    conn.rollback()
                    return None

                run_id: str = run_row["id"]
                version_id: str = run_row["workflow_version_id"]
                current_run_status: str = run_row["status"]

                cur.execute(query_find_ready_node, (run_id,))
                node_row = cur.fetchone()
                if not node_row:
                    cur.execute(query_touch_run, (run_id,))
                    conn.commit()
                    return None

                node_run_id: str = node_row["id"]
                node_key: str = node_row["node_key"]

                if current_run_status == "QUEUED":
                    cur.execute(query_transition_run, (run_id,))
                else:
                    cur.execute(query_touch_run, (run_id,))

                cur.execute(query_claim_node, (worker_id, lease_duration, node_run_id))
                claimed_row = cur.fetchone()
                if not claimed_row:
                    conn.rollback()
                    return None

                cur.execute(query_node_meta, (version_id, node_key))
                meta_row = cur.fetchone()
                raw_type = meta_row["type"] if meta_row else "manual_trigger"
                config = meta_row["config"] if meta_row else {}
                retry_limit = meta_row["retry_limit"] if meta_row else 0

                cur.execute(query_next_attempt, (node_run_id,))
                attempt_row = cur.fetchone()
                attempt_number: int = attempt_row["next_attempt"] if attempt_row else 1

                attempt_id = uuid.uuid4().hex
                cur.execute(
                    query_record_attempt,
                    (
                        attempt_id,
                        node_run_id,
                        attempt_number,
                        claimed_row["lease_generation"],
                        worker_id,
                    ),
                )

                event_id = uuid.uuid4().hex
                cur.execute(
                    query_record_event,
                    (
                        event_id,
                        run_id,
                        json.dumps(
                            {
                                "node_key": node_key,
                                "worker_id": worker_id,
                                "lease_generation": claimed_row["lease_generation"],
                                "attempt_number": attempt_number,
                            }
                        ),
                    ),
                )

                conn.commit()

                return ClaimedNode(
                    run_id=run_id,
                    node_run_id=node_run_id,
                    node_key=node_key,
                    node_type=NodeType(raw_type),
                    worker_id=worker_id,
                    lease_generation=claimed_row["lease_generation"],
                    lease_expires_at=claimed_row["lease_expires_at"],
                    attempt_number=attempt_number,
                    config=config if isinstance(config, dict) else {},
                    retry_limit=retry_limit,
                )
