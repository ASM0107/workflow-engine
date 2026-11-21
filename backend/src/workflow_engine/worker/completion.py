import json
import uuid
from typing import Any

import psycopg

from workflow_engine.dag.evaluator import evaluate_outgoing_edges
from workflow_engine.domain.entities import ClaimedNode, Edge, Node
from workflow_engine.domain.errors import FencingTokenError
from workflow_engine.persistence.connection import get_connection


class CompletionEngine:
    def complete_success(
        self,
        claimed_node: ClaimedNode,
        output: dict[str, Any] | None = None,
        selected_edge_ids: list[str] | None = None,
    ) -> None:
        query_lock_run = """
            SELECT id, status
            FROM runs
            WHERE id = %s
            FOR UPDATE;
        """

        query_lock_node_runs = """
            SELECT id, node_key, status, lease_owner, lease_generation,
                   (lease_expires_at > clock_timestamp()) AS is_unexpired
            FROM node_runs
            WHERE run_id = %s
            ORDER BY node_key ASC
            FOR UPDATE;
        """

        query_lock_edge_runs = """
            SELECT id, edge_key, from_node_key, to_node_key, status
            FROM edge_runs
            WHERE run_id = %s
            ORDER BY edge_key ASC
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
            try:
                with conn.cursor() as cur:
                    cur.execute(query_lock_run, (claimed_node.run_id,))
                    run_row = cur.fetchone()
                    if not run_row or run_row["status"] != "RUNNING":
                        conn.rollback()
                        raise FencingTokenError(
                            f"Run '{claimed_node.run_id}' not in RUNNING state. "
                            "Completion rejected."
                        )

                    cur.execute(query_lock_node_runs, (claimed_node.run_id,))
                    locked_nodes = cur.fetchall()

                    cur.execute(query_lock_edge_runs, (claimed_node.run_id,))
                    cur.fetchall()

                    target_node = next(
                        (n for n in locked_nodes if n["id"] == claimed_node.node_run_id),
                        None,
                    )
                    if (
                        not target_node
                        or target_node["lease_owner"] != claimed_node.worker_id
                        or target_node["lease_generation"] != claimed_node.lease_generation
                        or target_node["status"] != "RUNNING"
                        or not target_node["is_unexpired"]
                    ):
                        conn.rollback()
                        raise FencingTokenError(
                            f"Fencing token check failed for node '{claimed_node.node_key}' "
                            f"(generation {claimed_node.lease_generation}). Completion rejected."
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

                    self._resolve_outgoing_edges(
                        cur,
                        claimed_node,
                        output,
                        selected_edge_ids,
                    )

                    self._propagate_multi_hop_skips(cur, claimed_node.run_id)

                    self._promote_eligible_nodes(cur, claimed_node.run_id)

                    self._check_run_completion(cur, claimed_node.run_id)

                    conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _resolve_outgoing_edges(
        self,
        cur: psycopg.Cursor[Any],
        claimed_node: ClaimedNode,
        output: dict[str, Any] | None,
        selected_edge_ids: list[str] | None,
    ) -> None:
        query_fetch_edges = """
            SELECT er.edge_key, er.from_node_key, er.to_node_key, we.condition_match
            FROM edge_runs er
            JOIN workflow_edges we
              ON we.workflow_version_id = er.workflow_version_id
             AND we.edge_key = er.edge_key
            WHERE er.run_id = %s
              AND er.from_node_key = %s
            ORDER BY er.edge_key ASC;
        """
        cur.execute(query_fetch_edges, (claimed_node.run_id, claimed_node.node_key))
        outgoing_rows = cur.fetchall()
        if not outgoing_rows:
            return

        taken_edge_keys: list[str] = []
        dead_edge_keys: list[str] = []

        if selected_edge_ids is not None:
            selected_set = set(selected_edge_ids)
            for row in outgoing_rows:
                key = str(row["edge_key"])
                if key in selected_set:
                    taken_edge_keys.append(key)
                else:
                    dead_edge_keys.append(key)
        else:
            node_def = Node(
                id=claimed_node.node_key,
                type=claimed_node.node_type,
                name=claimed_node.node_key,
                config=claimed_node.config,
                retry_limit=claimed_node.retry_limit,
            )
            edge_defs = [
                Edge(
                    id=str(row["edge_key"]),
                    from_node_id=str(row["from_node_key"]),
                    to_node_id=str(row["to_node_key"]),
                    condition_match=row["condition_match"],
                )
                for row in outgoing_rows
            ]
            eval_result = evaluate_outgoing_edges(node_def, edge_defs, output)
            taken_edge_keys = eval_result.taken_edge_ids
            dead_edge_keys = eval_result.dead_edge_ids

        edge_meta_by_key = {str(row["edge_key"]): row for row in outgoing_rows}

        query_update_taken = """
            UPDATE edge_runs
            SET status = 'TAKEN',
                updated_at = clock_timestamp()
            WHERE run_id = %s
              AND edge_key = %s
              AND status = 'PENDING';
        """
        query_event = """
            INSERT INTO run_events (id, run_id, event_type, payload)
            VALUES (%s, %s, %s, %s);
        """

        for edge_key in taken_edge_keys:
            cur.execute(query_update_taken, (claimed_node.run_id, edge_key))
            meta = edge_meta_by_key.get(edge_key)
            from_key = str(meta["from_node_key"]) if meta else claimed_node.node_key
            to_key = str(meta["to_node_key"]) if meta else ""
            cur.execute(
                query_event,
                (
                    uuid.uuid4().hex,
                    claimed_node.run_id,
                    "EDGE_TAKEN",
                    json.dumps(
                        {
                            "edge_key": edge_key,
                            "from_node_key": from_key,
                            "to_node_key": to_key,
                        }
                    ),
                ),
            )

        query_update_dead = """
            UPDATE edge_runs
            SET status = 'DEAD',
                updated_at = clock_timestamp()
            WHERE run_id = %s
              AND edge_key = %s
              AND status = 'PENDING';
        """

        for edge_key in dead_edge_keys:
            cur.execute(query_update_dead, (claimed_node.run_id, edge_key))
            meta = edge_meta_by_key.get(edge_key)
            from_key = str(meta["from_node_key"]) if meta else claimed_node.node_key
            to_key = str(meta["to_node_key"]) if meta else ""
            cur.execute(
                query_event,
                (
                    uuid.uuid4().hex,
                    claimed_node.run_id,
                    "EDGE_DEAD",
                    json.dumps(
                        {
                            "edge_key": edge_key,
                            "from_node_key": from_key,
                            "to_node_key": to_key,
                        }
                    ),
                ),
            )

    def _propagate_multi_hop_skips(
        self,
        cur: psycopg.Cursor[Any],
        run_id: str,
    ) -> list[str]:
        query_candidates = """
            SELECT nr.id, nr.node_key
            FROM node_runs nr
            WHERE nr.run_id = %s
              AND nr.status = 'PENDING'
              AND EXISTS (
                  SELECT 1
                  FROM edge_runs er
                  WHERE er.run_id = nr.run_id
                    AND er.to_node_key = nr.node_key
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM edge_runs er
                  WHERE er.run_id = nr.run_id
                    AND er.to_node_key = nr.node_key
                    AND er.status != 'DEAD'
              )
            ORDER BY nr.node_key ASC;
        """

        query_skip_node = """
            UPDATE node_runs
            SET status = 'SKIPPED',
                updated_at = clock_timestamp()
            WHERE id = %s;
        """

        query_event = """
            INSERT INTO run_events (id, run_id, event_type, payload)
            VALUES (%s, %s, %s, %s);
        """

        query_cascade_edges = """
            UPDATE edge_runs
            SET status = 'DEAD',
                updated_at = clock_timestamp()
            WHERE run_id = %s
              AND from_node_key = %s
              AND status = 'PENDING'
            RETURNING edge_key, to_node_key;
        """

        skipped_nodes: list[str] = []

        while True:
            cur.execute(query_candidates, (run_id,))
            rows = cur.fetchall()
            if not rows:
                break

            for row in rows:
                node_run_id = row["id"]
                node_key = str(row["node_key"])

                cur.execute(query_skip_node, (node_run_id,))
                cur.execute(
                    query_event,
                    (
                        uuid.uuid4().hex,
                        run_id,
                        "NODE_SKIPPED",
                        json.dumps({"node_key": node_key}),
                    ),
                )

                cur.execute(query_cascade_edges, (run_id, node_key))
                cascaded = cur.fetchall()

                for edge in cascaded:
                    cur.execute(
                        query_event,
                        (
                            uuid.uuid4().hex,
                            run_id,
                            "EDGE_DEAD",
                            json.dumps(
                                {
                                    "edge_key": str(edge["edge_key"]),
                                    "from_node_key": node_key,
                                    "to_node_key": str(edge["to_node_key"]),
                                    "reason": "cascade_from_skipped_node",
                                }
                            ),
                        ),
                    )

                skipped_nodes.append(node_key)

        return skipped_nodes

    def _promote_eligible_nodes(
        self,
        cur: psycopg.Cursor[Any],
        run_id: str,
    ) -> list[str]:
        query_promotable = """
            SELECT nr.id, nr.node_key
            FROM node_runs nr
            WHERE nr.run_id = %s
              AND nr.status = 'PENDING'
              AND EXISTS (
                  SELECT 1
                  FROM edge_runs er
                  WHERE er.run_id = nr.run_id
                    AND er.to_node_key = nr.node_key
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM edge_runs er
                  WHERE er.run_id = nr.run_id
                    AND er.to_node_key = nr.node_key
                    AND er.status = 'PENDING'
              )
              AND EXISTS (
                  SELECT 1
                  FROM edge_runs er
                  WHERE er.run_id = nr.run_id
                    AND er.to_node_key = nr.node_key
                    AND er.status = 'TAKEN'
              )
            ORDER BY nr.node_key ASC;
        """

        query_promote = """
            UPDATE node_runs
            SET status = 'READY',
                updated_at = clock_timestamp()
            WHERE id = %s;
        """

        query_event = """
            INSERT INTO run_events (id, run_id, event_type, payload)
            VALUES (%s, %s, %s, %s);
        """

        cur.execute(query_promotable, (run_id,))
        rows = cur.fetchall()
        promoted_keys: list[str] = []

        for row in rows:
            node_run_id = row["id"]
            node_key = str(row["node_key"])

            cur.execute(query_promote, (node_run_id,))
            cur.execute(
                query_event,
                (
                    uuid.uuid4().hex,
                    run_id,
                    "NODE_PROMOTED",
                    json.dumps({"node_key": node_key}),
                ),
            )
            promoted_keys.append(node_key)

        return promoted_keys

    def _check_run_completion(
        self,
        cur: psycopg.Cursor[Any],
        run_id: str,
    ) -> bool:
        query_non_terminal = """
            SELECT COUNT(*) AS non_terminal_count
            FROM node_runs
            WHERE run_id = %s
              AND status NOT IN ('SUCCEEDED', 'SKIPPED');
        """
        cur.execute(query_non_terminal, (run_id,))
        row = cur.fetchone()
        non_terminal_count = int(row["non_terminal_count"]) if row else 0

        if non_terminal_count == 0:
            query_complete_run = """
                UPDATE runs
                SET status = 'SUCCEEDED',
                    updated_at = clock_timestamp()
                WHERE id = %s
                  AND status = 'RUNNING';
            """
            cur.execute(query_complete_run, (run_id,))

            query_event = """
                INSERT INTO run_events (id, run_id, event_type, payload)
                VALUES (%s, %s, %s, %s);
            """
            cur.execute(
                query_event,
                (
                    uuid.uuid4().hex,
                    run_id,
                    "RUN_SUCCEEDED",
                    json.dumps({"run_id": run_id}),
                ),
            )
            return True

        return False

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

        query_lock_node_runs = """
            SELECT id, node_key, status, lease_owner, lease_generation,
                   (lease_expires_at > clock_timestamp()) AS is_unexpired
            FROM node_runs
            WHERE run_id = %s
            ORDER BY node_key ASC
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

                cur.execute(query_lock_node_runs, (claimed_node.run_id,))
                locked_nodes = cur.fetchall()

                target_node = next(
                    (n for n in locked_nodes if n["id"] == claimed_node.node_run_id),
                    None,
                )
                if (
                    not target_node
                    or target_node["lease_owner"] != claimed_node.worker_id
                    or target_node["lease_generation"] != claimed_node.lease_generation
                    or target_node["status"] != "RUNNING"
                    or not target_node["is_unexpired"]
                ):
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
