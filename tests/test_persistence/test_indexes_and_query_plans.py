import json

from workflow_engine.domain.entities import WorkflowDefinition
from workflow_engine.persistence.connection import get_connection
from workflow_engine.persistence.repositories import RunRepository, WorkflowRepository


def test_partial_and_scheduling_index_query_plans(
    workflow_repo: WorkflowRepository,
    run_repo: RunRepository,
    sample_definition: WorkflowDefinition,
) -> None:
    wf_id = workflow_repo.create_workflow(name="Index Flow")
    workflow_repo.publish_version(wf_id, 1, sample_definition)
    run_id = run_repo.create_run(wf_id, 1)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SET enable_seqscan = off;")

            cur.execute(
                """
                EXPLAIN (FORMAT JSON)
                SELECT id, node_key, run_after
                FROM node_runs
                WHERE status = 'READY' AND run_id = %s;
                """,
                (run_id,),
            )
            row_ready = cur.fetchone()
            assert row_ready is not None
            plan_ready = row_ready["QUERY PLAN"]
            plan_str = json.dumps(plan_ready)
            assert "idx_node_runs_ready" in plan_str

            cur.execute(
                """
                EXPLAIN (FORMAT JSON)
                SELECT id, node_key, lease_expires_at
                FROM node_runs
                WHERE status = 'RUNNING' AND run_id = %s;
                """,
                (run_id,),
            )
            row_lease = cur.fetchone()
            assert row_lease is not None
            plan_lease = row_lease["QUERY PLAN"]
            plan_str = json.dumps(plan_lease)
            assert "idx_node_runs_running_lease" in plan_str

            cur.execute(
                """
                EXPLAIN (FORMAT JSON)
                SELECT id, status, last_claimed_at, created_at
                FROM runs
                WHERE status IN ('QUEUED', 'RUNNING')
                ORDER BY last_claimed_at ASC NULLS FIRST, created_at ASC;
                """
            )
            row_claimable = cur.fetchone()
            assert row_claimable is not None
            plan_claimable = row_claimable["QUERY PLAN"]
            plan_str = json.dumps(plan_claimable)
            assert "idx_runs_claimable" in plan_str
