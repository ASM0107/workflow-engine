import concurrent.futures
import threading
import time
from typing import Any, Callable

from workflow_engine.domain.entities import Node, WorkflowDefinition
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.connection import get_connection
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.worker import WorkflowWorker


def test_concurrent_workers_claim_and_execute_without_deadlocks(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
) -> None:
    for i in range(1, 6):
        wf = WorkflowDefinition(
            id=f"wf_concurrent_{i}",
            version=1,
            nodes={
                "n1": Node(id="n1", type=NodeType.MANUAL_TRIGGER),
            },
            edges={},
        )
        wf_id = create_and_publish(wf)
        run_repo.create_run(wf_id, version=1)

    workers = [
        WorkflowWorker(worker_id=f"concurrent-worker-{idx}", lease_duration_seconds=10.0)
        for idx in range(4)
    ]

    def worker_loop(w: WorkflowWorker) -> int:
        count = 0
        while True:
            handled = w.execute_one(lambda claimed: {"worker": w.worker_id})
            if handled:
                count += 1
            else:
                break
        return count

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(worker_loop, w) for w in workers]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    total_executed = sum(results)
    assert total_executed == 5

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM node_runs;")
            rows = cur.fetchall()
            assert len(rows) == 5
            for r in rows:
                assert r["status"] == "SUCCEEDED"


def test_same_run_concurrent_workers_execute_without_deadlocks_or_duplicate_claims(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
) -> None:
    node_count = 10
    nodes = {
        f"root_{i}": Node(id=f"root_{i}", type=NodeType.MANUAL_TRIGGER, retry_limit=0)
        for i in range(node_count)
    }
    wf = WorkflowDefinition(
        id="wf_same_run_concurrent",
        version=1,
        nodes=nodes,
        edges={},
    )
    wf_id = create_and_publish(wf)
    run_id = run_repo.create_run(wf_id, version=1)

    workers = [
        WorkflowWorker(worker_id=f"same-run-worker-{idx}", lease_duration_seconds=10.0)
        for idx in range(4)
    ]

    lock = threading.Lock()
    active_claims: set[str] = set()
    completed_nodes: list[tuple[str, str]] = []
    duplicate_detected = False

    def executor_callback(claimed: Any) -> dict[str, Any]:
        nonlocal duplicate_detected
        node_key: str = claimed.node_key
        worker_id: str = claimed.worker_id

        with lock:
            if node_key in active_claims:
                duplicate_detected = True
            active_claims.add(node_key)

        time.sleep(0.01)

        with lock:
            active_claims.remove(node_key)
            completed_nodes.append((worker_id, node_key))

        return {"executed_by": worker_id}

    def worker_loop(w: WorkflowWorker) -> int:
        count = 0
        consecutive_idle = 0
        while consecutive_idle < 5:
            handled = w.execute_one(executor_callback)
            if handled:
                count += 1
                consecutive_idle = 0
            else:
                consecutive_idle += 1
                time.sleep(0.01)
        return count

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(worker_loop, w) for w in workers]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    total_claims = sum(results)
    assert total_claims == node_count
    assert not duplicate_detected
    assert len(completed_nodes) == node_count

    executed_keys = {node_key for _, node_key in completed_nodes}
    assert executed_keys == {f"root_{i}" for i in range(node_count)}

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT node_key, status, lease_owner, lease_expires_at
                FROM node_runs
                WHERE run_id = %s;
                """,
                (run_id,),
            )
            node_rows = cur.fetchall()
            assert len(node_rows) == node_count
            for r in node_rows:
                assert r["status"] == "SUCCEEDED"
                assert r["lease_owner"] is None
                assert r["lease_expires_at"] is None

            cur.execute(
                """
                SELECT na.attempt_number, na.status, na.worker_id
                FROM node_attempts na
                JOIN node_runs nr ON na.node_run_id = nr.id
                WHERE nr.run_id = %s;
                """,
                (run_id,),
            )
            attempt_rows = cur.fetchall()
            assert len(attempt_rows) == node_count
            for a in attempt_rows:
                assert a["status"] == "SUCCEEDED"
                assert a["attempt_number"] == 1
