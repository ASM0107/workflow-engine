import concurrent.futures
from typing import Callable

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
