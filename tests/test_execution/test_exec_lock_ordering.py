import re
import threading
from typing import Any, Callable

import psycopg
import pytest
from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.repositories import RunRepository
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.completion import CompletionEngine
from workflow_engine.worker.worker import WorkflowWorker


class LockingOrderSpyCursor:
    def __init__(self, real_cursor: Any, recorded_locks: list[tuple[str, str]]) -> None:
        self._real_cursor = real_cursor
        self._recorded_locks = recorded_locks

    def execute(self, query: Any, params: Any = None) -> Any:
        query_str = str(query).strip()
        if "FOR UPDATE" in query_str.upper():
            table_match = re.search(r"FROM\s+([a-zA-Z0-9_]+)", query_str, re.IGNORECASE)
            table_name = table_match.group(1).lower() if table_match else "unknown"
            self._recorded_locks.append((table_name, query_str))
        return self._real_cursor.execute(query, params)

    def fetchone(self) -> Any:
        return self._real_cursor.fetchone()

    def fetchall(self) -> Any:
        return self._real_cursor.fetchall()

    @property
    def rowcount(self) -> int:
        return int(self._real_cursor.rowcount)

    def __enter__(self) -> "LockingOrderSpyCursor":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
        return self._real_cursor.__exit__(exc_type, exc_val, exc_tb)


def test_completion_lock_acquisition_sequence_is_strictly_runs_node_runs_edge_runs(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    diamond_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wf_id = create_and_publish(diamond_workflow)
    _ = run_repo.create_run(wf_id, 1)

    node_trigger = claim_engine.discover_and_claim_node("worker-1")
    assert node_trigger is not None

    recorded_locks: list[tuple[str, str]] = []
    original_cursor = psycopg.Connection.cursor

    def spy_cursor_factory(conn_self: Any, *args: Any, **kwargs: Any) -> Any:
        real_cur = original_cursor(conn_self, *args, **kwargs)
        return LockingOrderSpyCursor(real_cur, recorded_locks)

    engine = CompletionEngine()

    monkeypatch.setattr(psycopg.Connection, "cursor", spy_cursor_factory)
    engine.complete_success(node_trigger, output={"started": True})

    locked_tables = [table for table, _ in recorded_locks]

    assert locked_tables == ["runs", "node_runs", "edge_runs"]

    node_runs_query = recorded_locks[1][1]
    assert "ORDER BY node_key ASC" in node_runs_query

    edge_runs_query = recorded_locks[2][1]
    assert "ORDER BY edge_key ASC" in edge_runs_query


def test_failure_lock_acquisition_sequence_is_strictly_runs_node_runs(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    diamond_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wf_id = create_and_publish(diamond_workflow)
    _ = run_repo.create_run(wf_id, 1)

    node_trigger = claim_engine.discover_and_claim_node("worker-1")
    assert node_trigger is not None

    recorded_locks: list[tuple[str, str]] = []
    original_cursor = psycopg.Connection.cursor

    def spy_cursor_factory(conn_self: Any, *args: Any, **kwargs: Any) -> Any:
        real_cur = original_cursor(conn_self, *args, **kwargs)
        return LockingOrderSpyCursor(real_cur, recorded_locks)

    engine = CompletionEngine()

    monkeypatch.setattr(psycopg.Connection, "cursor", spy_cursor_factory)
    engine.complete_failure(node_trigger, error_message="fatal error")

    locked_tables = [table for table, _ in recorded_locks]
    assert locked_tables == ["runs", "node_runs"]

    node_runs_query = recorded_locks[1][1]
    assert "ORDER BY node_key ASC" in node_runs_query


def test_cancel_run_lock_acquisition_sequence_is_strictly_runs_node_runs(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    linear_workflow: WorkflowDefinition,
    claim_engine: ClaimEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wf_id = create_and_publish(linear_workflow)
    run_id = run_repo.create_run(wf_id, 1)

    node_1 = claim_engine.discover_and_claim_node("worker-1")
    assert node_1 is not None

    recorded_locks: list[tuple[str, str]] = []
    original_cursor = psycopg.Connection.cursor

    def spy_cursor_factory(conn_self: Any, *args: Any, **kwargs: Any) -> Any:
        real_cur = original_cursor(conn_self, *args, **kwargs)
        return LockingOrderSpyCursor(real_cur, recorded_locks)

    monkeypatch.setattr(psycopg.Connection, "cursor", spy_cursor_factory)
    run_repo.cancel_run(run_id)

    locked_tables = [table for table, _ in recorded_locks]
    assert locked_tables == ["runs", "node_runs"]

    node_runs_query = recorded_locks[1][1]
    assert "ORDER BY node_key ASC" in node_runs_query

    locked_tables = [table for table, _ in recorded_locks]
    assert locked_tables == ["runs", "node_runs"]

    node_runs_query = recorded_locks[1][1]
    assert "ORDER BY node_key ASC" in node_runs_query


def test_concurrent_completion_preserves_lock_ordering_without_deadlocks(
    create_and_publish: Callable[[WorkflowDefinition], str],
    run_repo: RunRepository,
    claim_engine: ClaimEngine,
    completion_engine: CompletionEngine,
) -> None:
    fan_out_def = WorkflowDefinition(
        id="wf_concurrent_lock_ordering",
        version=1,
        nodes={
            "root": Node(id="root", type=NodeType.MANUAL_TRIGGER),
            "b1": Node(id="b1", type=NodeType.HTTP),
            "b2": Node(id="b2", type=NodeType.HTTP),
            "b3": Node(id="b3", type=NodeType.HTTP),
            "join": Node(id="join", type=NodeType.JSON_TRANSFORM),
        },
        edges={
            "e_root_1": Edge(id="e_root_1", from_node_id="root", to_node_id="b1"),
            "e_root_2": Edge(id="e_root_2", from_node_id="root", to_node_id="b2"),
            "e_root_3": Edge(id="e_root_3", from_node_id="root", to_node_id="b3"),
            "e_1_join": Edge(id="e_1_join", from_node_id="b1", to_node_id="join"),
            "e_2_join": Edge(id="e_2_join", from_node_id="b2", to_node_id="join"),
            "e_3_join": Edge(id="e_3_join", from_node_id="b3", to_node_id="join"),
        },
    )

    wf_id = create_and_publish(fan_out_def)
    run_id = run_repo.create_run(wf_id, 1)

    node_root = claim_engine.discover_and_claim_node("worker-root")
    assert node_root is not None
    completion_engine.complete_success(node_root, output={})

    errors: list[Exception] = []

    def worker_loop(w_id: str) -> None:
        try:
            worker = WorkflowWorker(worker_id=w_id)
            for _ in range(5):
                claimed = worker.execute_one(lambda n: {"processed_by": w_id})
                if not claimed:
                    break
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker_loop, args=(f"worker-lock-{i}",)) for i in range(3)]

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert not errors

    final_run = run_repo.get_run(run_id)
    assert final_run["status"] == "SUCCEEDED"
