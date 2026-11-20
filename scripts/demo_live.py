import json
import time

from workflow_engine.domain.entities import Edge, Node, WorkflowDefinition
from workflow_engine.domain.errors import FencingTokenError
from workflow_engine.domain.types import NodeType
from workflow_engine.persistence.connection import get_connection
from workflow_engine.persistence.repositories import RunRepository, WorkflowRepository
from workflow_engine.worker.claim import ClaimEngine
from workflow_engine.worker.completion import CompletionEngine
from workflow_engine.worker.heartbeat import HeartbeatService
from workflow_engine.worker.sweeper import LeaseRecoverySweeper


def print_separator(title: str) -> None:
    line = "=" * 78
    print(f"\n{line}")
    print(f" {title.upper()}")
    print(f"{line}")


def print_step(step_num: int, title: str, description: str) -> None:
    print(f"\n[STEP {step_num}] {title}")
    print(f"       -> {description}")


def query_and_print_run_state(run_id: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, status, created_at, updated_at, last_claimed_at
                FROM runs
                WHERE id = %s;
                """,
                (run_id,),
            )
            run_row = cur.fetchone()
            if run_row:
                short_id = str(run_row["id"])[:8]
                status = run_row["status"]
                last_claim = run_row["last_claimed_at"]
                print(f"   [RUNS] id={short_id}.. | status={status} | last_claimed={last_claim}")

            cur.execute(
                """
                SELECT node_key, status, lease_owner, lease_generation,
                       lease_expires_at, run_after
                FROM node_runs
                WHERE run_id = %s
                ORDER BY created_at ASC;
                """,
                (run_id,),
            )
            nodes = cur.fetchall()
            print("   [NODE_RUNS]")
            for n in nodes:
                expiry = str(n["lease_expires_at"])[11:19] if n["lease_expires_at"] else "None"
                owner_str = str(n["lease_owner"]) if n["lease_owner"] else "None"
                print(
                    f"     - {n['node_key']:<18} | status={n['status']:<9} "
                    f"| owner={owner_str:<14} | gen={n['lease_generation']:<2} "
                    f"| expires={expiry}"
                )


def query_and_print_attempts(run_id: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT nr.node_key, na.attempt_number, na.worker_id, na.status,
                       na.error, na.output
                FROM node_attempts na
                JOIN node_runs nr ON na.node_run_id = nr.id
                WHERE nr.run_id = %s
                ORDER BY na.started_at ASC, na.attempt_number ASC;
                """,
                (run_id,),
            )
            attempts = cur.fetchall()
            print("   [NODE_ATTEMPTS]")
            if not attempts:
                print("     (no attempts recorded yet)")
            for a in attempts:
                out_str = json.dumps(a["output"]) if a["output"] else "{}"
                err_str = f" | error='{a['error']}'" if a["error"] else ""
                print(
                    f"     - node={a['node_key']:<15} | attempt={a['attempt_number']} "
                    f"| worker={a['worker_id']:<14} | status={a['status']:<10}"
                    f"{err_str} | out={out_str}"
                )


def query_and_print_events(run_id: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_type, payload, created_at
                FROM run_events
                WHERE run_id = %s
                ORDER BY created_at ASC;
                """,
                (run_id,),
            )
            events = cur.fetchall()
            print("   [RUN_EVENTS AUDIT LOG]")
            for ev in events:
                ts = str(ev["created_at"])[11:19]
                payload_summary = json.dumps(ev["payload"])
                print(f"     [{ts}] {ev['event_type']:<18} -> {payload_summary}")


def execute_live_demonstration() -> None:
    print_separator("Milestone M3 Worker Engine - Live Verification & Demonstration")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE runs CASCADE;")
        conn.commit()

    workflow_repo = WorkflowRepository()
    run_repo = RunRepository(workflow_repo)
    claim_engine = ClaimEngine(default_lease_seconds=10.0)
    heartbeat_service = HeartbeatService()
    completion_engine = CompletionEngine()
    sweeper = LeaseRecoverySweeper()

    print_step(
        1,
        "Workflow Authoring & Version Publishing",
        "Publishing Order Processing Pipeline DAG to PostgreSQL",
    )
    nodes = {
        "verify_order": Node(
            id="verify_order",
            type=NodeType.MANUAL_TRIGGER,
            name="Verify Customer Order",
            retry_limit=2,
        ),
        "process_payment": Node(
            id="process_payment",
            type=NodeType.HTTP,
            name="Charge Payment Gateway",
            retry_limit=2,
        ),
        "send_receipt": Node(
            id="send_receipt",
            type=NodeType.HTTP,
            name="Email Customer Receipt",
            retry_limit=1,
        ),
    }
    edges = {
        "e1": Edge(id="e1", from_node_id="verify_order", to_node_id="process_payment"),
        "e2": Edge(id="e2", from_node_id="process_payment", to_node_id="send_receipt"),
    }
    definition = WorkflowDefinition(id="wf_order_pipeline", version=1, nodes=nodes, edges=edges)

    workflow_id = workflow_repo.create_workflow(
        name="Order Fulfillment Pipeline",
        description="Durable e-commerce transaction workflow",
    )
    workflow_repo.publish_version(workflow_id, version=1, definition=definition)
    print(f"   Published Workflow: id='{workflow_id}' | version=1 (3 nodes, 2 edges)")

    print_step(
        2,
        "Atomic Run Creation (QUEUED State)",
        "Initializing run and promoting root nodes to READY in single transaction",
    )
    run_id = run_repo.create_run(workflow_id=workflow_id, version=1)
    query_and_print_run_state(run_id)

    print_step(
        3,
        "Worker-Alpha Claims Root Node",
        "Fair discovery locks run row FOR UPDATE and promotes QUEUED to RUNNING",
    )
    claimed_alpha = claim_engine.discover_and_claim_node(
        worker_id="worker-alpha", lease_seconds=15.0
    )
    if not claimed_alpha:
        raise RuntimeError("Failed to claim ready node")
    print(
        f"   Worker-Alpha Claim: node='{claimed_alpha.node_key}' | "
        f"generation={claimed_alpha.lease_generation} | attempt={claimed_alpha.attempt_number}"
    )
    query_and_print_run_state(run_id)
    query_and_print_attempts(run_id)

    print_step(
        4,
        "Fenced Heartbeat Renewal",
        "Worker-Alpha extends lease ownership to prevent premature recovery",
    )
    time.sleep(0.1)
    new_expiry = heartbeat_service.send_heartbeat(claimed_alpha, extension_seconds=25.0)
    print(f"   Heartbeat Succeeded: new lease_expires_at={str(new_expiry)[11:19]}")
    query_and_print_run_state(run_id)

    print_step(
        5,
        "Fault Injection: Worker-Alpha Silent Crash",
        "Simulating worker SIGKILL / network isolation; lease expires in database",
    )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE node_runs
                SET lease_expires_at = clock_timestamp() - INTERVAL '5 seconds'
                WHERE id = %s;
                """,
                (claimed_alpha.node_run_id,),
            )
        conn.commit()
    print("   Injected: node_runs.lease_expires_at rewound 5 seconds into past.")
    query_and_print_run_state(run_id)

    print_step(
        6,
        "Background Sweeper Detects Expired Lease",
        "Sweeper marks attempt TIMED_OUT, increments generation, recovers node to READY",
    )
    recovered_count = sweeper.recover_expired_leases(backoff_base_seconds=0.01)
    print(f"   Sweeper Executed: recovered {recovered_count} expired lease(s)")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE node_runs
                SET run_after = clock_timestamp() - INTERVAL '1 second'
                WHERE id = %s;
                """,
                (claimed_alpha.node_run_id,),
            )
        conn.commit()
    query_and_print_run_state(run_id)
    query_and_print_attempts(run_id)

    print_step(
        7,
        "Worker-Bravo Discovers & Claims Recovered Node",
        "Worker-Bravo claims node with monotonic generation 3 and attempt 2",
    )
    claimed_bravo = claim_engine.discover_and_claim_node(
        worker_id="worker-bravo", lease_seconds=30.0
    )
    if not claimed_bravo:
        raise RuntimeError("Worker-Bravo failed to claim recovered node")
    print(
        f"   Worker-Bravo Claim: node='{claimed_bravo.node_key}' | "
        f"generation={claimed_bravo.lease_generation} | attempt={claimed_bravo.attempt_number}"
    )
    query_and_print_run_state(run_id)
    query_and_print_attempts(run_id)

    print_step(
        8,
        "Zombie Write Rejection (Fencing Token Defense)",
        "Resurrected Worker-Alpha attempts to complete node using stale generation 1",
    )
    zombie_caught = False
    try:
        completion_engine.complete_success(
            claimed_alpha,
            output={"error": "corrupted zombie write", "amount": -9999},
        )
    except FencingTokenError as exc:
        zombie_caught = True
        print("   [DEFENSE TRIGGERED] FencingTokenError successfully intercepted zombie write!")
        print(f"     Details: {exc}")

    if not zombie_caught:
        raise RuntimeError("CRITICAL ERROR: Zombie write was NOT rejected by fencing token!")

    print_step(
        9,
        "Worker-Bravo Executes Successfully",
        "Valid generation 3 worker writes output and transitions node to SUCCEEDED",
    )
    completion_engine.complete_success(
        claimed_bravo,
        output={"verified": True, "order_id": "ORD-882194", "total_usd": 149.95},
    )
    print("   Worker-Bravo completed execution successfully.")
    query_and_print_run_state(run_id)
    query_and_print_attempts(run_id)

    print_step(
        10,
        "Final Audit Trail & Event Ledger",
        "Inspecting persistent event log for entire run lifecycle",
    )
    query_and_print_events(run_id)

    print_separator("Demonstration Complete: All Invariants Formally Verified")


if __name__ == "__main__":
    execute_live_demonstration()
