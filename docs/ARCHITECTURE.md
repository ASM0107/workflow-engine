# Architecture Specification — Workflow Execution Engine

## 1. System Overview
The Workflow Execution Engine is a durable, DAG-based orchestration engine designed to execute complex, multi-step automated workflows with concurrent workers, deterministic failure recovery, conditional branching, and persistent execution state.

```text
React + TypeScript (Canvas Editor & Observability)
                    |
                    v
          FastAPI (REST & WebSockets)
                    |
                    v
          PostgreSQL (Durable Source of Truth)
                    |
        +-----------+-----------+
        |                       |
        v                       v
     Worker 1                Worker 2
  (Fenced Leases)         (Fenced Leases)
        |                       |
        +-----------+-----------+
                    |
                    v
          External Integrations (HTTP, JMESPath)
```

## 2. In-Memory DAG Reference Engine (Milestone M1)
Before introducing persistence or distributed workers, the core graph algorithms and state machines are modeled as a pure Python in-memory reference engine.

### 2.1 Graph Representation
- **Node**: A discrete unit of execution with an identifier, type (`manual_trigger`, `http`, `delay`, `json_transform`, `condition`), configuration, and retry policy.
- **Edge**: A directed connection from a source node to a target node, optionally carrying a branch condition (`true`, `false`, or custom label).
- **WorkflowDefinition**: An immutable specification comprising unique nodes and validated edges.

### 2.2 Core Algorithms
- **Cycle Detection & Topological Sorting**: Kahn's algorithm computing in-degrees and resolving dependencies. Graphs with cycles are rejected before execution.
- **Root Node Detection**: Nodes with in-degree zero ($d_{in} = 0$) are identified as entry points and initialized to `READY`.
- **Branch Evaluation**: Evaluates condition outputs against outgoing edges, marking chosen edges `TAKEN` and unchosen edges `DEAD`.
- **Multi-Hop Skip Propagation**: Fixed-point propagation loop cascading `DEAD` edge signals through downstream dependent nodes until convergence.
- **Reconvergent Join Resolution**: Evaluates nodes with multiple incoming dependencies once all parents are resolved.
- **Terminal Execution Detection**: A run transitions to `SUCCEEDED` when all nodes are terminal (`SUCCEEDED` or `SKIPPED`). If any node fails permanently, the run transitions to `FAILED` and cancels pending work.

## 3. PostgreSQL Persistence Architecture (Milestone M2)

### 3.1 Schema & Data Integrity
The persistence layer guarantees durable execution history, strict foreign-key integrity, and isolation between workflow definitions and active executions:

- `workflows`: Parent entity representing workflow identity, name, description, and status.
- `workflow_versions`: Immutable published definition snapshots. Version numbers are monotonically increasing per workflow. Once published, definition rows cannot be mutated or deleted.
- `workflow_nodes` & `workflow_edges`: Structural components defining the DAG graph associated with an immutable version.
- `runs`: Runtime execution instance tracking overall lifecycle state (`QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`), worker claiming timestamps, and execution boundaries.
- `node_runs`: Per-run node execution state (`PENDING`, `READY`, `RUNNING`, `SUCCEEDED`, `FAILED`, `SKIPPED`, `CANCELLED`), lease owner, lease expiration, and fence generation counter.
- `edge_runs`: Per-run edge evaluation state (`PENDING`, `TAKEN`, `DEAD`).
- `node_attempts`: Append-only audit log recording each execution attempt for a node, including worker ID, start/end timestamps, exit status, and error payload.
- `run_events`: Append-only timeline stream of discrete lifecycle events for observability and auditability.

### 3.2 Atomic Run Creation Invariants
When a run is created via `RunRepository.create_run`:
1. It validates that the referenced `workflow_version` exists.
2. Inside a single atomic transaction:
   - Inserts `runs` row with initial status `QUEUED`.
   - Inserts `node_runs` for all nodes in the version.
   - Inserts `edge_runs` for all edges in the version with status `PENDING`.
   - Automatically identifies root nodes ($d_{in} = 0$) and initializes their status to `READY` (`run_after = NOW()`).
   - Initializes non-root nodes to status `PENDING`.
3. If any step fails, the entire transaction rolls back, preventing partial or orphaned runs.

### 3.3 Indexing & Scheduling Strategy
To ensure optimal performance and eliminate table scans during high-throughput polling:
- `idx_runs_claimable`: `runs (last_claimed_at ASC NULLS FIRST, created_at ASC) WHERE status IN ('QUEUED', 'RUNNING')` prevents worker starvation by maintaining a fair round-robin scheduling order across active runs.
- `idx_node_runs_ready`: `node_runs (run_id, run_after) WHERE status = 'READY'` facilitates instant retrieval of eligible nodes ready for immediate worker execution.
- `idx_node_runs_running_lease`: `node_runs (run_id, lease_expires_at) WHERE status = 'RUNNING'` enables instant detection of crashed workers and expired leases.

