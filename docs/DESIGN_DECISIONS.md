# Design Decisions Record (ADR)

## ADR-001: Pure In-Memory DAG Engine as Reference Oracle
- **Context**: The workflow engine requires complex DAG semantics including cycle detection, topological ordering, multi-hop skip propagation, and reconvergent join resolution.
- **Decision**: Build an independent, pure Python in-memory DAG reference engine in Milestone M1 before introducing persistence or database transactions in M2.
- **Consequences**:
  - Provides a single source of truth for graph semantics.
  - Serves as the differential testing oracle against the future PostgreSQL execution engine.
  - Decouples graph theory algorithms from database locking and concurrency mechanics.

## ADR-002: Fixed-Point Multi-Hop Skip Propagation
- **Context**: When a conditional branch is unselected (marked DEAD) or a node fails/skips, downstream nodes whose incoming paths are all dead must be skipped, cascading through arbitrary depths.
- **Decision**: Implement a fixed-point iteration algorithm:
  ```text
  REPEAT:
      Find PENDING nodes whose incoming edges are all DEAD.
      Mark these nodes SKIPPED.
      Mark their outgoing PENDING edges DEAD.
  UNTIL no additional nodes transition to SKIPPED.
  Promote eligible remaining nodes to READY.
  ```
- **Consequences**:
  - Handles arbitrarily deep cascading branches deterministically.
  - Guaranteed termination because valid workflows are finite DAGs ($O(|V| + |E|)$ complexity).
  - Directly maps to future PostgreSQL run-locked transaction logic in Milestone M4.

## ADR-003: Reconvergent Join Rule Semantics
- **Context**: A downstream node may depend on multiple incoming branches, some of which may be taken and some dead.
- **Decision**: Enforce exact join invariants:
  1. A node becomes `READY` when all incoming edges are resolved (either `TAKEN` or `DEAD`) and at least one is `TAKEN`.
  2. A node becomes `SKIPPED` when all incoming edges are resolved as `DEAD`.
  3. A node remains `PENDING` if any incoming edge is `PENDING`.
- **Consequences**:
  - Enables diamond graph reconvergence without deadlock or spurious execution.
  - Distinguishes inclusive OR joins from dead-path elimination.

## ADR-004: Explicit State Separation for Nodes, Edges, and Runs
- **Context**: Execution lifecycles require distinct invariants for workflow runs, individual nodes, and edge transitions.
- **Decision**:
  - Node States: `PENDING`, `READY`, `RUNNING`, `SUCCEEDED`, `FAILED`, `SKIPPED`, `CANCELLED`.
  - Edge States: `PENDING`, `TAKEN`, `DEAD`.
  - Run States: `QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`.
- **Consequences**:
  - Prevents state conflation between graph topology and runtime attempts.
  - Enforces atomic transitions governed by well-defined state machines.

## ADR-005: Dedicated Local PostgreSQL Cluster for Deterministic Integration Testing
- **Context**: Real database integration testing was required for constraints, indexes, migrations, and transactional isolation without relying on mocked databases.
- **Decision**: Initialize a standalone, isolated PostgreSQL 18 instance running in user-space at `projects/workflow-engine/pgdata` on port 5433 with trust authentication for `engine_user`.
- **Consequences**:
  - Eliminates dependency on host service state or Docker Desktop permissions.
  - Provides identical execution environment across developer sessions and automated testing.
  - Excluded via `.gitignore` to prevent repository bloat while preserving reproducible migration scripts.

## ADR-006: Atomic Run Creation and Immutable Workflow Versioning
- **Context**: Workflow execution state must remain completely isolated from subsequent workflow authoring or modifications. Starting a run must create an atomic snapshot.
- **Decision**:
  - Publish workflows into immutable version records (`workflow_versions`). Workflow modifications produce a new version number; existing versions cannot be mutated or deleted.
  - Create runs inside a single database transaction: insert the `runs` record with initial status `QUEUED`, populate all `node_runs` and `edge_runs`, and immediately promote root nodes to `READY` while keeping downstream nodes `PENDING`.
- **Consequences**:
  - Eliminates half-initialized run states or orphan records.
  - Guarantees execution traceability: runs always point to an exact immutable definition snapshot.

## ADR-007: Partial Indexes for Fair Run Scheduling and Fenced Leases
- **Context**: High-frequency worker polling and claiming on `node_runs` and `runs` could degrade as history tables grow.
- **Decision**: Create targeted partial indexes:
  1. `idx_runs_claimable`: `runs (last_claimed_at ASC NULLS FIRST, created_at ASC) WHERE status IN ('QUEUED', 'RUNNING')` for starvation-free round-robin run discovery.
  2. `idx_node_runs_ready`: `node_runs (run_id, run_after) WHERE status = 'READY'` for instant ready node candidate lookup.
  3. `idx_node_runs_running_lease`: `node_runs (run_id, lease_expires_at) WHERE status = 'RUNNING'` for fast expired lease recovery.
- **Consequences**:
  - Index size remains bounded proportional to active workload, not cumulative history.
  - Query plans consistently utilize index scans even across high-volume terminal history.

## ADR-008: Workflow Deletion and Execution History Retention Policy
- **Context**: Modifying or deleting workflows must not corrupt historical audit trails, invalidate telemetry, or create dangling foreign keys.
- **Decision**:
  1. **Strict Immutability with RESTRICT Constraints**: Foreign key from `runs` to `workflow_versions` uses `ON DELETE RESTRICT`. A published version referenced by any execution run cannot be dropped or deleted.
  2. **Soft Deletion for Workflow Lifecycle**: Workflows can be archived/deactivated, which hides them from active authoring without touching database definitions or historical records.
  3. **Hard Deletion Exclusively for Unreferenced Drafts**: A workflow and its versions can only be hard-deleted if zero associated `runs` exist.
  4. **Targeted Execution Purge Procedures**: When pruning stale telemetry for compliance or storage retention (e.g. runs older than 90 days), batch purges operate exclusively on terminal `runs` (`status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`). Cascade rules on `node_runs`, `edge_runs`, `node_attempts`, and `run_events` cleanly purge child records while preserving the underlying `workflow_versions`.
- **Consequences**:
  - Guarantees strict non-repudiation and auditable historical execution records.
  - Prevents inadvertent catastrophic cascading deletion of production execution data.

## ADR-009: Monotonically Fenced Leases and Hierarchical Row Locking
- **Context**: Distributed workers executing nodes must handle transient worker crashes, network partitions, and stale completions without split-brain corruption or deadlocks.
- **Decision**:
  1. **Strict Hierarchical Locking**: Every database transaction touching run execution acquires `runs` row lock via `FOR UPDATE` before accessing `node_runs` or `edge_runs`.
  2. **Monotonic Lease Generation**: Each claim, recovery, or retry increments a 64-bit integer `lease_generation`.
  3. **Fenced Updates**: Heartbeat extensions, successes, and failures include `WHERE lease_owner = $worker AND lease_generation = $gen AND status = 'RUNNING'`.
  4. **Active Abort via Heartbeat Monitor**: Workers spawn a background heartbeat monitor that aborts local processing if lease ownership is lost.
- **Consequences**:
  - Complete elimination of database deadlocks between concurrent workers.
  - Guaranteed immunity against split-brain execution or zombie worker state overwrite.



