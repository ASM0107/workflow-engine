# Changelog

All notable changes to the Workflow Execution Engine are documented in this file.

## [Unreleased]

### Added - Milestone M0 (Foundation)
- Initial repository scaffolding (`backend/`, `frontend/`, `migrations/`, `tests/`, `docs/`).
- Python virtual environment configuration using `uv` with Python 3.14/3.11+.
- React + TypeScript + Vite frontend scaffolding.
- Baseline configuration files (`pyproject.toml`, `.gitignore`, `docker-compose.yml`, GitHub Actions CI).
- Core documentation framework (`ARCHITECTURE.md`, `DESIGN_DECISIONS.md`, `THREAT_MODEL.md`, `TESTING.md`).

### Added - Milestone M1 (Pure In-Memory DAG Engine)
- Pure Python domain models: `NodeState`, `EdgeState`, `RunState`, `NodeType`, `Node`, `Edge`, `WorkflowDefinition`.
- Graph validation and Kahn's algorithm cycle detection in `workflow_engine.dag.validator`.
- Topological sorting and dependency analysis in `workflow_engine.dag.topology`.
- Branch evaluator for conditional nodes in `workflow_engine.dag.evaluator`.
- In-memory execution engine and semantic oracle with fixed-point multi-hop skip propagation in `workflow_engine.dag.engine`.
- Comprehensive automated test suite across validation, topology, branching, cascades, joins, and terminal detection.


### Added - Milestone M2 (PostgreSQL Persistence & Alembic Migrations)
- Alembic database migration environment and initial schema migration (`0001_initial_schema.py`).
- 9 PostgreSQL tables: `workflows`, `workflow_versions`, `workflow_nodes`, `workflow_edges`, `runs`, `node_runs`, `edge_runs`, `node_attempts`, and `run_events`.
- Enforced database constraints: foreign keys with cascade/restrict rules, state check constraints, and unique compound constraints.
- Partial and scheduling indexes:
  - `idx_node_runs_ready` on `node_runs (run_id, run_after) WHERE status = 'READY'`
  - `idx_node_runs_running_lease` on `node_runs (run_id, lease_expires_at) WHERE status = 'RUNNING'`
  - `idx_runs_claimable` on `runs (last_claimed_at ASC NULLS FIRST, created_at ASC) WHERE status IN ('QUEUED', 'RUNNING')`
- Implemented `WorkflowRepository` with immutable version publishing and declarative graph decomposition.
- Implemented `RunRepository` with atomic transactional run creation initializing `QUEUED` run status, `READY` root nodes, and `PENDING` downstream entities.
- Comprehensive integration test suites verifying migrations, database constraints, immutable version isolation, atomic rollbacks, query plan index scans, and differential agreement with the M1 reference oracle.

### Added - Milestone M3 (Worker Engine & Fenced Execution)
- Migration `0002_runtime_definition_composite_fks`: Composite foreign keys enforcing runtime-to-definition integrity between `runs`, `node_runs`, `edge_runs`, `workflow_nodes`, and `workflow_edges`.
- Migration `0003_worker_execution_schema`: Added `lease_owner` to `node_runs`, and `worker_id` + `output` to `node_attempts`.
- Strict hierarchical row-level locking protocol: `runs` row locked `FOR UPDATE` before any child `node_runs` or `edge_runs` row is accessed, eliminating deadlocks.
- Starvation-free candidate discovery query using `statement_timestamp()` with `idx_runs_claimable` and `idx_node_runs_ready`, updating `last_claimed_at = clock_timestamp()` upon claiming.
- Atomic `QUEUED` to `RUNNING` transition on initial node claim in the same transaction.
- Monotonically increasing `lease_generation` counter on `node_runs`.
- Fenced heartbeat mechanism renewing `lease_expires_at` protected by `WHERE lease_owner = %s AND lease_generation = %s AND status = 'RUNNING'`.
- `HeartbeatMonitor` background loop detecting lost ownership and aborting local execution immediately upon fence mismatch.
- Fenced completion transaction rejecting stale/zombie worker writes via generation validation.
- Retry budget enforcement with exponential backoff delay scheduling on failure.
- `LeaseRecoverySweeper` discovering expired leases via `idx_node_runs_running_lease` and recovering nodes back to `READY` with generation increments or failing the run if retries are exhausted.
- Full integration and crash injection test suites verifying worker death, lease timeout, recovery, stale write rejection, and concurrent worker execution without deadlocks.
