# Workflow Engine (#102)

A durable, DAG-based workflow orchestration system designed with concurrent workers, failure recovery, conditional execution, and persistent relational state.

---

## 1. Project Overview

The **Workflow Engine** is an enterprise-grade workflow orchestration system engineered for correctness, durability, and resilience under concurrent execution. The system models complex workflows as directed acyclic graphs (DAGs) with dynamic conditional branching, multi-hop dead-path elimination, inclusive OR joins, and atomic execution snapshots.

Published workflow definitions are strictly immutable, while runtime executions maintain isolated per-run node and edge state machines in PostgreSQL.

---

## 2. Architecture

The system follows a modular monolith architecture with PostgreSQL as the durable source of truth.

```text
React + TypeScript Canvas UI (Planned M6)
                  |
                  v
       FastAPI REST & WebSockets (Planned M6)
                  |
                  v
      PostgreSQL 16+ (Durable Source of Truth)
      [Immutable Versions | Runs | Node States | Edge States]
                  |
         +--------+--------+
         |                 |
         v                 v
      Worker 1          Worker 2  (M3 & M4)
   [Fenced Leases]   [Fenced Leases]
   [Heartbeat Mon]   [Heartbeat Mon]
         |                 |
         +--------+--------+
                  |
                  v
     Database-Backed Execution Engine (M4)
     - Fenced Completion Transaction
     - Atomic Edge Resolution (TAKEN / DEAD)
     - Transactional Multi-Hop Skip Cascade
     - Inclusive OR Join Reconvergence
     - Terminal Run Detection (SUCCEEDED)
```

### Architectural Principles:
1. **Separation of Definition and Execution**: Workflows are published as immutable versions (`workflow_versions`). Active runs point to an immutable snapshot and are never altered by workflow authoring changes.
2. **Outside-Transaction Execution**: External I/O operations (HTTP requests, delays, transforms) are strictly performed outside database transactions.
3. **Hierarchical Row-Level Locking**: Concurrency control enforces strict lock ordering (`runs` $\to$ `node_runs` $\to$ `edge_runs`) to prevent database deadlocks.
4. **Fenced Leases**: Monotonically increasing lease generations ensure stale or expired workers cannot corrupt state upon late completion.
5. **Atomic Downstream Resolution**: In a single run-locked transaction, successful node completion updates `node_runs`, resolves outgoing `edge_runs`, cascades multi-hop skips to a fixed point, promotes eligible inclusive OR joins to `READY`, and detects terminal run completion.

---

## 3. Technology Stack

- **Core Backend**: Python 3.11+ (tested on Python 3.11 and 3.14)
- **Database Driver**: Psycopg 3 (`psycopg[binary]`)
- **Database Engine**: PostgreSQL 16+ (tested against PostgreSQL 18.6)
- **Database Migrations**: Alembic & SQLAlchemy core DDL
- **Testing**: Pytest, Pytest-Cov, Pytest-Asyncio
- **Static Analysis & Linting**: Mypy (strict type checking), Ruff (linter & formatter)
- **Frontend Scaffolding**: React, TypeScript, Vite (UI canvas planned in M6)
- **Infrastructure & CI**: Docker Compose, GitHub Actions CI

---

## 4. Core Engineering Capabilities

### Graph Invariant Enforcement & Validation (M1)
- **Cycle Detection**: Kahn's algorithm resolves in-degrees and rejects cyclic graphs before execution.
- **Topological Sorting**: Generates deterministic execution orders.
- **Structural Integrity**: Rejects dangling edges, unknown nodes, and empty workflow graphs.

### Branch Evaluation & Cascading Skips (M1)
- **Dynamic Branching**: Evaluates conditional expressions on condition nodes, marking matching branch edges `TAKEN` and unselected edges `DEAD`.
- **Fixed-Point Multi-Hop Skip Propagation**: Deterministically cascades dead paths through arbitrary dependency chains until mathematical convergence ($O(|V| + |E|)$ complexity).
- **Inclusive OR Join Resolution**: Reconvergent join nodes execute when all incoming dependencies have resolved and at least one path is `TAKEN`. If all incoming dependencies are `DEAD`, the join node is marked `SKIPPED`.

### Persistent Relational Foundation (M2)
- **Normalized Schema (9 Tables)**: `workflows`, `workflow_versions`, `workflow_nodes`, `workflow_edges`, `runs`, `node_runs`, `edge_runs`, `node_attempts`, and `run_events`.
- **Atomic Run Creation**: Creates the run in `QUEUED` status, initializes all node and edge runtime states, and promotes root nodes ($d_{in} = 0$) to `READY` within a single atomic database transaction.
- **Partial & Scheduling Indexes**:
  - `idx_runs_claimable`: `runs (last_claimed_at ASC NULLS FIRST, created_at ASC) WHERE status IN ('QUEUED', 'RUNNING')`, which, in conjunction with the candidate claiming protocol (`ORDER BY last_claimed_at ASC NULLS FIRST, created_at ASC FOR UPDATE SKIP LOCKED` and rotation timestamp updates), provides efficient, starvation-free round-robin run discovery.
  - `idx_node_runs_ready`: `node_runs (run_id, run_after) WHERE status = 'READY'` for instant ready node candidate queries.
  - `idx_node_runs_running_lease`: `node_runs (run_id, lease_expires_at) WHERE status = 'RUNNING'` for fast expired lease detection.
- **Differential Oracle Agreement**: 100% agreement between database state transitions and the pure Python in-memory semantic reference oracle.

### Worker Engine & Lease Fencing (M3)
- **Hierarchical Run-First Locking**: Every claiming, heartbeat, completion, and sweeper transaction locks the parent run row first (`SELECT id FROM runs WHERE id = %s FOR UPDATE;`), preventing deadlocks and race conditions.
- **Fair Round-Robin Scheduling**: Fair run candidate discovery uses `SKIP LOCKED` and updates `last_claimed_at` upon claim, preventing starvation of active runs.
- **Monotonic Lease Generations**: Atomic node claim increments `lease_generation` and sets a short-lived `lease_expires_at`.
- **Fenced Heartbeat Monitor**: Background heartbeat thread extends leases with monotonic token fencing; immediately aborts local execution via `CancellationToken` if lease is lost.
- **Background Sweeper Recovery**: Sweeper identifies expired leases, safely transitions orphaned nodes to `READY` or `FAILED`, enforces exponential retry backoff, and performs atomic fail-fast cancellation on permanent failure.

### Database-Backed DAG Execution (M4)
- **Database-Backed Edge Transitions**: Node completion updates outgoing `edge_runs` to `TAKEN` or `DEAD` and emits `EDGE_TAKEN` / `EDGE_DEAD` audit events.
- **Dynamic Branch Evaluation**: Evaluates conditional branch outputs (`result: bool`, `branch: str`, or fallback `default`), seamlessly integrated with the pure M1 evaluator logic.
- **Inclusive OR Join Reconvergence**: Reconvergent join nodes are evaluated in the database: promoted to `READY` iff all incoming edges are resolved and at least one is `TAKEN`; transitioned to `SKIPPED` iff all incoming edges are `DEAD`.
- **Transactional Multi-Hop Skip Cascade**: When a path becomes dead, all downstream dependencies cascade transitively to `SKIPPED` until fixed-point convergence within the single parent run-locked transaction.
- **Terminal Run Detection**: When non-terminal nodes reach zero (`COUNT(status NOT IN ('SUCCEEDED', 'SKIPPED')) == 0`), parent run atomically transitions from `RUNNING` to `SUCCEEDED` and emits `RUN_SUCCEEDED`.
- **Differential Oracle Verification**: Full execution across diamond, multi-root, cascading, and reconvergent workflows verified for 100% differential state agreement against M1 in-memory DAG oracle.

---

## 5. Implementation Status & Milestones

| Milestone | Scope | Status | Verification |
|---|---|---|---|
| **M0: Foundation** | Project scaffolding, tooling, uv venv, CI setup | **Completed** | Clean build & CI pipeline |
| **M1: Pure In-Memory DAG Engine** | Graph validator, topological sorter, branch evaluator, skip propagation, in-memory reference oracle | **Completed** | 54 unit tests, 99% coverage |
| **M2: PostgreSQL Persistence** | Schema, Alembic migrations, partial indexes, atomic run creation, repositories | **Completed** | 18 integration tests, 98% coverage |
| **M3: Worker Engine** | Run-first locking, fair candidate discovery, atomic claiming, fenced leases, heartbeats, sweeper recovery | **Completed** | 27 integration & crash tests, 95% coverage |
| **M4: DAG Execution** | Database-backed edge states, branch evaluation, reconvergent joins, transactional multi-hop skips, terminal detection | **Completed** | 35 integration & oracle tests, 95% coverage |
| **M5: Resilience & Recovery** | Crash recovery sweeper, retry scheduling, exponential backoff, cooperative cancellation | *Awaiting Authorization* | Planned |
| **M6: REST API & Canvas UI** | FastAPI routes, WebSockets execution stream, React canvas editor | *Pending M5* | Planned |
| **M7: Deployment & Acceptance** | Docker Compose orchestration, comprehensive end-to-end integration, benchmarks | *Pending M6* | Planned |

> [!IMPORTANT]
> **Milestone Status Notice**: Milestones M0, M1, M2, M3, and M4 are fully implemented and verified against real PostgreSQL 18 (134/134 passing tests, 95% code coverage). Milestone M5 is awaiting human authorization before implementation begins.

---

## 6. Known Limitations

- **No External HTTP Execution Yet (M5 Planned)**: Full external HTTP requests and payload transforms are staged for the worker integration phase.
- **No Web UI / REST Endpoints Yet (M6 Planned)**: Interaction is currently programmatically driven via the Python repository API, worker engines, and test suites.

---

## 7. Local Setup Instructions

### Prerequisites
- Python 3.11+ (Python 3.14 supported)
- [uv](https://github.com/astral-sh/uv) (recommended) or `pip`
- PostgreSQL 16+ (running locally or via Docker)

### 1. Clone & Set Up Python Environment
```bash
git clone https://github.com/ASM0107/workflow-engine.git
cd workflow-engine

# Create virtual environment
uv venv .venv

# Activate virtual environment
# On Linux/macOS:
source .venv/bin/activate
# On Windows:
.venv\Scripts\activate

# Install package and development dependencies
uv pip install -e ".[dev]"
```

### 2. Configure Environment Variables
Copy the sample environment file:
```bash
cp .env.example .env
```
Edit `.env` to match your local PostgreSQL configuration:
```env
DATABASE_URL=postgresql://engine_user:engine_password@localhost:5432/workflow_engine
```

### 3. Start PostgreSQL with Docker Compose (Optional)
If you do not have a local PostgreSQL instance running:
```bash
docker compose up -d postgres
```

### 4. Run Database Migrations
Apply the initial schema migration using Alembic:
```bash
alembic upgrade head
```

To roll back migrations:
```bash
alembic downgrade base
```

---

## 8. Testing Instructions

The test suite runs against the live PostgreSQL database and includes unit tests, constraint verifications, index query plan assertions, and differential tests against the reference oracle.

### Run Full Test Suite with Coverage
```bash
pytest --cov=workflow_engine --cov-report=term-missing
```

### Run Static Type Analysis
```bash
mypy backend/src tests
```

### Run Linter & Formatter Check
```bash
ruff check .
```

### Verification Output Sample
```text
tests/test_dag/test_*.py (54 items) .................... PASSED
tests/test_persistence/test_*.py (18 items) ........... PASSED
tests/test_worker/test_*.py (27 items) ................ PASSED
tests/test_execution/test_exec_*.py (35 items) ........ PASSED

============================= 134 passed in 67.72s =============================
Coverage: 95%
```

---

## 9. License & Author

Developed by ASM0107 as part of the Master Engineering Portfolio.
Licensed under the MIT License.
