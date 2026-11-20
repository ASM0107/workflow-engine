# No-Code Automation Workflow Builder (#102)

A durable, DAG-based workflow orchestration system designed with concurrent workers, failure recovery, conditional execution, and persistent state.

---

## 1. Project Overview
This project implements an enterprise-grade workflow orchestration engine. It combines an intuitive visual node-based editor with an execution engine engineered for correctness, resilience, and horizontal worker concurrency.

### Key Capabilities
- **Graph Invariant Enforcement**: Topological sorting, pre-execution cycle detection, and dangling reference rejection.
- **Dynamic Branching & Edge States**: Conditional edge routing marking active paths as `TAKEN` and unselected branches as `DEAD`.
- **Fixed-Point Multi-Hop Skip Propagation**: Deterministic cascading of dead paths through downstream dependencies of arbitrary depth.
- **Reconvergent Join Resolution**: Inclusive OR joins where nodes run when all incoming dependencies resolve and at least one is taken.
- **Fenced Execution & Leases (M3+)**: Monotonically increasing lease generations preventing stale worker overwrite.
- **PostgreSQL Concurrency Locking (M3+)**: Per-run row-level locking preserving strict hierarchy (`runs` -> `node_runs` -> `edge_runs`).

---

## 2. Directory Layout
```text
projects/workflow-engine/
├── backend/
│   ├── src/workflow_engine/
│   │   ├── domain/           # Core entities, state machines, and errors
│   │   └── dag/              # Pure Python graph algorithms, validator, and oracle
│   └── pyproject.toml
├── frontend/                 # React + TypeScript + Vite canvas UI
├── migrations/               # Alembic database migrations (Milestone M2+)
├── tests/                    # Pytest automated test suites
├── docs/                     # Architectural documentation, ADRs, threat models
├── docker-compose.yml        # PostgreSQL service definition
└── pyproject.toml
```

---

## 3. Milestones Roadmap
- [x] **M0: Foundation** — Tooling, repository structure, environment setup, and CI.
- [x] **M1: Pure In-Memory DAG Engine** — Cycle detection, topological sorting, skip propagation, join resolution, and reference oracle.
- [ ] **M2: PostgreSQL Persistence** — Immutable workflow versions, Alembic migrations, partial indexes, and run creation.
- [ ] **M3: Worker Engine** — Run-first locking, fair candidate discovery, atomic claiming, and fenced leases.
- [ ] **M4: DAG Execution** — Distributed conditional branching, per-run edge state persistence, and run-locked skip loops.
- [ ] **M5: Resilience & Recovery** — Exponential backoff, lease recovery, fail-fast cancellation, and idempotency.
- [ ] **M6: User Interface** — Interactive workflow canvas, execution timeline, and node inspector.
- [ ] **M7: Deployment & Acceptance** — Docker deployment, full end-to-end integration tests, and security review.

---

## 4. Getting Started (M0 / M1)

### Requirements
- Python 3.11+ (Python 3.14 supported)
- Node.js 20+

### Setup & Run Tests
```bash
# Set up virtual environment
uv venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv pip install -e ".[dev]"

# Run test suite
pytest
```
