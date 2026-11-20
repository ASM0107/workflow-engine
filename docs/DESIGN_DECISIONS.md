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
