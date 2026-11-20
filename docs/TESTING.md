# Testing Strategy & Verification Protocol

## 1. Testing Philosophy
- **Correctness Over Feature Count**: Every architectural guarantee is verified by automated tests.
- **No Mocking of Core Domain Logic**: The DAG engine, cycle detector, topological sorter, branch evaluator, and skip propagator are tested against genuine pure-Python implementations.
- **Differential Oracle Testing**: The in-memory reference engine built in Milestone M1 serves as the semantic oracle against which subsequent PostgreSQL-backed worker execution (Milestone M4) is differentially tested.

## 2. Test Suites (Milestone M1 Focus)
- `tests/test_dag/test_validation.py`: Tests empty workflows, disconnected edge references, self-loops, simple cycles, complex multi-node cycles, and valid topologies.
- `tests/test_dag/test_topology.py`: Tests topological sort order correctness, in-degree calculations, and multi-root handling.
- `tests/test_dag/test_branching.py`: Tests condition node evaluation, true/false branch resolution, and unselected edge marking as `DEAD`.
- `tests/test_dag/test_skip_propagation.py`: Tests fixed-point skip cascades across linear chains, multiple independent paths, and deep multi-hop hierarchies (3+ hops).
- `tests/test_dag/test_joins.py`: Tests reconvergent join resolution:
  - Inclusive OR joins (one `TAKEN`, one `DEAD` -> node becomes `READY`).
  - Total dead path joins (all incoming `DEAD` -> node becomes `SKIPPED`).
  - Partial pending joins (node waits until all incoming edges resolve).
- `tests/test_dag/test_terminal_detection.py`: Tests run lifecycle transitions (`QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`).
- `tests/test_dag/test_oracle.py`: Comprehensive end-to-end execution of complex workflow graphs using the in-memory oracle engine.
