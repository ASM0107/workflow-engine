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
