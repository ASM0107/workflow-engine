# Threat Model & Security Posture

## 1. System Boundaries
The Workflow Execution Engine interacts with user input (workflow definitions, execution triggers) and external endpoints (HTTP integration nodes).

```text
[Untrusted Client] ---> [FastAPI Gateway] ---> [PostgreSQL (Fenced Locks)]
                                                   |
                                            [Worker Pool]
                                                   |
                                                   v
                                        [External HTTP Targets]
```

## 2. Identified Threats & Mitigations

### 2.1 Graph DoS & Algorithmic Exploits
- **Threat**: Maliciously crafted workflow graphs with cyclic dependencies or exponential branching intended to cause server hangs or memory exhaustion.
- **Mitigation**:
  - Pre-execution validation enforcing finite DAG rules via Kahn's algorithm.
  - Cycle detection rejecting graphs with unresolved dependencies.
  - Strict limits on maximum node count, edge count, and graph depth.

### 2.2 Server-Side Request Forgery (SSRF)
- **Threat**: HTTP nodes configured to target private subnets, cloud metadata services (`169.254.169.254`), or loopback interfaces (`127.0.0.1`).
- **Mitigation**:
  - Connection-time IP address resolution and blocking of private/link-local/loopback CIDRs.
  - Strict origin allowlisting.
  - Prohibition of unsafe redirects and DNS rebinding protections.

### 2.3 Stale Worker Race Conditions & Zombie Overwrites
- **Threat**: A worker experiencing network partition or long GC pause wakes up and attempts to write completion state over a newly elected worker's state.
- **Mitigation**:
  - Monotonically increasing `lease_generation` numbers.
  - Fenced SQL updates requiring matching lease generation and `RUNNING` status.
  - Heartbeat checks utilizing `clock_timestamp()` with immediate worker abortion upon zero affected rows.

### 2.4 Code Injection via Transformations
- **Threat**: Remote code execution through JSON transformation expressions.
- **Mitigation**:
  - Strict prohibition of `eval()`, `exec()`, or dynamic scripting.
  - Constrained JMESPath transformation engine with enforced input and output size boundaries.
