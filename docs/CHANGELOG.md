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
