import json
import uuid
from typing import Any

from psycopg.errors import ForeignKeyViolation, UniqueViolation

from workflow_engine.dag.validator import find_root_nodes, validate_workflow
from workflow_engine.domain.entities import WorkflowDefinition
from workflow_engine.domain.errors import (
    DuplicateVersionError,
    RunNotFoundError,
    VersionNotFoundError,
    WorkflowNotFoundError,
)
from workflow_engine.persistence.connection import get_connection


class WorkflowRepository:
    def create_workflow(
        self,
        name: str,
        description: str | None = None,
        workflow_id: str | None = None,
    ) -> str:
        assigned_id = workflow_id or uuid.uuid4().hex
        query = """
            INSERT INTO workflows (id, name, description)
            VALUES (%s, %s, %s)
            RETURNING id;
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (assigned_id, name, description))
                row = cur.fetchone()
                conn.commit()
                if not row:
                    raise WorkflowNotFoundError("Failed to create workflow")
                return str(row["id"])

    def publish_version(
        self,
        workflow_id: str,
        version: int,
        definition: WorkflowDefinition,
    ) -> str:
        validate_workflow(definition)

        version_id = uuid.uuid4().hex
        def_json = definition.model_dump_json()

        query_version = """
            INSERT INTO workflow_versions (id, workflow_id, version, definition)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
        """
        query_node = """
            INSERT INTO workflow_nodes (
                id, workflow_version_id, node_key, type, name, config, retry_limit
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        query_edge = """
            INSERT INTO workflow_edges (
                id, workflow_version_id, edge_key, from_node_key, to_node_key, condition_match
            )
            VALUES (%s, %s, %s, %s, %s, %s);
        """

        with get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(query_version, (version_id, workflow_id, version, def_json))

                    for node_key, node in definition.nodes.items():
                        node_row_id = uuid.uuid4().hex
                        cur.execute(
                            query_node,
                            (
                                node_row_id,
                                version_id,
                                node_key,
                                node.type.value,
                                node.name,
                                json.dumps(node.config),
                                node.retry_limit,
                            ),
                        )

                    for edge_key, edge in definition.edges.items():
                        edge_row_id = uuid.uuid4().hex
                        cur.execute(
                            query_edge,
                            (
                                edge_row_id,
                                version_id,
                                edge_key,
                                edge.from_node_id,
                                edge.to_node_id,
                                edge.condition_match,
                            ),
                        )

                conn.commit()
                return version_id
            except UniqueViolation as err:
                conn.rollback()
                raise DuplicateVersionError(
                    f"Version {version} already published for workflow '{workflow_id}'"
                ) from err
            except ForeignKeyViolation as err:
                conn.rollback()
                raise WorkflowNotFoundError(f"Workflow '{workflow_id}' not found") from err

    def get_workflow(self, workflow_id: str) -> dict[str, Any]:
        query = "SELECT id, name, description, created_at, updated_at FROM workflows WHERE id = %s;"
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (workflow_id,))
                row = cur.fetchone()
                if not row:
                    raise WorkflowNotFoundError(f"Workflow '{workflow_id}' not found")
                return dict(row)

    def get_version(self, workflow_id: str, version: int) -> dict[str, Any]:
        query = """
            SELECT id, workflow_id, version, definition, created_at
            FROM workflow_versions
            WHERE workflow_id = %s AND version = %s;
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (workflow_id, version))
                row = cur.fetchone()
                if not row:
                    raise VersionNotFoundError(
                        f"Version {version} not found for workflow '{workflow_id}'"
                    )
                return dict(row)

    def get_workflow_definition(self, workflow_id: str, version: int) -> WorkflowDefinition:
        ver_record = self.get_version(workflow_id, version)
        raw_def = ver_record["definition"]
        if isinstance(raw_def, str):
            raw_def = json.loads(raw_def)
        return WorkflowDefinition.model_validate(raw_def)


class RunRepository:
    def __init__(self, workflow_repo: WorkflowRepository | None = None) -> None:
        self.workflow_repo = workflow_repo or WorkflowRepository()

    def create_run(
        self,
        workflow_id: str,
        version: int,
        run_id: str | None = None,
    ) -> str:
        definition = self.workflow_repo.get_workflow_definition(workflow_id, version)
        version_record = self.workflow_repo.get_version(workflow_id, version)
        workflow_version_id = version_record["id"]

        assigned_run_id = run_id or uuid.uuid4().hex
        root_node_ids = set(find_root_nodes(definition))

        query_run = """
            INSERT INTO runs (id, workflow_id, workflow_version_id, status)
            VALUES (%s, %s, %s, 'QUEUED')
            RETURNING id;
        """
        query_node_run = """
            INSERT INTO node_runs (id, run_id, node_key, status, lease_generation)
            VALUES (%s, %s, %s, %s, 0);
        """
        query_edge_run = """
            INSERT INTO edge_runs (id, run_id, edge_key, from_node_key, to_node_key, status)
            VALUES (%s, %s, %s, %s, %s, 'PENDING');
        """
        query_event = """
            INSERT INTO run_events (id, run_id, event_type, payload)
            VALUES (%s, %s, 'RUN_CREATED', %s);
        """

        with get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(query_run, (assigned_run_id, workflow_id, workflow_version_id))

                    for node_key in definition.nodes:
                        initial_status = "READY" if node_key in root_node_ids else "PENDING"
                        node_run_id = uuid.uuid4().hex
                        cur.execute(
                            query_node_run,
                            (node_run_id, assigned_run_id, node_key, initial_status),
                        )

                    for edge_key, edge in definition.edges.items():
                        edge_run_id = uuid.uuid4().hex
                        cur.execute(
                            query_edge_run,
                            (
                                edge_run_id,
                                assigned_run_id,
                                edge_key,
                                edge.from_node_id,
                                edge.to_node_id,
                            ),
                        )

                    event_id = uuid.uuid4().hex
                    cur.execute(
                        query_event,
                        (
                            event_id,
                            assigned_run_id,
                            json.dumps({"workflow_id": workflow_id, "version": version}),
                        ),
                    )

                conn.commit()
                return assigned_run_id
            except Exception:
                conn.rollback()
                raise

    def get_run(self, run_id: str) -> dict[str, Any]:
        query = """
            SELECT id, workflow_id, workflow_version_id, status, last_claimed_at,
                   created_at, updated_at
            FROM runs
            WHERE id = %s;
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (run_id,))
                row = cur.fetchone()
                if not row:
                    raise RunNotFoundError(f"Run '{run_id}' not found")
                return dict(row)

    def get_node_runs(self, run_id: str) -> dict[str, dict[str, Any]]:
        query = """
            SELECT id, run_id, node_key, status, output, error, run_after,
                   lease_generation, lease_expires_at, created_at, updated_at
            FROM node_runs
            WHERE run_id = %s;
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (run_id,))
                rows = cur.fetchall()
                return {row["node_key"]: dict(row) for row in rows}

    def get_edge_runs(self, run_id: str) -> dict[str, dict[str, Any]]:
        query = """
            SELECT id, run_id, edge_key, from_node_key, to_node_key, status,
                   created_at, updated_at
            FROM edge_runs
            WHERE run_id = %s;
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (run_id,))
                rows = cur.fetchall()
                return {row["edge_key"]: dict(row) for row in rows}

    def get_run_events(self, run_id: str) -> list[dict[str, Any]]:
        query = """
            SELECT id, run_id, event_type, payload, created_at
            FROM run_events
            WHERE run_id = %s
            ORDER BY created_at ASC;
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (run_id,))
                rows = cur.fetchall()
                return [dict(row) for row in rows]
