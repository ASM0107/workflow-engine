from enum import StrEnum


class NodeState(StrEnum):
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"


class EdgeState(StrEnum):
    PENDING = "PENDING"
    TAKEN = "TAKEN"
    DEAD = "DEAD"


class RunState(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class NodeType(StrEnum):
    MANUAL_TRIGGER = "manual_trigger"
    HTTP = "http"
    DELAY = "delay"
    JSON_TRANSFORM = "json_transform"
    CONDITION = "condition"
