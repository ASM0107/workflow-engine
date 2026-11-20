class WorkflowEngineError(Exception):
    pass


class EmptyWorkflowError(WorkflowEngineError):
    pass


class CycleDetectedError(WorkflowEngineError):
    pass


class DanglingEdgeError(WorkflowEngineError):
    pass


class DuplicateNodeError(WorkflowEngineError):
    pass


class DuplicateEdgeError(WorkflowEngineError):
    pass


class UnknownNodeError(WorkflowEngineError):
    pass


class UnknownEdgeError(WorkflowEngineError):
    pass


class InvalidNodeStateTransitionError(WorkflowEngineError):
    def __init__(self, node_id: str, current_state: str, attempted_state: str) -> None:
        self.node_id = node_id
        self.current_state = current_state
        self.attempted_state = attempted_state
        super().__init__(
            f"Illegal node state transition for '{node_id}': {current_state} -> {attempted_state}"
        )


class InvalidEdgeStateTransitionError(WorkflowEngineError):
    def __init__(self, edge_id: str, current_state: str, attempted_state: str) -> None:
        self.edge_id = edge_id
        self.current_state = current_state
        self.attempted_state = attempted_state
        super().__init__(
            f"Illegal edge state transition for '{edge_id}': {current_state} -> {attempted_state}"
        )


class InvalidRunStateTransitionError(WorkflowEngineError):
    def __init__(self, run_id: str, current_state: str, attempted_state: str) -> None:
        self.run_id = run_id
        self.current_state = current_state
        self.attempted_state = attempted_state
        super().__init__(
            f"Illegal run state transition for '{run_id}': {current_state} -> {attempted_state}"
        )
