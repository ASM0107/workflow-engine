import threading

from workflow_engine.domain.errors import LeaseLostError


class CancellationToken:
    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def is_cancelled(self) -> bool:
        return self._event.is_set()

    def check_cancelled(self) -> None:
        if self._event.is_set():
            raise LeaseLostError("Execution cancelled due to lost heartbeat lease")
