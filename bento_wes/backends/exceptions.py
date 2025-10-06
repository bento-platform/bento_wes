from bento_wes.states import FailureState

__all__ = [
    "RunExceptionWithFailState",
]


class RunExceptionWithFailState(Exception):
    def __init__(self, state: FailureState, message: str):
        self._state: FailureState = state
        super().__init__(message)
        self.args = (state, message)  # required for pickling (for Celery)

    @property
    def state(self) -> FailureState:
        return self._state
