from bento_wes.states import FailureState

__all__ = [
    "RunExceptionWithFailState",
]


class RunExceptionWithFailState(Exception):
    def __init__(self, state: FailureState, message: str):
        self._state: FailureState = state
        super().__init__(message)

    @property
    def state(self) -> FailureState:
        return self._state
