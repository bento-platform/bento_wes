from typing import NewType, Tuple

__all__ = [
    "Command",
    "ProcessResult",
]

Command = NewType("Command", Tuple[str, ...])

ProcessResult = NewType("ProcessResult", Tuple[str, str, int, bool])
