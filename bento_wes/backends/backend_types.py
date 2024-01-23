from typing import NewType

__all__ = [
    "Command",
    "ProcessResult",
]

Command = NewType("Command", tuple[str, ...])

ProcessResult = NewType("ProcessResult", tuple[str, str, int, bool])
