from typing import NewType, Tuple

__all__ = [
    "Command",
    "ProcessResult",
    "WorkflowType",
    "WES_WORKFLOW_TYPE_WDL",
    "WES_WORKFLOW_TYPE_CWL",
]

Command = NewType("Command", Tuple[str, ...])

ProcessResult = NewType("ProcessResult", Tuple[str, str, int, bool])

WorkflowType = NewType("WorkflowType", str)

WES_WORKFLOW_TYPE_WDL = WorkflowType("WDL")
WES_WORKFLOW_TYPE_CWL = WorkflowType("CWL")
