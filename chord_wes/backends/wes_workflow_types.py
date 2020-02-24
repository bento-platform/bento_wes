from typing import NewType

__all__ = [
    "WorkflowType",
    "WES_WORKFLOW_TYPE_WDL",
    "WES_WORKFLOW_TYPE_CWL",
]

WorkflowType = NewType("WorkflowType", str)

WES_WORKFLOW_TYPE_WDL = WorkflowType("WDL")
WES_WORKFLOW_TYPE_CWL = WorkflowType("CWL")
