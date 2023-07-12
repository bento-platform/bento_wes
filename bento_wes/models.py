from datetime import datetime
from pydantic import BaseModel, ConfigDict, AnyUrl
from typing import Literal

__all__ = [
    "BentoWorkflowInput",
    "BentoWorkflowOutput",
    "BentoWorkflowMetadata",
    "BentoRunRequestTags",
    "RunRequest",
    "RunLog",
    "Run",
    "RunWithDetails",
    "RunWithDetailsAndOutput",
]


class BentoWorkflowInput(BaseModel):
    id: str
    type: Literal["string", "string[]", "number", "number[]", "enum", "enum[]", "file", "file[]"]
    required: bool = False,
    extensions: list[str] | None = None
    value: str | None = None


class BentoWorkflowOutput(BaseModel):
    id: str
    type: Literal["string", "string[]", "number", "number[]", "enum", "enum[]", "file", "file[]"]
    value: str


# TODO: Move to bento_lib
class BentoWorkflowMetadata(BaseModel):
    name: str
    description: str
    action: Literal["BentoWorkflowMetadata", "analysis", "export"]
    data_type: str | None = None
    file: str
    inputs: list[BentoWorkflowInput]
    outputs: list[BentoWorkflowOutput]


class BentoRunRequestTags(BaseModel):
    model_config = ConfigDict(extra="allow")

    workflow_id: str
    workflow_metadata: BentoWorkflowMetadata

    project_id: str
    dataset_id: str | None = None


class RunRequest(BaseModel):
    workflow_params: dict[str, str | int | float | bool]
    workflow_type: Literal["WDL"]
    workflow_type_version: Literal["1.0"]
    workflow_engine_parameters: dict[str, str]
    workflow_url: AnyUrl
    tags: BentoRunRequestTags


class RunLog(BaseModel):
    name: str
    cmd: str
    start_time: datetime | None
    end_time: datetime | None
    stdout: str
    stderr: str
    exit_code: int | None
    celery_id: int | None = None  # Bento-specific extension


class Run(BaseModel):
    run_id: str
    state: str  # TODO: Literal


class RunWithDetails(Run):
    request: RunRequest
    run_log: RunLog
    task_logs: list[dict]  # TODO: model


class RunWithDetailsAndOutput(RunWithDetails):
    outputs: dict[str, str | int | float | bool]  # Bento-specific extension
