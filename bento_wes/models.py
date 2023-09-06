from datetime import datetime
from pydantic import BaseModel, ConfigDict, AnyUrl, Json
from typing import Literal

__all__ = [
    "BentoWorkflowInput",
    "BentoWorkflowInputWithValue",
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


class BentoWorkflowInputWithFileExtensions(BentoWorkflowInput):
    type: Literal["file", "file[]"]
    extensions: list[str] | None = None


class BentoWorkflowInputWithValue(BentoWorkflowInput):
    value: Literal["FROM_CONFIG"]
    hidden: bool = True


# TODO: Move to bento_lib
class BentoWorkflowMetadata(BaseModel):
    name: str
    description: str
    action: Literal["ingestion", "analysis", "export"]
    file: str
    inputs: list[BentoWorkflowInputWithValue | BentoWorkflowInputWithFileExtensions | BentoWorkflowInput]


class BentoRunRequestTags(BaseModel):
    model_config = ConfigDict(extra="allow")

    workflow_id: str
    workflow_metadata: BentoWorkflowMetadata


class RunRequest(BaseModel):
    workflow_params: Json[dict[str, str | int | float | bool | list[str]]]
    workflow_type: Literal["WDL"]
    workflow_type_version: Literal["1.0"]
    workflow_engine_parameters: Json[dict[str, str]]
    workflow_url: AnyUrl
    tags: Json[BentoRunRequestTags]


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
