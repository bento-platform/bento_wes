from bento_lib.workflows.models import WorkflowDefinition
from datetime import datetime
from pydantic import BaseModel, ConfigDict, AnyUrl, Json
from typing import Literal

__all__ = [
    "BentoRunRequestTags",
    "RunRequest",
    "RunLog",
    "Run",
    "RunWithDetails",
    "RunOutput",
    "RunWithDetailsAndOutput",
]


class BentoRunRequestTags(BaseModel):
    model_config = ConfigDict(extra="allow")

    workflow_id: str
    workflow_metadata: WorkflowDefinition


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


class RunOutput(BaseModel):
    type: str  # WDL / (workflow descriptor language) type
    value: str | int | float | bool | list  # Output value


class RunWithDetailsAndOutput(RunWithDetails):
    outputs: dict[str, RunOutput]  # Bento-specific extension
