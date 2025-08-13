from bento_lib.workflows.models import WorkflowDefinition
from datetime import datetime
from pydantic import BaseModel, ConfigDict, AnyUrl, Json
from typing import Literal, Annotated
from fastapi import Form

__all__ = [
    "BentoRunRequestTags",
    "RunRequest",
    "RunLog",
    "Run",
    "RunWithDetails",
    "RunOutput",
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

    @classmethod
    def as_form(
        cls,
        workflow_params: Annotated[str, Form(...)],
        workflow_type: Annotated[Literal["WDL"], Form(...)],
        workflow_type_version: Annotated[Literal["1.0"], Form(...)],
        workflow_engine_parameters: Annotated[str, Form(...)],
        workflow_url: Annotated[str, Form(...)],
        tags: Annotated[str, Form(...)],
    ) -> "RunRequest":
        return cls(
            workflow_params=workflow_params,
            workflow_type=workflow_type,
            workflow_type_version=workflow_type_version,
            workflow_engine_parameters=workflow_engine_parameters,
            workflow_url=workflow_url,
            tags=tags,
        )

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


class RunOutput(BaseModel):  # Bento-specific schema
    type: str  # WDL / (workflow descriptor language) type
    value: str | int | float | bool | list | None  # Output value


class RunWithDetails(Run):
    request: RunRequest
    run_log: RunLog
    task_logs: list[dict]  # TODO: model
    outputs: dict[str, RunOutput]
