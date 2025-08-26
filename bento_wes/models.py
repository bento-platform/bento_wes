from bento_lib.workflows.models import WorkflowDefinition, WorkflowProjectDatasetInput
from bento_lib.workflows.utils import namespaced_input
from bento_lib.auth.resources import RESOURCE_EVERYTHING, build_resource
from bento_lib.auth.permissions import P_ANALYZE_DATA, P_INGEST_DATA, P_EXPORT_DATA
from bento_lib.workflows.models import WorkflowType

from datetime import datetime
from pydantic import BaseModel, ConfigDict, AnyUrl, Json
from typing import Literal, Annotated
from fastapi import Form

from .constants import PUBLIC_RUN_DETAILS_SHAPE, PRIVATE_RUN_DETAILS_SHAPE

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
    
    def get_authz_resource(self):
        wi, wm = self.tags.workflow_id, self.tags.workflow_metadata
        resource = RESOURCE_EVERYTHING

        inp = next((i for i in wm.inputs if isinstance(i, WorkflowProjectDatasetInput)), None)
        if inp and (val := self.workflow_params.get(namespaced_input(wi, inp.id))):
            project, dataset = val.split(":")
            resource = build_resource(project, dataset, data_type=wm.data_type)

        return resource

    def get_workflow_permission(self) -> str:
        match self.tags.workflow_metadata.type:
            case "ingestion":
                return P_INGEST_DATA
            case "analysis":
                return P_ANALYZE_DATA
            case "export": 
                return P_EXPORT_DATA


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

    def list_format(self, is_public: bool, with_details: bool) -> dict:
        return {
            **self.model_dump(mode="json", include={"run_id", "state"}),
            **(
                {
                    "details": self.model_dump(
                        mode="json",
                        include={
                            "run_id": True,
                            "state": True,
                            **(PUBLIC_RUN_DETAILS_SHAPE if is_public else PRIVATE_RUN_DETAILS_SHAPE),
                        },
                    ),
                }
                if with_details
                else {}
            ),
        }
