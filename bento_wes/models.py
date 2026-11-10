from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from bento_lib.auth.permissions import P_ANALYZE_DATA, P_EXPORT_DATA, P_INGEST_DATA, Permission
from bento_lib.auth.resources import RESOURCE_EVERYTHING, build_resource
from bento_lib.workflows.models import (
    WorkflowDefinition,
    WorkflowDirectoryInput,
    WorkflowFileArrayInput,
    WorkflowFileInput,
    WorkflowInjectedInput,
    WorkflowProjectDatasetInput,
)
from bento_lib.workflows.utils import namespaced_input
from fastapi import Form
from pydantic import AnyUrl, BaseModel, ConfigDict, Json

from .constants import PRIVATE_RUN_DETAILS_SHAPE, PUBLIC_RUN_DETAILS_SHAPE

__all__ = [
    "InvalidRunRequest",
    "BentoRunRequestTags",
    "RunRequest",
    "RunLog",
    "Run",
    "RunWithDetails",
    "RunOutput",
]


class InvalidRunRequest(Exception):
    pass


def _parse_project_dataset(input_key: str, value: object) -> tuple[str, str]:
    if isinstance(value, str):
        parts = value.split(":")
        if len(parts) == 2 and all(parts):
            return parts[0], parts[1]
    raise InvalidRunRequest(f"Invalid value for {input_key}: expected 'project:dataset'")


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
    ) -> RunRequest:
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

    def get_authz_resources(self) -> list[dict]:
        """
        Strict version of get_authz_resource() for use at run creation, once tags.workflow_metadata has been replaced
        with the server-resolved workflow definition. Returns one resource per project:dataset input, so that every
        dataset the workflow touches gets checked.
        :raises InvalidRunRequest: if a project:dataset value is malformed, or a required one is missing.
        """
        wi, wm = self.tags.workflow_id, self.tags.workflow_metadata
        resources: list[dict] = []

        for inp in wm.inputs:
            if not isinstance(inp, WorkflowProjectDatasetInput):
                continue
            key = namespaced_input(wi, inp.id)
            val = self.workflow_params.get(key)
            if val is None or val == "":
                if inp.required:
                    raise InvalidRunRequest(f"Missing required input: {key}")
                continue
            project, dataset = _parse_project_dataset(key, val)
            resources.append(build_resource(project, dataset, data_type=wm.data_type))

        return resources or [RESOURCE_EVERYTHING]

    def user_workflow_params(self) -> dict[str, str | int | float | bool | list[str]]:
        """
        Filter workflow_params against the workflow definition: only declared, user-settable inputs are accepted.
        Values for server-injected inputs (config, service URLs, secrets) are dropped, since the server always sets
        those itself.
        :raises InvalidRunRequest: if a parameter does not correspond to any input in the workflow definition.
        """
        wi, wm = self.tags.workflow_id, self.tags.workflow_metadata
        user_keys = {namespaced_input(wi, i.id) for i in wm.inputs if not isinstance(i, WorkflowInjectedInput)}
        injected_keys = {namespaced_input(wi, i.id) for i in wm.inputs if isinstance(i, WorkflowInjectedInput)}

        if unexpected := sorted(set(self.workflow_params) - user_keys - injected_keys):
            raise InvalidRunRequest(f"Unexpected workflow parameters: {unexpected}")

        return {k: v for k, v in self.workflow_params.items() if k in user_keys}

    def has_drop_box_inputs(self) -> bool:
        """
        Whether any file, file array or directory input (i.e., a path into the drop box) has a non-blank value.
        Blank values (empty strings/lists) are skipped, since the backend never downloads anything for them.
        """
        wi, wm = self.tags.workflow_id, self.tags.workflow_metadata
        for inp in wm.inputs:
            if not isinstance(inp, (WorkflowFileInput, WorkflowFileArrayInput, WorkflowDirectoryInput)):
                continue
            val = self.workflow_params.get(namespaced_input(wi, inp.id))
            if isinstance(val, list):
                if any(v != "" for v in val):
                    return True
            elif val is not None and val != "":
                return True
        return False

    def get_workflow_permission(self) -> Permission:
        match self.tags.workflow_metadata.type:
            case "ingestion":
                return P_INGEST_DATA
            case "analysis":
                return P_ANALYZE_DATA
            case "export":
                return P_EXPORT_DATA
            case other:  # fail closed if bento_lib ever adds a workflow type we don't know how to authorize
                raise InvalidRunRequest(f"Unsupported workflow type: {other}")


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

    def list_format(self, with_details: bool = False, is_public: bool = False) -> dict:
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
