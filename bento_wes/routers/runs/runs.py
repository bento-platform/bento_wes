import httpx
import uuid

from bento_lib.workflows.utils import namespaced_input
from bento_lib.workflows.models import WorkflowConfigInput, WorkflowServiceUrlInput
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, status
from fastapi.responses import JSONResponse
from pathlib import Path
from pydantic import BaseModel
from typing import Annotated

from bento_wes import states
from bento_wes.config import Settings, SettingsDep
from bento_wes.db import DatabaseDep
from bento_wes.logger import LoggerDep
from bento_wes.models import RunRequest
from bento_wes.runner import run_workflow
from bento_wes.service_registry import ServiceManagerDep
from bento_wes.workflows import (
    WorkflowType,
    UnsupportedWorkflowType,
    WorkflowDownloadError,
    WorkflowManagerDep,
)
from bento_wes.types import AuthHeaderModel
from bento_wes.utils import save_upload_files

from .deps import AuthzDep, AuthzCompletionDep, AuthzViewRunsEvaluateDep

runs_router = APIRouter(prefix="/runs", tags=["runs"])


def _config_for_run(settings: Settings, run_dir: Path):
    return {
        # In production, workflows should validate SSL (i.e., omit the curl -k flag).
        # In development, SSL certificates are usually self-signed, so they will not validate.
        "validate_ssl": settings.bento_validate_ssl,
        "run_dir": str(run_dir),
        # Variant effect predictor cache (large directory):
        "vep_cache_dir": settings.vep_cache_dir,
    }


class RunIDResponse(BaseModel):
    run_id: uuid.UUID


@runs_router.post("")
async def create_run(
    run: Annotated[RunRequest, Depends(RunRequest.as_form)],
    authorization: Annotated[AuthHeaderModel, Depends(AuthHeaderModel.from_header)],
    db: DatabaseDep,
    authz_check: AuthzDep,
    settings: SettingsDep,
    logger: LoggerDep,
    service_manager: ServiceManagerDep,
    workflow_manager: WorkflowManagerDep,
    workflow_attachment: list[UploadFile] | None = File(None),
) -> RunIDResponse:
    # Authz: check permission corresponding to the workflow definition before continuing
    await authz_check(run.get_workflow_permission(), run.get_authz_resource())

    logger.info(f"Starting run creation for workflow {run.tags.workflow_id}")

    auth_header = authorization.as_dict()

    try:
        await workflow_manager.download_or_copy_workflow(
            run.workflow_url, WorkflowType(run.workflow_type), auth_headers=auth_header
        )
    except UnsupportedWorkflowType:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported workflow type: {run.workflow_type}"
        )
    except (WorkflowDownloadError, httpx.RequestError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Could not access workflow file: {run.workflow_url} (Python error: {e})",
        )

    run_id = uuid.uuid4()

    run_dir: Path = settings.service_temp / str(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    if workflow_attachment:
        for file in workflow_attachment:
            # TODO: Check and fix input if filename is non-secure
            # TODO: Do we put these in a subdirectory?
            # TODO: Support WDL uploads for workflows
            logger.info("Received file: %s with size %d bytes", file.filename, file.size or -1)
        response = await save_upload_files(workflow_attachment, run_dir)
        logger.info(response)
    else:
        logger.info("No workflow attachments provided")

    # Process parameters & inject non-secret values
    #  - Get injectable run config for processing inputs
    run_injectable_config = _config_for_run(settings, run_dir)
    #  - Set up parameters
    run_params = {**run.workflow_params}
    for run_input in run.tags.workflow_metadata.inputs:
        input_key = namespaced_input(run.tags.workflow_id, run_input.id)
        if isinstance(run_input, WorkflowConfigInput):
            config_value = run_injectable_config.get(run_input.key)
            if config_value is None:
                err = f"Could not find injectable configuration value for key {run_input.key}"
                logger.error(err)
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=err)
            logger.debug(
                f"Injecting configuration parameter '{run_input.key}' into run {run_id}: {run_input.id}={config_value}"
            )
            run_params[input_key] = config_value
        elif isinstance(run_input, WorkflowServiceUrlInput):
            config_value: str | None = await service_manager.get_bento_service_url_by_kind(run_input.service_kind)
            sk = run_input.service_kind
            if config_value is None:
                err = f"Could not find URL/service record for service kind '{sk}'"
                logger.error(err)
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=err)
            logger.debug(f"Injecting URL for service kind '{sk}' into run {run_id}: {run_input.id}={config_value}")
            run_params[input_key] = config_value

    db.insert_run(run_id, run, run_params)

    # TODO: figure out timeout
    # TODO: retry policy

    db.update_run_state_and_commit(run_id, states.STATE_QUEUED, publish_event=False)

    run_workflow.delay(run_id)

    return RunIDResponse(run_id=run_id)


@runs_router.get("")
async def list_runs(
    db: DatabaseDep,
    mark_authz_done: AuthzCompletionDep,
    authz_evaluate: AuthzViewRunsEvaluateDep,
    public: bool = False,
    with_details: bool = False,
):
    res_list = []

    if public:
        # Only COMPLETE runs can be viewed in public mode
        for run in db.fetch_runs_by_state(states.STATE_COMPLETE):
            res_list.append(run.list_format(with_details, public))
    else:
        runs = list(db.fetch_all_runs())
        resources = [run.request.get_authz_resource() for run in runs]

        # Filter runs to just those which we have permission to view
        allowed_iter = authz_evaluate(resources)
        for run, allowed in zip(runs, allowed_iter):
            if allowed:
                res_list.append(run.list_format(with_details, public))

    await mark_authz_done()

    return JSONResponse(res_list)
