import uuid
from pathlib import Path
from typing import Annotated

import httpx
from bento_lib.auth.permissions import P_VIEW_DROP_BOX
from bento_lib.auth.resources import RESOURCE_EVERYTHING
from bento_lib.workflows.models import WorkflowConfigInput, WorkflowServiceUrlInput
from bento_lib.workflows.utils import namespaced_input
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import AnyUrl, BaseModel

from bento_wes import states
from bento_wes.config import Settings, SettingsDep
from bento_wes.db import DatabaseDep
from bento_wes.logger import LoggerDep
from bento_wes.models import InvalidRunRequest, RunRequest
from bento_wes.runner import run_workflow
from bento_wes.service_registry import ServiceManagerDep
from bento_wes.types import AuthHeaderModel
from bento_wes.workflow_resolver import WorkflowResolutionError, WorkflowResolverDep
from bento_wes.workflows import (
    UnsupportedWorkflowType,
    WorkflowDownloadError,
    WorkflowManagerDep,
    WorkflowType,
)

from .deps import AuthzCompletionDep, AuthzDep, AuthzViewRunsEvaluateDep

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
    workflow_resolver: WorkflowResolverDep,
) -> RunIDResponse:
    try:
        resolved = await workflow_resolver.resolve(run.tags.workflow_id, run.workflow_url)
    except WorkflowResolutionError as e:
        logger.warning(f"Could not resolve workflow {run.tags.workflow_id!r}: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    run.tags.workflow_metadata = resolved.definition
    run.workflow_url = AnyUrl(resolved.wdl_url)

    try:
        permission = run.get_workflow_permission()
        resources = run.get_authz_resources()
    except InvalidRunRequest as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    # Authz: check permission corresponding to the workflow definition before continuing
    for resource in resources:
        await authz_check(permission, resource)

    try:
        run.workflow_params = run.user_workflow_params()
    except InvalidRunRequest as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    # The worker fetches drop-box inputs with the WES service token, which can read the whole drop box, so the
    # caller must be able to view the drop box themselves. view:drop_box can only be granted instance-wide.
    if run.has_drop_box_inputs():
        await authz_check(P_VIEW_DROP_BOX, RESOURCE_EVERYTHING)

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
