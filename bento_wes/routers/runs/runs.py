from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from typing import Annotated, List, Optional
import httpx
import uuid
from pathlib import Path

from bento_lib.workflows.utils import namespaced_input
from bento_lib.workflows.models import WorkflowConfigInput, WorkflowServiceUrlInput
from bento_lib.auth.permissions import P_VIEW_RUNS
from bento_lib.auth.exceptions import BentoAuthException

from bento_wes import states
from bento_wes.models import RunRequest
from bento_wes.logger import logger
from bento_wes.config import SettingsDep
from bento_wes.db import DatabaseDep
from bento_wes.workflows import (
    parse_workflow_host_allow_list, 
    WorkflowManager, 
    WorkflowType, 
    UnsupportedWorkflowType,
    WorkflowDownloadError,
)
from bento_wes.utils import save_upload_files
from bento_wes.service_registry import get_bento_services
from bento_wes.runner import run_workflow
from bento_wes.types import AuthHeaderModel

from .deps import AuthzDep, AuthzCompletionDep

runs_router = APIRouter(prefix="/runs", tags=["runs"])

@runs_router.post("")
async def create_run(
    run: Annotated[RunRequest, Depends(RunRequest.as_form)],
    authorization: Annotated[AuthHeaderModel, Depends(AuthHeaderModel.from_header)], 
    db: DatabaseDep,
    authz_check: AuthzDep,
    settings: SettingsDep,
    workflow_attachment: Optional[List[UploadFile]] = File(None),
):
    # authz
    await authz_check(run.get_workflow_permission(), run.get_authz_resource())

    logger.info(f"Starting run creation for workflow {run.tags.workflow_id}")

    # Parse workflow host allow list from config
    workflow_host_allow_list = parse_workflow_host_allow_list(settings.workflow_host_allow_list)

    wm = WorkflowManager(
        settings.service_temp,
        service_base_url=settings.service_base_url,
        logger=logger,
        workflow_host_allow_list=workflow_host_allow_list,
        validate_ssl=settings.bento_validate_ssl,
        debug=settings.bento_debug
    )

    auth_header = authorization.as_dict()

    try:
        await wm.download_or_copy_workflow(
            run.workflow_url, WorkflowType(run.workflow_type), auth_headers=auth_header
        )
    except UnsupportedWorkflowType:
        raise HTTPException(status_code=400, detail=f"Unsupported workflow type: {run.workflow_type}")
    except (WorkflowDownloadError, httpx.RequestError) as e:
        raise HTTPException(status_code=400, detail=f"Could not access workflow file: {run.workflow_url} (Python error: {e})")

    run_id = uuid.uuid4()

    run_dir: Path = settings.service_temp / str(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    if workflow_attachment:
        for file in workflow_attachment:
            contents = await file.read()
            print(f"Received file: {file.filename} with size {len(contents)} bytes")
        response = await save_upload_files(workflow_attachment, run_dir)
        logger.info(response)
    else: 
        logger.info("No workflow attachments provided")
    
    run_injectable_config = {
        "validate_ssl": settings.bento_validate_ssl,
        "run_dir": str(run_dir),
        "vep_cache_dir": settings.vep_cache_dir,
    }
    run_params = {**run.workflow_params}
    bento_services_data = None
    for run_input in run.tags.workflow_metadata.inputs:
        input_key = namespaced_input(run.tags.workflow_id, run_input.id)
        if isinstance(run_input, WorkflowConfigInput):
            config_value =run_injectable_config.get(run_input.key)
            if config_value is None:
                err = f"Could not find injectable configuration value for key {run_input.key}"
                logger.error(err)
                raise HTTPException(status_code=400, detail=err)
            logger.debug(f"Injecting configuration parameter '{run_input.key}' into run {run_id}: {run_input.id}={config_value}")
            run_params[input_key] = config_value
        elif isinstance(run_input, WorkflowServiceUrlInput):
            bento_services_data = bento_services_data or get_bento_services()
            config_value: str | None = bento_services_data.get(run_input.service_kind).get("url")
            sk = run_input.service_kind
            if config_value is None:
                err = f"Could not find URL/service record for service kind '{sk}'"
                logger.error(err)
                raise HTTPException(status_code=400, detail=err)
            logger.debug(f"Injecting URL for service kind '{sk}' into run {run_id}: {run_input.id}={config_value}")
            run_params[input_key] = config_value
    
    db.insert_run(run_id, run, run_params)
    db.update_run_state_and_commit(run_id, states.STATE_QUEUED, publish_event=False)

    run_workflow.delay(run_id)

    return JSONResponse(
        content={"run_id": str(run_id)}
    )


@runs_router.get("")
async def list_runs(db: DatabaseDep, mark_authz_done: AuthzCompletionDep, authz_check: AuthzDep, public: bool = False, with_details: bool = False):
    res_list = []

    if public:
        await mark_authz_done()
        for run in db.fetch_runs_by_state(states.STATE_COMPLETE):
            res_list.append(run.list_format(public, with_details))
    else:
        for run in db.fetch_all_runs():
            try:
                await authz_check(P_VIEW_RUNS, run.request.get_authz_resource())
                res_list.append(run.list_format(public, with_details))
            except BentoAuthException:
                pass
        
        await mark_authz_done()

    return JSONResponse(res_list)
