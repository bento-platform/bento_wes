from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from typing import Annotated, List, Optional
import httpx
import uuid
from pathlib import Path
import json

from bento_lib.workflows.utils import namespaced_input
from bento_lib.workflows.models import WorkflowConfigInput, WorkflowServiceUrlInput

from bento_wes import states
from bento_wes.models import RunRequest
from bento_wes.authz import authz_middleware
from bento_wes.logger import logger
from bento_wes.config import config
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

runs_router = APIRouter(prefix="/runs", tags=["runs"])
runs_router.dependencies.append(authz_middleware.dep_public_endpoint())

#TODO: add auth


@runs_router.post("")
async def create_run(
    run: Annotated[RunRequest, Depends(RunRequest.as_form)],
    authorization: Annotated[AuthHeaderModel, Depends(AuthHeaderModel.from_header)], 
    db: DatabaseDep,
    workflow_attachment: Optional[List[UploadFile]] = File(None),
):
    logger.info(f"Starting run creation for workflow {run.tags.workflow_id}")

    # Parse workflow host allow list from config
    workflow_host_allow_list = parse_workflow_host_allow_list(config.workflow_host_allow_list)

    wm = WorkflowManager(
        config.service_temp,
        service_base_url=config.service_base_url,
        logger=logger,
        workflow_host_allow_list=workflow_host_allow_list,
        validate_ssl=config.bento_validate_ssl,
        debug=config.bento_debug
    )

    auth_header = authorization.as_dict()
    logger.info(f"Authorization header dict: {auth_header}")

    try:
        await wm.download_or_copy_workflow(
            run.workflow_url, WorkflowType(run.workflow_type), auth_headers=auth_header
        )
    except UnsupportedWorkflowType:
        raise HTTPException(status_code=400, detail=f"Unsupported workflow type: {run.workflow_type}")
    except (WorkflowDownloadError, httpx.RequestError) as e:
        raise HTTPException(status_code=400, detail=f"Could not access workflow file: {run.workflow_url} (Python error: {e})")

    run_id = uuid.uuid4()

    run_dir: Path = config.service_temp / str(run_id)
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
        "validate_ssl": config.bento_validate_ssl,
        "run_dir": str(run_dir),
        "vep_cache_dir": config.vep_cache_dir,
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
    
    db.c.execute(
        """
        INSERT INTO runs (
            id,
            state,
            outputs,

            request__workflow_params,
            request__workflow_type,
            request__workflow_type_version,
            request__workflow_engine_parameters,
            request__workflow_url,
            request__tags,

            run_log__name
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            str(run_id),
            states.STATE_UNKNOWN,
            json.dumps({}),
            json.dumps(run_params),
            run.workflow_type,
            run.workflow_type_version,
            json.dumps(run.workflow_engine_parameters),
            str(run.workflow_url),
            run.tags.model_dump_json(),
            run.tags.workflow_id,
        ),
    )
    db.commit()
    db.update_run_state_and_commit(db.c, run_id, states.STATE_QUEUED, publish_event=False)

    run_workflow.delay(run_id)

    return JSONResponse(
        content={"run_id": str(run_id)}
    )

PUBLIC_RUN_DETAILS_SHAPE = {
    "request": {
        "workflow_type": True,
        "tags": {
            "workflow_id": True,
            "workflow_metadata": {
                "data_type": True,
            },
            "project_id": True,
            "dataset_id": True,
        },
    },
    "run_log": {
        "start_time": True,
        "end_time": True,
    },
}


PRIVATE_RUN_DETAILS_SHAPE = {
    "request": True,
    "run_log": True,
    "task_logs": True,
    "outputs": True,
}

@runs_router.get("")
async def list_runs(db: DatabaseDep, public: bool = False, with_details: bool = False):
    res_list = []
    perms_list: list[RunRequest] = []

    for r in db.c.execute("SELECT * FROM runs").fetchall():
        run = db.run_with_details_from_row(db.c, r, stream_content=False)
        perms_list.append(run.request)

        if not public or run.state == states.STATE_COMPLETE:
            res_list.append(
                {
                    **run.model_dump(mode="json", include={"run_id", "state"}),
                    **(
                        {
                            "details": run.model_dump(
                                mode="json",
                                include={
                                    "run_id": True,
                                    "state": True,
                                    **(PUBLIC_RUN_DETAILS_SHAPE if public else PRIVATE_RUN_DETAILS_SHAPE),
                                },
                            ),
                        }
                        if with_details
                        else {}
                    ),
                }
            )
    
    return JSONResponse(res_list)
