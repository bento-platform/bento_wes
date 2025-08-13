from fastapi import APIRouter, Depends, UploadFile, File, Header, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel
from typing import Annotated, List, Optional,Callable
import requests # TODO: change to httpx
import uuid
from pathlib import Path
import json

from bento_lib.workflows.utils import namespaced_input
from bento_lib.workflows.models import WorkflowConfigInput, WorkflowServiceUrlInput

from . import states
from .models import RunRequest
from .authz import authz_middleware
from .logger import logger
from .config import config
from .db import Database, get_db
from .workflows import (
    parse_workflow_host_allow_list, 
    WorkflowManager, 
    WorkflowType, 
    UnsupportedWorkflowType,
    WorkflowDownloadError,
)
from .utils import save_upload_files
from .service_registry import get_bento_services
from .runner import run_workflow
from .types import RunStream

runs_router = APIRouter(prefix="/runs", tags=["runs"])


class AuthHeaderModel(BaseModel):
    Authorization: Optional[str] = None

    def as_dict(self) -> dict:
        return self.model_dump(exclude_none=True)
    
    @classmethod
    def from_header(
        cls,
        Authorization: Annotated[str | None, Header()] = None
    ) -> "AuthHeaderModel":
        return cls(Authorization=Authorization)


#TODO: add auth


@runs_router.post("", dependencies=[authz_middleware.dep_public_endpoint()])
async def create_run(
    run: Annotated[RunRequest, Depends(RunRequest.as_form)],
    authorization: Annotated[AuthHeaderModel, Depends(AuthHeaderModel.from_header)], 
    db: Annotated[Database, Depends(get_db)],
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
        wm.download_or_copy_workflow(
            run.workflow_url, WorkflowType(run.workflow_type), auth_headers=auth_header
        )
    except UnsupportedWorkflowType:
        raise HTTPException(status_code=400, detail=f"Unsupported workflow type: {run.workflow_type}")
    except (WorkflowDownloadError, requests.exceptions.ConnectionError) as e:
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
    
    c = db.cursor()
    c.execute(
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
    db.update_run_state_and_commit(c, run_id, states.STATE_QUEUED, publish_event=False)

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

@runs_router.get("", dependencies=[authz_middleware.dep_public_endpoint()])
async def list_runs(db: Annotated[Database, Depends(get_db)], public: bool = False, with_details: bool = False):
    res_list = []
    perms_list: list[RunRequest] = []

    c = db.cursor()
    for r in c.execute("SELECT * FROM runs").fetchall():
        run = db.run_with_details_from_row(c, r, stream_content=False)
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

@runs_router.get("/{run_id}",  dependencies=[authz_middleware.dep_public_endpoint()])
def get_run(run_id: uuid.UUID, db: Annotated[Database, Depends(get_db)]):
    run_details = db.get_run_with_details(db.cursor(), run_id, stream_content=False)

    if run_details is None:
        raise HTTPException(status_code=404, detail=f"Run {str(run_id)} not found")
    
    return JSONResponse(json.loads(run_details.model_dump_json()))

@runs_router.post("/{run_id}/download-artifact")
def run_download_artifact(run_id: uuid.UUID):
    # TODO
    pass

def get_stream(db: Database, stream: RunStream, run_id: uuid.UUID):
    c = db.cursor()
    run = db.get_run_with_details(c, run_id, stream_content=True)
    if run is None:
            raise HTTPException(f"Stream {stream} not found for run {run_id}")
    
    cache_control = (
        "private, max-age=86400"
        if run.state in states.TERMINATED_STATES
        else "no-cache, no-store, must-revalidate, max-age=0"
    )

    content = run.run_log.stdout if stream == "stdout" else run.run_log.stderr

    return PlainTextResponse(
        content,
        status_code=200,
        headers={
            "Cache-Control": cache_control
        }
    )

@runs_router.get("/{run_id}/stdout", dependencies=[authz_middleware.dep_public_endpoint()])
def run_stdout(run_id: uuid.UUID, db: Annotated[Database, Depends(get_db)]):
    #TODO: add auth
    #TODO: check if run id is valid
    return get_stream(db, "stdout", run_id)

@runs_router.get("/{run_id}/stderr",  dependencies=[authz_middleware.dep_public_endpoint()])
def run_stderr(run_id: uuid.UUID, db: Annotated[Database, Depends(get_db)]):
    #TODO: add auth
    #TODO: check if run id is valid
    return get_stream(db, "stderr", run_id)


@runs_router.post("/{run_id}/cancel")
def run_cancel(run_id: uuid.UUID, db: Annotated[Database, Depends(get_db)]):
    pass

@runs_router.get("/{run_id}/status")
def run_status(run_id: uuid.UUID):
    pass