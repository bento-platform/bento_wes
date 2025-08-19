from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.exceptions import HTTPException
import uuid
import json
import shutil

from bento_wes import states
from bento_wes.db import DatabaseDep
from bento_wes.types import RunStream
from bento_wes.celery import celery
from bento_wes.config import config

from .deps import stash_run_or_404, get_stream, RunDep, RUN_CANCEL_BAD_REQUEST_STATES

detail_router = APIRouter(prefix="/{run_id}")
detail_router.dependencies.append(Depends(stash_run_or_404))

@detail_router.get("")
def get_run(run: RunDep):
    return JSONResponse(json.loads(run.model_dump_json()))

@detail_router.post("/download-artifact")
def run_download_artifact(run_id: uuid.UUID):
    # TODO
    pass

@detail_router.get(
    "/{stream}",
    response_class=PlainTextResponse
)
def run_stream(
    run_id: uuid.UUID,
    stream: RunStream,
    db: DatabaseDep,
):
    # TODO: add auth
    return get_stream(db, stream, run_id)


@detail_router.post("/cancel")
def cancel_run(run: RunDep, db: DatabaseDep):
    # TODO: Check if already completed
    # TODO: Check if run log exists
    # TODO: from celery.task.control import revoke; revoke(celery_id, terminate=True)
    
    for bad_req_states, bad_req_err in RUN_CANCEL_BAD_REQUEST_STATES:
        if run.state in bad_req_states:
            raise HTTPException(status_code=400, detail=bad_req_err)
    
    celery_id = run.run_log.celery_id

    if celery_id is None:
        raise HTTPException(status_code=500, detail=f"No Celery ID present for run {run.run_id}")

    db.update_run_state_and_commit(db.c, run.run_id, states.STATE_CANCELING)
    celery.control.revoke(celery_id, terminate=True) 

    run_dir = config.service_temp / str(run.run_id)
    if not config.bento_debug:
            shutil.rmtree(run_dir, ignore_errors=True)

    db.update_run_state_and_commit(db.c, run.run_id, states.STATE_CANCELED)

    return PlainTextResponse("Run Cancelled", status_code=204)

@detail_router.get("/status")
def run_status(run_id: uuid.UUID, db: DatabaseDep):
    run = db.get_run(db.c, run_id)
    return JSONResponse(run.model_dump())