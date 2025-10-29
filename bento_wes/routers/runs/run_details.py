from fastapi import APIRouter, Depends, Form
from fastapi.responses import PlainTextResponse, FileResponse
from fastapi.exceptions import HTTPException
import shutil
import urllib.parse
from uuid import UUID
from pathlib import Path

from bento_lib.auth.permissions import P_VIEW_RUNS

from bento_wes import states
from bento_wes.db import DatabaseDep
from bento_wes.types import RunStream
from bento_wes.celery import celery
from bento_wes.config import SettingsDep

from .deps import stash_run_or_404, get_stream, RunDep, AuthzDep
from .utils import _denest_list
from .constants import RUN_CANCEL_BAD_REQUEST_STATES

detail_router = APIRouter(prefix="/{run_id}")
detail_router.dependencies.append(Depends(stash_run_or_404))


@detail_router.get("")
async def get_run(run: RunDep, authz_check: AuthzDep):
    await authz_check(P_VIEW_RUNS, run.request.get_authz_resource())
    return run.model_dump(mode="json")


@detail_router.get("/status")
async def run_status(run: RunDep, authz_check: AuthzDep):
    await authz_check(P_VIEW_RUNS, run.request.get_authz_resource())
    return run.model_dump(mode="json")


@detail_router.post("/download-artifact")
async def run_download_artifact(
    run_id: UUID,
    run: RunDep,
    authz_check: AuthzDep,
    path: str = Form(...),
):
    await authz_check(P_VIEW_RUNS, run.request.get_authz_resource())

    artifact_path = path.strip()
    if not artifact_path:
        raise HTTPException(status_code=400, detail="Requested artifact path is blank or unspecified")

    artifacts: set[str] = set()
    for o in run.outputs.values():
        if "File" in o.type:
            artifacts.update(set(_denest_list(o.value)))

    if artifact_path not in artifacts:
        raise HTTPException(status_code=404, detail=f"Requested artifact path not found in run {run_id}")

    p = Path(artifact_path)
    if not p.exists():
        raise HTTPException(status_code=500, detail=f"Artifact path does not exist on filesystem: {artifact_path}")

    safe_name = urllib.parse.quote(p.name, encoding="utf-8")
    return FileResponse(
        path=str(p),
        media_type="application/octet-stream",
        filename=p.name,
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{safe_name}"},
    )


@detail_router.get("/{stream}", response_class=PlainTextResponse)
async def run_stream(stream: RunStream, run_id: UUID, run: RunDep, db: DatabaseDep, authz_check: AuthzDep):
    await authz_check(P_VIEW_RUNS, run.request.get_authz_resource())
    return get_stream(db, stream, run_id)


@detail_router.post("/cancel")
async def cancel_run(run: RunDep, db: DatabaseDep, authz_check: AuthzDep, settings: SettingsDep):
    # TODO: Check if already completed
    # TODO: Check if run log exists
    # TODO: from celery.task.control import revoke; revoke(celery_id, terminate=True)
    await authz_check(run.request.get_workflow_permission(), run.request.get_authz_resource())

    for bad_req_states, bad_req_err in RUN_CANCEL_BAD_REQUEST_STATES:
        if run.state in bad_req_states:
            raise HTTPException(status_code=400, detail=bad_req_err)

    celery_id = run.run_log.celery_id

    if celery_id is None:
        raise HTTPException(status_code=500, detail=f"No Celery ID present for run {run.run_id}")

    db.update_run_state_and_commit(run.run_id, states.STATE_CANCELING)
    celery.control.revoke(celery_id, terminate=True)

    run_dir = settings.service_temp / str(run.run_id)
    if not settings.bento_debug:
        shutil.rmtree(run_dir, ignore_errors=True)

    db.update_run_state_and_commit(run.run_id, states.STATE_CANCELED)

    return PlainTextResponse("Run Cancelled", status_code=204)
