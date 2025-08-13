"""
Goal: Incrementally migrate the Flask `runs` blueprint to FastAPI, endpoint-by-endpoint.
This file is designed to be dropped alongside existing code during a phased migration.

Status in this draft:
- ✅ GET /runs
- ✅ POST /runs  (multipart form with optional file uploads)
- ✅ GET /runs/{run_id}
- ✅ POST /runs/{run_id}/download-artifact
- ✅ GET /runs/{run_id}/stdout
- ✅ GET /runs/{run_id}/stderr
- ✅ POST /runs/{run_id}/cancel
- ✅ GET /runs/{run_id}/status

Notes:
- This preserves existing sync sqlite calls. If you introduce true async DB later, convert `def` → `async def` and use `await` where applicable.
- AuthZ hooks still rely on `authz_middleware` from your codebase. Plumb any FastAPI-specific adapters as needed.
- Celery integration is kept as-is; consider a FastAPI lifespan event for init/cleanup.
- Responses are FastAPI/Starlette primitives (JSONResponse/StreamingResponse/Response).
"""
from __future__ import annotations

import itertools
import json
import os
import shutil
import sqlite3
import traceback
import urllib.parse
import uuid
from pathlib import Path
from typing import Any, Callable, Iterator, Iterable

import pydantic
import requests
from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    Response,
    UploadFile,
    status,
)
from fastapi.responses import JSONResponse, StreamingResponse, PlainTextResponse

# --- External project imports (unchanged) ---
from bento_lib.auth.permissions import P_INGEST_DATA, P_VIEW_RUNS
from bento_lib.auth.resources import RESOURCE_EVERYTHING, build_resource
from bento_lib.workflows.models import (
    WorkflowProjectDatasetInput,
    WorkflowConfigInput,
    WorkflowServiceUrlInput,
)
from bento_lib.workflows.utils import namespaced_input
from bento_lib.utils.headers import authz_bearer_header

from . import states
from .authz import authz_middleware
from .celery import celery
from .config import config
from .db import Database, get_db
from .events import get_flask_event_bus  # TODO: replace with FastAPI-native event bus if available
from .logger import logger
from .models import RunRequest
from .runner import run_workflow
from .service_registry import get_bento_services
from .states import STATE_COMPLETE
from .types import RunStream
from .workflows import (
    WorkflowType,
    UnsupportedWorkflowType,
    WorkflowDownloadError,
    WorkflowManager,
    parse_workflow_host_allow_list,
)

MIME_OCTET_STREAM = "application/octet-stream"
CHUNK_SIZE = 1024 * 16  # 16KB

router = APIRouter(prefix="/runs", tags=["runs"])

# -------------------------
# Helpers & AuthZ adapters
# -------------------------

def _get_resource_for_run_request(run_req: RunRequest) -> dict:
    wi = run_req.tags.workflow_id
    wm = run_req.tags.workflow_metadata

    resource = RESOURCE_EVERYTHING

    project_dataset_inputs = [i for i in wm.inputs if isinstance(i, WorkflowProjectDatasetInput)]
    if len(project_dataset_inputs) == 1:
        inp = project_dataset_inputs[0]
        inp_key = namespaced_input(wi, inp.id)
        inp_val = run_req.workflow_params.get(inp_key)
        if inp_val:
            project, dataset = inp_val.split(":", 1)
            resource = build_resource(project, dataset, data_type=wm.data_type)

    return resource


def authz_enabled() -> bool:
    return config.authz_enabled


def _check_runs_permission(
    request: Request, run_requests: list[RunRequest], permission: str
) -> Iterable[bool]:
    """Yield booleans for each run request.
    If authz is disabled, allow all.
    """
    if not authz_enabled():
        return [True] * len(run_requests)

    # /policy/evaluate returns a matrix of booleans of row: resource, col: permission
    matrix = authz_middleware.evaluate(
        request,
        [_get_resource_for_run_request(run_request) for run_request in run_requests],
        [permission],
    )
    # Flatten first (and only) column
    return (row[0] for row in matrix)


def _post_headers_getter_from_form(token: str | None) -> dict[str, str]:
    return authz_bearer_header(token) if token else {}


def _check_single_run_permission_and_mark(
    request: Request, run_req: RunRequest, permission: str, *, token_from_form: str | None = None
) -> bool:
    if not authz_enabled():
        return True
    return authz_middleware.evaluate_one(
        request,
        _get_resource_for_run_request(run_req),
        permission,
        headers_getter=(lambda: _post_headers_getter_from_form(token_from_form)) if token_from_form else None,
        mark_authz_done=True,
    )


def _config_for_run(run_dir: Path) -> dict[str, str | bool | None]:
    return {
        "validate_ssl": config.bento_validate_ssl,
        "run_dir": str(run_dir),
        "vep_cache_dir": config.vep_cache_dir,
    }


# -------------------------
# POST /runs helper (internal)
# -------------------------
async def _create_run(
    request: Request,
    db: Database,
    c: sqlite3.Cursor,
    workflow_attachment: list[UploadFile] | None,
) -> JSONResponse:
    # Parse multipart form
    form = await request.form()
    form_dict = {k: v for k, v in form.multi_items() if not hasattr(v, "filename")}  # exclude files

    try:
        run_req = RunRequest.model_validate(form_dict)
    except pydantic.ValidationError:
        logger.error(
            "Encountered validation error during run creation: %s", traceback.format_exc()
        )
        authz_middleware.mark_authz_done(request)
        return JSONResponse({"detail": "Validation error: bad run request format"}, status_code=400)

    # Check ingest permissions before continuing
    token_from_form = form_dict.get("token")
    if not _check_single_run_permission_and_mark(
        request, run_req, P_INGEST_DATA, token_from_form=token_from_form
    ):
        return JSONResponse({"detail": "Forbidden"}, status_code=status.HTTP_403_FORBIDDEN)

    logger.info("Starting run creation for workflow %s", run_req.tags.workflow_id)

    # Allowed workflow hosts
    workflow_host_allow_list = parse_workflow_host_allow_list(config.workflow_host_allow_list)

    # Workflow manager setup
    wm = WorkflowManager(
        config.service_temp,
        service_base_url=config.service_base_url,
        bento_url=config.bento_url,
        logger=logger,
        workflow_host_allow_list=workflow_host_allow_list,
        validate_ssl=config.bento_validate_ssl,
        debug=config.bento_debug,
    )

    # Optional Authorization header to forward
    auth_header = request.headers.get("Authorization")
    auth_header_dict = {"Authorization": auth_header} if auth_header else {}

    # Download/copy workflow
    try:
        wm.download_or_copy_workflow(
            run_req.workflow_url, WorkflowType(run_req.workflow_type), auth_headers=auth_header_dict
        )
    except UnsupportedWorkflowType:
        return JSONResponse({"detail": f"Unsupported workflow type: {run_req.workflow_type}"}, status_code=400)
    except (WorkflowDownloadError, requests.exceptions.ConnectionError) as e:
        return JSONResponse(
            {"detail": f"Could not access workflow file: {run_req.workflow_url} (Python error: {e})"},
            status_code=400,
        )

    # Begin creating the job after validating the request
    run_id = uuid.uuid4()

    # Create run directory
    run_dir: Path = config.service_temp / str(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    # Move workflow attachments to run directory
    if workflow_attachment:
        for up in workflow_attachment:
            if not up.filename:
                continue
            # Save to disk
            dest = run_dir / os.path.basename(up.filename)
            with dest.open("wb") as fh:
                while chunk := await up.read(CHUNK_SIZE):
                    fh.write(chunk)
            await up.close()

    # Process parameters & inject non-secret values
    run_injectable_config = _config_for_run(run_dir)
    run_params = {**run_req.workflow_params}

    bento_services_data = None
    for run_input in run_req.tags.workflow_metadata.inputs:
        input_key = namespaced_input(run_req.tags.workflow_id, run_input.id)
        if isinstance(run_input, WorkflowConfigInput):
            config_value = run_injectable_config.get(run_input.key)
            if config_value is None:
                err = f"Could not find injectable configuration value for key {run_input.key}"
                logger.error(err)
                return JSONResponse({"detail": err}, status_code=400)
            logger.debug(
                "Injecting configuration parameter '%s' into run %s: %s=%s",
                run_input.key,
                run_id,
                run_input.id,
                config_value,
            )
            run_params[input_key] = config_value
        elif isinstance(run_input, WorkflowServiceUrlInput):
            bento_services_data = bento_services_data or get_bento_services()
            sk = run_input.service_kind
            config_value = (bento_services_data.get(sk) or {}).get("url")
            if config_value is None:
                err = f"Could not find URL/service record for service kind '{sk}'"
                logger.error(err)
                return JSONResponse({"detail": err}, status_code=400)
            logger.debug(
                "Injecting URL for service kind '%s' into run %s: %s=%s",
                sk,
                run_id,
                run_input.id,
                config_value,
            )
            run_params[input_key] = config_value

    # Persist run
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
            run_req.workflow_type,
            run_req.workflow_type_version,
            json.dumps(run_req.workflow_engine_parameters),
            str(run_req.workflow_url),
            run_req.tags.model_dump_json(),
            run_req.tags.workflow_id,
        ),
    )
    db.commit()

    # Move to queued + dispatch
    db.update_run_state_and_commit(c, run_id, states.STATE_QUEUED, logger=logger, publish_event=False)
    run_workflow.delay(run_id)

    return JSONResponse({"run_id": str(run_id)})


# -------------------------
# Public/Private detail shapes
# -------------------------
PUBLIC_RUN_DETAILS_SHAPE = {
    "request": {
        "workflow_type": True,
        "tags": {
            "workflow_id": True,
            "workflow_metadata": {"data_type": True},
            "project_id": True,
            "dataset_id": True,
        },
    },
    "run_log": {"start_time": True, "end_time": True},
}

PRIVATE_RUN_DETAILS_SHAPE = {
    "request": True,
    "run_log": True,
    "task_logs": True,
    "outputs": True,
}


# -------------------------
# GET/POST /runs
# -------------------------
@router.get("")
def run_list(
    request: Request,
    public: bool = Query(False, description="If true, only completed runs are included and details are restricted."),
    with_details: bool = Query(False, description="If true, include run details payload in each list item."),
    db: Database = Depends(get_db),
):
    c = db.cursor()

    res_list: list[dict] = []
    perms_list: list[RunRequest] = []

    for r in c.execute("SELECT * FROM runs").fetchall():
        run = db.run_with_details_from_row(c, r, stream_content=False)
        perms_list.append(run.request)

        if (not public) or (run.state == STATE_COMPLETE):
            base = run.model_dump(mode="json", include={"run_id", "state"})
            if with_details:
                details = run.model_dump(
                    mode="json",
                    include={
                        "run_id": True,
                        "state": True,
                        **(PUBLIC_RUN_DETAILS_SHAPE if public else PRIVATE_RUN_DETAILS_SHAPE),
                    },
                )
                base = {**base, "details": details}
            res_list.append(base)

    if not public:
        # Filter by permission
        perms = list(_check_runs_permission(request, perms_list, P_VIEW_RUNS))
        res_list = [v for v, p in zip(res_list, perms) if p]

    authz_middleware.mark_authz_done(request)
    return JSONResponse(res_list)


@router.post("")
async def run_create(
    request: Request,
    workflow_attachment: list[UploadFile] | None = File(None),
    db: Database = Depends(get_db),
):
    c = db.cursor()
    try:
        return await _create_run(request, db, c, workflow_attachment)
    except pydantic.ValidationError:
        logger.error("Encountered validation error during run creation: %s", traceback.format_exc())
        authz_middleware.mark_authz_done(request)
        raise HTTPException(status_code=400, detail="Validation error: bad run request format")
    except ValueError:
        logger.error("Encountered value error during run creation: %s", traceback.format_exc())
        authz_middleware.mark_authz_done(request)
        raise HTTPException(status_code=400, detail="Value error")


# -------------------------
# GET /runs/{run_id}
# -------------------------
@router.get("/{run_id}")
def run_detail(request: Request, run_id: uuid.UUID, db: Database = Depends(get_db)):
    run_details = db.get_run_with_details(db.cursor(), run_id, stream_content=False)

    if run_details is None:
        return _run_none_response(request, run_id)

    if not _check_single_run_permission_and_mark(request, run_details.request, P_VIEW_RUNS):
        raise HTTPException(status_code=403, detail="Forbidden")

    return JSONResponse(run_details.model_dump(mode="json"))


# -------------------------
# POST /runs/{run_id}/download-artifact
# -------------------------
@router.post("/{run_id}/download-artifact")
async def run_download_artifact(
    request: Request,
    run_id: uuid.UUID,
    path: str = Form(..., description="Artifact path on disk to download"),
    db: Database = Depends(get_db),
):
    run_details = db.get_run_with_details(db.cursor(), run_id, stream_content=False)

    if run_details is None:
        return _run_none_response(request, run_id)

    # token may arrive in the same multipart form
    form = await request.form()
    token_from_form = form.get("token") if hasattr(form, "get") else None

    if not _check_single_run_permission_and_mark(
        request, run_details.request, P_VIEW_RUNS, token_from_form=token_from_form
    ):
        raise HTTPException(status_code=403, detail="Forbidden")

    artifact_path = path
    if not artifact_path:
        raise HTTPException(status_code=400, detail="Requested artifact path is blank or unspecified")

    # Collect file artifacts
    artifacts: set[str] = set()
    for o in run_details.outputs.values():
        if "File" in o.type:
            dn: set[str] = set(_denest_list(o.value))
            artifacts.update(dn)

    if artifact_path not in artifacts:
        raise HTTPException(status_code=404, detail=f"Requested artifact path not found in run {run_id}")

    p = Path(artifact_path)

    if not p.exists():
        raise HTTPException(status_code=500, detail=f"Artifact path does not exist on filesystem: {artifact_path}")

    def generate_bytes():
        with open(p, "rb") as fh:
            while True:
                data = fh.read(CHUNK_SIZE)
                if not data:
                    break
                yield data

    headers = {
        "Content-Length": str(p.stat().st_size),
        "Content-Disposition": f"attachment; filename*=UTF-8''{urllib.parse.quote(p.name, encoding='utf-8')}",
    }
    return StreamingResponse(generate_bytes(), media_type=MIME_OCTET_STREAM, headers=headers)


# -------------------------
# GET stdout/stderr helpers
# -------------------------

def _get_stream(request: Request, c: sqlite3.Cursor, stream: RunStream, run_id: uuid.UUID):
    db: Database = get_db()
    run = db.get_run_with_details(c, run_id, stream_content=True)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Stream {stream} not found for run {run_id}")

    cache_control = (
        "private, max-age=86400" if run.state in states.TERMINATED_STATES else "no-cache, no-store, must-revalidate, max-age=0"
    )
    body = run.run_log.stdout if stream == "stdout" else run.run_log.stderr
    return PlainTextResponse(content=body or "", status_code=200, headers={"Cache-Control": cache_control})


def _run_none_response(request: Request, run_id: uuid.UUID):
    if authz_enabled():
        authz_middleware.mark_authz_done(request)
        raise HTTPException(status_code=403, detail="Forbidden")
    raise HTTPException(status_code=404, detail=f"Run {str(run_id)} not found")


# -------------------------
# GET /runs/{run_id}/stdout & /stderr
# -------------------------
@router.get("/{run_id}/stdout")
def run_stdout(request: Request, run_id: uuid.UUID, db: Database = Depends(get_db)):
    c = db.cursor()
    return _check_then(request, c, run_id, lambda: _get_stream(request, c, "stdout", run_id))


@router.get("/{run_id}/stderr")
def run_stderr(request: Request, run_id: uuid.UUID, db: Database = Depends(get_db)):
    c = db.cursor()
    return _check_then(request, c, run_id, lambda: _get_stream(request, c, "stderr", run_id))


# -------------------------
# Cancel helpers & endpoint
# -------------------------
RUN_CANCEL_BAD_REQUEST_STATES: tuple[tuple[Iterable[str], str], ...] = (
    ((states.STATE_CANCELING, states.STATE_CANCELED), "Run already canceled"),
    (states.FAILURE_STATES, "Run already terminated with error"),
    (states.SUCCESS_STATES, "Run already completed"),
)


def _check_then(
    request: Request,
    c: sqlite3.Cursor,
    run_id: uuid.UUID,
    cb: Callable[[], Response | dict | JSONResponse | PlainTextResponse | StreamingResponse],
    permission: str = P_VIEW_RUNS,
):
    db: Database = get_db()
    run = db.get_run_with_details(c, run_id, stream_content=False)
    if run is None:
        return _run_none_response(request, run_id)

    if not _check_single_run_permission_and_mark(request, run.request, permission):
        raise HTTPException(status_code=403, detail="Forbidden")

    return cb()


@router.post("/{run_id}/cancel", status_code=204)
def run_cancel(request: Request, run_id: uuid.UUID, db: Database = Depends(get_db)):
    c = db.cursor()

    def perform_cancel() -> Response:
        run = db.get_run_with_details(c, str(run_id), stream_content=False)
        if run is None:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

        for bad_req_states, bad_req_err in RUN_CANCEL_BAD_REQUEST_STATES:
            if run.state in bad_req_states:
                raise HTTPException(status_code=400, detail=bad_req_err)

        celery_id = run.run_log.celery_id
        if celery_id is None:
            # Never made it into the queue
            raise HTTPException(status_code=500, detail=f"No Celery ID present for run {run_id}")

        event_bus = get_flask_event_bus()  # TODO: swap to FastAPI-native bus if available

        # Update state and attempt to revoke
        db.update_run_state_and_commit(c, str(run_id), states.STATE_CANCELING, event_bus=event_bus)
        celery.control.revoke(celery_id, terminate=True)

        # Clean up run dir unless in debug
        run_dir = config.service_temp / str(run_id)
        if not config.bento_debug:
            shutil.rmtree(run_dir, ignore_errors=True)

        db.update_run_state_and_commit(c, str(run_id), states.STATE_CANCELED, event_bus=event_bus)

        return Response(status_code=204)

    return _check_then(request, c, run_id, perform_cancel)


# -------------------------
# GET /runs/{run_id}/status
# -------------------------
@router.get("/{run_id}/status")
def run_status(request: Request, run_id: uuid.UUID, db: Database = Depends(get_db)):
    c = db.cursor()

    def run_status_response():
        run = db.get_run(c, run_id)
        if run:
            return JSONResponse(run.model_dump())
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    return _check_then(request, c, run_id, run_status_response)
