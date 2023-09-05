import json
import os
import sqlite3
import pydantic
import requests
import shutil
import traceback
import uuid

from bento_lib.responses.flask_errors import (
    flask_bad_request_error,
    flask_internal_server_error,
    flask_not_found_error,
    flask_forbidden_error,
)
from flask import Blueprint, Response, current_app, jsonify, request
from typing import Callable
from werkzeug.utils import secure_filename

from . import states
from .authz import authz_middleware, PERMISSION_INGEST_DATA, PERMISSION_VIEW_RUNS
from .celery import celery
from .db import (
    get_db,
    run_with_details_and_output_from_row,
    get_run,
    get_run_with_details,
    update_run_state_and_commit,
)
from .events import get_flask_event_bus
from .logger import logger
from .models import RunRequest
from .runner import run_workflow
from .states import STATE_COMPLETE
from .types import RunStream
from .workflows import (
    WorkflowType,
    UnsupportedWorkflowType,
    WorkflowDownloadError,
    WorkflowManager,
    parse_workflow_host_allow_list,
)


bp_runs = Blueprint("runs", __name__)


def _check_runs_permission(run_requests: list[RunRequest], permission: str) -> tuple[bool, ...]:
    if not current_app.config["AUTHZ_ENABLED"]:
        return tuple([True] * len(run_requests))  # Assume we have permission for everything if authz disabled

    # /policy/evaluate returns a LIST of booleans when a LIST of requested_resource[s] is passed. Thus, we can
    # return this list directly *rather* than wrapping it like the above case where authz was disabled.
    return authz_middleware.authz_post(request, "/policy/evaluate", body={
        "requested_resource": [
            {
                "project": run_request.tags.project_id,
                **({"dataset": run_request.tags.dataset_id} if run_request.tags.dataset_id else {}),
            }
            for run_request in run_requests
        ],
        "required_permissions": [permission],
    })["result"]


def _check_single_run_permission_and_mark(run_req: RunRequest, permission: str) -> bool:
    p_res = _check_runs_permission([run_req], permission)
    # By calling this, the developer indicates that they will have handled permissions adequately:
    authz_middleware.mark_authz_done(request)
    return p_res and p_res[0]


def _create_run(db: sqlite3.Connection, c: sqlite3.Cursor) -> Response:
    run_req = RunRequest(**request.form)

    # TODO: Use this fully
    #  - files inside the workflow
    #  - workflow_url can refer to an attachment
    workflow_attachment_list = request.files.getlist("workflow_attachment")

    # Check ingest permissions before continuing

    if not _check_single_run_permission_and_mark(run_req, PERMISSION_INGEST_DATA):
        return flask_forbidden_error("Forbidden")

    # We have permission - so continue ---------

    # Get list of allowed workflow hosts from configuration for any checks inside the runner
    # If it's blank, assume that means "any host is allowed" and pass None to the runner
    workflow_host_allow_list = parse_workflow_host_allow_list(current_app.config["WORKFLOW_HOST_ALLOW_LIST"])

    # Download workflow file, potentially using passed auth headers if they're present
    # and we're querying our own node.

    # TODO: Move this back to runner, since we'll need to handle the callback anyway with local URLs...

    bento_url = current_app.config["BENTO_URL"]

    wm = WorkflowManager(
        current_app.config["SERVICE_TEMP"],
        service_base_url=current_app.config["SERVICE_BASE_URL"],
        bento_url=bento_url,
        logger=logger,
        workflow_host_allow_list=workflow_host_allow_list,
        validate_ssl=current_app.config["BENTO_VALIDATE_SSL"],
        debug=current_app.config["BENTO_DEBUG"],
    )

    # Optional Authorization HTTP header to forward to nested requests
    auth_header = request.headers.get("Authorization")
    auth_header_dict = {"Authorization": auth_header} if auth_header else {}

    try:
        wm.download_or_copy_workflow(
            run_req.workflow_url, WorkflowType(run_req.workflow_type), auth_headers=auth_header_dict)
    except UnsupportedWorkflowType:
        return flask_bad_request_error(f"Unsupported workflow type: {run_req.workflow_type}")
    except (WorkflowDownloadError, requests.exceptions.ConnectionError) as e:
        return flask_bad_request_error(f"Could not access workflow file: {run_req.workflow_url} (Python error: {e})")

    # ---

    # Begin creating the job after validating the request
    run_id = uuid.uuid4()

    # Create run directory

    run_dir = os.path.join(current_app.config["SERVICE_TEMP"], str(run_id))

    if os.path.exists(run_dir):
        return flask_internal_server_error("UUID collision")

    os.makedirs(run_dir, exist_ok=True)
    # TODO: Delete run dir if something goes wrong...

    # Move workflow attachments to run directory

    for attachment in workflow_attachment_list:
        # TODO: Check and fix input if filename is non-secure
        # TODO: Do we put these in a subdirectory?
        # TODO: Support WDL uploads for workflows
        attachment.save(os.path.join(run_dir, secure_filename(attachment.filename)))

    # Will be updated to STATE_QUEUED once submitted
    c.execute("""
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
    """, (
        str(run_id),
        states.STATE_UNKNOWN,
        json.dumps({}),

        json.dumps(run_req.workflow_params),
        run_req.workflow_type,
        run_req.workflow_type_version,
        json.dumps(run_req.workflow_engine_parameters),
        str(run_req.workflow_url),
        run_req.tags.model_dump_json(),

        run_req.tags.workflow_id,
    ))
    db.commit()

    # TODO: figure out timeout
    # TODO: retry policy

    update_run_state_and_commit(db, c, run_id, states.STATE_QUEUED, logger=logger, publish_event=False)

    run_workflow.delay(run_id)

    return jsonify({"run_id": str(run_id)})


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
}


@bp_runs.route("/runs", methods=["GET", "POST"])
def run_list():
    db = get_db()
    c = db.cursor()

    if request.method == "POST":
        try:
            return _create_run(db, c)
        except pydantic.ValidationError:  # TODO: Better error messages
            authz_middleware.mark_authz_done(request)
            logger.error(f"Encountered validation error: {traceback.format_exc()}")
            return flask_bad_request_error("Validation error: bad run request format")
        except ValueError:
            authz_middleware.mark_authz_done(request)
            return flask_bad_request_error("Value error")

    # GET
    # Bento Extension: Include run public details with /runs request
    public_endpoint = request.args.get("public", "false").lower() == "true"
    # Bento Extension: Include run details with /runs request
    with_details = request.args.get("with_details", "false").lower() == "true"

    res_list = []
    perms_list: list[RunRequest] = []

    for r in c.execute("SELECT * FROM runs").fetchall():
        run = run_with_details_and_output_from_row(c, r, stream_content=False)
        perms_list.append(run.request)

        if not public_endpoint or run.state == STATE_COMPLETE:
            res_list.append({
                **run.model_dump(mode="json", include={"run_id", "state"}),
                **(
                    {
                        "details": run.model_dump(mode="json", include={
                            "run_id": True,
                            "state": True,
                            **(PUBLIC_RUN_DETAILS_SHAPE if public_endpoint else PRIVATE_RUN_DETAILS_SHAPE),
                        }),
                    }
                    if with_details else {}
                ),
            })

    if not public_endpoint:
        # Filter runs to just those which we have permission to view
        p_res = _check_runs_permission(perms_list, PERMISSION_VIEW_RUNS)
        res_list = [v for v, p in zip(res_list, p_res) if p]

    authz_middleware.mark_authz_done(request)

    return jsonify(res_list)


@bp_runs.route("/runs/<uuid:run_id>", methods=["GET"])
def run_detail(run_id: uuid.UUID):
    authz_enabled = current_app.config["AUTHZ_ENABLED"]
    run_details = get_run_with_details(get_db().cursor(), run_id, stream_content=False)

    if run_details is None:
        if authz_enabled:
            return flask_forbidden_error("Forbidden")
        else:
            return flask_not_found_error(f"Run {run_id} not found")

    if not _check_single_run_permission_and_mark(run_details.request, PERMISSION_VIEW_RUNS):
        return flask_forbidden_error("Forbidden")

    return jsonify(run_details.model_dump(mode="json"))


def get_stream(c: sqlite3.Cursor, stream: RunStream, run_id: uuid.UUID):
    run = get_run_with_details(c, run_id, stream_content=True)
    return (current_app.response_class(
        headers={
            # If we've finished, we allow long-term (24h) caching of the stdout/stderr responses.
            # Otherwise, no caching allowed!
            "Cache-Control": (
                "private, max-age=86400" if run.state in states.TERMINATED_STATES
                else "no-cache, no-store, must-revalidate, max-age=0"
            ),
        },
        response=run.run_log.stdout if stream == "stdout" else run.run_log.stderr,
        mimetype="text/plain",
        status=200,
    ) if run is not None else flask_not_found_error(f"Stream {stream} not found for run {run_id}"))


def check_run_authz_then_return_response(
    c: sqlite3.Cursor,
    run_id: uuid.UUID,
    cb: Callable[[], Response | dict],
    permission: str = PERMISSION_VIEW_RUNS,
):
    run = get_run_with_details(c, run_id, stream_content=False)

    if run is None:
        if current_app.config["AUTHZ_ENABLED"]:
            # Without the required permissions, don't even leak if this run exists - just return forbidden
            authz_middleware.mark_authz_done(request)
            return flask_forbidden_error("Forbidden")
        else:
            return flask_not_found_error(f"Run {run_id} not found")

    if not _check_single_run_permission_and_mark(run.request, permission):
        return flask_forbidden_error("Forbidden")

    return cb()


@bp_runs.route("/runs/<uuid:run_id>/stdout", methods=["GET"])
def run_stdout(run_id: uuid.UUID):
    c = get_db().cursor()
    return check_run_authz_then_return_response(c, run_id, lambda: get_stream(c, "stdout", run_id))


@bp_runs.route("/runs/<uuid:run_id>/stderr", methods=["GET"])
def run_stderr(run_id: uuid.UUID):
    c = get_db().cursor()
    return check_run_authz_then_return_response(c, run_id, lambda: get_stream(c, "stderr", run_id))


RUN_CANCEL_BAD_REQUEST_STATES = (
    ((states.STATE_CANCELING, states.STATE_CANCELED), "Run already canceled"),
    (states.FAILURE_STATES, "Run already terminated with error"),
    (states.SUCCESS_STATES, "Run already completed"),
)


@bp_runs.route("/runs/<uuid:run_id>/cancel", methods=["POST"])
def run_cancel(run_id: uuid.UUID):
    # TODO: Check if already completed
    # TODO: Check if run log exists
    # TODO: from celery.task.control import revoke; revoke(celery_id, terminate=True)
    db = get_db()
    c = db.cursor()

    run_id_str = str(run_id)

    def perform_run_cancel() -> Response:
        run = get_run_with_details(c, run_id_str, stream_content=False)

        if run is None:
            return flask_not_found_error(f"Run {run_id_str} not found")

        for bad_req_states, bad_req_err in RUN_CANCEL_BAD_REQUEST_STATES:
            if run.state in bad_req_states:
                return flask_bad_request_error(bad_req_err)

        celery_id = run.run_log.celery_id

        if celery_id is None:
            # Never made it into the queue, so "cancel" it
            return flask_internal_server_error(f"No Celery ID present for run {run_id_str}")

        event_bus = get_flask_event_bus()

        # TODO: terminate=True might be iffy
        update_run_state_and_commit(db, c, run_id_str, states.STATE_CANCELING, event_bus=event_bus)
        celery.control.revoke(celery_id, terminate=True)  # Remove from queue if there, terminate if running

        # TODO: wait for revocation / failure and update status...

        # TODO: Generalize clean-up code / fetch from back-end
        run_dir = os.path.join(current_app.config["SERVICE_TEMP"], run_id_str)
        if not current_app.config["BENTO_DEBUG"]:
            shutil.rmtree(run_dir, ignore_errors=True)

        update_run_state_and_commit(db, c, run_id_str, states.STATE_CANCELED, event_bus=event_bus)

        return current_app.response_class(status=204)  # TODO: Better response

    return check_run_authz_then_return_response(c, run_id, perform_run_cancel)


@bp_runs.route("/runs/<uuid:run_id>/status", methods=["GET"])
def run_status(run_id: uuid.UUID):
    c = get_db().cursor()

    def run_status_response() -> Response:
        if run := get_run(c, run_id):
            return jsonify(run.model_dump())
        return flask_not_found_error(f"Run {run_id} not found")

    return check_run_authz_then_return_response(c, run_id, run_status_response)
