import json
import os
import sqlite3

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
from .events import get_flask_event_bus
from .logger import logger
from .runner import run_workflow
from .types import RunStream
from .workflows import (
    WorkflowType,
    UnsupportedWorkflowType,
    WorkflowDownloadError,
    WorkflowManager,
    parse_workflow_host_allow_list,
)

from .db import get_db, run_request_dict, run_log_dict, get_task_logs, get_run_details, update_run_state_and_commit


bp_runs = Blueprint("runs", __name__)


def _get_project_and_dataset_id_from_tags(tags: dict) -> tuple[str, str | None]:
    project_id = tags["project_id"]
    dataset_id = tags.get("dataset_id", None)
    return project_id, dataset_id


def _get_project_and_dataset_id_from_run_request(run_request: dict) -> tuple[str, str | None]:
    return _get_project_and_dataset_id_from_tags(run_request["tags"])


def _check_runs_permission(runs_project_datasets: list[tuple[str, str | None]], permission: str) -> tuple[bool, ...]:
    if not current_app.config["AUTHZ_ENABLED"]:
        return tuple([True] * len(runs_project_datasets))  # Assume we have permission for everything if authz disabled

    return authz_middleware.authz_post(request, "/policy/evaluate", body={
        "requested_resource": [
            {
                "project": project_id,
                **({"dataset": dataset_id} if dataset_id else {}),
            }
            for project_id, dataset_id in runs_project_datasets
        ],
        "required_permissions": [permission],
    }).json()["result"]


def _check_single_run_permission_and_mark(project_and_dataset: tuple[str, str | None], permission: str) -> bool:
    p_res = _check_runs_permission([project_and_dataset], permission)
    # By calling this, the developer indicates that they will have handled permissions adequately:
    authz_middleware.mark_authz_done(request)
    return p_res and p_res[0]


def _create_run(db: sqlite3.Connection, c: sqlite3.Cursor) -> Response:
    assert "workflow_params" in request.form
    assert "workflow_type" in request.form
    assert "workflow_type_version" in request.form
    assert "workflow_engine_parameters" in request.form
    assert "workflow_url" in request.form
    assert "tags" in request.form

    workflow_params = json.loads(request.form["workflow_params"])
    workflow_type = request.form["workflow_type"].upper().strip()
    workflow_type_version = request.form["workflow_type_version"].strip()
    workflow_engine_parameters = json.loads(request.form["workflow_engine_parameters"])  # TODO: Unused
    workflow_url = request.form["workflow_url"].lower()  # TODO: This can refer to an attachment
    workflow_attachment_list = request.files.getlist("workflow_attachment")  # TODO: Use this fully
    tags = json.loads(request.form["tags"])

    # TODO: Move Bento-specific stuff out somehow?

    # Bento-specific required tags
    assert "workflow_id" in tags
    assert "workflow_metadata" in tags
    workflow_metadata = tags["workflow_metadata"]
    assert "action" in workflow_metadata

    workflow_id = tags.get("workflow_id", workflow_url)

    # Check ingest permissions before continuing

    if not _check_single_run_permission_and_mark(
            _get_project_and_dataset_id_from_tags(tags), PERMISSION_INGEST_DATA):
        return flask_forbidden_error("Forbidden")

    # We have permission - so continue ---------

    # Don't accept anything (ex. CWL) other than WDL
    assert workflow_type == "WDL"
    assert workflow_type_version == "1.0"

    assert isinstance(workflow_params, dict)
    assert isinstance(workflow_engine_parameters, dict)
    assert isinstance(tags, dict)

    # Some workflow parameters depend on the WES application configuration
    # and need to be added from there.
    # The reserved keyword `FROM_CONFIG` is used to detect those inputs.
    # All parameters in config are upper case. e.g. drs_url --> DRS_URL
    for i in workflow_metadata["inputs"]:
        if i.get("value") != "FROM_CONFIG":
            continue
        param_name = i["id"]
        workflow_params[f"{workflow_id}.{param_name}"] = current_app.config.get(param_name.upper(), "")

    # TODO: Use JSON schemas for workflow params / engine parameters / tags

    # Get list of allowed workflow hosts from configuration for any checks inside the runner
    # If it's blank, assume that means "any host is allowed" and pass None to the runner
    workflow_host_allow_list = parse_workflow_host_allow_list(current_app.config["WORKFLOW_HOST_ALLOW_LIST"])

    # Download workflow file, potentially using passed auth headers if they're present
    # and we're querying our own node.

    # TODO: Move this back to runner, since we'll need to handle the callback anyway with local URLs...

    bento_url = current_app.config["BENTO_URL"]

    wm = WorkflowManager(
        current_app.config["SERVICE_TEMP"],
        bento_url,
        logger=logger,
        workflow_host_allow_list=workflow_host_allow_list,
        validate_ssl=current_app.config["BENTO_VALIDATE_SSL"],
        debug=current_app.config["BENTO_DEBUG"],
    )

    # Optional Authorization HTTP header to forward to nested requests
    auth_header = request.headers.get("Authorization")
    auth_header_dict = {"Authorization": auth_header} if auth_header else {}

    try:
        wm.download_or_copy_workflow(workflow_url, WorkflowType(workflow_type), auth_headers=auth_header_dict)
    except UnsupportedWorkflowType:
        return flask_bad_request_error(f"Unsupported workflow type: {workflow_type}")
    except (WorkflowDownloadError, requests.exceptions.ConnectionError) as e:
        return flask_bad_request_error(f"Could not access workflow file: {workflow_url} (Python error: {e})")

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

        json.dumps(workflow_params),
        workflow_type,
        workflow_type_version,
        json.dumps(workflow_engine_parameters),
        workflow_url,
        json.dumps(tags),

        workflow_id,
    ))
    db.commit()

    # TODO: figure out timeout
    # TODO: retry policy

    update_run_state_and_commit(db, c, run_id, states.STATE_QUEUED, logger=logger, publish_event=False)

    run_workflow.delay(run_id)

    return jsonify({"run_id": str(run_id)})


@bp_runs.route("/runs", methods=["GET", "POST"])
def run_list():
    db = get_db()
    c = db.cursor()

    if request.method == "POST":
        try:
            return _create_run(db, c)
        except ValueError:
            authz_middleware.mark_authz_done(request)
            return flask_bad_request_error("Value error")
        except AssertionError:  # TODO: Better error messages
            authz_middleware.mark_authz_done(request)
            logger.error(f"Encountered assertion error: {traceback.format_exc()}")
            return flask_bad_request_error("Assertion error: bad run request format")

    # GET
    # Bento Extension: Include run details with /runs request
    with_details = request.args.get("with_details", "false").lower() == "true"

    res_list = []
    perms_list: list[tuple[str, str | None]] = []

    c.execute("SELECT * FROM runs")

    for r in c.fetchall():
        run = {
            "run_id": r["id"],
            "state": r["state"],
        }

        run_req = run_request_dict(r)

        project_id, dataset_id = _get_project_and_dataset_id_from_run_request(run_req)
        perms_list.append((project_id, dataset_id))

        if with_details:
            run["details"] = {
                "run_id": r["id"],
                "state": r["state"],
                "request": run_req,
                "run_log": run_log_dict(r),
                "task_logs": get_task_logs(c, r["id"])
            }

        res_list.append(run)

    p_res = _check_runs_permission(perms_list, PERMISSION_VIEW_RUNS)
    res_list = [v for v, p in zip(res_list, p_res) if p]

    authz_middleware.mark_authz_done(request)

    return jsonify(res_list)


@bp_runs.route("/runs/<uuid:run_id>", methods=["GET"])
def run_detail(run_id: uuid.UUID):
    authz_enabled = current_app.config["AUTHZ_ENABLED"]
    run_details, err = get_run_details(get_db().cursor(), run_id)

    if run_details is None:
        if authz_enabled:
            return flask_forbidden_error("Forbidden")
        else:
            return flask_not_found_error(f"Run {run_id} not found ({err})")

    if not _check_single_run_permission_and_mark(
            _get_project_and_dataset_id_from_run_request(run_details["request"]), PERMISSION_VIEW_RUNS):
        return flask_forbidden_error("Forbidden")

    if run_details is None and not authz_enabled:
        return flask_not_found_error(f"Run {run_id} not found ({err})")

    return jsonify(run_details)


def get_stream(c: sqlite3.Cursor, stream: RunStream, run_id: uuid.UUID):
    c.execute("SELECT * FROM runs WHERE id = ?", (str(run_id),))
    run = c.fetchone()
    return (current_app.response_class(
        headers={
            # If we've finished, we allow long-term (24h) caching of the stdout/stderr responses.
            # Otherwise, no caching allowed!
            "Cache-Control": (
                "private, max-age=86400" if run["state"] in states.TERMINATED_STATES
                else "no-cache, no-store, must-revalidate, max-age=0"
            ),
        },
        response=run[f"run_log__{stream}"],
        mimetype="text/plain",
        status=200,
    ) if run is not None else flask_not_found_error(f"Stream {stream} not found for run {run_id}"))


def check_run_authz_then_return_response(
    c: sqlite3.Cursor,
    run_id: uuid.UUID,
    cb: Callable[[], Response | dict],
    permission: str = PERMISSION_VIEW_RUNS,
):
    run_details, rd_err = get_run_details(c, run_id)

    if rd_err:
        if current_app.config["AUTHZ_ENABLED"]:
            # Without the required permissions, don't even leak if this run exists - just return forbidden
            authz_middleware.mark_authz_done(request)
            return flask_forbidden_error("Forbidden")
        else:
            return flask_not_found_error(rd_err)

    if not _check_single_run_permission_and_mark(
            _get_project_and_dataset_id_from_run_request(run_details["request"]), permission):
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


@bp_runs.route("/runs/<uuid:run_id>/cancel", methods=["POST"])
def run_cancel(run_id: uuid.UUID):
    # TODO: Check if already completed
    # TODO: Check if run log exists
    # TODO: from celery.task.control import revoke; revoke(celery_id, terminate=True)
    db = get_db()
    c = db.cursor()

    def perform_run_cancel():
        c.execute("SELECT * FROM runs WHERE id = ?", (str(run_id),))
        run = c.fetchone()

        if run is None:
            return flask_not_found_error(f"Run {run_id} not found")

        if run["state"] in (states.STATE_CANCELING, states.STATE_CANCELED):
            return flask_bad_request_error("Run already canceled")

        if run["state"] in states.FAILURE_STATES:
            return flask_bad_request_error("Run already terminated with error")

        if run["state"] in states.SUCCESS_STATES:
            return flask_bad_request_error("Run already completed")

        celery_id = run["run_log__celery_id"]

        if celery_id is None:
            # Never made it into the queue, so "cancel" it
            return flask_internal_server_error(f"No Celery ID present for run {run_id}")

        event_bus = get_flask_event_bus()

        # TODO: terminate=True might be iffy
        update_run_state_and_commit(db, c, run["id"], states.STATE_CANCELING, event_bus=event_bus)
        celery.control.revoke(celery_id, terminate=True)  # Remove from queue if there, terminate if running

        # TODO: wait for revocation / failure and update status...

        # TODO: Generalize clean-up code / fetch from back-end
        run_dir = os.path.join(current_app.config["SERVICE_TEMP"], run["run_id"])
        if not current_app.config["BENTO_DEBUG"]:
            shutil.rmtree(run_dir, ignore_errors=True)

        update_run_state_and_commit(db, c, run["id"], states.STATE_CANCELED, event_bus=event_bus)

        return current_app.response_class(status=204)  # TODO: Better response

    return check_run_authz_then_return_response(c, run_id, perform_run_cancel)


@bp_runs.route("/runs/<uuid:run_id>/status", methods=["GET"])
def run_status(run_id: uuid.UUID):
    c = get_db().cursor()

    def run_status_response():
        c.execute("SELECT * FROM runs WHERE id = ?", (str(run_id),))
        run = c.fetchone()

        if run is None:
            return flask_not_found_error(f"Run {run_id} not found")

        return jsonify({
            "run_id": run["id"],
            "state": run["state"]
        })

    return check_run_authz_then_return_response(c, run_id, run_status_response)
