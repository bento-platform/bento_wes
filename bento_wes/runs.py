import json
import os
import requests
import shutil
import uuid

from bento_lib.auth.flask_decorators import flask_permissions_owner
from bento_lib.responses.flask_errors import (
    flask_bad_request_error,
    flask_internal_server_error,
    flask_not_found_error,
)
from flask import Blueprint, current_app, jsonify, request
from urllib.parse import urljoin, urlparse
from werkzeug.utils import secure_filename

from . import states
from .celery import celery
from .events import get_flask_event_bus
from .runner import run_workflow
from .workflows import (
    WorkflowType,
    UnsupportedWorkflowType,
    WorkflowDownloadError,
    WorkflowManager,
    count_bento_workflow_file_outputs
)

from .db import get_db, run_request_dict, run_log_dict, get_task_logs, get_run_details, update_run_state_and_commit


bp_runs = Blueprint("runs", __name__)


def _create_run(db, c):
    try:
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

        # TODO: Move CHORD-specific stuff out somehow?

        # Only "turn on" CHORD-specific features if specific tags are present

        chord_mode = all((
            "workflow_id" in tags,
            "workflow_metadata" in tags,

            # Allow either a path to be specified for ingestion (for the 'classic'
            # Bento singularity architecture) or
            "ingestion_path" in tags or "ingestion_url" in tags,

            "table_id" in tags,
        ))

        workflow_id = tags.get("workflow_id", workflow_url)
        workflow_metadata = tags.get("workflow_metadata", {})
        workflow_ingestion_path = tags.get("ingestion_path", None)
        workflow_ingestion_url = tags.get(
            "ingestion_url",
            (f"http+unix://{current_app.config['NGINX_INTERNAL_SOCKET']}{workflow_ingestion_path}"
             if workflow_ingestion_path else None))
        table_id = tags.get("table_id", None)

        # Don't accept anything (ex. CWL) other than WDL TODO: CWL support
        assert workflow_type == "WDL"
        assert workflow_type_version == "1.0"

        assert isinstance(workflow_params, dict)
        assert isinstance(workflow_engine_parameters, dict)
        assert isinstance(tags, dict)

        if chord_mode:
            table_id = str(uuid.UUID(table_id))  # Check and standardize table ID

        # TODO: Use JSON schemas for workflow params / engine parameters / tags

        # Get list of allowed workflow hosts from configuration for any checks inside the runner
        # If it's blank, assume that means "any host is allowed" and pass None to the runner
        workflow_host_allow_list = {a.strip() for a in current_app.config["WORKFLOW_HOST_ALLOW_LIST"].split(",")
                                    if a.strip()} or None

        # Download workflow file (potentially using passed auth headers, if
        # present and we're querying ourself)

        # TODO: Move this back to runner, since we'll need to handle the callback anyway with local URLs...

        wm = WorkflowManager(
            current_app.config["SERVICE_TEMP"],
            current_app.config["CHORD_URL"],
            logger=current_app.logger,
            workflow_host_allow_list=workflow_host_allow_list,
            debug=current_app.config["DEBUG"],
        )

        # Optional Authorization HTTP header to forward to nested requests
        # TODO: Move X-Auth... constant to bento_lib
        auth_header = request.headers.get("X-Authorization", request.headers.get("Authorization"))

        try:
            wm.download_or_copy_workflow(
                workflow_url,
                WorkflowType(workflow_type),
                auth_headers={"Authorization": auth_header} if auth_header else {})
        except UnsupportedWorkflowType:
            return flask_bad_request_error(f"Unsupported workflow type: {workflow_type}")
        except (WorkflowDownloadError, requests.exceptions.ConnectionError) as e:
            return flask_bad_request_error(f"Could not access workflow file: {workflow_url} (Python error: {e})")

        # Generate one-time tokens for ingestion purposes if in Bento mode

        one_time_tokens = []
        drs_url: str = current_app.config["DRS_URL"]
        ott_endpoint_namespace: str = current_app.config["OTT_ENDPOINT_NAMESPACE"]  # TODO: py3.9: walrus operator
        if chord_mode and ott_endpoint_namespace and urlparse(drs_url).scheme != "http+unix":
            # Generate the correct number of one-time tokens for the DRS scope
            # to allow for the callback to ingest files
            # Skip doing this if the DRS URL is an internal UNIX socket
            # TODO: Remove this ^ bit and pull the plug on socket requests
            tr = requests.post(
                urljoin(ott_endpoint_namespace.rstrip("/") + "/", "generate"),
                headers={"Authorization": auth_header} if auth_header else {},  # TODO: Host?
                json={
                    # TODO: This assumes DRS is on the same domain as WES, which isn't necessarily correct
                    #  An error should be thrown if there's a mismatch and we're still trying to do OTT stuff, probably
                    "scope": urlparse(drs_url).path + "/",
                    # Number of one-time-use tokens =
                    #   Number of files to ingest into DRS
                    #   + 1 for the service ingest request
                    "number": count_bento_workflow_file_outputs(workflow_id, workflow_params, workflow_metadata) + 1,
                },
                verify=not current_app.config["DEBUG"]
            )

            if not tr.ok:
                # An error occurred while requesting OTTs, so we cannot complete the run request
                return flask_internal_server_error(f"Got error while requesting one-time-use tokens: {tr.content}")

            one_time_tokens = tr.json()

        # Begin creating the job after validating the request

        req_id = uuid.uuid4()
        run_id = uuid.uuid4()
        log_id = uuid.uuid4()

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

        # Will be updated to STATE_ QUEUED once submitted
        c.execute("INSERT INTO run_requests (id, workflow_params, workflow_type, workflow_type_version, "
                  "workflow_engine_parameters, workflow_url, tags) VALUES (?, ?, ?, ?, ?, ?, ?)",
                  (str(req_id), json.dumps(workflow_params), workflow_type, workflow_type_version,
                   json.dumps(workflow_engine_parameters), workflow_url, json.dumps(tags)))
        c.execute("INSERT INTO run_logs (id, name) VALUES (?, ?)", (str(log_id), workflow_id))
        c.execute("INSERT INTO runs (id, request, state, run_log, outputs) VALUES (?, ?, ?, ?, ?)",
                  (str(run_id), str(req_id), states.STATE_UNKNOWN, str(log_id), json.dumps({})))
        db.commit()

        # TODO: figure out timeout
        # TODO: retry policy
        c.execute("UPDATE runs SET state = ? WHERE id = ?", (states.STATE_QUEUED, str(run_id)))
        db.commit()

        run_workflow.delay(run_id, chord_mode, workflow_metadata, workflow_ingestion_url, table_id, one_time_tokens)

        return jsonify({"run_id": str(run_id)})

    except ValueError:
        return flask_bad_request_error("Value error")

    except AssertionError:
        return flask_bad_request_error("Assertion error")


@bp_runs.route("/runs", methods=["GET", "POST"])
@flask_permissions_owner  # TODO: Allow others to submit analysis runs?
def run_list():
    db = get_db()
    c = db.cursor()

    if request.method == "POST":
        return _create_run(db, c)

    # GET
    # CHORD Extension: Include run details with /runs request
    with_details = request.args.get("with_details", "false").lower() == "true"

    if not with_details:
        c.execute("SELECT * FROM runs")

        return jsonify([{
            "run_id": run["id"],
            "state": run["state"]
        } for run in c.fetchall()])

    c.execute("SELECT r.id AS run_id, r.state AS state, rr.*, rl.* "
              "FROM runs AS r, run_requests AS rr, run_logs AS rl "
              "WHERE r.request = rr.id AND r.run_log = rl.id")

    return jsonify([{
        "run_id": r["run_id"],
        "state": r["state"],
        "details": {
            "run_id": r["run_id"],
            "state": r["state"],
            "request": run_request_dict(r),
            "run_log": run_log_dict(r["run_id"], r),
            "task_logs": get_task_logs(c, r["run_id"])
        }
    } for r in c.fetchall()])


@bp_runs.route("/runs/<uuid:run_id>", methods=["GET"])
@flask_permissions_owner
def run_detail(run_id):
    run_details, err = get_run_details(get_db().cursor(), run_id)
    return jsonify(run_details) if run_details is not None else flask_not_found_error(f"Run {run_id} not found ({err})")


def get_stream(c, stream, run_id):
    c.execute("SELECT * FROM runs AS r, run_logs AS rl WHERE r.id = ? AND r.run_log = rl.id", (str(run_id),))
    run = c.fetchone()
    return (current_app.response_class(response=run[stream], mimetype="text/plain", status=200) if run is not None
            else flask_not_found_error(f"Stream {stream} not found for run {run_id}"))


@bp_runs.route("/runs/<uuid:run_id>/stdout", methods=["GET"])
@flask_permissions_owner
def run_stdout(run_id):
    return get_stream(get_db().cursor(), "stdout", run_id)


@bp_runs.route("/runs/<uuid:run_id>/stderr", methods=["GET"])
@flask_permissions_owner
def run_stderr(run_id):
    return get_stream(get_db().cursor(), "stderr", run_id)


@bp_runs.route("/runs/<uuid:run_id>/cancel", methods=["POST"])
@flask_permissions_owner
def run_cancel(run_id):
    # TODO: Check if already completed
    # TODO: Check if run log exists
    # TODO: from celery.task.control import revoke; revoke(celery_id, terminate=True)
    db = get_db()
    c = db.cursor()
    event_bus = get_flask_event_bus()

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

    c.execute("SELECT * FROM run_logs WHERE id = ?", (run["run_log"],))
    run_log = c.fetchone()

    if run_log is None:
        return flask_internal_server_error(f"No run log present for run {run_id}")

    if run_log["celery_id"] is None:
        # Never made it into the queue, so "cancel" it
        return flask_internal_server_error(f"No Celery ID present for run {run_id}")

    # TODO: terminate=True might be iffy
    update_run_state_and_commit(db, c, event_bus, run["id"], states.STATE_CANCELING)
    celery.control.revoke(run_log["celery_id"], terminate=True)  # Remove from queue if there, terminate if running

    # TODO: wait for revocation / failure and update status...

    # TODO: Generalize clean-up code / fetch from back-end
    run_dir = os.path.join(current_app.config["SERVICE_TEMP"], run["run_id"])
    shutil.rmtree(run_dir, ignore_errors=True)

    update_run_state_and_commit(db, c, event_bus, run["id"], states.STATE_CANCELED)

    return current_app.response_class(status=204)  # TODO: Better response


@bp_runs.route("/runs/<uuid:run_id>/status", methods=["GET"])
@flask_permissions_owner
def run_status(run_id):
    c = get_db().cursor()

    c.execute("SELECT * FROM runs WHERE id = ?", (str(run_id),))
    run = c.fetchone()

    if run is None:
        return flask_not_found_error(f"Run {run_id} not found")

    return jsonify({
        "run_id": run["id"],
        "state": run["state"]
    })
