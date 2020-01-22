import json
import os
import uuid

from chord_lib.auth.flask_decorators import flask_permissions_owner
from chord_lib.responses.flask_errors import *
from flask import Blueprint, current_app, jsonify, request
from werkzeug.utils import secure_filename

from .celery import celery
from .states import *
from .runner import update_run_state_and_commit, run_workflow

from .db import get_db, run_request_dict, run_log_dict, get_task_logs, get_run_details


bp_runs = Blueprint("runs", __name__)


@bp_runs.route("/runs", methods=["GET", "POST"])
@flask_permissions_owner  # TODO: Allow others to submit analysis runs?
def run_list():
    db = get_db()
    c = db.cursor()

    if request.method == "POST":
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

            chord_mode = ("workflow_id" in tags and "workflow_metadata" in tags and "ingestion_path" in tags
                          and "table_id" in tags)

            workflow_id = tags.get("workflow_id", workflow_url)
            workflow_metadata = tags.get("workflow_metadata", {})
            workflow_ingestion_path = tags.get("ingestion_path", None)
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
                      (str(run_id), str(req_id), STATE_UNKNOWN, str(log_id), json.dumps({})))
            db.commit()

            # TODO: figure out timeout
            # TODO: retry policy
            c.execute("UPDATE runs SET state = ? WHERE id = ?", (STATE_QUEUED, str(run_id)))
            db.commit()

            run_workflow.delay(run_id, chord_mode, workflow_metadata, workflow_ingestion_path, table_id)

            return jsonify({"run_id": str(run_id)})

        except ValueError:
            return flask_bad_request_error("Value error")

        except AssertionError:
            return flask_bad_request_error("Assertion error")

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
    run_details = get_run_details(get_db().cursor(), run_id)
    return jsonify(run_details) if run_details is not None else flask_not_found_error(f"Run {run_id} not found")


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

    c.execute("SELECT * FROM runs WHERE id = ?", (str(run_id),))
    run = c.fetchone()

    if run is None:
        return flask_not_found_error(f"Run {run_id} not found")

    if run["state"] in (STATE_CANCELING, STATE_CANCELED):
        return flask_bad_request_error("Run already canceled")

    if run["state"] in (STATE_SYSTEM_ERROR, STATE_EXECUTOR_ERROR):
        return flask_bad_request_error("Run already terminated with error")

    if run["state"] == STATE_COMPLETE:
        return flask_bad_request_error("Run already completed")

    c.execute("SELECT * FROM run_logs WHERE id = ?", (run["run_log"],))
    run_log = c.fetchone()

    if run_log is None:
        return flask_internal_server_error(f"No run log present for run {run_id}")

    if run_log["celery_id"] is None:
        # Never made it into the queue, so "cancel" it
        return flask_internal_server_error(f"No Celery ID present for run {run_id}")

    # TODO: This only removes it from the queue... what if it's already executing?

    celery.control.revoke(run_log["celery_id"])
    update_run_state_and_commit(db, c, run["id"], STATE_CANCELING)

    # TODO: wait for revocation / failure and update status...


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
