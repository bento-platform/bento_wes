import json
import os
import uuid

from flask import Blueprint, current_app, jsonify, request
from urllib.parse import urljoin
from werkzeug.utils import secure_filename

from .celery import celery
from .states import *
from .runner import update_run_state, run_workflow

from .db import get_db


bp_runs = Blueprint("runs", __name__)


def make_error(status_code: int, message: str):
    return jsonify({
        "msg": message,
        "status_code": status_code
    }), status_code


@bp_runs.route("/runs", methods=["GET", "POST"])
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

            chord_mode = ("workflow_id" in tags and "workflow_metadata" in tags and "ingestion_url" in tags
                          and "dataset_id" in tags)  # TODO: table_id

            workflow_id = tags.get("workflow_id", workflow_url)
            workflow_metadata = tags.get("workflow_metadata", {})
            workflow_ingestion_url = tags.get("ingestion_url", None)
            table_id = tags.get("dataset_id", None)  # TODO: table_id

            # Don't accept anything (ex. CWL) other than WDL TODO: CWL support
            assert workflow_type == "WDL"
            assert workflow_type_version == "1.0"

            assert isinstance(workflow_params, dict)
            assert isinstance(workflow_engine_parameters, dict)
            assert isinstance(tags, dict)

            if chord_mode:
                table_id = str(uuid.UUID(table_id))  # Check and standardize dataset ID

            # TODO: Use JSON schemas for workflow params / engine parameters / tags

            # Begin creating the job after validating the request

            req_id = uuid.uuid4()
            run_id = uuid.uuid4()
            log_id = uuid.uuid4()

            # Create run directory

            run_dir = os.path.join(current_app.config["SERVICE_TEMP"], str(run_id))

            if os.path.exists(run_dir):
                return make_error(500, "UUID collision")

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

            run_workflow.delay(run_id, chord_mode, workflow_metadata, workflow_ingestion_url, table_id)

            return jsonify({"run_id": str(run_id)})

        except (ValueError, AssertionError):
            return make_error(400, "Invalid request")

    c.execute("SELECT * FROM runs")

    return jsonify([{
        "run_id": run["id"],
        "state": run["state"]
    } for run in c.fetchall()])


@bp_runs.route("/runs/<uuid:run_id>", methods=["GET"])
def run_detail(run_id):
    db = get_db()
    c = db.cursor()

    # Runs, run requests, and run logs are created at the same time, so if either of them is missing throw a 404.

    c.execute("SELECT * FROM runs WHERE id = ?", (str(run_id),))
    run = c.fetchone()

    if run is None:
        return make_error(404, "Not found")

    c.execute("SELECT * from run_requests WHERE id = ?", (run["request"],))
    run_request = c.fetchone()

    if run_request is None:
        return make_error(404, "Not found")

    c.execute("SELECT * from run_logs WHERE id = ?", (run["run_log"],))
    run_log = c.fetchone()

    if run_log is None:
        return make_error(404, "Not found")

    c.execute("SELECT * FROM task_logs WHERE run_id = ?", (str(run_id),))

    return jsonify({
        "run_id": run["id"],
        "request": {
            "workflow_params": json.loads(run_request["workflow_params"]),
            "workflow_type": run_request["workflow_type"],
            "workflow_type_version": run_request["workflow_type_version"],
            "workflow_engine_parameters": json.loads(run_request["workflow_engine_parameters"]),  # TODO
            "workflow_url": run_request["workflow_url"],
            "tags": json.loads(run_request["tags"])
        },
        "state": run["state"],
        "run_log": {
            "name": run_log["name"],
            "cmd": run_log["cmd"],
            "start_time": run_log["start_time"],
            "end_time": run_log["end_time"],
            "stdout": urljoin(
                urljoin(current_app.config["CHORD_URL"], current_app.config["SERVICE_URL_BASE_PATH"] + "/"),
                "runs/{}/stdout".format(run["id"])
            ),
            "stderr": urljoin(
                urljoin(current_app.config["CHORD_URL"], current_app.config["SERVICE_URL_BASE_PATH"] + "/"),
                "runs/{}/stderr".format(run["id"])
            ),
            "exit_code": run_log["exit_code"]
        },
        "task_logs": [{
            "name": task["name"],
            "cmd": task["cmd"],
            "start_time": task["start_time"],
            "end_time": task["end_time"],
            "stdout": task["stdout"],
            "stderr": task["stderr"],
            "exit_code": task["exit_code"]
        } for task in c.fetchall()],
        "outputs": json.loads(run["outputs"])
    })


def get_stream(c, stream, run_id):
    c.execute("SELECT * FROM runs AS r, run_logs AS rl WHERE r.id = ? AND r.run_log = rl.id", (str(run_id),))
    run = c.fetchone()

    if run is None:
        return make_error(404, "Not found")

    return current_app.response_class(response=run[stream], mimetype="text/plain", status=200)


@bp_runs.route("/runs/<uuid:run_id>/stdout", methods=["GET"])
def run_stdout(run_id):
    db = get_db()
    return get_stream(db.cursor(), "stdout", run_id)


@bp_runs.route("/runs/<uuid:run_id>/stderr", methods=["GET"])
def run_stderr(run_id):
    db = get_db()
    return get_stream(db.cursor(), "stderr", run_id)


@bp_runs.route("/runs/<uuid:run_id>/cancel", methods=["POST"])
def run_cancel(run_id):
    # TODO: Check if already completed
    # TODO: Check if run log exists
    # TODO: from celery.task.control import revoke; revoke(celery_id, terminate=True)
    db = get_db()
    c = db.cursor()

    c.execute("SELECT * FROM runs WHERE id = ?", (str(run_id),))
    run = c.fetchone()

    if run is None:
        return make_error(404, "Not found")

    if run["state"] in (STATE_CANCELING, STATE_CANCELED):
        return make_error(500, "Already cancelled")

    if run["state"] in (STATE_SYSTEM_ERROR, STATE_EXECUTOR_ERROR):
        return make_error(500, "Already terminated with error")

    if run["state"] == STATE_COMPLETE:
        return make_error(500, "Already completed")

    c.execute("SELECT * FROM run_logs WHERE id = ?", (run["run_log"],))
    run_log = c.fetchone()

    if run_log is None:
        return make_error(500, "No run log present")

    if run_log["celery_id"] is None:
        # Never made it into the queue, so "cancel" it

        return make_error(500, "No Celery ID present")

    # TODO: This only removes it from the queue... what if it's already executing?

    celery.control.revoke(run_log["celery_id"])
    update_run_state(db, c, run["id"], STATE_CANCELING)

    # TODO: wait for revocation / failure and update status...


@bp_runs.route("/runs/<uuid:run_id>/status", methods=["GET"])
def run_status(run_id):
    db = get_db()
    c = db.cursor()

    c.execute("SELECT * FROM runs WHERE id = ?", (str(run_id),))
    run = c.fetchone()

    if run is None:
        return make_error(404, "Not found")

    return jsonify({
        "run_id": run["id"],
        "state": run["state"]
    })
