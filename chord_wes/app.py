import chord_lib.ingestion
import chord_wes
import os
import re
import requests
import shutil
import sqlite3
import subprocess
import uuid

from base64 import urlsafe_b64encode
from celery import Celery
from datetime import datetime
from flask import Flask, g, json, jsonify, request
from typing import Optional
from urllib.parse import urljoin, urlparse


MIME_TYPE = "application/json"
ALLOWED_WORKFLOW_URL_SCHEMES = ("http", "https", "file")
ALLOWED_WORKFLOW_REQUEST_SCHEMES = ("http", "https")

WORKFLOW_TIMEOUT = 60 * 60 * 24  # 24 hours

MAX_WDL_BYTES = 10000000  # 10 Mb

STATE_UNKNOWN = "UNKNOWN"
STATE_QUEUED = "QUEUED"
STATE_INITIALIZING = "INITIALIZING"
STATE_RUNNING = "RUNNING"
STATE_PAUSED = "PAUSED"
STATE_COMPLETE = "COMPLETE"
STATE_EXECUTOR_ERROR = "EXECUTOR_ERROR"
STATE_SYSTEM_ERROR = "SYSTEM_ERROR"
STATE_CANCELED = "CANCELED"
STATE_CANCELING = "CANCELING"

# Spec: https://software.broadinstitute.org/wdl/documentation/spec#whitespace-strings-identifiers-constants
WDL_WORKSPACE_NAME_REGEX = re.compile(r"workflow\s+([a-zA-Z][a-zA-Z0-9_]+)")


def make_celery(app):
    c = Celery(app.import_name, backend=app.config["CELERY_RESULT_BACKEND"], broker=app.config["CELERY_BROKER_URL"])
    c.conf.update(app.config)

    class ContextTask(c.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                self.run(*args, **kwargs)

    # noinspection PyPropertyAccess
    c.Task = ContextTask
    return c


application = Flask(__name__)
application.config.from_mapping(
    CHORD_SERVICES=os.environ.get("CHORD_SERVICES", "chord_services.json"),
    CHORD_URL=os.environ.get("CHORD_URL", "http://127.0.0.1:5000/"),
    CELERY_RESULT_BACKEND=os.environ.get("CELERY_RESULT_BACKEND", "redis://"),
    CELERY_BROKER_URL=os.environ.get("CELERY_BROKER_URL", "redis://"),
    DATABASE=os.environ.get("DATABASE", "chord_wes.db"),
    SERVICE_BASE_URL=os.environ.get("SERVICE_BASE_URL", "/"),
    SERVICE_TEMP=os.environ.get("SERVICE_TEMP", "tmp"),
    WOM_TOOL_LOCATION=os.environ.get("WOM_TOOL_LOCATION", "womtool.jar")
)
celery = make_celery(application)


with open(application.config["CHORD_SERVICES"]) as cf:
    SERVICES = json.load(cf)


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(application.config["DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row

    return g.db


def close_db(_e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()
    c = db.cursor()

    with application.open_resource("schema.sql") as sf:
        c.executescript(sf.read().decode("utf-8"))

    db.commit()


def update_db():
    db = get_db()
    c = db.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='runs'")
    if c.fetchone() is None:
        init_db()
        return

    # TODO


application.teardown_appcontext(close_db)

with application.app_context():
    if not os.path.exists(os.path.join(os.getcwd(), application.config["DATABASE"])):
        init_db()
    else:
        update_db()


def make_error(status_code: int, message: str):
    return application.response_class(response=json.dumps({
        "msg": message,
        "status_code": status_code
    }), mimetype=MIME_TYPE, status=status_code)


def update_run_state(db, c, run_id, state):
    c.execute("UPDATE runs SET state = ? WHERE id = ?", (state, str(run_id)))
    db.commit()


def iso_now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO date format


def finish_run(db, c, run_id, run_log_id, run_dir, state):
    c.execute("UPDATE run_logs SET end_time = ? WHERE id = ?", (iso_now(), run_log_id))
    db.commit()

    update_run_state(db, c, run_id, state)

    if run_dir is None:
        return

    # Clean up any run files at the end, after they've been either copied or "rejected" due to some failure.
    # TODO: SECURITY: Check run_dir
    # TODO: May want to keep them around for a retry depending on how the retry operation will work.

    shutil.rmtree(run_dir, ignore_errors=True)


@celery.task(bind=True)
def run_workflow(self, run_id: uuid.UUID, run_request: dict, chord_mode: bool, c_workflow_metadata: dict,
                 c_workflow_ingestion_url: Optional[str], c_dataset_id: Optional[str]):
    db = get_db()
    c = db.cursor()

    # Check that workflow ingestion URL is set if CHORD mode is on

    if chord_mode and c_workflow_ingestion_url is None:
        print("An ingestion URL must be set.")
        return

    # TODO: Check workflow_ingestion_url is valid

    # Fetch run from the database, checking that it exists

    c.execute("SELECT run_log FROM runs WHERE id = ?", (str(run_id),))
    run = c.fetchone()
    if run is None:
        print("Cannot find run {}".format(run_id))
        return

    # Check run log exists

    c.execute("SELECT * FROM run_logs WHERE id = ?", (run["run_log"],))
    if c.fetchone() is None:
        print("Cannot find run log {} for run {}".format(run["run_log"], run_id))
        return

    # Begin initialization (loading / downloading files)

    update_run_state(db, c, run_id, STATE_INITIALIZING)

    workflow_params = run_request["workflow_params"]
    workflow_url = run_request["workflow_url"]
    parsed_workflow_url = urlparse(workflow_url)  # TODO: Handle errors

    # Check that the URL scheme is something that can be either moved or downloaded

    if parsed_workflow_url.scheme not in ALLOWED_WORKFLOW_URL_SCHEMES:
        finish_run(db, c, run_id, run["run_log"], None, STATE_SYSTEM_ERROR)
        return

    tmp_dir = application.config["SERVICE_TEMP"]
    run_dir = os.path.join(tmp_dir, str(run_id))

    os.makedirs(run_dir, exist_ok=True)

    workflow_path = os.path.join(tmp_dir, "workflow_{}.wdl".format(
        str(urlsafe_b64encode(bytes(workflow_url, encoding="utf-8")), encoding="utf-8")))
    workflow_params_path = os.path.join(run_dir, "params.json")
    # TODO: Check UUID collision

    # Store input strings for the WDL file in a JSON file in the temporary folder

    with open(workflow_params_path, "w") as wpf:
        json.dump(workflow_params, wpf)

    # Run the WDL with the Toil runner, placing all outputs into the job directory

    cmd = ("toil-wdl-runner", workflow_path, workflow_params_path, "-o", run_dir)

    # Update run log with command and Celery ID

    c.execute("UPDATE run_logs SET cmd = ?, celery_id = ? WHERE id = ?",
              (" ".join(cmd), self.request.id, run["run_log"]))
    db.commit()

    # Download or move workflow

    # TODO: Auth
    if parsed_workflow_url.scheme in ALLOWED_WORKFLOW_REQUEST_SCHEMES:
        try:
            wr = requests.get(workflow_url)

            if wr.status_code == 200 and len(wr.content) < MAX_WDL_BYTES:
                if os.path.exists(workflow_path):
                    os.remove(workflow_path)

                with open(workflow_path, "wb") as nwf:
                    nwf.write(wr.content)

            elif not os.path.exists(workflow_path):  # Use cached version if needed, otherwise error
                # Request issues
                finish_run(db, c, run_id, run["run_log"], run_dir, STATE_SYSTEM_ERROR)
                return

        except requests.exceptions.ConnectionError:
            if not os.path.exists(workflow_path):  # Use cached version if needed, otherwise error
                # Network issues
                finish_run(db, c, run_id, run["run_log"], run_dir, STATE_SYSTEM_ERROR)
                return

    else:
        # file://
        # TODO: Handle exceptions
        shutil.copyfile(parsed_workflow_url.path, workflow_path)

    # Validate WDL, listing dependencies
    #  - since Toil doesn't support imports right now, any dependencies will result in an error

    # Check for java first
    try:
        subprocess.run(["java", "-version"])
    except FileNotFoundError:
        finish_run(db, c, run_id, run["run_log"], run_dir, STATE_SYSTEM_ERROR)
        return

    vr = subprocess.Popen(["java", "-jar", application.config["WOM_TOOL_LOCATION"], "validate", "-1", workflow_path],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

    v_out, _v_err = vr.communicate()

    if vr.returncode != 0:
        # Validation error with WDL file
        # TODO: Add some stdout or stderr to logs?
        finish_run(db, c, run_id, run["run_log"], run_dir, STATE_EXECUTOR_ERROR)
        return

    if "None" not in v_out:  # No dependencies
        # Toil can't process WDL dependencies right now  TODO
        # TODO: Add some stdout or stderr to logs?
        finish_run(db, c, run_id, run["run_log"], run_dir, STATE_EXECUTOR_ERROR)
        return

    # TODO: SECURITY: MAKE SURE NOTHING REFERENCED IS OUTSIDE OF ALLOWED AREAS!
    # TODO: SECURITY: Maybe don't allow external downloads, only run things in the container?

    # Find "real" (WDL) workflow name from WDL file

    with open(workflow_path, "r") as wdf:
        wdl_contents = wdf.read()
        workflow_id_match = WDL_WORKSPACE_NAME_REGEX.search(wdl_contents)

        if not workflow_id_match:
            # Invalid/non-workflow-specifying WDL file
            # TODO: Validate before this
            finish_run(db, c, run_id, run["run_log"], run_dir, STATE_SYSTEM_ERROR)
            return

        workflow_id = workflow_id_match.group(1)

    # TODO: To avoid having multiple names, we should maybe only set this once?
    c.execute("UPDATE run_logs SET name = ? WHERE id = ?", (workflow_id, run["run_log"],))

    # TODO: Initialization, input file downloading, etc.

    # Start run

    wdl_runner = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
    update_run_state(db, c, run_id, STATE_RUNNING)
    c.execute("UPDATE run_logs SET start_time = ? WHERE id = ?", (iso_now(), run["run_log"]))

    # Wait for output

    timed_out = False

    try:
        stdout, stderr = wdl_runner.communicate(timeout=WORKFLOW_TIMEOUT)
        exit_code = wdl_runner.returncode

    except subprocess.TimeoutExpired:
        wdl_runner.kill()
        stdout, stderr = wdl_runner.communicate()
        exit_code = wdl_runner.returncode

        timed_out = True

    c.execute("UPDATE run_logs SET stdout = ?, stderr = ?, exit_code = ? WHERE id = ?",
              (stdout, stderr, exit_code, run["run_log"]))
    db.commit()

    # Final steps: check exit code and report results

    if exit_code == 0 and not timed_out:
        try:
            if chord_mode:
                # TODO: Verify ingestion URL (vulnerability??)

                output_params = chord_lib.ingestion.make_output_params(workflow_id, workflow_params,
                                                                       c_workflow_metadata["inputs"])

                # TODO: Allow outputs to be served over different URL schemes instead of just an absolute file location

                workflow_outputs = {}
                for f in c_workflow_metadata["outputs"]:
                    workflow_outputs[f] = os.path.abspath(
                        os.path.join(run_dir, chord_lib.ingestion.output_file_name(f, output_params)))

                c.execute("UPDATE runs SET outputs = ? WHERE id = ?", (json.dumps(workflow_outputs), str(run_id)))

                # TODO: Just post run ID, fetch rest from the WES service?
                # TODO: This is CHORD-specific, move it out into a callback or something...

                r = requests.post(c_workflow_ingestion_url, {
                    "dataset_id": c_dataset_id,
                    "workflow_id": workflow_id,
                    "workflow_metadata": json.dumps(c_workflow_metadata),
                    "workflow_outputs": json.dumps(workflow_outputs),
                    "workflow_params": json.dumps(workflow_params)
                })

                if str(r.status_code)[0] != "2":  # If non-2XX error code
                    # Ingestion failed for some reason
                    finish_run(db, c, run_id, run["run_log"], run_dir, STATE_SYSTEM_ERROR)
                    return

            c.execute("UPDATE run_logs SET end_time = ? WHERE id = ?", (iso_now(), run["run_log"]))
            db.commit()

            finish_run(db, c, run_id, run["run_log"], run_dir, STATE_COMPLETE)

        except requests.exceptions.ConnectionError:
            # Ingestion failed due to a network error
            # TODO: Retry a few times...
            # TODO: Report error somehow
            finish_run(db, c, run_id, run["run_log"], run_dir, STATE_SYSTEM_ERROR)

    elif exit_code == 1 and not timed_out:
        # TODO: Report error somehow
        finish_run(db, c, run_id, run["run_log"], run_dir, STATE_EXECUTOR_ERROR)

    else:
        # TODO: Report error somehow
        finish_run(db, c, run_id, run["run_log"], run_dir, STATE_SYSTEM_ERROR)


@application.route("/runs", methods=["GET", "POST"])
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
            # assert "workflow_attachment" in request.form  # TODO: Fix
            assert "tags" in request.form

            workflow_params = json.loads(request.form["workflow_params"])
            workflow_type = request.form["workflow_type"].upper().strip()
            workflow_type_version = request.form["workflow_type_version"].strip()
            workflow_engine_parameters = json.loads(request.form["workflow_engine_parameters"])  # TODO: Unused
            workflow_url = request.form["workflow_url"].lower()
            # workflow_attachment_list = request.files.getlist("workflow_attachment")  # TODO
            tags = json.loads(request.form["tags"])

            # TODO: Move CHORD-specific stuff out somehow?

            # Only "turn on" CHORD-specific features if specific tags are present

            chord_mode = ("workflow_id" in tags and "workflow_metadata" in tags and "ingestion_url" in tags
                          and "dataset_id" in tags)

            workflow_id = tags.get("workflow_id", workflow_url)
            workflow_metadata = tags.get("workflow_metadata", {})
            workflow_ingestion_url = tags.get("ingestion_url", None)
            dataset_id = tags.get("dataset_id", None)

            # Don't accept anything (ex. CWL) other than WDL
            assert workflow_type == "WDL"
            assert workflow_type_version == "1.0"

            assert isinstance(workflow_params, dict)
            assert isinstance(workflow_engine_parameters, dict)
            assert isinstance(tags, dict)

            if chord_mode:
                dataset_id = str(uuid.UUID(dataset_id))  # Check and standardize dataset ID

            # TODO: Use JSON schemas for workflow params / engine parameters / tags

            # Begin creating the job after validating the request

            req_id = uuid.uuid4()
            run_id = uuid.uuid4()
            log_id = uuid.uuid4()

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
            run_workflow.delay(run_id, {
                "workflow_params": workflow_params,
                "workflow_url": workflow_url
            }, chord_mode, workflow_metadata, workflow_ingestion_url, dataset_id)

            return jsonify({"run_id": str(run_id)})

        except (ValueError, AssertionError):
            return make_error(400, "Invalid request")

    c.execute("SELECT * FROM runs")

    return jsonify([{
        "run_id": run["id"],
        "state": run["state"]
    } for run in c.fetchall()])


@application.route("/runs/<uuid:run_id>", methods=["GET"])
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
                urljoin(application.config["CHORD_URL"], application.config["SERVICE_BASE_URL"] + "/"),
                "runs/{}/stdout".format(run["id"])
            ),
            "stderr": urljoin(
                urljoin(application.config["CHORD_URL"], application.config["SERVICE_BASE_URL"] + "/"),
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

    return application.response_class(response=run[stream], mimetype="text/plain", status=200)


@application.route("/runs/<uuid:run_id>/stdout", methods=["GET"])
def run_stdout(run_id):
    db = get_db()
    return get_stream(db.cursor(), "stdout", run_id)


@application.route("/runs/<uuid:run_id>/stderr", methods=["GET"])
def run_stderr(run_id):
    db = get_db()
    return get_stream(db.cursor(), "stderr", run_id)


@application.route("/runs/<uuid:run_id>/cancel", methods=["POST"])
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


@application.route("/runs/<uuid:run_id>/status", methods=["GET"])
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


# TODO: Not compatible with GA4GH WES due to conflict with GA4GH service-info (preferred)
@application.route("/service-info", methods=["GET"])
def service_info():
    return jsonify({
        "id": "ca.distributedgenomics.chord_wes",  # TODO: Should be globally unique?
        "name": "CHORD WES",  # TODO: Should be globally unique?
        "type": "ca.distributedgenomics:chord_wes:{}".format(chord_wes.__version__),  # TODO
        "description": "Workflow execution service for a CHORD application.",
        "organization": {
            "name": "GenAP",
            "url": "https://genap.ca/"
        },
        "contactUrl": "mailto:david.lougheed@mail.mcgill.ca",
        "version": chord_wes.__version__
    })
