import os
import requests
import shutil
import sqlite3
import subprocess
import uuid

from base64 import urlsafe_b64encode
from celery import Celery
from datetime import datetime
from flask import Flask, g, json, jsonify, request
from urllib.parse import urljoin, urlparse


MIME_TYPE = "application/json"
ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
ALLOWED_WORKFLOW_URL_SCHEMES = ("http", "https", "file")

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
    CELERY_RESULT_BACKEND="redis://",  # TODO
    CELERY_BROKER_URL="redis://",  # TODO
    DATABASE=os.environ.get("DATABASE", "chord_wes.db"),
    SERVICE_BASE_URL=os.environ.get("SERVICE_BASE_URL", "/"),
    SERVICE_TEMP=os.environ.get("SERVICE_TEMP", "tmp")
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


def make_error(status_code, message):
    return application.response_class(response=json.dumps({
        "msg": message,
        "status_code": status_code
    }), mimetype=MIME_TYPE, status=status_code)


def update_run_state(c, db, run_id, state):
    c.execute("UPDATE runs SET state = ? WHERE id = ?", (state, str(run_id)))
    db.commit()


@celery.task(bind=True)
def run_workflow(self, run_id, run_request, workflow_name):
    db = get_db()
    c = db.cursor()

    update_run_state(c, db, run_id, STATE_INITIALIZING)

    workflow_params = run_request["workflow_params"]
    workflow_url = run_request["workflow_url"]
    parsed_workflow_url = urlparse(workflow_url)  # TODO: Handle errors

    if parsed_workflow_url.scheme not in ALLOWED_WORKFLOW_URL_SCHEMES:
        # TODO: Handle file://
        update_run_state(c, db, run_id, STATE_SYSTEM_ERROR)
        return

    tmp_dir = application.config["SERVICE_TEMP"]
    workflow_path = os.path.join(tmp_dir, "workflow_{}.wdl".format(str(urlsafe_b64encode(bytes(workflow_url)))))
    workflow_params_path = os.path.join(tmp_dir, "params_{}.wdl".format(run_id))
    # TODO: Check UUID collision

    with open(workflow_params_path, "w") as wpf:
        json.dump(workflow_params, wpf)

    cmd = ["toil-wdl-runner", workflow_path, workflow_params_path]

    # Create run log

    run_log_id = uuid.uuid4()
    c.execute("INSERT INTO run_logs (id, name, cmd, celery_id) VALUES (?, ?, ?, ?)",
              (str(run_log_id), workflow_name, " ".join(cmd), self.request.id))
    c.execute("UPDATE runs SET run_log = ? WHERE id = ?", (str(run_log_id), str(run_id)))
    db.commit()

    # Download or move workflow if needed

    os.makedirs(tmp_dir)
    if not os.path.exists(workflow_path):
        # If the workflow has not been downloaded, download it
        # TODO: Auth
        if parsed_workflow_url.scheme in ("http", "https"):
            wr = requests.get(workflow_url)
            if wr.status_code == 200:
                with open(workflow_path, "wb") as nwf:
                    nwf.write(wr.content)
            else:
                # Network issues
                update_run_state(c, db, run_id, STATE_SYSTEM_ERROR)
                return
        else:
            # file://
            # TODO: SPEC: MAKE SURE COPIED FILES AREN'T OUTSIDE OF ALLOWED AREAS!
            # TODO: SECURITY FLAW
            shutil.copyfile(parsed_workflow_url.path, workflow_path)

    # TODO: Validate WDL

    # TODO: Initialization, input file downloading, etc.

    # Start run

    wdl_runner = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    update_run_state(c, db, run_id, STATE_RUNNING)
    c.execute("UPDATE run_logs SET start_time = ? WHERE id = ?",
              (datetime.utcnow().strftime(ISO_FORMAT), str(run_log_id)))

    # Wait for output

    try:
        output, errors = wdl_runner.communicate(timeout=60 * 60 * 24)  # TODO: Configurable timeout, not one day
        return_code = wdl_runner.returncode

        # TODO: Upload data if return_code == 0

        update_run_state(c, db, run_id, STATE_EXECUTOR_ERROR if return_code != 0 else STATE_COMPLETE)

        # TODO: Output

    except subprocess.TimeoutExpired:
        wdl_runner.kill()
        # TODO: Status, Output
        output, errors = wdl_runner.communicate()
        return_code = wdl_runner.returncode

        update_run_state(c, db, run_id, STATE_SYSTEM_ERROR)

    c.execute("UPDATE run_logs SET end_time = ?, stdout = ?, stderr = ?, exit_code = ? WHERE id = ?",
              (datetime.utcnow().strftime(ISO_FORMAT), output.read(), errors.read(), return_code, run_log_id))
    db.commit()

    # Final steps: check status and report results

    c.execute("SELECT * FROM runs WHERE id = ?", (str(run_id),))
    run = c.fetchone()  # TODO: What if it's gone? Hope not
    if run["state"] == STATE_COMPLETE:
        # TODO: Upload results to data service or report them some other way
        pass
    else:
        # TODO: Report error somehow
        pass


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
            assert "workflow_attachment" in request.form
            assert "tags" in request.form

            workflow_params = json.loads(request.form["workflow_params"])
            workflow_type = request.form["workflow_type"].upper().strip()
            workflow_type_version = request.form["workflow_type_version"].strip()
            workflow_engine_parameters = json.loads(request.form["workflow_engine_parameters"])
            workflow_url = request.form["workflow_url"].lower()
            workflow_attachment_list = request.files.getlist("workflow_attachment")  # TODO
            tags = json.loads(request.form["tags"])

            workflow_name = tags["workflow_name"] if "workflow_name" in tags else workflow_url

            # Don't accept anything (ex. CWL) other than WDL
            assert workflow_type == "WDL"
            assert workflow_type_version == "1.0"

            assert isinstance(workflow_params, dict)
            assert isinstance(workflow_engine_parameters, dict)
            assert isinstance(tags, dict)

            # TODO

            req_id = uuid.uuid4()
            run_id = uuid.uuid4()

            # Will be updated to STATE_ QUEUED once submitted
            c.execute("INSERT INTO run_requests (id, workflow_params, workflow_type, workflow_type_version, "
                      "workflow_url) VALUES (?, ?, ?, ?, ?)",
                      (str(req_id), json.dumps(workflow_params), workflow_type, workflow_type_version, workflow_url))
            c.execute("INSERT INTO runs (id, request, state, run_log, outputs) VALUES (?, ?, ?, ?, ?)",
                      (str(run_id), str(req_id), STATE_UNKNOWN, None, json.dumps({})))  # TODO: What goes in output?
            db.commit()

            # TODO

            # TODO: arguments
            # TODO: figure out timeout
            # TODO: retry policy
            c.execute("UPDATE runs SET state = ? WHERE id = ?", (STATE_QUEUED, str(run_id)))
            db.commit()
            run_workflow.delay(run_id, {
                "workflow_params": workflow_params,
                "workflow_url": workflow_url
            }, workflow_name)

            # TODO

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

    c.execute("SELECT * FROM runs AS r, run_requests AS rr, run_logs AS rl "
              "WHERE r.id = ? AND r.request = rr.id AND r.run_log = rl.id",
              (str(run_id),))
    run = c.fetchone()

    if run is None:
        return make_error(404, "Not found")

    c.execute("SELECT * FROM task_logs WHERE run_id = ?", (str(run_id),))

    return jsonify({
        "run_id": run["r.id"],
        "request": {
            "workflow_params": json.loads(run["rr.workflow_params"]),
            "workflow_type": run["rr.workflow_type"],
            "workflow_type_version": run["rr.workflow_type_version"],
            "workflow_engine_parameters": {},
            "workflow_url": run["rr.workflow_url"],
            "tags": {}
        },
        "state": run["r.state"],
        "run_log": {
            "name": run["rl.name"],
            "cmd": run["rl.cmd"],
            "start_time": run["rl.start_time"],
            "end_time": run["rl.end_time"],
            "stdout": urljoin(
                urljoin(application.config["CHORD_URL"], application.config["SERVICE_BASE_URL"] + "/"),
                "runs/{}/stdout".format(run["r.id"])
            ),
            "stderr": urljoin(
                urljoin(application.config["CHORD_URL"], application.config["SERVICE_BASE_URL"] + "/"),
                "runs/{}/stderr".format(run["r.id"])
            ),
            "exit_code": run["rl.exit_code"]
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
        "outputs": {}  # TODO: ?
    })


def get_stream(c, stream, run_id):
    c.execute("SELECT * FROM runs AS r, run_logs AS rl WHERE r.id = ? AND r.run_log = rl.id", (str(run_id),))
    run = c.fetchone()

    if run is None:
        return make_error(404, "Not found")

    return application.response_class(response=run["rl.{}".format(stream)], mimetype="text/plain", status=200)


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
        return make_error(500, "No Celery ID present")

    celery.control.revoke(run_log["celery_id"])
    update_run_state(c, db, run["id"], STATE_CANCELING)

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
@application.route("/service_info", methods=["GET"])
def service_info():
    pass
