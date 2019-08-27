import os
import sqlite3
import uuid

from celery import Celery
from flask import Flask, g, json, jsonify, request


MIME_TYPE = "application/json"

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
    CELERY_RESULT_BACKEND="redis://",  # TODO
    CELERY_BROKER_URL="redis://",  # TODO
    DATABASE=os.environ.get("DATABASE", "chord_wes.db")
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


@celery.task()
def run_workflow(run_id):
    db = get_db()
    c = db.cursor()

    c.execute("UPDATE runs SET state = ? WHERE id = ?", (STATE_INITIALIZING, str(run_id)))
    db.commit()

    # TODO
    # TODO: Check status


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

            # Don't accept anything (ex. CWL) other than WDL
            assert workflow_type == "WDL"
            assert workflow_type_version == "1.0"

            assert isinstance(workflow_params, dict)
            assert isinstance(workflow_engine_parameters, dict)
            assert isinstance(tags, dict)

            # TODO

            req_id = uuid.uuid4()
            run_id = uuid.uuid4()

            # Will be updated to queued once submitted
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
            run_workflow.delay(run_id)
            db.commit()

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
            "stdout": run["rl.stdout"],
            "stderr": run["rl.stderr"],
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


@application.route("/runs/<uuid:run_id>/cancel", methods=["POST"])
def run_cancel(run_id):
    pass


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
