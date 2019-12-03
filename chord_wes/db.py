import json
import sqlite3

from flask import current_app, g
from typing import Optional
from urllib.parse import urljoin

from .constants import SERVICE_ARTIFACT
from .events import *
from .states import *


__all__ = [
    "get_db",
    "close_db",
    "init_db",
    "update_db",
    "get_run_details",
    "update_run_state",
]


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(current_app.config["DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row

    return g.db


def close_db(_e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()
    c = db.cursor()

    with current_app.open_resource("schema.sql") as sf:
        c.executescript(sf.read().decode("utf-8"))

    db.commit()


def update_db():
    db = get_db()
    c = db.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='runs'")
    if c.fetchone() is None:
        init_db()
        return

    # Update all runs that have "stuck" states to have an error state instead on restart. This way, systems don't get
    # stuck checking their status, and if they're in a weird state at boot they should receive an error status anyway.
    c.execute("UPDATE runs SET state = ? WHERE state = ? OR state = ?",
              (STATE_SYSTEM_ERROR, STATE_INITIALIZING, STATE_RUNNING))
    db.commit()

    # TODO: Migrations if needed


def get_run_details(c, run_id) -> Optional[dict]:
    # Runs, run requests, and run logs are created at the same time, so if any of them is missing return None.

    c.execute("SELECT * FROM runs WHERE id = ?", (str(run_id),))
    run = c.fetchone()
    if run is None:
        return None

    c.execute("SELECT * from run_requests WHERE id = ?", (run["request"],))
    run_request = c.fetchone()
    if run_request is None:
        return None

    c.execute("SELECT * from run_logs WHERE id = ?", (run["run_log"],))
    run_log = c.fetchone()
    if run_log is None:
        return None

    c.execute("SELECT * FROM task_logs WHERE run_id = ?", (str(run_id),))

    return {
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
    }


def update_run_state(db, c, run_id, state):
    c.execute("UPDATE runs SET state = ? WHERE id = ?", (state, str(run_id)))
    db.commit()
    if event_bus is not None:
        event_bus.publish_service_event(SERVICE_ARTIFACT, EVENT_WES_RUN_UPDATED, get_run_details(c, run_id))
