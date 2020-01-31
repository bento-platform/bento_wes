import json
import sqlite3
import uuid

from chord_lib.events import EventBus
from chord_lib.events.types import EVENT_WES_RUN_UPDATED
from flask import current_app, g
from typing import Optional, Union
from urllib.parse import urljoin

from .constants import SERVICE_ARTIFACT
from .states import *


__all__ = [
    "get_db",
    "close_db",
    "init_db",
    "update_db",
    "run_request_dict",
    "run_log_dict",
    "task_log_dict",
    "get_task_logs",
    "get_run_details",
    "update_run_state_and_commit",
]


def get_db() -> sqlite3.Connection:
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


def run_request_dict(run_request: sqlite3.Row) -> dict:
    return {
        "workflow_params": json.loads(run_request["workflow_params"]),
        "workflow_type": run_request["workflow_type"],
        "workflow_type_version": run_request["workflow_type_version"],
        "workflow_engine_parameters": json.loads(run_request["workflow_engine_parameters"]),  # TODO
        "workflow_url": run_request["workflow_url"],
        "tags": json.loads(run_request["tags"])
    }


def run_log_dict(run_id: Union[uuid.UUID, str], run_log: sqlite3.Row) -> dict:
    return {
        "name": run_log["name"],
        "cmd": run_log["cmd"],
        "start_time": run_log["start_time"],
        "end_time": run_log["end_time"],
        "stdout": urljoin(
            urljoin(current_app.config["CHORD_URL"], current_app.config["SERVICE_URL_BASE_PATH"] + "/"),
            "runs/{}/stdout".format(str(run_id))
        ),
        "stderr": urljoin(
            urljoin(current_app.config["CHORD_URL"], current_app.config["SERVICE_URL_BASE_PATH"] + "/"),
            "runs/{}/stderr".format(str(run_id))
        ),
        "exit_code": run_log["exit_code"]
    }


def task_log_dict(task_log: sqlite3.Row) -> dict:
    return {
        "name": task_log["name"],
        "cmd": task_log["cmd"],
        "start_time": task_log["start_time"],
        "end_time": task_log["end_time"],
        "stdout": task_log["stdout"],
        "stderr": task_log["stderr"],
        "exit_code": task_log["exit_code"]
    }


def get_task_logs(c: sqlite3.Cursor, run_id: Union[uuid.UUID, str]) -> list:
    c.execute("SELECT * FROM task_logs WHERE run_id = ?", (str(run_id),))
    return [task_log_dict(task_log) for task_log in c.fetchall()]


def get_run_details(c: sqlite3.Cursor, run_id: Union[uuid.UUID, str]) -> Optional[dict]:
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
        "request": run_request_dict(run_request),
        "state": run["state"],
        "run_log": run_log_dict(run["id"], run_log),
        "task_logs": get_task_logs(c, run["id"]),
        "outputs": json.loads(run["outputs"])
    }


def update_run_state_and_commit(db: sqlite3.Connection, c: sqlite3.Cursor, event_bus: EventBus,
                                run_id: Union[uuid.UUID, str], state: str):
    c.execute("UPDATE runs SET state = ? WHERE id = ?", (state, str(run_id)))
    db.commit()
    event_bus.publish_service_event(SERVICE_ARTIFACT, EVENT_WES_RUN_UPDATED, get_run_details(c, run_id))
