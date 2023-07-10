import logging
import json
import sqlite3
import uuid

from bento_lib.events import EventBus
from bento_lib.events.notifications import format_notification
from bento_lib.events.types import EVENT_CREATE_NOTIFICATION, EVENT_WES_RUN_UPDATED
from flask import current_app, g
from urllib.parse import urljoin

from . import states
from .constants import SERVICE_ARTIFACT
from .events import get_flask_event_bus
from .utils import iso_now


__all__ = [
    "get_db",
    "close_db",
    "init_db",
    "finish_run",
    "update_stuck_runs",
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


NOTIFICATION_WES_RUN_FAILED = "wes_run_failed"
NOTIFICATION_WES_RUN_COMPLETED = "wes_run_completed"


def finish_run(
    db: sqlite3.Connection,
    c: sqlite3.Cursor,
    event_bus: EventBus,
    run: dict,
    state: str,
    logger: logging.Logger | None = None,
) -> None:
    """
    Updates a run's state, sets the run log's end time, and publishes an event corresponding with a run failure
    or a run success, depending on the state.
    :param db: A SQLite database connection
    :param c: An SQLite connection cursor
    :param event_bus: A chord_lib-defined event bus implementation for sending events
    :param run: The run which just finished
    :param state: The terminal state for the finished run
    :param logger: An optionally-provided logger object.
    :return:
    """

    run_id = run["run_id"]
    run_log_id = run["run_log"]["id"]
    end_time = iso_now()

    # Explicitly don't commit here to sync with state update
    c.execute("UPDATE run_logs SET end_time = ? WHERE id = ?", (end_time, run_log_id))
    update_run_state_and_commit(db, c, event_bus, run_id, state, logger=logger)

    if logger:
        logger.info(f"Run {run_id} finished with state {state} at {end_time}")

    if state in states.FAILURE_STATES:
        event_bus.publish_service_event(
            SERVICE_ARTIFACT,
            EVENT_CREATE_NOTIFICATION,
            format_notification(
                title="WES Run Failed",
                description=f"WES run '{run_id}' failed with state {state}",
                notification_type=NOTIFICATION_WES_RUN_FAILED,
                action_target=run_id
            )
        )

    elif state in states.SUCCESS_STATES:
        event_bus.publish_service_event(
            SERVICE_ARTIFACT,
            EVENT_CREATE_NOTIFICATION,
            format_notification(
                title="WES Run Completed",
                description=f"WES run '{run_id}' completed successfully",
                notification_type=NOTIFICATION_WES_RUN_COMPLETED,
                action_target=run_id
            )
        )


def update_stuck_runs(db: sqlite3.Connection):
    # Update all runs that have "stuck" states to have an error state instead on restart. This way, systems don't get
    # stuck checking their status, and if they're in a weird state at boot they should receive an error status anyway.

    event_bus = get_flask_event_bus()

    c = db.cursor()
    logger: logging.Logger = current_app.logger

    c.execute("SELECT id FROM runs WHERE state = ? OR state = ?", (states.STATE_INITIALIZING, states.STATE_RUNNING))
    stuck_run_ids: list[sqlite3.Row] = c.fetchall()

    for run, err in (get_run_details(c, r["id"]) for r in stuck_run_ids):
        if err:
            logger.error(f"Encountered error while updating stuck runs: {err}")
            continue

        logger.info(
            f"Found stuck run: {run['run_id']} at state {run['state']}. Setting state to {states.STATE_SYSTEM_ERROR}")
        finish_run(db, c, event_bus, run, states.STATE_SYSTEM_ERROR)

    db.commit()


def update_db():
    db = get_db()
    c = db.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='runs'")
    if c.fetchone() is None:
        init_db()
        return

    update_stuck_runs(db)

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


def _strip_first_slash(string: str) -> str:
    return string[1:] if len(string) > 0 and string[0] == "/" else string


def _stream_url(run_id: uuid.UUID | str, stream: str) -> str:
    return urljoin(current_app.config["SERVICE_BASE_URL"], f"runs/{str(run_id)}/{stream}")


def run_log_dict(run_id: uuid.UUID | str, run_log: sqlite3.Row) -> dict:
    return {
        "id": run_log["id"],  # TODO: This is non-WES-compliant
        "name": run_log["name"],
        "cmd": run_log["cmd"],
        "start_time": run_log["start_time"],
        "end_time": run_log["end_time"],
        "stdout": _stream_url(run_id, "stdout"),
        "stderr": _stream_url(run_id, "stderr"),
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


def get_task_logs(c: sqlite3.Cursor, run_id: uuid.UUID | str) -> list:
    c.execute("SELECT * FROM task_logs WHERE run_id = ?", (str(run_id),))
    return [task_log_dict(task_log) for task_log in c.fetchall()]


def get_run_details(c: sqlite3.Cursor, run_id: uuid.UUID | str) -> tuple[None, str] | tuple[dict, None]:
    # Runs, run requests, and run logs are created at the same time, so if any of them is missing return None.

    c.execute("SELECT * FROM runs WHERE id = ?", (str(run_id),))
    run = c.fetchone()
    if run is None:
        return None, "Missing entry in table 'runs'"

    c.execute("SELECT * from run_requests WHERE id = ?", (run["request"],))
    run_request = c.fetchone()
    if run_request is None:
        return None, "Missing entry in table 'run_requests'"

    c.execute("SELECT * from run_logs WHERE id = ?", (run["run_log"],))
    run_log = c.fetchone()
    if run_log is None:
        return None, "Missing entry in table 'run_logs'"

    c.execute("SELECT * FROM task_logs WHERE run_id = ?", (str(run_id),))

    return {
        "run_id": run["id"],
        "request": run_request_dict(run_request),
        "state": run["state"],
        "run_log": run_log_dict(run["id"], run_log),
        "task_logs": get_task_logs(c, run["id"]),
        "outputs": json.loads(run["outputs"])
    }, None


def update_run_state_and_commit(
    db: sqlite3.Connection,
    c: sqlite3.Cursor,
    event_bus: EventBus,
    run_id: uuid.UUID | str,
    state: str,
    logger: logging.Logger | None = None,
):
    if logger:
        logger.info(f"Updating run state of {run_id} to {state}")
    c.execute("UPDATE runs SET state = ? WHERE id = ?", (state, str(run_id)))
    db.commit()
    event_bus.publish_service_event(SERVICE_ARTIFACT, EVENT_WES_RUN_UPDATED, get_run_details(c, run_id)[0])
