import logging
import json
import sqlite3
import uuid

from bento_lib.events import EventBus
from bento_lib.events.notifications import format_notification
from bento_lib.events.types import EVENT_CREATE_NOTIFICATION, EVENT_WES_RUN_UPDATED
from flask import current_app, g
from typing import Any
from urllib.parse import urljoin

from . import states
from .constants import SERVICE_ARTIFACT
from .events import get_flask_event_bus
from .models import RunLog, RunRequest, Run, RunWithDetailsAndOutput
from .types import RunStream
from .utils import iso_now


__all__ = [
    "Database",
    "get_db",
    "close_db",
    "init_db",
    "update_db",
]


NOTIFICATION_WES_RUN_FAILED = "wes_run_failed"
NOTIFICATION_WES_RUN_COMPLETED = "wes_run_completed"


def run_request_from_row(run: sqlite3.Row) -> RunRequest:
    return RunRequest(
        workflow_params=run["request__workflow_params"],
        workflow_type=run["request__workflow_type"],
        workflow_type_version=run["request__workflow_type_version"],
        workflow_engine_parameters=run["request__workflow_engine_parameters"],
        workflow_url=run["request__workflow_url"],
        tags=run["request__tags"],
    )


def _strip_first_slash(string: str) -> str:
    return string[1:] if len(string) > 0 and string[0] == "/" else string


def _stream_url(run_id: uuid.UUID | str, stream: RunStream) -> str:
    return urljoin(current_app.config["SERVICE_BASE_URL"], f"runs/{str(run_id)}/{stream}")


def run_log_from_row(run: sqlite3.Row, stream_content: bool) -> RunLog:
    run_id = run["id"]
    return RunLog(
        name=run["run_log__name"],
        cmd=run["run_log__cmd"],
        start_time=run["run_log__start_time"] or None,
        end_time=run["run_log__end_time"] or None,
        stdout=run["run_log__stdout"] if stream_content else _stream_url(run_id, "stdout"),
        stderr=run["run_log__stderr"] if stream_content else _stream_url(run_id, "stderr"),
        exit_code=run["run_log__exit_code"],
    )


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


def run_from_row(run: sqlite3.Row) -> Run:
    return Run(run_id=run["id"], state=run["state"])


class Database:

    def __init__(self):
        self._conn = sqlite3.connect(current_app.config["DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES)
        self._conn.row_factory = sqlite3.Row

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()

    def init(self):
        c = self.cursor()

        with current_app.open_resource("schema.sql") as sf:
            c.executescript(sf.read().decode("utf-8"))

        self.commit()

    def finish_run(
        self,
        event_bus: EventBus,
        run: Run,
        state: str,
        cursor: sqlite3.Cursor | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Updates a run's state, sets the run log's end time, and publishes an event corresponding with a run failure
        or a run success, depending on the state.
        :param c: An SQLite connection cursor
        :param event_bus: A bento_lib-defined event bus implementation for sending events
        :param run: The run which just finished
        :param state: The terminal state for the finished run
        :param logger: An optionally-provided logger object.
        :return:
        """

        c: sqlite3.Cursor = cursor or self.cursor()

        run_id = run.run_id
        end_time = iso_now()

        # Explicitly don't commit here to sync with state update
        c.execute("UPDATE runs SET run_log__end_time = ? WHERE id = ?", (end_time, run_id))
        self.update_run_state_and_commit(c, run_id, state, event_bus=event_bus, logger=logger)

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

    def update_stuck_runs(self):
        # Update all runs that have "stuck" states to have an error state instead on restart. This way, systems don't
        # get stuck checking their status, and if they're in a weird state at boot they should receive an error status
        # anyway.

        event_bus = get_flask_event_bus()

        c = self.cursor()
        logger: logging.Logger = current_app.logger

        c.execute("SELECT id FROM runs WHERE state IN (?, ?)", (states.STATE_INITIALIZING, states.STATE_RUNNING))
        stuck_run_ids: list[sqlite3.Row] = c.fetchall()

        for r in stuck_run_ids:
            run = self.get_run_with_details(c, r["id"], stream_content=True)
            if run is None:
                logger.error(f"Missing run: {r['id']}")
                continue

            logger.info(
                f"Found stuck run: {run.run_id} at state {run.state}. Setting state to {states.STATE_SYSTEM_ERROR}")
            self.finish_run(event_bus, run, states.STATE_SYSTEM_ERROR, cursor=c)

        self.commit()

    @staticmethod
    def get_task_logs(c: sqlite3.Cursor, run_id: uuid.UUID | str) -> list:
        c.execute("SELECT * FROM task_logs WHERE run_id = ?", (str(run_id),))
        return [task_log_dict(task_log) for task_log in c.fetchall()]

    @classmethod
    def run_with_details_and_output_from_row(
        cls,
        c: sqlite3.Cursor,
        run: sqlite3.Row,
        stream_content: bool,
    ) -> RunWithDetailsAndOutput:
        return RunWithDetailsAndOutput.model_validate(dict(
            run_id=run["id"],
            state=run["state"],
            request=run_request_from_row(run),
            run_log=run_log_from_row(run, stream_content),
            task_logs=cls.get_task_logs(c, run["id"]),
            outputs=json.loads(run["outputs"]),
        ))

    @staticmethod
    def _get_run_row(c: sqlite3.Cursor, run_id: uuid.UUID | str) -> sqlite3.Row | None:
        return c.execute("SELECT * FROM runs WHERE id = ?", (str(run_id),)).fetchone()

    @classmethod
    def get_run(cls, c: sqlite3.Cursor, run_id: uuid.UUID | str) -> Run | None:
        if run := cls._get_run_row(c, run_id):
            return run_from_row(run)
        return None

    @classmethod
    def get_run_with_details(
        cls,
        c: sqlite3.Cursor,
        run_id: uuid.UUID | str,
        stream_content: bool,
    ) -> RunWithDetailsAndOutput | None:
        if run := cls._get_run_row(c, run_id):
            return cls.run_with_details_and_output_from_row(c, run, stream_content)
        return None

    @staticmethod
    def set_run_outputs(c: sqlite3.Cursor, run_id: str, outputs: dict[str, Any]):
        c.execute("UPDATE runs SET outputs = ? WHERE id = ?", (json.dumps(outputs), str(run_id)))

    def update_run_state_and_commit(
        self,
        c: sqlite3.Cursor,
        run_id: uuid.UUID | str,
        state: str,
        event_bus: EventBus | None = None,
        logger: logging.Logger | None = None,
        publish_event: bool = True,
    ):
        if logger:
            logger.info(f"Updating run state of {run_id} to {state}")
        c.execute("UPDATE runs SET state = ? WHERE id = ?", (state, str(run_id)))
        self.commit()
        if event_bus and publish_event:
            event_bus.publish_service_event(
                SERVICE_ARTIFACT,
                EVENT_WES_RUN_UPDATED,
                self.get_run_with_details(c, run_id, stream_content=False).model_dump(mode="json"),
            )


def get_db() -> Database:
    if "db" not in g:
        g.db = Database()

    return g.db


def close_db(_e=None):
    db: Database | None = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db: Database = get_db()
    db.init()


def update_db():
    db: Database = get_db()
    c = db.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='runs'")
    if c.fetchone() is None:
        init_db()
        return

    db.update_stuck_runs()

    # TODO: Migrations if needed
