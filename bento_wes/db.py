import json
import sqlite3
import uuid
from typing import Any, Generator, Annotated
from urllib.parse import urljoin
from fastapi import Depends

from bento_lib.events import EventBus
from bento_lib.events.notifications import format_notification
from bento_lib.events.types import EVENT_CREATE_NOTIFICATION, EVENT_WES_RUN_UPDATED

from . import states
from .backends.backend_types import Command
from .config import config
from .constants import SERVICE_ARTIFACT
from .events import get_event_bus
from .logger import logger
from .models import Run, RunLog, RunRequest, RunWithDetails
from .types import RunStream
from .utils import iso_now


__all__ = [
    "Database",
    "get_db",
    "setup_database_on_startup",
    "repair_database_on_startup",
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


def _stream_url(run_id: uuid.UUID | str, stream: RunStream) -> str:
    return urljoin(config.service_base_url, f"runs/{str(run_id)}/{stream}")


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
        "exit_code": task_log["exit_code"],
    }


def run_from_row(run: sqlite3.Row) -> Run:
    return Run(run_id=run["id"], state=run["state"])


class Database:
    def __init__(self):
        # One connection per request; okay for FastAPI threadpools
        self._conn = sqlite3.connect(
            config.database,
            detect_types=sqlite3.PARSE_DECLTYPES,
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._apply_pragmas()
        self._cursor = None

    def _apply_pragmas(self) -> None:
        # Good defaults for web workloads with SQLite
        c = self._conn.cursor()
        c.execute("PRAGMA foreign_keys=ON;")
        c.execute("PRAGMA journal_mode=WAL;")
        c.close()

    @property
    def c(self):
        if self._cursor is None or getattr(self._cursor, "closed", False):
            self._cursor = self._conn.cursor()
        return self._cursor

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def init_schema(self) -> None:
        # Run once at startup (not per request!)
        with open("schema.sql", "r", encoding="utf-8") as sf:
            self.c.executescript(sf.read())
        self.commit()

    def finish_run(
        self,
        event_bus: EventBus,
        run: Run,
        state: str
    ) -> None:
        """
        Update a run's state, set the run log's end time, and publish a success/failure notification.
        """

        run_id = run.run_id
        end_time = iso_now()

        # Explicitly don't commit here to sync with state update
        self.c.execute("UPDATE runs SET run_log__end_time = ? WHERE id = ?", (end_time, run_id))
        self.update_run_state_and_commit(self.c, run_id, state)

        logger.info(f"Run {run_id} finished with state {state} at {end_time}")

        if state in states.FAILURE_STATES:
            event_bus.publish_service_event(
                SERVICE_ARTIFACT,
                EVENT_CREATE_NOTIFICATION,
                format_notification(
                    title="WES Run Failed",
                    description=f"WES run '{run_id}' failed with state {state}",
                    notification_type=NOTIFICATION_WES_RUN_FAILED,
                    action_target=run_id,
                ),
            )
        elif state in states.SUCCESS_STATES:
            event_bus.publish_service_event(
                SERVICE_ARTIFACT,
                EVENT_CREATE_NOTIFICATION,
                format_notification(
                    title="WES Run Completed",
                    description=f"WES run '{run_id}' completed successfully",
                    notification_type=NOTIFICATION_WES_RUN_COMPLETED,
                    action_target=run_id,
                ),
            )

    def update_stuck_runs(self) -> None:
        """
        On process boot, convert initializing/running states into system error so
        the UI/backend doesn't wait on orphaned work.
        """
        event_bus = get_event_bus()

        self.c.execute(
            "SELECT id FROM runs WHERE state IN (?, ?)",
            (states.STATE_INITIALIZING, states.STATE_RUNNING),
        )
        stuck_run_ids: list[sqlite3.Row] = self.c.fetchall()

        for r in stuck_run_ids:
            run = self.get_run_with_details(self.c, r["id"], stream_content=True)
            if run is None:
                logger.error(f"Missing run: {r['id']}")
                continue

            logger.info(
                f"Found stuck run: {run.run_id} at state {run.state}. "
                f"Setting state to {states.STATE_SYSTEM_ERROR}"
            )
            self.finish_run(event_bus, run, states.STATE_SYSTEM_ERROR)

        self.commit()

    @staticmethod
    def get_task_logs(c: sqlite3.Cursor, run_id: uuid.UUID | str) -> list:
        c.execute("SELECT * FROM task_logs WHERE run_id = ?", (str(run_id),))
        return [task_log_dict(task_log) for task_log in c.fetchall()]

    @classmethod
    def run_with_details_from_row(
        cls,
        c: sqlite3.Cursor,
        run: sqlite3.Row,
        stream_content: bool,
    ) -> RunWithDetails:
        return RunWithDetails.model_validate(
            dict(
                run_id=run["id"],
                state=run["state"],
                request=run_request_from_row(run),
                run_log=run_log_from_row(run, stream_content),
                task_logs=cls.get_task_logs(c, run["id"]),
                outputs=json.loads(run["outputs"]),
            )
        )

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
    ) -> RunWithDetails | None:
        if run := cls._get_run_row(c, run_id):
            return cls.run_with_details_from_row(c, run, stream_content)
        return None

    def set_run_log_name(self, run: Run, workflow_name: str) -> None:
        self.cursor().execute(
            "UPDATE runs SET run_log__name = ? WHERE id = ?",
            (workflow_name, run.run_id),
        )
        self.commit()

    def set_run_log_command_and_celery_id(self, run: Run, cmd: Command, celery_id: int) -> None:
        self.cursor().execute(
            "UPDATE runs SET run_log__cmd = ?, run_log__celery_id = ? WHERE id = ?",
            (" ".join(cmd), celery_id, run.run_id),
        )
        self.commit()

    @staticmethod
    def set_run_outputs(c: sqlite3.Cursor, run_id: str, outputs: dict[str, Any]) -> None:
        c.execute("UPDATE runs SET outputs = ? WHERE id = ?", (json.dumps(outputs), str(run_id)))

    def update_run_state_and_commit(
        self,
        c: sqlite3.Cursor,
        run_id: uuid.UUID | str,
        state: str,
        publish_event: bool = True,
    ) -> None:
        event_bus = get_event_bus()
        
        logger.info(f"Updating run state of {run_id} to {state}")
        c.execute("UPDATE runs SET state = ? WHERE id = ?", (state, str(run_id)))
        self.commit()
        if event_bus and publish_event:
            payload = self.get_run_with_details(c, run_id, stream_content=False).model_dump(mode="json")
            event_bus.publish_service_event(SERVICE_ARTIFACT, EVENT_WES_RUN_UPDATED, payload)


# === FastAPI dependency: one connection per request, auto-closed ===
def get_db() -> Generator["Database", None, None]:
    db = Database()
    try:
        yield db
    finally:
        db.close()

DatabaseDep = Annotated[Database, Depends(get_db)]

# === Startup helpers (call these from your FastAPI lifespan) ===
def setup_database_on_startup() -> None:
    """
    Ensure schema exists and apply PRAGMAs once at startup.
    Call from your FastAPI lifespan (startup phase).
    """
    db = Database()
    try:
        # If the 'runs' table isn't present, run full schema.sql
        db.c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='runs'")
        if db.c.fetchone() is None:
            db.init_schema()
    finally:
        db.close()


def repair_database_on_startup() -> None:
    """
    Perform boot-time repairs (e.g., mark stuck runs as system error).
    Call after setup_database_on_startup() during startup.
    """
    db = Database()
    try:
        db.update_stuck_runs()
    finally:
        db.close()
