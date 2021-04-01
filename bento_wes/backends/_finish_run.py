import sqlite3

from bento_lib.events import EventBus
from bento_lib.events.notifications import format_notification
from bento_lib.events.types import EVENT_CREATE_NOTIFICATION

from bento_wes import states
from bento_wes.constants import SERVICE_ARTIFACT
from bento_wes.db import update_run_state_and_commit
from bento_wes.utils import iso_now

__all__ = ["finish_run"]

NOTIFICATION_WES_RUN_FAILED = "wes_run_failed"
NOTIFICATION_WES_RUN_COMPLETED = "wes_run_completed"


def finish_run(db: sqlite3.Connection, c: sqlite3.Cursor, event_bus: EventBus, run: dict, state: str) -> None:
    """
    Updates a run's state, sets the run log's end time, and publishes an event corresponding with a run failure
    or a run success, depending on the state.
    :param db: A SQLite database connection
    :param c: An SQLite connection cursor
    :param event_bus: A chord_lib-defined event bus implementation for sending events
    :param run: The run which just finished
    :param state: The terminal state for the finished run
    :return:
    """

    run_id = run["run_id"]
    run_log_id = run["run_log"]["id"]

    # Explicitly don't commit here to sync with state update
    c.execute("UPDATE run_logs SET end_time = ? WHERE id = ?", (iso_now(), run_log_id))
    update_run_state_and_commit(db, c, event_bus, run_id, state)

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
