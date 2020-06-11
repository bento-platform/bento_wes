import os
import requests
import shutil
import sqlite3
import subprocess
import uuid

from abc import ABC, abstractmethod
from base64 import urlsafe_b64encode
from bento_lib.events import EventBus
from bento_lib.events.notifications import format_notification
from bento_lib.events.types import EVENT_CREATE_NOTIFICATION
from typing import Callable, Dict, Optional, Tuple, Union
from urllib.parse import urlparse

from bento_wes import states
from bento_wes.constants import SERVICE_ARTIFACT
from bento_wes.db import get_db, update_run_state_and_commit
from bento_wes.utils import iso_now

from .backend_types import Command, ProcessResult, WorkflowType, WES_WORKFLOW_TYPE_CWL, WES_WORKFLOW_TYPE_WDL


__all__ = [
    "finish_run",
    "WESBackend",
]


ALLOWED_WORKFLOW_URL_SCHEMES = ("http", "https", "file")
ALLOWED_WORKFLOW_REQUEST_SCHEMES = ("http", "https")

MAX_WORKFLOW_FILE_BYTES = 10000000  # 10 MB

WORKFLOW_TIMEOUT = 60 * 60 * 24  # 24 hours

NOTIFICATION_WES_RUN_FAILED = "wes_run_failed"
NOTIFICATION_WES_RUN_COMPLETED = "wes_run_completed"


WORKFLOW_EXTENSIONS: Dict[WorkflowType, str] = {
    WES_WORKFLOW_TYPE_WDL: "wdl",
    WES_WORKFLOW_TYPE_CWL: "cwl",
}


# TODO: Move
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


class WESBackend(ABC):
    def __init__(self, tmp_dir: str, chord_mode: bool = False, logger=None,
                 event_bus: Optional[EventBus] = None, chord_callback: Optional[Callable[["WESBackend"], str]] = None):
        self.db = get_db()
        self.tmp_dir = tmp_dir
        self.chord_mode = chord_mode
        self.chord_callback = chord_callback
        self.logger = logger
        self.event_bus = event_bus  # TODO: New event bus?
        self._runs = {}

        if chord_mode and not chord_callback:
            raise ValueError("Missing chord_callback for chord_mode backend run")

    def log_error(self, error: str) -> None:
        """
        Given an error string, logs the error.
        :param error: An error string
        """
        if self.logger:
            self.logger.error(error)

    @abstractmethod
    def _get_supported_types(self) -> Tuple[WorkflowType]:
        """
        Returns a tuple of the workflow types this backend supports.
        """
        pass

    @abstractmethod
    def _get_params_file(self, run: dict) -> str:
        """
         Returns the name of the params file to use for the workflow run.
        :param run: The run description
        :return: The name of the params file
        """
        pass

    @abstractmethod
    def _serialize_params(self, workflow_params: dict) -> str:
        """
        Serializes parameters for a particular workflow run into the format expected by the backend's runner.
        :param workflow_params: A dictionary of key-value pairs representing the workflow parameters
        :return: The serialized form of the parameters
        """
        pass

    @staticmethod
    def _workflow_file_name(run: dict) -> str:
        """
        Extract's a run's specified workflow's URI and generates a unique name for it.
        """
        workflow_uri: str = run["request"]["workflow_url"]
        workflow_name = str(urlsafe_b64encode(bytes(workflow_uri, encoding="utf-8")), encoding="utf-8")
        return f"workflow_{workflow_name}.{WORKFLOW_EXTENSIONS[WorkflowType(run['request']['workflow_type'])]}"

    def workflow_path(self, run: dict) -> str:
        """
        Gets the local filesystem path to the workflow file specified by a run's workflow URI.
        """
        return os.path.join(self.tmp_dir, self._workflow_file_name(run))

    def run_dir(self, run: dict) -> str:
        """
        Returns a path to the work directory for executing a run.
        """
        return os.path.join(self.tmp_dir, run["run_id"])

    def _params_path(self, run: dict) -> str:
        """
        Returns a path to the workflow parameters file for a run.
        """
        return os.path.join(self.run_dir(run), self._get_params_file(run))

    def _download_or_copy_workflow(self, run: dict) -> Optional[str]:
        """
        Given a particular run, downloads the specified workflow via its URI, or copies it over if it's on the local
        file system. # TODO: Local file system = security issue?
        :param run: The run from which to extract the workflow URI
        """

        workflow_uri: str = run["request"]["workflow_url"]
        parsed_workflow_url = urlparse(workflow_uri)  # TODO: Handle errors, handle references to attachments

        workflow_path = self.workflow_path(run)

        # TODO: Auth
        if parsed_workflow_url.scheme in ALLOWED_WORKFLOW_REQUEST_SCHEMES:
            try:
                wr = requests.get(workflow_uri)

                if wr.status_code == 200 and len(wr.content) < MAX_WORKFLOW_FILE_BYTES:
                    if os.path.exists(workflow_path):
                        os.remove(workflow_path)

                    with open(workflow_path, "wb") as nwf:
                        nwf.write(wr.content)

                elif not os.path.exists(workflow_path):  # Use cached version if needed, otherwise error
                    # Request issues
                    return states.STATE_SYSTEM_ERROR

            except requests.exceptions.ConnectionError:
                if not os.path.exists(workflow_path):  # Use cached version if needed, otherwise error
                    # Network issues
                    return states.STATE_SYSTEM_ERROR

        else:  # TODO: Other else cases
            # file://
            # TODO: Handle exceptions
            shutil.copyfile(parsed_workflow_url.path, workflow_path)

    @abstractmethod
    def _check_workflow(self, run: dict) -> Optional[Tuple[str, str]]:
        """
        Checks that a workflow can be executed by the backend via the workflow's URI.
        :param run: The run, including a request with the workflow URI
        :return: None if the workflow is valid; a tuple of an error message and an error state otherwise
        """
        pass

    def _download_and_check_workflow(self, run: dict) -> Optional[Tuple[str, str]]:
        """
        Downloads or copies a run's workflow file and checks it's validity.
        :param run: The run specifying the workflow in question
        :return: None if the workflow is valid; a tuple of an error message and an error state otherwise
        """

        workflow_type: WorkflowType = WorkflowType(run["request"]["workflow_type"])
        if workflow_type not in self._get_supported_types():
            raise NotImplementedError(f"The specified WES backend cannot execute workflows of type {workflow_type}")

        self._download_or_copy_workflow(run)
        return self._check_workflow(run)

    @abstractmethod
    def get_workflow_name(self, workflow_path: str) -> Optional[str]:
        """
        Extracts a workflow's name from it's file.
        :param workflow_path: The path to the workflow definition file
        :return: None if the file could not be parsed for some reason; the name string otherwise
        """
        pass

    @abstractmethod
    def _get_command(self, workflow_path: str, params_path: str, run_dir: str) -> Command:
        """
        Creates the command which will run the backend runner on the specified workflow, with the specified
        serialized parameters, and in the specified run directory.
        :param workflow_path: The path to the workflow file to execute
        :param params_path: The path to the file containing specified parameters for the workflow
        :param run_dir: The directory to run the workflow in
        :return: The command, in the form of a tuple of strings, to be passed to subprocess.run
        """
        pass

    def _update_run_state_and_commit(self, run_id: Union[uuid.UUID, str], state: str) -> None:
        """
        Wrapper for the database "update_run_state_and_commit" function, which updates a run's state in the database.
        :param run_id: The ID of the run whose state is getting updated
        :param state: The value to set the run's current state to
        """
        update_run_state_and_commit(self.db, self.db.cursor(), self.event_bus, run_id, state)

    def _finish_run_and_clean_up(self, run: dict, state: str) -> None:
        """
        Performs standard run-finishing operations (updating state, setting end time, etc.) as well as deleting the run
        folder if it exists.
        :param run: The run to perform "finishing" operations on
        :param state: The final state of the run
        """

        # Finish run ----------------------------------------------------------

        finish_run(self.db, self.db.cursor(), self.event_bus, run, state)

        # Clean up ------------------------------------------------------------

        del self._runs[run["run_id"]]

        # -- Clean up any run files at the end, after they've been either -----
        #    copied or "rejected" due to some failure.
        # TODO: SECURITY: Check run_dir
        # TODO: May want to keep them around for a retry depending on how the retry operation will work.

        shutil.rmtree(self.run_dir(run), ignore_errors=True)

    def _initialize_run_and_get_command(self, run: dict, celery_id) -> Optional[Command]:
        """
        Performs "initialization" operations on the run, including setting states, downloading and validating the
        workflow file, and generating and logging the workflow-running command.
        :param run: The run to initialize
        :param celery_id: The Celery ID of the Celery task responsible for executing the run
        :return: The command to execute, if no errors occurred; None otherwise
        """

        self._update_run_state_and_commit(run["run_id"], states.STATE_INITIALIZING)

        run_log_id: str = run["run_log"]["id"]

        # -- Check that the run directory exists ------------------------------
        if not os.path.exists(self.run_dir(run)):
            # TODO: Log error in run log
            self.log_error("Run directory not found")
            return self._finish_run_and_clean_up(run, states.STATE_SYSTEM_ERROR)

        c = self.db.cursor()

        workflow_params: dict = run["request"]["workflow_params"]

        # -- Download the workflow, if possible / needed ----------------------
        error = self._download_and_check_workflow(run)
        if error is not None:
            self.log_error(error[0])
            return self._finish_run_and_clean_up(run, error[1])

        # -- Find "real" workflow name from workflow file ---------------------
        workflow_name = self.get_workflow_name(self.workflow_path(run))
        if workflow_name is None:
            # Invalid/non-workflow-specifying workflow file
            self.log_error("Could not find workflow name in workflow file")
            return self._finish_run_and_clean_up(run, states.STATE_SYSTEM_ERROR)

        # TODO: To avoid having multiple names, we should maybe only set this once?
        c.execute("UPDATE run_logs SET name = ? WHERE id = ?", (workflow_name, run_log_id))
        self.db.commit()

        # -- Store input for the workflow in a file in the temporary folder ---
        with open(self._params_path(run), "w") as pf:
            pf.write(self._serialize_params(workflow_params))

        # -- Create the runner command based on inputs ------------------------
        cmd = self._get_command(self.workflow_path(run),
                                self._params_path(run),
                                self.run_dir(run))

        # -- Update run log with command and Celery ID ------------------------
        c.execute("UPDATE run_logs SET cmd = ?, celery_id = ? WHERE id = ?", (" ".join(cmd), celery_id, run_log_id))
        self.db.commit()

        return cmd

    def _perform_run(self, run: dict, cmd: Command) -> Optional[ProcessResult]:
        """
        Performs a run based on a provided command and returns stdout, stderr, exit code, and whether the process timed
        out while running.
        :param run: The run to execute
        :param cmd: The command used to execute the run
        :return: A ProcessResult tuple of (stdout, stderr, exit_code, timed_out)
        """

        # Perform run =========================================================

        # -- Start process running the generated command ----------------------
        runner_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        c = self.db.cursor()
        c.execute("UPDATE run_logs SET start_time = ? WHERE id = ?", (iso_now(), run["run_log"]["id"]))
        self._update_run_state_and_commit(run["run_id"], states.STATE_RUNNING)

        # -- Wait for output --------------------------------------------------

        timed_out = False

        try:
            stdout, stderr = runner_process.communicate(timeout=WORKFLOW_TIMEOUT)

        except subprocess.TimeoutExpired:
            runner_process.kill()
            stdout, stderr = runner_process.communicate()
            timed_out = True

        finally:
            exit_code = runner_process.returncode

        # Complete run ========================================================

        # -- Update run log with stdout/stderr, exit code ---------------------
        #     - Explicitly don't commit here; sync with state update
        c.execute("UPDATE run_logs SET stdout = ?, stderr = ?, exit_code = ? WHERE id = ?",
                  (stdout, stderr, exit_code, run["run_log"]["id"]))

        if timed_out:
            # TODO: Report error somehow
            return self._finish_run_and_clean_up(run, states.STATE_SYSTEM_ERROR)

        # -- Final steps: check exit code and report results ------------------

        if exit_code != 0:
            # TODO: Report error somehow
            return self._finish_run_and_clean_up(run, states.STATE_EXECUTOR_ERROR)

        # Exit code is 0 otherwise

        if not self.chord_mode:
            # TODO: What should be done if this run was not a CHORD routine?
            return self._finish_run_and_clean_up(run, states.STATE_COMPLETE)

        # If in CHORD mode, run the callback and finish the run with whatever state is returned.
        self._finish_run_and_clean_up(run, self.chord_callback(self))

        return ProcessResult((stdout, stderr, exit_code, timed_out))

    def perform_run(self, run: dict, celery_id) -> Optional[ProcessResult]:
        """
        Executes a run from start to finish (initialization, startup, and completion / cleanup.)
        :param run: The run to execute
        :param celery_id: The ID of the Celery task responsible for executing the workflow
        :return: A ProcessResult tuple of (stdout, stderr, exit_code, timed_out)
        """

        if run["run_id"] in self._runs:
            raise ValueError("Run has already been registered")

        self._runs[run["run_id"]] = run

        # Initialization (loading / downloading files) ------------------------
        cmd = self._initialize_run_and_get_command(run, celery_id)
        if cmd is None:
            return

        # Perform, finish, and clean up run -----------------------------------
        return self._perform_run(run, cmd)
