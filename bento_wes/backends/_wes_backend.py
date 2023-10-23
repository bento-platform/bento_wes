import json
import pathlib
import os
import re
import shutil
import sqlite3
import subprocess
import uuid

from abc import ABC, abstractmethod
from bento_lib.events import EventBus
from bento_lib.events.types import EVENT_WES_RUN_FINISHED
from bento_lib.workflows.models import WorkflowSecretInput
from bento_lib.workflows.utils import namespaced_input
from flask import current_app
from typing import Any

from bento_wes import states
from bento_wes.constants import SERVICE_ARTIFACT
from bento_wes.db import get_db, finish_run, update_run_state_and_commit
from bento_wes.models import Run, RunWithDetails
from bento_wes.states import STATE_EXECUTOR_ERROR, STATE_SYSTEM_ERROR
from bento_wes.utils import iso_now
from bento_wes.workflows import WorkflowType, WorkflowManager

from .backend_types import Command, ProcessResult

__all__ = ["WESBackend"]

# Spec: https://software.broadinstitute.org/wdl/documentation/spec#whitespace-strings-identifiers-constants
WDL_WORKSPACE_NAME_REGEX = re.compile(r"workflow\s+([a-zA-Z][a-zA-Z0-9_]+)")

ParamDict = dict[str, str | int | float | bool]


class WESBackend(ABC):
    def __init__(
        self,
        tmp_dir: str,
        workflow_timeout: int,  # Workflow timeout, in seconds
        logger=None,
        event_bus: EventBus | None = None,
        workflow_host_allow_list: set | None = None,
        bento_url: str | None = None,
        validate_ssl: bool = True,
        debug: bool = False,
    ):
        self._workflow_timeout: int = workflow_timeout

        self.db: sqlite3.Connection = get_db()

        self.tmp_dir: str = tmp_dir
        self.log_dir: str = tmp_dir.rstrip("/") + "/logs"  # Not used for most logs, but Toil wanted a log dir...

        pathlib.Path(self.log_dir).mkdir(parents=True, exist_ok=True)

        self.logger = logger
        self.event_bus = event_bus  # TODO: New event bus?

        self.workflow_host_allow_list = workflow_host_allow_list

        # Bento-specific parameters
        self.bento_url: str = bento_url

        self.validate_ssl: bool = validate_ssl
        self.debug: bool = debug

        self._workflow_manager: WorkflowManager = WorkflowManager(
            self.tmp_dir,
            service_base_url=current_app.config["SERVICE_BASE_URL"],
            bento_url=self.bento_url,
            logger=self.logger,
            workflow_host_allow_list=self.workflow_host_allow_list,
            validate_ssl=validate_ssl,
            debug=self.debug,
        )

        self._runs = {}

        self.log_debug(f"Instantiating WESBackend with debug={self.debug}")

    def log_debug(self, message: str) -> None:
        """
        Given a message, logs it as DEBUG.
        :param message: A message to log
        """
        if self.logger:
            self.logger.debug(message)

    def log_info(self, message: str) -> None:
        """
        Given a message, logs it as INFO.
        :param message: A message to log
        """
        if self.logger:
            self.logger.info(message)

    def log_warning(self, warning: str) -> None:
        """
        Given a warning string, logs the warning.
        :param warning: A warning string
        """
        if self.logger:
            self.logger.warning(warning)

    def log_error(self, error: str) -> None:
        """
        Given an error string, logs the error.
        :param error: An error string
        """
        if self.logger:
            self.logger.error(error)

    @abstractmethod
    def _get_supported_types(self) -> tuple[WorkflowType, ...]:
        """
        Returns a tuple of the workflow types this backend supports.
        """
        pass

    @abstractmethod
    def _get_params_file(self, run: Run) -> str:
        """
         Returns the name of the params file to use for the workflow run.
        :param run: The run description
        :return: The name of the params file
        """
        pass

    @abstractmethod
    def _serialize_params(self, workflow_params: ParamDict) -> str:
        """
        Serializes parameters for a particular workflow run into the format expected by the backend's runner.
        :param workflow_params: A dictionary of key-value pairs representing the workflow parameters
        :return: The serialized form of the parameters
        """
        pass

    def workflow_path(self, run: RunWithDetails) -> str:
        """
        Gets the local filesystem path to the workflow file specified by a run's workflow URI.
        """
        return self._workflow_manager.workflow_path(run.request.workflow_url, WorkflowType(run.request.workflow_type))

    def run_dir(self, run: Run) -> str:
        """
        Returns a path to the work directory for executing a run.
        """
        return os.path.join(self.tmp_dir, run.run_id)

    def _params_path(self, run: Run) -> str:
        """
        Returns a path to the workflow parameters file for a run.
        """
        return os.path.join(self.run_dir(run), self._get_params_file(run))

    @abstractmethod
    def _check_workflow(self, run: Run) -> tuple[str, str] | None:
        """
        Checks that a workflow can be executed by the backend via the workflow's URI.
        :param run: The run, including a request with the workflow URI
        :return: None if the workflow is valid; a tuple of an error message and an error state otherwise
        """
        pass

    def _check_workflow_wdl(self, run: RunWithDetails) -> tuple[str, str] | None:
        """
        Checks that a particular WDL workflow is valid.
        :param run: The run whose workflow is being checked
        :return: None if the workflow is valid; a tuple of an error message and an error state otherwise
        """

        womtool_path = current_app.config["WOM_TOOL_LOCATION"]

        # If WOMtool isn't specified, exit early (either as an error or just skipping validation)

        if not womtool_path:
            # WOMtool not specified; assume the WDL is valid if WORKFLOW_HOST_ALLOW_LIST has been adequately specified
            return None if self.workflow_host_allow_list else (
                f"Failed with {STATE_EXECUTOR_ERROR} due to missing or invalid WOMtool (Bad WOM_TOOL_LOCATION)\n"
                f"\tWOM_TOOL_LOCATION: {womtool_path}",
                STATE_EXECUTOR_ERROR
            )

        # Check for Java (needed to run WOMtool)
        try:
            subprocess.run(("java", "-version"))
        except FileNotFoundError:
            return "Java is missing (required to validate WDL files)", STATE_SYSTEM_ERROR

        # Validate WDL, listing dependencies

        vr = subprocess.Popen(("java", "-jar", womtool_path, "validate", "-l", self.workflow_path(run)),
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              encoding="utf-8")

        v_out, v_err = vr.communicate()

        if vr.returncode != 0:
            # Validation error with WDL file
            # TODO: Add some stdout or stderr to logs?
            return (
                f"Failed with {STATE_EXECUTOR_ERROR} due to non-0 validation return code:\n"
                f"\tstdout: {v_out}\n\tstderr: {v_err}",
                STATE_EXECUTOR_ERROR
            )

        #  - Since Toil doesn't support WDL imports right now, any dependencies will result in an error
        if "None" not in v_out:  # No dependencies
            # Toil can't process WDL dependencies right now  TODO
            # TODO: Add some stdout or stderr to logs?
            return (
                f"Failed with {STATE_EXECUTOR_ERROR} due to dependencies in WDL:\n"
                f"\tstdout: {v_out}\n\tstderr: {v_err}",
                STATE_EXECUTOR_ERROR
            )

    def _check_workflow_and_type(self, run: RunWithDetails) -> tuple[str, str] | None:
        """
        Checks a workflow file's validity.
        :param run: The run specifying the workflow in question
        :return: None if the workflow is valid; a tuple of an error message and an error state otherwise
        """

        workflow_type: WorkflowType = WorkflowType(run.request.workflow_type)
        if workflow_type not in self._get_supported_types():
            raise NotImplementedError(f"The specified WES backend cannot execute workflows of type {workflow_type}")

        return self._check_workflow(run)

    @abstractmethod
    def get_workflow_name(self, workflow_path: str) -> str | None:
        """
        Extracts a workflow's name from its file.
        :param workflow_path: The path to the workflow definition file
        :return: None if the file could not be parsed for some reason; the name string otherwise
        """
        pass

    @staticmethod
    def get_workflow_name_wdl(workflow_path: str) -> str | None:
        """
        Standard extractor for workflow names for WDL.
        :param workflow_path: The path to the workflow definition file
        :return: None if the file could not be parsed for some reason; the name string otherwise
        """

        with open(workflow_path, "r") as wdf:
            wdl_contents = wdf.read()
            workflow_id_match = WDL_WORKSPACE_NAME_REGEX.search(wdl_contents)

            # Invalid/non-workflow-specifying WDL file if false-y
            return workflow_id_match.group(1) if workflow_id_match else None

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

    def _update_run_state_and_commit(self, run_id: uuid.UUID | str, state: str) -> None:
        """
        Wrapper for the database "update_run_state_and_commit" function, which updates a run's state in the database.
        :param run_id: The ID of the run whose state is getting updated
        :param state: The value to set the run's current state to
        """
        self.log_debug(f"Setting state of run {run_id} to {state}")
        update_run_state_and_commit(self.db, self.db.cursor(), run_id, state, event_bus=self.event_bus)

    def _finish_run_and_clean_up(self, run: Run, state: str) -> None:
        """
        Performs standard run-finishing operations (updating state, setting end time, etc.) as well as deleting the run
        folder if it exists.
        :param run: The run to perform "finishing" operations on
        :param state: The final state of the run
        """

        # Finish run ----------------------------------------------------------

        finish_run(self.db, self.db.cursor(), self.event_bus, run, state)

        # Clean up ------------------------------------------------------------

        del self._runs[run.run_id]

        # -- Clean up any run files at the end, after they've been either -----
        #    copied or "rejected" due to some failure.
        # TODO: SECURITY: Check run_dir
        # TODO: May want to keep them around for a retry depending on how the retry operation will work.

        if not self.debug:
            shutil.rmtree(self.run_dir(run), ignore_errors=True)

    def _initialize_run_and_get_command(
        self,
        run: RunWithDetails,
        celery_id: int,
        secrets: dict[str, str],
    ) -> tuple[Command, dict] | None:
        """
        Performs "initialization" operations on the run, including setting states, downloading and validating the
        workflow file, and generating and logging the workflow-running command.
        :param run: The run to initialize
        :param celery_id: The Celery ID of the Celery task responsible for executing the run
        :param secrets: A dictionary of secrets (e.g., tokens) to be injected as parameters (potentially) but not stored
                        in the database.
        :return: The command to execute, if no errors occurred; None otherwise
        """

        self._update_run_state_and_commit(run.run_id, states.STATE_INITIALIZING)

        run_dir = self.run_dir(run)

        # -- Check that the run directory exists -----------------------------------------------------------------------
        if not os.path.exists(run_dir):
            # TODO: Log error in run log
            self.log_error("Run directory not found")
            return self._finish_run_and_clean_up(run, states.STATE_SYSTEM_ERROR)

        run_req = run.request

        # run_req.workflow_params now includes non-secret injected values since it was read from the database after
        # the run ID was passed to the runner:
        workflow_params_with_secrets: ParamDict = {**run_req.workflow_params}

        # -- Find which inputs are secrets, which need to be injected here (so they don't end up in the database) ------
        for run_input in run_req.tags.workflow_metadata.inputs:
            if isinstance(run_input, WorkflowSecretInput):
                secret_value = secrets.get(run_input.key)
                if secret_value is None:
                    err = f"Could not find injectable secret for key {run_input.key}"
                    self.log_error(err)
                    return self._finish_run_and_clean_up(run, STATE_EXECUTOR_ERROR)
                workflow_params_with_secrets[namespaced_input(run_req.tags.workflow_id, run_input.id)] = secret_value

        # -- Validate the workflow -------------------------------------------------------------------------------------
        error = self._check_workflow_and_type(run)
        if error is not None:
            self.log_error(error[0])
            return self._finish_run_and_clean_up(run, error[1])

        # -- Find "real" workflow name from workflow file --------------------------------------------------------------
        workflow_name = self.get_workflow_name(self.workflow_path(run))
        if workflow_name is None:
            # Invalid/non-workflow-specifying workflow file
            self.log_error("Could not find workflow name in workflow file")
            return self._finish_run_and_clean_up(run, states.STATE_SYSTEM_ERROR)

        c = self.db.cursor()

        # TODO: To avoid having multiple names, we should maybe only set this once?
        c.execute("UPDATE runs SET run_log__name = ? WHERE id = ?", (workflow_name, run.run_id))
        self.db.commit()

        # -- Store input for the workflow in a file in the temporary folder --------------------------------------------
        with open(self._params_path(run), "w") as pf:
            pf.write(self._serialize_params(workflow_params_with_secrets))

        # -- Create the runner command based on inputs -----------------------------------------------------------------
        cmd = self._get_command(self.workflow_path(run), self._params_path(run), self.run_dir(run))

        # -- Update run log with command and Celery ID -----------------------------------------------------------------
        c.execute(
            "UPDATE runs SET run_log__cmd = ?, run_log__celery_id = ? WHERE id = ?",
            (" ".join(cmd), celery_id, run.run_id))
        self.db.commit()

        return cmd, workflow_params_with_secrets

    @abstractmethod
    def get_workflow_outputs(self, run_dir: str) -> dict[str, Any]:
        pass

    def _perform_run(self, run: RunWithDetails, cmd: Command, params_with_secrets: ParamDict) -> ProcessResult | None:
        """
        Performs a run based on a provided command and returns stdout, stderr, exit code, and whether the process timed
        out while running.
        :param run: The run to execute
        :param cmd: The command used to execute the run
        :param params_with_secrets: A dictionary of parameters, including secret values
        :return: A ProcessResult tuple of (stdout, stderr, exit_code, timed_out)
        """

        c = self.db.cursor()

        # Perform run ==================================================================================================

        # -- Start process running the generated command ---------------------------------------------------------------
        runner_process = subprocess.Popen(
            cmd, cwd=self.tmp_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        c.execute("UPDATE runs SET run_log__start_time = ? WHERE id = ?", (iso_now(), run.run_id))
        self._update_run_state_and_commit(run.run_id, states.STATE_RUNNING)

        # -- Wait for and capture output -------------------------------------------------------------------------------

        timed_out = False

        try:
            stdout, stderr = runner_process.communicate(timeout=self._workflow_timeout)

        except subprocess.TimeoutExpired:
            runner_process.kill()
            stdout, stderr = runner_process.communicate()
            timed_out = True

        finally:
            exit_code = runner_process.returncode

        # -- Censor output in case it includes any secrets -------------------------------------------------------------

        workflow_id = run.request.tags.workflow_id
        req_secret_inputs = (i for i in run.request.tags.workflow_metadata.inputs if isinstance(i, WorkflowSecretInput))
        for req_secret_input in req_secret_inputs:
            v = params_with_secrets.get(namespaced_input(workflow_id, req_secret_input.id))
            if isinstance(v, str) and len(v) > 1:  # don't "censor" blank strings/single characters
                stdout = stdout.replace(v, "<redacted>")
                stderr = stderr.replace(v, "<redacted>")

        # Complete run =================================================================================================

        # -- Update run log with stdout/stderr, exit code --------------------------------------------------------------
        #     - Explicitly don't commit here; sync with state update
        c.execute("UPDATE runs SET run_log__stdout = ?, run_log__stderr = ?, run_log__exit_code = ? WHERE id = ?",
                  (stdout, stderr, exit_code, run.run_id))

        if timed_out:
            # TODO: Report error somehow
            self.log_error("Encountered timeout while performing run")
            return self._finish_run_and_clean_up(run, states.STATE_SYSTEM_ERROR)

        # -- Final steps: check exit code and report results -----------------------------------------------------------

        if exit_code != 0:
            # TODO: Report error somehow
            self.log_error("Encountered a non-zero exit code while performing run")
            return self._finish_run_and_clean_up(run, states.STATE_EXECUTOR_ERROR)

        # Exit code is 0 otherwise; complete the run

        run_dir = self.run_dir(run)
        workflow_name = self.get_workflow_name(self.workflow_path(run))

        workflow_outputs = self.get_workflow_outputs(run_dir)

        # Explicitly don't commit here; sync with state update
        c.execute("UPDATE runs SET outputs = ? WHERE id = ?", (json.dumps(workflow_outputs), str(run.run_id)))

        # Emit event if possible
        self.event_bus.publish_service_event(
            SERVICE_ARTIFACT,  # TODO: bento_lib: replace with service kind
            EVENT_WES_RUN_FINISHED,
            # Run result object:
            event_data={
                "workflow_id": workflow_name,
                "workflow_metadata": run.request.tags.workflow_metadata.model_dump_json(),
                "workflow_outputs": workflow_outputs,
                "workflow_params": run.request.workflow_params,
            },
        )

        # Finally, set our state to COMPLETE + finish up the run.
        self._finish_run_and_clean_up(run, states.STATE_COMPLETE)

        return ProcessResult((stdout, stderr, exit_code, timed_out))

    def perform_run(self, run: RunWithDetails, celery_id: int, secrets: dict[str, str]) -> ProcessResult | None:
        """
        Executes a run from start to finish (initialization, startup, and completion / cleanup.)
        :param run: The run to execute
        :param celery_id: The ID of the Celery task responsible for executing the workflow
        :param secrets: A dictionary of secrets (e.g., tokens) to be injected as parameters (potentially) but not stored
                        in the database.
        :return: A ProcessResult tuple of (stdout, stderr, exit_code, timed_out)
        """

        if run.run_id in self._runs:
            raise ValueError("Run has already been registered")

        self.log_debug(f"Performing run with ID {run.run_id} ({celery_id=})")

        self._runs[run.run_id] = run

        # Initialization (loading / downloading files + secrets injection) ---------------------------------------------
        init_vals = self._initialize_run_and_get_command(run, celery_id, secrets)
        if init_vals is None:
            return

        cmd, params_with_secrets = init_vals

        # Perform, finish, and clean up run ----------------------------------------------------------------------------
        return self._perform_run(run, cmd, params_with_secrets)
