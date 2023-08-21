import bento_lib.workflows as w
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
from flask import current_app
from typing import Optional, Tuple, Union

from bento_wes import states
from bento_wes.constants import SERVICE_ARTIFACT, RUN_PARAM_FROM_CONFIG
from bento_wes.db import get_db, finish_run, update_run_state_and_commit
from bento_wes.models import Run, RunWithDetails, BentoWorkflowMetadata, BentoWorkflowInputWithValue
from bento_wes.states import STATE_EXECUTOR_ERROR, STATE_SYSTEM_ERROR
from bento_wes.utils import iso_now
from bento_wes.workflows import WorkflowType, WorkflowManager

from .backend_types import Command, ProcessResult

__all__ = ["WESBackend"]

# Spec: https://software.broadinstitute.org/wdl/documentation/spec#whitespace-strings-identifiers-constants
WDL_WORKSPACE_NAME_REGEX = re.compile(r"workflow\s+([a-zA-Z][a-zA-Z0-9_]+)")

PARAM_SECRET_PREFIX = "secret__"
ParamDict = dict[str, str | int | float | bool]

FORBIDDEN_FROM_CONFIG_PARAMS = (
    "WES_CLIENT_SECRET",
)


class WESBackend(ABC):
    def __init__(
        self,
        tmp_dir: str,
        workflow_timeout: int,  # Workflow timeout, in seconds
        logger=None,
        event_bus: Optional[EventBus] = None,
        workflow_host_allow_list: Optional[set] = None,
        bento_url: Optional[str] = None,
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
    def _check_workflow(self, run: Run) -> Optional[Tuple[str, str]]:
        """
        Checks that a workflow can be executed by the backend via the workflow's URI.
        :param run: The run, including a request with the workflow URI
        :return: None if the workflow is valid; a tuple of an error message and an error state otherwise
        """
        pass

    def _check_workflow_wdl(self, run: RunWithDetails) -> Optional[Tuple[str, str]]:
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

    def _check_workflow_and_type(self, run: RunWithDetails) -> Optional[Tuple[str, str]]:
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
    def get_workflow_name(self, workflow_path: str) -> Optional[str]:
        """
        Extracts a workflow's name from its file.
        :param workflow_path: The path to the workflow definition file
        :return: None if the file could not be parsed for some reason; the name string otherwise
        """
        pass

    @staticmethod
    def get_workflow_name_wdl(workflow_path: str) -> Optional[str]:
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

    def _update_run_state_and_commit(self, run_id: Union[uuid.UUID, str], state: str) -> None:
        """
        Wrapper for the database "update_run_state_and_commit" function, which updates a run's state in the database.
        :param run_id: The ID of the run whose state is getting updated
        :param state: The value to set the run's current state to
        """
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
        access_token: str,
    ) -> tuple[Command, dict] | None:
        """
        Performs "initialization" operations on the run, including setting states, downloading and validating the
        workflow file, and generating and logging the workflow-running command.
        :param run: The run to initialize
        :param celery_id: The Celery ID of the Celery task responsible for executing the run
        :param access_token: An access token for talking with this Bento instance's services
        :return: The command to execute, if no errors occurred; None otherwise
        """

        self._update_run_state_and_commit(run.run_id, states.STATE_INITIALIZING)

        run_dir = self.run_dir(run)

        # -- Check that the run directory exists ------------------------------
        if not os.path.exists(run_dir):
            # TODO: Log error in run log
            self.log_error("Run directory not found")
            return self._finish_run_and_clean_up(run, states.STATE_SYSTEM_ERROR)

        c = self.db.cursor()

        workflow_id = run.request.tags.workflow_id
        workflow_params: ParamDict = {
            **run.request.workflow_params,
            f"{workflow_id}.{PARAM_SECRET_PREFIX}access_token": access_token,

            # In export/analysis mode, as we rely on services located in different containers
            # there is a need to have designated folders on shared volumes between
            # WES and the other services, to write files to.
            # This is possible because /wes/tmp is a volume mounted with the same
            # path in each data service (except Gohan which mounts the dropbox
            # data-x directory directly instead, to avoid massive duplicates).
            # Also, the Toil library creates directories in the generic /tmp/
            # directory instead of the one that is configured for the job execution,
            # to create the current working directories where tasks are executed.
            # These files are inaccessible to other containers in the context of a
            # task unless they are written arbitrarily to run_dir
            f"{workflow_id}.run_dir": run_dir,

            # TODO: in the long run, we shouldn't tie workflow runs to a specific dataset ID inherently, necessarily
            #  - instead, they can be special input parameters that get translated to strings
            f"{workflow_id}.project_id": run.request.tags.project_id,
            f"{workflow_id}.dataset_id": run.request.tags.dataset_id,
            # Don't use data_type from workflow metadata here - instead, workflows can say what they're ingesting

            f"{workflow_id}.service_url": run.request.tags.service_url,
            # TODO: more special parameters: service URLs, system__run_dir...
        }

        # Some workflow parameters depend on the WES application configuration
        # and need to be added from there.
        # The reserved keyword `FROM_CONFIG` is used to detect those inputs.
        # All parameters in config are upper case. e.g. drs_url --> DRS_URL
        for i in run.request.tags.workflow_metadata.inputs:
            if not isinstance(i, BentoWorkflowInputWithValue) or i.value != RUN_PARAM_FROM_CONFIG:
                continue
            if i.id.lower() in FORBIDDEN_FROM_CONFIG_PARAMS:  # Cannot grab WES client secret, for example
                continue
            workflow_params[f"{workflow_id}.{i.id}"] = current_app.config.get(i.id, "")

        # -- Validate the workflow --------------------------------------------
        error = self._check_workflow_and_type(run)
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
        c.execute("UPDATE runs SET run_log__name = ? WHERE id = ?", (workflow_name, run.run_id))
        self.db.commit()

        # -- Store input for the workflow in a file in the temporary folder ---
        with open(self._params_path(run), "w") as pf:
            pf.write(self._serialize_params(workflow_params))

        # -- Create the runner command based on inputs ------------------------
        cmd = self._get_command(self.workflow_path(run), self._params_path(run), self.run_dir(run))

        # -- Update run log with command and Celery ID ------------------------
        c.execute(
            "UPDATE runs SET run_log__cmd = ?, run_log__celery_id = ? WHERE id = ?",
            (" ".join(cmd), celery_id, run.run_id))
        self.db.commit()

        return cmd, workflow_params

    def _build_workflow_outputs(
        self,
        run_dir: str,
        workflow_id: str,
        workflow_params: dict,
        workflow_metadata: BentoWorkflowMetadata,
    ):
        self.logger.info(f"Building workflow outputs for workflow ID {workflow_id}")
        output_params = w.make_output_params(workflow_id, workflow_params, [dict(i) for i in workflow_metadata.inputs])

        workflow_outputs = {}
        for output in workflow_metadata.outputs:
            o_id = output.id
            fo = w.formatted_output(dict(output), output_params)

            # Skip optional outputs resulting from optional inputs
            if fo is None:
                continue

            # Rewrite file outputs to include full path to temporary location
            if output.type == w.WORKFLOW_TYPE_FILE:
                workflow_outputs[o_id] = os.path.abspath(os.path.join(run_dir, "output", fo))

            elif output.type == w.WORKFLOW_TYPE_FILE_ARRAY:
                workflow_outputs[o_id] = [os.path.abspath(os.path.join(run_dir, wo)) for wo in fo]
                self.logger.info(
                    f"Setting workflow output {o_id} to [{', '.join(workflow_outputs[o_id])}]")

            else:
                workflow_outputs[o_id] = fo
                self.logger.info(f"Setting workflow output {o_id} to {workflow_outputs[o_id]}")

        return workflow_outputs

    def _perform_run(self, run: RunWithDetails, cmd: Command, params_with_extras: ParamDict) -> Optional[ProcessResult]:
        """
        Performs a run based on a provided command and returns stdout, stderr, exit code, and whether the process timed
        out while running.
        :param run: The run to execute
        :param cmd: The command used to execute the run
        :param params_with_extras: A dictionary of parameters, including secret values
        :return: A ProcessResult tuple of (stdout, stderr, exit_code, timed_out)
        """

        c = self.db.cursor()

        # Perform run =========================================================

        # -- Start process running the generated command ----------------------
        runner_process = subprocess.Popen(
            cmd, cwd=self.tmp_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        c.execute("UPDATE runs SET run_log__start_time = ? WHERE id = ?", (iso_now(), run.run_id))
        self._update_run_state_and_commit(run.run_id, states.STATE_RUNNING)

        # -- Wait for and capture output --------------------------------------

        timed_out = False

        try:
            stdout, stderr = runner_process.communicate(timeout=self._workflow_timeout)

        except subprocess.TimeoutExpired:
            runner_process.kill()
            stdout, stderr = runner_process.communicate()
            timed_out = True

        finally:
            exit_code = runner_process.returncode

        # -- Censor output in case it includes any secrets

        for k, v in params_with_extras.items():
            if PARAM_SECRET_PREFIX not in k:
                continue
            if isinstance(v, str) and len(v) >= 5:  # redacted secrets must be somewhat lengthy
                stdout = stdout.replace(v, "<redacted>")
                stderr = stderr.replace(v, "<redacted>")

        # Complete run ========================================================

        # -- Get various Bento-specific data from tags ------------------------

        tags = run.request.tags

        workflow_metadata = tags.workflow_metadata
        project_id: str = tags.project_id
        dataset_id: str | None = tags.dataset_id

        # -- Update run log with stdout/stderr, exit code ---------------------
        #     - Explicitly don't commit here; sync with state update
        c.execute("UPDATE runs SET run_log__stdout = ?, run_log__stderr = ?, run_log__exit_code = ? WHERE id = ?",
                  (stdout, stderr, exit_code, run.run_id))

        if timed_out:
            # TODO: Report error somehow
            self.log_error("Encountered timeout while performing run")
            return self._finish_run_and_clean_up(run, states.STATE_SYSTEM_ERROR)

        # -- Final steps: check exit code and report results ------------------

        if exit_code != 0:
            # TODO: Report error somehow
            self.log_error("Encountered a non-zero exit code while performing run")
            return self._finish_run_and_clean_up(run, states.STATE_EXECUTOR_ERROR)

        # Exit code is 0 otherwise; complete the run

        run_dir = self.run_dir(run)
        workflow_name = self.get_workflow_name(self.workflow_path(run))
        workflow_params: dict = run.request.workflow_params

        workflow_outputs = self._build_workflow_outputs(run_dir, workflow_name, workflow_params, workflow_metadata)

        # Explicitly don't commit here; sync with state update
        c.execute("UPDATE runs SET outputs = ? WHERE id = ?", (json.dumps(workflow_outputs), str(run.run_id)))

        # Run result object
        run_results = {
            "project_id": project_id,
            **({"dataset_id": dataset_id} if dataset_id else {}),

            "workflow_id": workflow_name,
            "workflow_metadata": workflow_metadata.model_dump_json(),
            "workflow_outputs": workflow_outputs,
            "workflow_params": workflow_params
        }

        # Emit event if possible
        self.event_bus.publish_service_event(SERVICE_ARTIFACT, EVENT_WES_RUN_FINISHED, run_results)

        # Finally, set our state to COMPLETE + finish up the run.
        self._finish_run_and_clean_up(run, states.STATE_COMPLETE)

        return ProcessResult((stdout, stderr, exit_code, timed_out))

    def perform_run(self, run: RunWithDetails, celery_id: int, access_token: str) -> Optional[ProcessResult]:
        """
        Executes a run from start to finish (initialization, startup, and completion / cleanup.)
        :param run: The run to execute
        :param celery_id: The ID of the Celery task responsible for executing the workflow
        :param access_token: An access token for talking with this Bento instance's services
        :return: A ProcessResult tuple of (stdout, stderr, exit_code, timed_out)
        """

        if run.run_id in self._runs:
            raise ValueError("Run has already been registered")

        self._runs[run.run_id] = run

        # Initialization (loading / downloading files) ------------------------
        init_vals = self._initialize_run_and_get_command(run, celery_id, access_token)
        if init_vals is None:
            return

        cmd, params_with_extras = init_vals

        # Perform, finish, and clean up run -----------------------------------
        return self._perform_run(run, cmd, params_with_extras)
