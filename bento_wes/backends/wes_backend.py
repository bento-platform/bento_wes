import aiofiles.os
import aiofiles.ospath
import os
import re
import requests
import shutil
import subprocess
import uuid

from abc import ABC, abstractmethod
from bento_lib.events import EventBus
from bento_lib.events.types import EVENT_WES_RUN_FINISHED
from bento_lib.service_info.manager import ServiceManager
from bento_lib.utils.headers import authz_bearer_header
from bento_lib.workflows.models import (
    WorkflowSecretInput,
    WorkflowFileInput,
    WorkflowFileArrayInput,
    WorkflowDirectoryInput,
)
from bento_lib.workflows.utils import namespaced_input
from pathlib import Path
from structlog.stdlib import BoundLogger
from typing import overload, Sequence, Literal

from bento_wes import states
from bento_wes.config import Settings
from bento_wes.constants import SERVICE_ARTIFACT
from bento_wes.db import Database, get_db_with_event_bus
from bento_wes.models import Run, RunWithDetails, RunOutput
from bento_wes.states import STATE_EXECUTOR_ERROR, STATE_SYSTEM_ERROR
from bento_wes.utils import iso_now
from bento_wes.workflows import WORKFLOW_IGNORE_FILE_PATH_INJECTION, WorkflowType, WorkflowManager

from .backend_types import Command, ProcessResult
from .exceptions import RunExceptionWithFailState

__all__ = ["WESBackend"]

# Spec: https://software.broadinstitute.org/wdl/documentation/spec#whitespace-strings-identifiers-constants
WDL_WORKSPACE_NAME_REGEX = re.compile(r"workflow\s+([a-zA-Z][a-zA-Z0-9_]+)")

ParamDict = dict[str, str | int | float | bool]


class WESBackend(ABC):
    def __init__(
        self,
        event_bus: EventBus,
        logger: BoundLogger,
        service_manager: ServiceManager,
        settings: Settings,
        workflow_manager: WorkflowManager,
    ):
        self.event_bus: EventBus = event_bus
        self.logger: BoundLogger = logger
        self.service_manager: ServiceManager = service_manager
        self.settings: Settings = settings

        self._workflow_timeout: int = settings.workflow_timeout

        self.tmp_dir: Path = settings.service_temp
        self.data_dir: Path = settings.service_data

        self.output_dir: Path = self.data_dir / "output"  # For persistent file artifacts from workflows
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self._db_gen = get_db_with_event_bus(self.logger, self.event_bus)
        self.db: Database = next(self._db_gen)

        # Bento-specific parameters
        self.bento_url = str(settings.bento_url)

        self.validate_ssl: bool = settings.bento_validate_ssl
        self.debug: bool = settings.bento_debug

        self._workflow_manager: WorkflowManager = workflow_manager

        self._runs = {}

        self.logger.debug("Instantiating WESBackend with debug=%s", self.debug)

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
    def _serialize_params(self, workflow_params: ParamDict) -> bytes:
        """
        Serializes parameters for a particular workflow run into the format expected by the backend's runner.
        :param workflow_params: A dictionary of key-value pairs representing the workflow parameters
        :return: The serialized form of the parameters
        """
        pass

    def workflow_path(self, run: RunWithDetails) -> Path:
        """
        Gets the local filesystem path to the workflow file specified by a run's workflow URI.
        """
        return self._workflow_manager.workflow_path(run.request.workflow_url, WorkflowType(run.request.workflow_type))

    def run_dir(self, run: Run) -> Path:
        """
        Returns a path to the work directory for executing a run.
        """
        return self.tmp_dir / run.run_id

    def _params_path(self, run: Run) -> Path:
        """
        Returns a path to the workflow parameters file for a run.
        """
        return self.run_dir(run) / self._get_params_file(run)

    @abstractmethod
    def _check_workflow(self, run: Run) -> None:
        """
        Checks that a workflow can be executed by the backend via the workflow's URI. A RunExceptionWithFailState is
        raised if the workflow is not valid.
        :param run: The run, including a request with the workflow URI
        """
        pass

    def get_womtool_path_or_raise(self) -> str:
        womtool_path = self.settings.wom_tool_location
        if not womtool_path:
            raise RunExceptionWithFailState(
                STATE_SYSTEM_ERROR,
                f"Missing or invalid WOMtool (Bad WOM_TOOL_LOCATION)\n\tWOM_TOOL_LOCATION: {womtool_path}",
            )
        return womtool_path

    def execute_womtool_command(self, command: tuple[str, ...]) -> subprocess.Popen:
        womtool_path = self.get_womtool_path_or_raise()

        # Check for Java (needed to run WOMtool)
        try:
            subprocess.run(("java", "-version"))
        except FileNotFoundError:
            raise RunExceptionWithFailState(STATE_SYSTEM_ERROR, "Java is missing (required to validate WDL files)")

        # Execute WOMtool command
        return subprocess.Popen(
            ("java", "-jar", womtool_path, *command), stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8"
        )

    def _check_workflow_wdl(self, run: RunWithDetails) -> None:
        """
        Checks that a particular WDL workflow is valid. A RunExceptionWithFailState is raised if the WDL is not valid.
        :param run: The run whose workflow is being checked
        """

        # Validate WDL, listing dependencies:
        vr = self.execute_womtool_command(("validate", "-l", str(self.workflow_path(run))))

        v_out, v_err = vr.communicate()

        if vr.returncode != 0:
            # Validation error with WDL file
            raise RunExceptionWithFailState(
                STATE_EXECUTOR_ERROR,
                f"Failed with {STATE_EXECUTOR_ERROR} due to non-0 validation return code:\n"
                f"\tstdout: {v_out}\n\tstderr: {v_err}",
            )

        #  - Since Toil doesn't support WDL imports right now, any dependencies will result in an error
        if "None" not in v_out:  # No dependencies
            # Toil can't process WDL dependencies right now  TODO
            raise RunExceptionWithFailState(
                STATE_EXECUTOR_ERROR,
                f"Failed with {STATE_EXECUTOR_ERROR} due to dependencies in WDL:\n\tstdout: {v_out}\n\tstderr: {v_err}",
            )

    def _check_workflow_and_type(self, run: RunWithDetails) -> None:
        """
        Checks a workflow file's validity. A RunExceptionWithFailState is raised if the workflow file is not valid.
        A NotImplementedError is raised if the workflow type is not supported by the backend.
        :param run: The run specifying the workflow in question
        """

        workflow_type: WorkflowType = WorkflowType(run.request.workflow_type)
        if workflow_type not in self._get_supported_types():
            raise NotImplementedError(f"The specified WES backend cannot execute workflows of type {workflow_type}")

        return self._check_workflow(run)

    @abstractmethod
    def get_workflow_name(self, workflow_path: Path) -> str | None:
        """
        Extracts a workflow's name from its file.
        :param workflow_path: The path to the workflow definition file
        :return: None if the file could not be parsed for some reason; the name string otherwise
        """
        pass

    @staticmethod
    def get_workflow_name_wdl(workflow_path: Path) -> str | None:
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

    async def _download_to_path(self, url: str, token: str, destination: Path | str, logger: BoundLogger):
        """
        Download a file from a URL to a destination directory.
        Bearer token auth works with Drop-Box and DRS.
        """
        # TODO: use aiohttp
        with requests.get(url, headers=authz_bearer_header(token), verify=self.validate_ssl, stream=True) as response:
            if response.status_code != 200:
                raise RunExceptionWithFailState(
                    STATE_EXECUTOR_ERROR,
                    f"Download request to drop-box resulted in a non 200 status code: {response.status_code}",
                )
            with open(destination, "wb") as f:
                # chunk_size=None to use the chunk size from the stream
                for chunk in response.iter_content(chunk_size=None):
                    f.write(chunk)
        await logger.adebug("downloaded file", file_url=url, file_destination=destination)

    @overload
    async def _download_input_files(self, inputs: str, token: str, run_dir: Path, logger: BoundLogger) -> str: ...

    @overload
    async def _download_input_files(
        self, inputs: list[str], token: str, run_dir: Path, logger: BoundLogger
    ) -> list[str]: ...

    async def _download_input_files(
        self, inputs: str | list[str], token: str, run_dir: Path, logger: BoundLogger
    ) -> str | list[str]:
        if not inputs:
            # Ignore empty inputs
            return inputs

        if isinstance(inputs, list):
            return [await self._download_input_file(f, token, run_dir, logger) for f in inputs]
        else:
            return await self._download_input_file(inputs, token, run_dir, logger)

    @staticmethod
    def _build_download_path(run_dir: Path) -> Path:
        d = (run_dir / "downloaded").resolve()
        d.mkdir(parents=True, exist_ok=True)
        return d

    @staticmethod
    async def _validate_sub_path(parent_dir: Path, child_path: Path, logger: BoundLogger):
        # Validate our file path hasn't escaped the run directory
        if not str(child_path.resolve()).startswith(str(parent_dir.resolve())):
            await logger.aerror(
                "temporary path must be a sub-path of directory", child_path=child_path, parent_dir=parent_dir
            )
            raise RunExceptionWithFailState(
                STATE_EXECUTOR_ERROR, f"Temporary path bust be a sub-path of directory {parent_dir}"
            )

    async def _get_drop_box_resource_url(self, path: str, resource: Literal["objects", "tree"] = "objects") -> str:
        drop_box_url = await self.service_manager.get_bento_service_url_by_kind("drop-box")
        clean_path = path.lstrip("/")
        return f"{drop_box_url}/{resource}/{clean_path}"

    async def _download_input_file(self, obj_path: str, token: str, run_dir: Path, logger: BoundLogger) -> str:
        """
        Downloads an input file from Drop-Box in the run directory.
        Returns the path to the temp file to inject in the workflow params.
        """
        if not obj_path:
            # Ignore empty inputs (e.g. reference genome ingestion with no GFF3 files)
            return obj_path

        download_dir = self._build_download_path(run_dir)
        file_name = obj_path.split("/")[-1]
        tmp_file_path = download_dir / file_name

        # Validate our file path hasn't escaped the run directory
        await self._validate_sub_path(download_dir, tmp_file_path, logger)

        # Downloads file to /wes/tmp/<run_dir>/<file_name>
        download_url = await self._get_drop_box_resource_url(obj_path)
        await self._download_to_path(download_url, token, tmp_file_path, logger)
        return str(tmp_file_path)

    async def _download_directory_tree(
        self,
        tree: list[dict],
        token: str,
        download_dir: Path,
        logger: BoundLogger,
    ):
        """
        Downloads the contents of a given Drop Box tree or subtree to the temporary run_dir directory
        e.g. /wes/tmp/<Run ID>/<Dir Tree>
        """

        for node in tree:
            if contents := node.get("contents"):
                # Node is a directory: go inside recursively to find files
                await self._download_directory_tree(contents, token, download_dir, logger)
            elif uri := node.get("uri"):
                # Node is a file: download
                #  - we need to strip the starting "/", otherwise this escapes the run directory.
                tmp_path = download_dir.joinpath(node["relativePath"].lstrip("/"))

                logger = logger.bind(node_uri=node["uri"], tmp_path=tmp_path)

                # Validate our file path hasn't escaped the run directory
                await self._validate_sub_path(download_dir, tmp_path, logger)

                await logger.adebug("_download_directory_tree: downloading node to temporary path")
                await aiofiles.os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
                await self._download_to_path(uri, token, tmp_path, logger)

    async def _download_input_directory(
        self,
        directory: str,
        token: str,
        run_dir: Path,
        logger: BoundLogger,
        ignore_extensions: Sequence[str] | None = None,
    ) -> str:
        logger = logger.bind(directory_to_download=directory)
        await logger.adebug("_download_input_directory called")

        drop_box_url = await self.service_manager.get_bento_service_url_by_kind("drop-box")

        sub_tree = directory.lstrip("/")

        ignore_param = ""
        if ignore_extensions:
            # build query params to ignore extensions
            ignore_param = "&".join([f"ignore={ext}" for ext in ignore_extensions])

        url = f"{drop_box_url}/tree/{sub_tree}"
        if ignore_param:
            # add ignore query params
            url = f"{url}?{ignore_param}"

        download_dir = self._build_download_path(run_dir)
        final_dir = download_dir.joinpath(sub_tree)

        await self._validate_sub_path(download_dir, final_dir, logger)

        # Fetch directory subtree from Drop Box
        with requests.get(url, headers=authz_bearer_header(token), verify=self.validate_ssl, stream=True) as response:
            if response.status_code != 200:
                await logger.aerror(
                    "tree request to drop box gave error response",
                    status_code=response.status_code,
                    response_content=response.content.decode("utf-8"),
                )
                raise RunExceptionWithFailState(
                    STATE_EXECUTOR_ERROR,
                    f"Tree request to drop box resulted in a non 200 status code: {response.status_code}",
                )
            tree = response.json()

            # Download tree content under download_dir
            await self._download_directory_tree(tree, token, download_dir, logger)

        return str(final_dir)

    @abstractmethod
    async def _get_command(self, workflow_path: Path, params_path: Path, run_dir: Path) -> Command:
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
        self.logger.debug("setting state of run", run_state=state)
        self.db.update_run_state_and_commit(run_id, state)

    def _finish_run_and_clean_up(self, run: Run, state: str) -> None:
        """
        Performs standard run-finishing operations (updating state, setting end time, etc.) as well as deleting the run
        folder if it exists.
        :param run: The run to perform "finishing" operations on
        :param state: The final state of the run
        """

        # Finish run ----------------------------------------------------------

        self.db.finish_run(run, state)

        # Clean up ------------------------------------------------------------

        del self._runs[run.run_id]

        # -- Clean up any run files at the end, after they've been either -----
        #    copied or "rejected" due to some failure.
        # TODO: SECURITY: Check run_dir
        # TODO: May want to keep them around for a retry depending on how the retry operation will work.

        if not self.debug:
            shutil.rmtree(self.run_dir(run), ignore_errors=True)

    async def _initialize_run_and_get_command(
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

        logger = self.logger.bind(run_dir=run_dir)

        # -- Check that the run directory exists -----------------------------------------------------------------------
        if not await aiofiles.ospath.exists(run_dir):
            # TODO: Log error in run log
            await logger.aerror("run directory not found")
            return self._finish_run_and_clean_up(run, states.STATE_SYSTEM_ERROR)

        run_req = run.request

        # run_req.workflow_params now includes non-secret injected values since it was read from the database after
        # the run ID was passed to the runner:
        processed_workflow_params: ParamDict = {**run_req.workflow_params}

        # -- Check if file injection needed ----------------------------------------------------------------------------
        # Most workflow with input files expect the files to be accessible locally (temp file inject)
        # In some cases, the targeted service itself will obtain the file from Drop-Box (e.g. gohan VCFs ingestion)
        # For such cases, file and directory inputs are passed as-is, and the service can retrieve the files from:
        # Individual files:     https://<BENTO DOMAIN>/api/drop-box/objects/<OBJECT PATH>
        # Directories:          https://<BENTO DOMAIN>/api/drop-box/tree/<DIRECTORY PATH>
        skip_file_input_injection = run_req.tags.workflow_id in WORKFLOW_IGNORE_FILE_PATH_INJECTION

        # -- Inject workflow inputs that should NOT be stored in DB (secrets, temporary files) -------------------------
        for run_input in run_req.tags.workflow_metadata.inputs:
            lg = logger.bind(input_type=run_input.type, input_id=run_input.id)
            if isinstance(run_input, WorkflowSecretInput):
                # Find which inputs are secrets, which need to be injected here (so they don't end up in the database)
                secret_key = run_input.key
                secret_value = secrets.get(secret_key)
                lg_ = lg.bind(input_secret_key=secret_key)
                if secret_value is None:
                    await lg_.aerror("could not find injectable secret for key")
                    return self._finish_run_and_clean_up(run, STATE_EXECUTOR_ERROR)
                processed_workflow_params[namespaced_input(run_req.tags.workflow_id, run_input.id)] = secret_value
                # don't log actual secret or even actual secret length
                await lg_.adebug(
                    "injecting secret as parameter",
                    input_value="***",  # don't log actual secret or even actual secret length, but keep structure
                )
            elif isinstance(run_input, (WorkflowFileInput, WorkflowFileArrayInput)):
                # Finds workflow inputs for drop-box file(s)
                # Downloads the file(s) in a temp dir and injects the path(s)
                param_key = namespaced_input(run_req.tags.workflow_id, run_input.id)
                input_param = run_req.workflow_params.get(param_key)
                lg_ = lg.bind(input_param=input_param)
                if not skip_file_input_injection:
                    # inject input(s) as temp files
                    injected_input = await self._download_input_files(input_param, secrets["access_token"], run_dir, lg)
                else:
                    injected_input = input_param
                processed_workflow_params[param_key] = injected_input
                await lg_.adebug(
                    "injecting file or file array as parameter",
                    input_value=injected_input,
                )
            elif isinstance(run_input, WorkflowDirectoryInput):
                # Finds workflow inputs for a drop-box directory
                # Downloads the directory's contents to a temp directory and injects the path
                param_key = namespaced_input(run_req.tags.workflow_id, run_input.id)
                input_param = run_req.workflow_params.get(param_key)
                lg_ = lg.bind(input_param=input_param)

                # TODO: directory workflows should simply include a list of file extentions to filter out.
                filter_vcfs = run_req.workflow_params.get("experiments_json_with_files.filter_out_vcf_files")
                filter_extensions: tuple[str, ...] | None = (".vcf", ".vcf.gz") if filter_vcfs else None

                if not skip_file_input_injection:
                    injected_dir = await self._download_input_directory(
                        input_param, secrets["access_token"], run_dir, logger, filter_extensions
                    )
                    await lg_.ainfo(
                        "input parameter: injecting directory", input_param=input_param, injected_dir=injected_dir
                    )
                else:
                    injected_dir = input_param
                processed_workflow_params[param_key] = injected_dir
                await lg_.adebug(
                    "injecting directory as parameter",
                    input_value=injected_dir,
                )

        # -- Validate the workflow -------------------------------------------------------------------------------------

        self._check_workflow_and_type(run)  # RunExceptionWithFailState can be thrown, handled by caller of this fn.

        # -- Find "real" workflow name from workflow file --------------------------------------------------------------
        workflow_name = self.get_workflow_name(self.workflow_path(run))
        if workflow_name is None:
            # Invalid/non-workflow-specifying workflow file
            await logger.aerror("could not find workflow name in workflow file", workflow_name=workflow_name)
            return self._finish_run_and_clean_up(run, states.STATE_SYSTEM_ERROR)

        self.db.set_run_log_name(run, workflow_name)

        # -- Store input for the workflow in a file in the temporary folder --------------------------------------------
        async with aiofiles.open(self._params_path(run), "wb") as pf:
            await pf.write(self._serialize_params(processed_workflow_params))

        # -- Create the runner command based on inputs -----------------------------------------------------------------
        cmd = await self._get_command(self.workflow_path(run), self._params_path(run), self.run_dir(run))

        # -- Update run log with command and Celery ID -----------------------------------------------------------------
        self.db.set_run_log_command_and_celery_id(run, cmd, celery_id)

        return cmd, processed_workflow_params

    @abstractmethod
    async def get_workflow_outputs(self, run: RunWithDetails) -> dict[str, RunOutput]:
        pass

    async def _perform_run(
        self, run: RunWithDetails, cmd: Command, params_with_secrets: ParamDict
    ) -> ProcessResult | None:
        """
        Performs a run based on a provided command and returns stdout, stderr, exit code, and whether the process timed
        out while running.
        :param run: The run to execute
        :param cmd: The command used to execute the run
        :param params_with_secrets: A dictionary of parameters, including secret values
        :return: A ProcessResult tuple of (stdout, stderr, exit_code, timed_out)
        """

        # Perform run ==================================================================================================

        # -- Start process running the generated command ---------------------------------------------------------------
        #  - Cromwell creates the `cromwell-executions` and `cromwell-workflow-logs` folders in the CWD, so we set the
        #    CWD of the subprocess to our WES temporary directory.
        runner_process = subprocess.Popen(
            cmd, cwd=self.tmp_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8"
        )
        self.db.c().execute("UPDATE runs SET run_log__start_time = ? WHERE id = ?", (iso_now(), run.run_id))
        self.db.commit()
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
        self.db.c().execute(
            "UPDATE runs SET run_log__stdout = ?, run_log__stderr = ?, run_log__exit_code = ? WHERE id = ?",
            (stdout, stderr, exit_code, run.run_id),
        )

        if timed_out:
            # TODO: Report error somehow
            await self.logger.aerror("encountered timeout while performing run")
            return self._finish_run_and_clean_up(run, states.STATE_SYSTEM_ERROR)

        # -- Final steps: check exit code and report results -----------------------------------------------------------

        if exit_code != 0:
            # TODO: Report error somehow
            await self.logger.aerror("encountered a non-zero exit code while performing run")
            return self._finish_run_and_clean_up(run, states.STATE_EXECUTOR_ERROR)

        # Exit code is 0 otherwise; complete the run

        workflow_outputs = await self.get_workflow_outputs(run)

        # Explicitly don't commit here; sync with state update
        self.db.set_run_outputs(run.run_id, workflow_outputs)

        # Emit event if possible
        self.event_bus.publish_service_event(
            SERVICE_ARTIFACT,  # TODO: bento_lib: replace with service kind
            EVENT_WES_RUN_FINISHED,
            # Run result object:
            event_data={
                "workflow_id": self.get_workflow_name(self.workflow_path(run)),
                "workflow_metadata": run.request.tags.workflow_metadata.model_dump_json(),
                "workflow_outputs": workflow_outputs,
                "workflow_params": run.request.workflow_params,
            },
        )

        # Finally, set our state to COMPLETE + finish up the run. This commits all our changes to the database.
        self._finish_run_and_clean_up(run, states.STATE_COMPLETE)

        return ProcessResult((stdout, stderr, exit_code, timed_out))

    async def perform_run(self, run: RunWithDetails, celery_id: int, secrets: dict[str, str]) -> ProcessResult | None:
        """
        Executes a run from start to finish (initialization, startup, and completion / cleanup.)
        :param run: The run to execute
        :param celery_id: The ID of the Celery task responsible for executing the workflow
        :param secrets: A dictionary of secrets (e.g., tokens) to be injected as parameters (potentially) but not stored
                        in the database.
        :return: A ProcessResult tuple of (stdout, stderr, exit_code, timed_out)
        """

        # Logger should already have bound:
        #  - run_id
        #  - celery_id

        if run.run_id in self._runs:
            raise ValueError("Run has already been registered")

        await self.logger.adebug("performing run")

        self._runs[run.run_id] = run

        # Initialization (loading / downloading files + secrets injection) ---------------------------------------------
        try:
            init_vals = await self._initialize_run_and_get_command(run, celery_id, secrets)
        except RunExceptionWithFailState as e:
            self.logger.error(str(e))
            self._finish_run_and_clean_up(run, e.state)
            return None

        if init_vals is None:
            return None

        cmd, params_with_secrets = init_vals

        # Perform, finish, and clean up run ----------------------------------------------------------------------------
        return await self._perform_run(run, cmd, params_with_secrets)

    def close(self):
        try:
            next(self._db_gen)
        except StopIteration:
            pass
