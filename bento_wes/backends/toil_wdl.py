import os
import re
import subprocess

from flask import current_app, json
from typing import Optional, Tuple

from bento_wes.backends import WESBackend
from bento_wes.backends.backend_types import Command
from bento_wes.states import STATE_EXECUTOR_ERROR, STATE_SYSTEM_ERROR
from bento_wes.workflows import WorkflowType, WES_WORKFLOW_TYPE_WDL


__all__ = [
    "ToilWDLBackend"
]


# Spec: https://software.broadinstitute.org/wdl/documentation/spec#whitespace-strings-identifiers-constants
WDL_WORKSPACE_NAME_REGEX = re.compile(r"workflow\s+([a-zA-Z][a-zA-Z0-9_]+)")


class ToilWDLBackend(WESBackend):
    def _get_supported_types(self) -> Tuple[WorkflowType]:
        """
        Returns a tuple of the workflow types this backend supports. In this case, only WDL is supported.
        """
        return WES_WORKFLOW_TYPE_WDL,

    def _get_params_file(self, run: dict) -> str:
        """
        Returns the name of the params file to use for the workflow run.
        :param run: The run description; unused here
        :return: The name of the params file; params.json in this case
        """
        return "params.json"

    def _serialize_params(self, workflow_params: dict) -> str:
        """
        Serializes parameters for a particular workflow run into the format expected by toil-wdl-runner.
        :param workflow_params: A dictionary of key-value pairs representing the workflow parameters
        :return: The serialized form of the parameters
        """
        return json.dumps(workflow_params)

    def _check_workflow(self, run: dict) -> Optional[Tuple[str, str]]:
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

    def get_workflow_name(self, workflow_path: str) -> Optional[str]:
        """
        Extracts a workflow's name from a WDL file.
        :param workflow_path: The path to the WDL file
        :return: None if the file could not be parsed for some reason; the name string otherwise
        """

        with open(workflow_path, "r") as wdf:
            wdl_contents = wdf.read()
            workflow_id_match = WDL_WORKSPACE_NAME_REGEX.search(wdl_contents)

            # Invalid/non-workflow-specifying WDL file if false-y
            return workflow_id_match.group(1) if workflow_id_match else None

    def _get_command(self, workflow_path: str, params_path: str, run_dir: str) -> Command:
        """
        Creates the command which will run toil-wdl-runner on the specified WDL workflow, with the specified
        serialized parameters in JSON format, and in the specified run directory.
        :param workflow_path: The path to the WDL file to execute
        :param params_path: The path to the file containing specified parameters for the workflow
        :param run_dir: The directory to run the workflow in
        :return: The command, in the form of a tuple of strings, to be passed to subprocess.run
        """
        # TODO: Separate cleaning process from run?
        return Command((
            "toil-wdl-runner",
            # Output more logging if in debug mode and avoid cleaning up helpful logs
            workflow_path,
            params_path,
            "-o", run_dir,
            *(("--logLevel=DEBUG", "--clean=never", "--cleanWorkDir", "never", "--stats") if self.debug else ()),
            "--workDir", self.tmp_dir,
            "--jobStore", "file:" + os.path.abspath(os.path.join(self.tmp_dir, "toil_job_store"))
        ))
