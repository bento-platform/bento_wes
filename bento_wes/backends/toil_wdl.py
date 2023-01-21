import json
import os
import re

from typing import Optional, Tuple

from bento_wes.backends import WESBackend
from bento_wes.backends.backend_types import Command
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
        return self._check_workflow_wdl(run)

    def get_workflow_name(self, workflow_path: str) -> Optional[str]:
        return self.get_workflow_name_wdl(workflow_path)

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
            *(("--logLevel=DEBUG", "--clean=never", "--cleanWorkDir", "never") if self.debug else ()),
            "--workDir", self.tmp_dir,
            "--writeLogs", self.log_dir,
            "--writeLogsFromAllJobs",
            "--noStdOutErr",
            "--jobStore", "file:" + os.path.abspath(os.path.join(self.tmp_dir, "toil_job_store"))
        ))
