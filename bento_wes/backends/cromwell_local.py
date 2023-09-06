import os.path
import re

from flask import current_app, json
from typing import Any

from bento_wes.backends import WESBackend
from bento_wes.backends.backend_types import Command
from bento_wes.models import Run, RunWithDetails
from bento_wes.workflows import WorkflowType, WES_WORKFLOW_TYPE_WDL


__all__ = [
    "CromwellLocalBackend"
]


# Spec: https://software.broadinstitute.org/wdl/documentation/spec#whitespace-strings-identifiers-constants
WDL_WORKSPACE_NAME_REGEX = re.compile(r"workflow\s+([a-zA-Z][a-zA-Z0-9_]+)")


class CromwellLocalBackend(WESBackend):
    def _get_supported_types(self) -> tuple[WorkflowType, ...]:
        """
        Returns a tuple of the workflow types this backend supports. In this case, only WDL is supported.
        """
        return WES_WORKFLOW_TYPE_WDL,

    def _get_params_file(self, run: Run) -> str:
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

    def _check_workflow(self, run: RunWithDetails) -> tuple[str, str] | None:
        return self._check_workflow_wdl(run)

    def get_workflow_name(self, workflow_path: str) -> str | None:
        return self.get_workflow_name_wdl(workflow_path)

    @staticmethod
    def get_workflow_metadata_output_json_path(run_dir: str) -> str:
        return os.path.join(run_dir, "_job_metadata_output.json")

    def _get_command(self, workflow_path: str, params_path: str, run_dir: str) -> Command:
        """
        Creates the command which will run Cromwell in CLI mode on the specified WDL workflow, with the specified
        serialized parameters in JSON format, and in the specified run directory.
        :param workflow_path: The path to the WDL file to execute
        :param params_path: The path to the file containing specified parameters for the workflow
        :param run_dir: The directory to run the workflow in
        :return: The command, in the form of a tuple of strings, to be passed to subprocess.run
        """

        cromwell = current_app.config["CROMWELL_LOCATION"]

        # Create workflow options file
        options_file = run_dir + "/_workflow_options.json"
        with open(options_file, "w") as of:
            json.dump({
                "final_workflow_outputs_dir": run_dir + "/output",
                "use_relative_output_paths": True,
                "final_workflow_log_dir": run_dir + "/wf_logs",
                "final_call_logs_dir": run_dir + "/call_logs",
            }, of)

        # TODO: Separate cleaning process from run?
        return Command((
            "java", "-jar", cromwell, "run",
            "--inputs", params_path,
            "--options", options_file,
            "--workflow-root", run_dir,
            "--metadata-output", self.get_workflow_metadata_output_json_path(run_dir),
            workflow_path,
        ))

    def get_workflow_outputs(self, run_dir: str) -> dict[str, Any]:
        with open(self.get_workflow_metadata_output_json_path(run_dir), "r") as fh:
            return json.load(fh).get("outputs", {})
