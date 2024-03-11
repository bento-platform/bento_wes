import re

from flask import current_app, json
from pathlib import Path

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

    def _check_workflow(self, run: RunWithDetails) -> None:
        return self._check_workflow_wdl(run)

    def get_workflow_name(self, workflow_path: Path) -> str | None:
        return self.get_workflow_name_wdl(workflow_path)

    @staticmethod
    def get_workflow_metadata_output_json_path(run_dir: Path) -> Path:
        return run_dir / "_job_metadata_output.json"

    def _get_command(self, workflow_path: Path, params_path: Path, run_dir: Path) -> Command:
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
        options_file = run_dir / "_workflow_options.json"
        with open(options_file, "w") as of:
            json.dump({
                # already namespaced by cromwell ID, so don't need to incorporate run ID into this path:
                "final_workflow_outputs_dir": str(self.output_dir),
                "final_workflow_log_dir": str(run_dir / "wf_logs"),
                "final_call_logs_dir": str(run_dir / "call_logs"),
            }, of)

        # TODO: Separate cleaning process from run?
        return Command((
            "java",
            "-DLOG_MODE=pretty",
            # We don't set Cromwell into debug logging mode here even if self.debug is True,
            # since it's intensely verbose.
            "-jar", cromwell, "run",
            "--inputs", str(params_path),
            "--options", str(options_file),
            "--workflow-root", str(run_dir),
            "--metadata-output", str(self.get_workflow_metadata_output_json_path(run_dir)),
            str(workflow_path),
        ))

    def get_workflow_outputs(self, run: RunWithDetails) -> dict[str, dict]:
        p = self.execute_womtool_command(("outputs", str(self.workflow_path(run))))

        stdout, _ = p.communicate()
        workflow_types = json.loads(stdout)

        with open(self.get_workflow_metadata_output_json_path(self.run_dir(run)), "r") as fh:
            outputs = json.load(fh).get("outputs", {})

        # Re-point temporary file outputs to a permanent location (as copied by Cromwell) for future download, and
        # annotate all output values with their type from the WDL.

        tmp_dir_str = str(self.tmp_dir / "cromwell-executions")
        output_dir_str = str(self.output_dir)

        outputs_with_type = {}
        for k, v in outputs.items():
            if isinstance(v, str) and v.startswith(tmp_dir_str):
                v = output_dir_str + v[len(tmp_dir_str):]
            outputs_with_type[k] = {
                "type": workflow_types[k],
                "value": v,
            }

        return outputs_with_type
