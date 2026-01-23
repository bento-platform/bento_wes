import aiofiles
import orjson
import re
from pathlib import Path
from typing import TypeVar

from bento_wes.models import Run, RunWithDetails
from bento_wes.workflows import WorkflowType, WES_WORKFLOW_TYPE_WDL

from .backend_types import Command
from .wes_backend import WESBackend


__all__ = ["CromwellLocalBackend"]


T = TypeVar("T")

# Spec: https://software.broadinstitute.org/wdl/documentation/spec#whitespace-strings-identifiers-constants
WDL_WORKSPACE_NAME_REGEX = re.compile(r"workflow\s+([a-zA-Z][a-zA-Z0-9_]+)")


class CromwellLocalBackend(WESBackend):
    def _get_supported_types(self) -> tuple[WorkflowType, ...]:
        """
        Returns a tuple of the workflow types this backend supports. In this case, only WDL is supported.
        """
        return (WES_WORKFLOW_TYPE_WDL,)

    def _get_params_file(self, run: Run) -> str:
        """
        Returns the name of the params file to use for the workflow run.
        :param run: The run description; unused here
        :return: The name of the params file; params.json in this case
        """
        return "params.json"

    def _serialize_params(self, workflow_params: dict) -> bytes:
        """
        Serializes parameters for a particular workflow run into the format expected by toil-wdl-runner.
        :param workflow_params: A dictionary of key-value pairs representing the workflow parameters
        :return: The serialized form of the parameters
        """
        return orjson.dumps(workflow_params)

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

        cromwell = self.settings.cromwell_location

        # Create workflow options file
        options_file = run_dir / "_workflow_options.json"
        with open(options_file, "w") as of:
            json.dump(
                {
                    # already namespaced by cromwell ID, so don't need to incorporate run ID into this path:
                    "final_workflow_outputs_dir": str(self.output_dir),
                    "final_workflow_log_dir": str(run_dir / "wf_logs"),
                    "final_call_logs_dir": str(run_dir / "call_logs"),
                },
                of,
            )

        # TODO: Separate cleaning process from run?
        return Command(
            (
                "java",
                "-DLOG_MODE=pretty",
                # We don't set Cromwell into debug logging mode here even if self.debug is True,
                # since it's intensely verbose.
                "-jar",
                cromwell,
                "run",
                "--inputs",
                str(params_path),
                "--options",
                str(options_file),
                "--workflow-root",
                str(run_dir),
                "--metadata-output",
                str(self.get_workflow_metadata_output_json_path(run_dir)),
                str(workflow_path),
            )
        )

    @staticmethod
    def _rewrite_tmp_dir_paths(output_type: str, v: T, tmp_dir_str: str, output_dir_str: str) -> T:
        if output_type == "Path" and isinstance(v, str) and v.startswith(tmp_dir_str):
            # If we have a file output, it should be a path starting with a prefix like
            # /<tmp_dir>/cromwell-executions/... from executing Cromwell with the PWD as /<tmp_dir>/.
            # Cromwell outputs the same folder structure in whatever is set for `final_workflow_outputs_dir` in
            # _get_command() above, so we can rewrite this prefix to be the output directory instead, since this
            # will be preserved after the run is finished:
            return output_dir_str + v.removeprefix(tmp_dir_str)
        elif output_type.startswith("Array") and isinstance(v, list):
            # If we have a list, it may be a nested list of paths, in which case we need to recursively rewrite:
            output_type = output_type.removeprefix("Array[").removesuffix("]")
            return [CromwellLocalBackend._rewrite_tmp_dir_paths(output_type, w, tmp_dir_str, output_dir_str) for w in v]
        else:
            return v

    @staticmethod
    def process_workflow_outputs(outputs: dict, output_types: dict, tmp_dir: Path, output_dir: Path) -> dict:
        """
        Re-points temporary file outputs to a permanent location (as copied by Cromwell) for future download, and
        annotates all output values with their type from the WDL.
        :param outputs: A key-value dictionary of outputs.
        :param output_types: A key-value dictionary of output keys to their corresponding types.
        :param tmp_dir: A path to the Cromwell execution directory.
        :param output_dir: A path to the permanent output directory.
        :return: Processed outputs: a dictionary of output key --> { type: <WDL type>, value: <output value> }
        """

        tmp_dir_str = str(tmp_dir)
        output_dir_str = str(output_dir)

        return {
            k: {
                "type": output_types[k],
                "value": CromwellLocalBackend._rewrite_tmp_dir_paths(output_types[k], v, tmp_dir_str, output_dir_str),
            }
            for k, v in outputs.items()
        }

    async def get_workflow_outputs(self, run: RunWithDetails) -> dict[str, dict]:
        p = self.execute_womtool_command(("outputs", str(self.workflow_path(run))))

        stdout, _ = p.communicate()
        output_types = orjson.loads(stdout)

        async with aiofiles.open(self.get_workflow_metadata_output_json_path(self.run_dir(run)), "r") as fh:
            outputs = orjson.loads(await fh.read()).get("outputs", {})

        return self.process_workflow_outputs(
            outputs=outputs,
            output_types=output_types,
            tmp_dir=self.tmp_dir,
            output_dir=self.output_dir,
        )
