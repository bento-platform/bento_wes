import os
import re
import subprocess

from flask import current_app, json
from typing import Optional, Tuple

from chord_wes.backends import WESBackend
from chord_wes.states import *


__all__ = [
    "ToilWDLBackend"
]


# Spec: https://software.broadinstitute.org/wdl/documentation/spec#whitespace-strings-identifiers-constants
WDL_WORKSPACE_NAME_REGEX = re.compile(r"workflow\s+([a-zA-Z][a-zA-Z0-9_]+)")


class ToilWDLBackend(WESBackend):
    def _get_supported_types(self) -> Tuple[str]:
        return "wdl",

    def _get_params_file(self, run: dict) -> str:
        return "params.json"

    def _serialize_params(self, workflow_params: dict) -> str:
        return json.dumps(workflow_params)

    def _check_workflow(self, run: dict) -> Optional[Tuple[str, str]]:
        """
        Checks that a particular WDL workflow is valid.
        :param run: The run whose workflow is being checked
        :return: None if the workflow is valid; a tuple of an error message and an error state otherwise
        """

        # Check for Java (needed to run the WOM tool)
        try:
            subprocess.run(("java", "-version"))
        except FileNotFoundError:
            return "Java is missing (required to validate WDL files)", STATE_SYSTEM_ERROR

        # Validate WDL, listing dependencies

        vr = subprocess.Popen(("java", "-jar", current_app.config["WOM_TOOL_LOCATION"], "validate", "-l",
                               self.workflow_path(run)),
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              encoding="utf-8")

        v_out, v_err = vr.communicate()

        if vr.returncode != 0:
            # Validation error with WDL file
            # TODO: Add some stdout or stderr to logs?
            return (
                "Failed with {} due to non-0 validation return code:\n\tstdout: {}\n\tstderr: {}".format(
                    STATE_EXECUTOR_ERROR, v_out, v_err),
                STATE_EXECUTOR_ERROR
            )

        #  - Since Toil doesn't support WDL imports right now, any dependencies will result in an error
        if "None" not in v_out:  # No dependencies
            # Toil can't process WDL dependencies right now  TODO
            # TODO: Add some stdout or stderr to logs?
            return (
                "Failed with {} due to dependencies in WDL:\n\tstdout: {}\n\tstderr: {}".format(
                    STATE_EXECUTOR_ERROR, v_out, v_err),
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

    def _get_command(self, workflow_path: str, params_path: str, run_dir: str) -> Tuple[str, ...]:
        return (
            "toil-wdl-runner",
            workflow_path,
            params_path,
            "-o", run_dir,
            "--workDir", self.tmp_dir,
            "--jobStore", "file:" + os.path.abspath(os.path.join(self.tmp_dir, "toil_job_store"))
        )
