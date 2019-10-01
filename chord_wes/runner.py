import chord_lib.ingestion
import os
import re
import requests
import shutil
import subprocess
import uuid

from base64 import urlsafe_b64encode
from chord_lib.ingestion import WORKFLOW_TYPE_FILE
from collections import namedtuple
from datetime import datetime
from flask import current_app, json
from typing import Optional
from urllib.parse import urlparse

from .celery import celery
from .db import get_db
from .states import *


ALLOWED_WORKFLOW_URL_SCHEMES = ("http", "https", "file")
ALLOWED_WORKFLOW_REQUEST_SCHEMES = ("http", "https")

MAX_WORKFLOW_FILE_BYTES = 10000000  # 10 Mb

# Spec: https://software.broadinstitute.org/wdl/documentation/spec#whitespace-strings-identifiers-constants
WDL_WORKSPACE_NAME_REGEX = re.compile(r"workflow\s+([a-zA-Z][a-zA-Z0-9_]+)")

WORKFLOW_TIMEOUT = 60 * 60 * 24  # 24 hours


WES_TYPE_WDL = "WDL"
WES_TYPE_CWL = "CWL"


# TODO: Make these data classes instead?
WorkflowRunner = namedtuple("WorkflowRunner", ("params_file", "type", "extension", "command_fn"))

WDL_RUNNER = WorkflowRunner(
    params_file="params.json",
    type=WES_TYPE_WDL,
    extension="wdl",
    command_fn=lambda tmp_dir, run_dir, workflow_path, params_path: (
        "toil-wdl-runner", workflow_path, params_path, "-o", run_dir, "--workDir", current_app.config["SERVICE_TEMP"],
        "--jobStore", "file:" + os.path.abspath(os.path.join(tmp_dir, "toil_job_store"))
    )
)

# TODO: Test this / make it work
CWL_RUNNER = WorkflowRunner(
    params_file="params.yml",
    type=WES_TYPE_CWL,
    extension="cwl",
    command_fn=lambda tmp_dir, run_dir, workflow_path, params_path: (
        "toil-cwl-runner", workflow_path, params_path, "-o", run_dir, "--workDir", current_app.config["SERVICE_TEMP"],
        "--jobStore", "file:" + os.path.abspath(os.path.join(tmp_dir, "toil_job_store"))
    )
)

WORKFLOW_RUNNERS = {r.type: r for r in (WDL_RUNNER, CWL_RUNNER)}


def get_wdl_workflow_name(workflow_path: str) -> Optional[str]:
    with open(workflow_path, "r") as wdf:
        wdl_contents = wdf.read()
        workflow_id_match = WDL_WORKSPACE_NAME_REGEX.search(wdl_contents)

        if not workflow_id_match:
            # Invalid/non-workflow-specifying WDL file
            return None

        return workflow_id_match.group(1)


def iso_now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO date format


def update_run_state(db, c, run_id, state):
    c.execute("UPDATE runs SET state = ? WHERE id = ?", (state, str(run_id)))
    db.commit()


def finish_run(db, c, run_id, run_log_id, run_dir, state):
    c.execute("UPDATE run_logs SET end_time = ? WHERE id = ?", (iso_now(), run_log_id))
    db.commit()

    update_run_state(db, c, run_id, state)

    if run_dir is None:
        return

    # Clean up any run files at the end, after they've been either copied or "rejected" due to some failure.
    # TODO: SECURITY: Check run_dir
    # TODO: May want to keep them around for a retry depending on how the retry operation will work.

    shutil.rmtree(run_dir, ignore_errors=True)


def build_workflow_outputs(run_dir, workflow_id, workflow_params, c_workflow_metadata):
    output_params = chord_lib.ingestion.make_output_params(workflow_id, workflow_params,
                                                           c_workflow_metadata["inputs"])

    # TODO: Allow outputs to be served over different URL schemes instead of just an absolute file location

    workflow_outputs = {}
    for output in c_workflow_metadata["outputs"]:
        workflow_outputs[output["id"]] = chord_lib.ingestion.formatted_output(output, output_params)
        if output["type"] == WORKFLOW_TYPE_FILE:
            workflow_outputs[output["id"]] = os.path.abspath(os.path.join(run_dir, workflow_outputs[output["id"]]))

    return workflow_outputs


@celery.task(bind=True)
def run_workflow(self, run_id: uuid.UUID, chord_mode: bool, c_workflow_metadata: dict,
                 c_workflow_ingestion_url: Optional[str], c_dataset_id: Optional[str]):
    db = get_db()
    c = db.cursor()

    # Check that workflow ingestion URL is set if CHORD mode is on
    if chord_mode and c_workflow_ingestion_url is None:
        print("An ingestion URL must be set.")
        return

    # TODO: Check workflow_ingestion_url is valid

    # Fetch run from the database, checking that it exists
    c.execute("SELECT request, run_log FROM runs WHERE id = ?", (str(run_id),))
    run = c.fetchone()
    if run is None:
        print("Cannot find run {}".format(run_id))
        return

    # Fetch run request from the database, checking that it exists
    c.execute("SELECT * FROM run_requests WHERE id = ?", (run["request"],))
    run_request = c.fetchone()
    if run_request is None:
        print("Cannot find run request {} for run {}".format(run["request"], run_id))
        return

    # Check run log exists
    c.execute("SELECT * FROM run_logs WHERE id = ?", (run["run_log"],))
    if c.fetchone() is None:
        print("Cannot find run log {} for run {}".format(run["run_log"], run_id))
        return

    # Begin initialization (loading / downloading files)

    update_run_state(db, c, run_id, STATE_INITIALIZING)

    workflow_type = run_request["workflow_type"]
    workflow_params = run_request["workflow_params"]
    workflow_url = run_request["workflow_url"]
    parsed_workflow_url = urlparse(workflow_url)  # TODO: Handle errors, handle references to attachments

    workflow_runner = WORKFLOW_RUNNERS[workflow_type]

    # Check that the URL scheme is something that can be either moved or downloaded
    if parsed_workflow_url.scheme not in ALLOWED_WORKFLOW_URL_SCHEMES:
        # TODO: Log error in run log
        print("Invalid workflow URL scheme")
        finish_run(db, c, run_id, run["run_log"], None, STATE_SYSTEM_ERROR)
        return

    tmp_dir = current_app.config["SERVICE_TEMP"]
    run_dir = os.path.join(tmp_dir, str(run_id))

    if not os.path.exists(run_dir):
        # TODO: Log error in run log
        print("Run directory not found")
        finish_run(db, c, run_id, run["run_log"], None, STATE_SYSTEM_ERROR)
        return

    workflow_path = os.path.join(tmp_dir, "workflow_{w}.{ext}}".format(
        w=str(urlsafe_b64encode(bytes(workflow_url, encoding="utf-8")), encoding="utf-8"),
        ext=workflow_runner.extension))
    workflow_params_path = os.path.join(run_dir, workflow_runner.params_file)

    # Store input strings for the WDL file in a JSON file in the temporary folder
    with open(workflow_params_path, "w") as wpf:
        # TODO: Make this generic among workflow languages
        json.dump(workflow_params, wpf)

    # Create the runner command based on inputs
    cmd = workflow_runner.command_fn(tmp_dir, run_dir, workflow_path, workflow_params_path)

    # Update run log with command and Celery ID
    c.execute("UPDATE run_logs SET cmd = ?, celery_id = ? WHERE id = ?",
              (" ".join(cmd), self.request.id, run["run_log"]))
    db.commit()

    # Download or move workflow

    # TODO: Auth
    if parsed_workflow_url.scheme in ALLOWED_WORKFLOW_REQUEST_SCHEMES:
        try:
            wr = requests.get(workflow_url)

            if wr.status_code == 200 and len(wr.content) < MAX_WORKFLOW_FILE_BYTES:
                if os.path.exists(workflow_path):
                    os.remove(workflow_path)

                with open(workflow_path, "wb") as nwf:
                    nwf.write(wr.content)

            elif not os.path.exists(workflow_path):  # Use cached version if needed, otherwise error
                # Request issues
                finish_run(db, c, run_id, run["run_log"], run_dir, STATE_SYSTEM_ERROR)
                return

        except requests.exceptions.ConnectionError:
            if not os.path.exists(workflow_path):  # Use cached version if needed, otherwise error
                # Network issues
                finish_run(db, c, run_id, run["run_log"], run_dir, STATE_SYSTEM_ERROR)
                return

    else:
        # file://
        # TODO: Handle exceptions
        shutil.copyfile(parsed_workflow_url.path, workflow_path)

    # Check for Java if trying to run a WDL file
    if workflow_type == WES_TYPE_WDL:
        try:
            subprocess.run(("java", "-version"))
        except FileNotFoundError:
            finish_run(db, c, run_id, run["run_log"], run_dir, STATE_SYSTEM_ERROR)
            return

    # Validate WDL, listing dependencies
    #  - TODO: make this generic among workflow languages

    vr = subprocess.Popen(["java", "-jar", current_app.config["WOM_TOOL_LOCATION"], "validate", "-l", workflow_path],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

    v_out, v_err = vr.communicate()

    if vr.returncode != 0:
        # Validation error with WDL file
        # TODO: Add some stdout or stderr to logs?
        print("Failed with {} due to non-0 validation return code:".format(STATE_EXECUTOR_ERROR))
        print("\tstdout: {}\n\tstderr: {}".format(v_out, v_err))
        finish_run(db, c, run_id, run["run_log"], run_dir, STATE_EXECUTOR_ERROR)
        return

    #  - Since Toil doesn't support WDL imports right now, any dependencies will result in an error
    if "None" not in v_out:  # No dependencies
        # Toil can't process WDL dependencies right now  TODO
        # TODO: Add some stdout or stderr to logs?
        print("Failed with {} due to dependencies in WDL:".format(STATE_EXECUTOR_ERROR))
        print("\tstdout: {}\n\tstderr: {}".format(v_out, v_err))
        finish_run(db, c, run_id, run["run_log"], run_dir, STATE_EXECUTOR_ERROR)
        return

    # TODO: SECURITY: MAKE SURE NOTHING REFERENCED IS OUTSIDE OF ALLOWED AREAS!
    # TODO: SECURITY: Maybe don't allow external downloads, only run things in the container?

    # Find "real" (WDL) workflow name from WDL file
    #  - TODO: make this generic among workflow languages
    workflow_id = get_wdl_workflow_name(workflow_path)
    if workflow_id is None:
        # Invalid/non-workflow-specifying WDL file
        finish_run(db, c, run_id, run["run_log"], run_dir, STATE_SYSTEM_ERROR)
        return

    # TODO: To avoid having multiple names, we should maybe only set this once?
    c.execute("UPDATE run_logs SET name = ? WHERE id = ?", (workflow_id, run["run_log"],))
    db.commit()

    # TODO: Input file downloading if needed

    # Run the WDL with the Toil runner, placing all outputs into the job directory

    # Start run

    workflow_runner_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
    update_run_state(db, c, run_id, STATE_RUNNING)

    c.execute("UPDATE run_logs SET start_time = ? WHERE id = ?", (iso_now(), run["run_log"]))
    db.commit()

    # Wait for output

    timed_out = False

    try:
        stdout, stderr = workflow_runner_process.communicate(timeout=WORKFLOW_TIMEOUT)
        exit_code = workflow_runner_process.returncode

    except subprocess.TimeoutExpired:
        workflow_runner_process.kill()
        stdout, stderr = workflow_runner_process.communicate()
        exit_code = workflow_runner_process.returncode

        timed_out = True

    c.execute("UPDATE run_logs SET stdout = ?, stderr = ?, exit_code = ? WHERE id = ?",
              (stdout, stderr, exit_code, run["run_log"]))
    db.commit()

    if timed_out:
        # TODO: Report error somehow
        finish_run(db, c, run_id, run["run_log"], run_dir, STATE_SYSTEM_ERROR)
        return

    # Final steps: check exit code and report results

    if exit_code != 0:
        # TODO: Report error somehow
        finish_run(db, c, run_id, run["run_log"], run_dir, STATE_EXECUTOR_ERROR)

    # Exit code is 0 otherwise

    if not chord_mode:
        # TODO: What should be done if this run was not a CHORD routine?
        finish_run(db, c, run_id, run["run_log"], run_dir, STATE_COMPLETE)
        return

    # CHORD ingestion run

    try:
        # TODO: Verify ingestion URL (vulnerability??)

        workflow_outputs_json = json.dumps(build_workflow_outputs(run_dir, workflow_id, workflow_params,
                                                                  c_workflow_metadata))

        c.execute("UPDATE runs SET outputs = ? WHERE id = ?", (workflow_outputs_json, str(run_id)))
        db.commit()

        # TODO: Just post run ID, fetch rest from the WES service?

        r = requests.post(c_workflow_ingestion_url, {
            "dataset_id": c_dataset_id,
            "workflow_id": workflow_id,
            "workflow_metadata": json.dumps(c_workflow_metadata),
            "workflow_outputs": workflow_outputs_json,
            "workflow_params": json.dumps(workflow_params)
        })

        if str(r.status_code)[0] != "2":  # If non-2XX error code
            # Ingestion failed for some reason
            finish_run(db, c, run_id, run["run_log"], run_dir, STATE_SYSTEM_ERROR)
            return

        finish_run(db, c, run_id, run["run_log"], run_dir, STATE_COMPLETE)

    except requests.exceptions.ConnectionError:
        # Ingestion failed due to a network error
        # TODO: Retry a few times...
        # TODO: Report error somehow
        finish_run(db, c, run_id, run["run_log"], run_dir, STATE_SYSTEM_ERROR)
