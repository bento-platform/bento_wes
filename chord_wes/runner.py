import chord_lib.ingestion
import os
import re
import requests
import shutil
import subprocess
import uuid

from base64 import urlsafe_b64encode
from celery.utils.log import get_task_logger
from chord_lib.events.notifications import format_notification
from chord_lib.events.types import EVENT_CREATE_NOTIFICATION, EVENT_WES_RUN_FINISHED
from chord_lib.ingestion import WORKFLOW_TYPE_FILE, WORKFLOW_TYPE_FILE_ARRAY
from collections import namedtuple
from datetime import datetime
from flask import current_app, json
from typing import Optional, Tuple
from urllib.parse import urlparse, ParseResult

from .celery import celery
from .constants import *
from .db import get_db, update_run_state_and_commit
from .events import *
from .states import *


NOTIFICATION_WES_RUN_FAILED = "wes_run_failed"
NOTIFICATION_WES_RUN_COMPLETED = "wes_run_completed"


ALLOWED_WORKFLOW_URL_SCHEMES = ("http", "https", "file")
ALLOWED_WORKFLOW_REQUEST_SCHEMES = ("http", "https")

MAX_WORKFLOW_FILE_BYTES = 10000000  # 10 MB

# Spec: https://software.broadinstitute.org/wdl/documentation/spec#whitespace-strings-identifiers-constants
WDL_WORKSPACE_NAME_REGEX = re.compile(r"workflow\s+([a-zA-Z][a-zA-Z0-9_]+)")

WORKFLOW_TIMEOUT = 60 * 60 * 24  # 24 hours

INGEST_POST_TIMEOUT = 60 * 10  # 10 minutes


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


def workflow_file_name(workflow_runner: WorkflowRunner, workflow_url: str):
    return "workflow_{w}.{ext}".format(
        w=str(urlsafe_b64encode(bytes(workflow_url, encoding="utf-8")), encoding="utf-8"),
        ext=workflow_runner.extension)


def download_or_move_workflow(workflow_url: str, parsed_workflow_url: ParseResult, workflow_path: str) -> Optional[str]:
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
                return STATE_SYSTEM_ERROR

        except requests.exceptions.ConnectionError:
            if not os.path.exists(workflow_path):  # Use cached version if needed, otherwise error
                # Network issues
                return STATE_SYSTEM_ERROR

    else:
        # file://
        # TODO: Handle exceptions
        shutil.copyfile(parsed_workflow_url.path, workflow_path)


def get_wdl_workflow_name(workflow_path: str) -> Optional[str]:
    with open(workflow_path, "r") as wdf:
        wdl_contents = wdf.read()
        workflow_id_match = WDL_WORKSPACE_NAME_REGEX.search(wdl_contents)

        if not workflow_id_match:
            # Invalid/non-workflow-specifying WDL file
            return None

        return workflow_id_match.group(1)


def validate_wdl(workflow_path: str) -> Optional[Tuple[str, str]]:
    # Check for Java (needed to run the WOM tool)
    try:
        subprocess.run(("java", "-version"))
    except FileNotFoundError:
        return "Java is missing (required to validate WDL files)", STATE_SYSTEM_ERROR

    # Validate WDL, listing dependencies
    #  - TODO: make this generic among workflow languages

    vr = subprocess.Popen(("java", "-jar", current_app.config["WOM_TOOL_LOCATION"], "validate", "-l", workflow_path),
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

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


def iso_now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO date format


def build_workflow_outputs(run_dir, workflow_id, workflow_params: dict, c_workflow_metadata: dict):
    output_params = chord_lib.ingestion.make_output_params(workflow_id, workflow_params,
                                                           c_workflow_metadata["inputs"])

    # TODO: Allow outputs to be served over different URL schemes instead of just an absolute file location

    workflow_outputs = {}
    for output in c_workflow_metadata["outputs"]:
        workflow_outputs[output["id"]] = chord_lib.ingestion.formatted_output(output, output_params)

        # Rewrite file outputs to include full path to temporary location
        if output["type"] == WORKFLOW_TYPE_FILE:
            workflow_outputs[output["id"]] = os.path.abspath(os.path.join(run_dir, workflow_outputs[output["id"]]))
        elif output["type"] == WORKFLOW_TYPE_FILE_ARRAY:
            workflow_outputs[output["id"]] = [os.path.abspath(os.path.join(run_dir, wo))
                                              for wo in workflow_outputs[output["id"]]]

    return workflow_outputs


logger = get_task_logger(__name__)


def finish_run(db, c, run_id: uuid.UUID, run_log_id: str, state: str) -> None:
    # Explicitly don't commit here to sync with state update
    c.execute("UPDATE run_logs SET end_time = ? WHERE id = ?", (iso_now(), run_log_id))
    update_run_state_and_commit(db, c, run_id, state)

    if state in FAILURE_STATES:
        event_bus.publish_service_event(
            SERVICE_ARTIFACT,
            EVENT_CREATE_NOTIFICATION,
            format_notification(
                title="WES Run Failed",
                description=f"WES run '{str(run_id)}' failed with state {state}",
                notification_type=NOTIFICATION_WES_RUN_FAILED,
                action_target=str(run_id)
            )
        )

    elif state in SUCCESS_STATES:
        event_bus.publish_service_event(
            SERVICE_ARTIFACT,
            EVENT_CREATE_NOTIFICATION,
            format_notification(
                title="WES Run Completed",
                description=f"WES run '{str(run_id)}' completed successfully",
                notification_type=NOTIFICATION_WES_RUN_COMPLETED,
                action_target=str(run_id)
            )
        )


def _run_workflow(db, c, celery_request_id, run_id: uuid.UUID, run: dict, run_request: dict, chord_mode: bool,
                  c_workflow_metadata: dict, c_workflow_ingestion_url: Optional[str], c_table_id: Optional[str]):
    # Setup ---------------------------------------------------------------

    tmp_dir = current_app.config["SERVICE_TEMP"]
    run_dir = os.path.join(tmp_dir, str(run_id))

    # Set up scoped helpers

    def _update_run_state_and_commit(state: str) -> None:
        update_run_state_and_commit(db, c, run_id, state)

    def _finish_run_and_clean_up(state: str) -> None:
        finish_run(db, c, run_id, run["run_log"], state)

        if run_dir is None:
            return

        # Clean up any run files at the end, after they've been either copied or "rejected" due to some failure.
        # TODO: SECURITY: Check run_dir
        # TODO: May want to keep them around for a retry depending on how the retry operation will work.

        shutil.rmtree(run_dir, ignore_errors=True)

    # Initialization (loading / downloading files) ----------------------------

    _update_run_state_and_commit(STATE_INITIALIZING)

    workflow_type = run_request["workflow_type"]
    workflow_params = json.loads(run_request["workflow_params"])
    workflow_url = run_request["workflow_url"]
    parsed_workflow_url = urlparse(workflow_url)  # TODO: Handle errors, handle references to attachments

    workflow_runner = WORKFLOW_RUNNERS[workflow_type]

    # Check that the URL scheme is something that can be either moved or downloaded
    if parsed_workflow_url.scheme not in ALLOWED_WORKFLOW_URL_SCHEMES:
        # TODO: Log error in run log
        logger.error("Invalid workflow URL scheme")
        return _finish_run_and_clean_up(STATE_SYSTEM_ERROR)

    if not os.path.exists(run_dir):
        # TODO: Log error in run log
        logger.error("Run directory not found")
        return _finish_run_and_clean_up(STATE_SYSTEM_ERROR)

    workflow_path = os.path.join(tmp_dir, workflow_file_name(workflow_runner, workflow_url))
    workflow_params_path = os.path.join(run_dir, workflow_runner.params_file)

    # Store input strings for the WDL file in a JSON file in the temporary folder
    with open(workflow_params_path, "w") as wpf:
        # TODO: Make this generic among workflow languages
        json.dump(workflow_params, wpf)

    # Create the runner command based on inputs
    cmd = workflow_runner.command_fn(tmp_dir, run_dir, workflow_path, workflow_params_path)

    # Update run log with command and Celery ID
    c.execute("UPDATE run_logs SET cmd = ?, celery_id = ? WHERE id = ?",
              (" ".join(cmd), celery_request_id, run["run_log"]))
    db.commit()

    # Download or move workflow
    error_state = download_or_move_workflow(workflow_url, parsed_workflow_url, workflow_path)
    if error_state is not None:
        return _finish_run_and_clean_up(error_state)

    # Run checks if trying to run a WDL file
    if workflow_type == WES_TYPE_WDL:
        error = validate_wdl(workflow_path)
        if error is not None:
            logger.error(error[0])
            return _finish_run_and_clean_up(error[1])

    # TODO: Validate CWL

    # TODO: SECURITY: MAKE SURE NOTHING REFERENCED IS OUTSIDE OF ALLOWED AREAS!
    # TODO: SECURITY: Maybe don't allow external downloads, only run things in the container?

    # Find "real" (WDL) workflow name from WDL file
    #  - TODO: make this generic among workflow languages
    workflow_id = get_wdl_workflow_name(workflow_path)
    if workflow_id is None:
        # Invalid/non-workflow-specifying WDL file
        return _finish_run_and_clean_up(STATE_SYSTEM_ERROR)

    # TODO: To avoid having multiple names, we should maybe only set this once?
    c.execute("UPDATE run_logs SET name = ? WHERE id = ?", (workflow_id, run["run_log"],))
    db.commit()

    # TODO: Input file downloading if needed

    # Start run ---------------------------------------------------------------
    # Run the WDL with the Toil runner, placing all outputs into the job directory

    workflow_runner_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

    # Sync start time commit with state update
    c.execute("UPDATE run_logs SET start_time = ? WHERE id = ?", (iso_now(), run["run_log"]))
    _update_run_state_and_commit(STATE_RUNNING)

    # -- Wait for output ------------------------------------------------------

    timed_out = False

    try:
        stdout, stderr = workflow_runner_process.communicate(timeout=WORKFLOW_TIMEOUT)

    except subprocess.TimeoutExpired:
        workflow_runner_process.kill()
        stdout, stderr = workflow_runner_process.communicate()
        timed_out = True

    finally:
        exit_code = workflow_runner_process.returncode

    # Finish run --------------------------------------------------------------

    # Explicitly don't commit here; sync with state update
    c.execute("UPDATE run_logs SET stdout = ?, stderr = ?, exit_code = ? WHERE id = ?",
              (stdout, stderr, exit_code, run["run_log"]))

    if timed_out:
        # TODO: Report error somehow
        return _finish_run_and_clean_up(STATE_SYSTEM_ERROR)

    # Final steps: check exit code and report results

    if exit_code != 0:
        # TODO: Report error somehow
        return _finish_run_and_clean_up(STATE_EXECUTOR_ERROR)

    # Exit code is 0 otherwise

    if not chord_mode:
        # TODO: What should be done if this run was not a CHORD routine?
        return _finish_run_and_clean_up(STATE_COMPLETE)

    # CHORD ingestion ---------------------------------------------------------

    # TODO: Verify ingestion URL (vulnerability??)

    workflow_outputs = build_workflow_outputs(run_dir, workflow_id, workflow_params, c_workflow_metadata)

    # Explicitly don't commit here; sync with state update
    c.execute("UPDATE runs SET outputs = ? WHERE id = ?", (json.dumps(workflow_outputs), str(run_id)))

    # Run result object
    run_results = {
        "dataset_id": c_table_id,  # TODO: Table ID
        "workflow_id": workflow_id,
        "workflow_metadata": c_workflow_metadata,
        "workflow_outputs": workflow_outputs,
        "workflow_params": workflow_params
    }

    # Emit event if possible
    event_bus.publish_service_event(SERVICE_ARTIFACT, EVENT_WES_RUN_FINISHED, run_results)

    # Try to complete ingest POST request

    try:
        # TODO: Just post run ID, fetch rest from the WES service?
        r = requests.post(c_workflow_ingestion_url, json=run_results, timeout=INGEST_POST_TIMEOUT)
        return _finish_run_and_clean_up(STATE_COMPLETE if r.status_code < 400 else STATE_SYSTEM_ERROR)

    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        # Ingestion failed due to a network error, or was too slow.
        # TODO: Retry a few times...
        # TODO: Report error somehow
        return _finish_run_and_clean_up(STATE_SYSTEM_ERROR)


@celery.task(bind=True)
def run_workflow(self, run_id: uuid.UUID, chord_mode: bool, c_workflow_metadata: dict,
                 c_workflow_ingestion_url: Optional[str], c_table_id: Optional[str]):
    db = get_db()
    c = db.cursor()

    # Checks ------------------------------------------------------------------

    # Check that workflow ingestion URL is set if CHORD mode is on
    if chord_mode and c_workflow_ingestion_url is None:
        logger.error("An ingestion URL must be set.")
        return

    # TODO: Check workflow_ingestion_url is valid

    # Fetch run from the database, checking that it exists
    c.execute("SELECT request, run_log FROM runs WHERE id = ?", (str(run_id),))
    run = c.fetchone()
    if run is None:
        logger.error("Cannot find run {}".format(run_id))
        return

    # Fetch run request from the database, checking that it exists
    c.execute("SELECT * FROM run_requests WHERE id = ?", (run["request"],))
    run_request = c.fetchone()
    if run_request is None:
        logger.error("Cannot find run request {} for run {}".format(run["request"], run_id))
        return

    # Check run log exists
    c.execute("SELECT * FROM run_logs WHERE id = ?", (run["run_log"],))
    if c.fetchone() is None:
        logger.error("Cannot find run log {} for run {}".format(run["run_log"], run_id))
        return

    # Pass to runner function -------------------------------------------------

    try:
        _run_workflow(db, c, self.request.id, run_id, run, run_request, chord_mode, c_workflow_metadata,
                      c_workflow_ingestion_url, c_table_id)
    except Exception as e:
        # Intercept any uncaught exceptions and finish with an error state
        finish_run(db, c, run_id, run["run_log"], STATE_SYSTEM_ERROR)
        raise e
