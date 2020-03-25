import os
import uuid
from typing import Optional
from urllib.parse import quote

from celery.utils.log import get_task_logger
import chord_lib.ingestion
from chord_lib.events.types import EVENT_WES_RUN_FINISHED
from chord_lib.ingestion import WORKFLOW_TYPE_FILE, WORKFLOW_TYPE_FILE_ARRAY
from flask import current_app, json
import requests
import requests_unixsocket

from . import states
from .backends import finish_run, WESBackend
from .backends.toil_wdl import ToilWDLBackend
from .celery import celery
from .constants import SERVICE_ARTIFACT, SERVICE_NAME
from .db import get_db, get_run_details
from .events import get_new_event_bus


requests_unixsocket.monkeypatch()


NGINX_INTERNAL_SOCKET = quote(os.environ.get("NGINX_INTERNAL_SOCKET", "/chord/tmp/nginx_internal.sock"), safe="")

INGEST_POST_TIMEOUT = 60 * 10  # 10 minutes


def ingest_in_drs(path):
    # TODO: might want to refactor at some point
    url = f"http+unix://{NGINX_INTERNAL_SOCKET}/api/drs/ingest"
    params = {"path": path}

    try:
        r = requests.post(url, json=params, timeout=INGEST_POST_TIMEOUT)
        r.raise_for_status()
    except requests.RequestException:
        return None

    data = r.json()

    print(f"[{SERVICE_NAME}] Ingested {path} as {data['self_uri']}", flush=True)

    return data["self_uri"]


def build_workflow_outputs(run_dir, workflow_id, workflow_params: dict, c_workflow_metadata: dict):
    output_params = chord_lib.ingestion.make_output_params(workflow_id, workflow_params,
                                                           c_workflow_metadata["inputs"])

    workflow_outputs = {}
    for output in c_workflow_metadata["outputs"]:
        workflow_outputs[output["id"]] = chord_lib.ingestion.formatted_output(output, output_params)

        # Rewrite file outputs to include full path to temporary location
        if output["type"] == WORKFLOW_TYPE_FILE:
            full_path = os.path.abspath(os.path.join(run_dir, workflow_outputs[output["id"]]))
            drs_url = None

            if current_app.config['WRITE_OUTPUT_TO_DRS']:
                # As it stands, will return None in case of failure
                drs_url = ingest_in_drs(full_path)

            workflow_outputs[output["id"]] = drs_url if drs_url else full_path

        elif output["type"] == WORKFLOW_TYPE_FILE_ARRAY:
            new_outputs = []

            for wo in workflow_outputs[output["id"]]:
                full_path = os.path.abspath(os.path.join(run_dir, wo))
                drs_url = None

                if current_app.config['WRITE_OUTPUT_TO_DRS']:
                    drs_url = ingest_in_drs(full_path)

                new_outputs.append(drs_url if drs_url else full_path)

            workflow_outputs[output["id"]] = new_outputs

    return workflow_outputs


logger = get_task_logger(__name__)


@celery.task(bind=True)
def run_workflow(self, run_id: uuid.UUID, chord_mode: bool, c_workflow_metadata: dict,
                 c_workflow_ingestion_path: Optional[str], c_table_id: Optional[str]):
    db = get_db()
    c = db.cursor()
    event_bus = get_new_event_bus()

    # Checks ------------------------------------------------------------------

    # Check that workflow ingestion URL is set if CHORD mode is on
    if chord_mode and c_workflow_ingestion_path is None:
        logger.error("An ingestion URL must be set.")
        return

    # TODO: Check c_workflow_ingestion_path is valid

    # Check that the run and its associated objects exist
    run = get_run_details(c, run_id)
    if run is None:
        logger.error("Cannot find run {} (missing run, run request, or run_log)".format(run_id))
        return

    # Pass to workflow execution backend---------------------------------------

    def chord_callback(b: WESBackend):
        run_dir = b.run_dir(run)
        workflow_name = b.get_workflow_name(b.workflow_path(run))
        workflow_params: dict = run["request"]["workflow_params"]

        # TODO: Verify ingestion URL (vulnerability??)

        workflow_outputs = build_workflow_outputs(run_dir, workflow_name, workflow_params, c_workflow_metadata)

        # Explicitly don't commit here; sync with state update
        c.execute("UPDATE runs SET outputs = ? WHERE id = ?", (json.dumps(workflow_outputs), str(run["run_id"])))

        # Run result object
        run_results = {
            "table_id": c_table_id,
            "workflow_id": workflow_name,
            "workflow_metadata": c_workflow_metadata,
            "workflow_outputs": workflow_outputs,
            "workflow_params": workflow_params
        }

        # Emit event if possible
        event_bus.publish_service_event(SERVICE_ARTIFACT, EVENT_WES_RUN_FINISHED, run_results)
        # TODO: If this is used to ingest, we'll have to wait for a confirmation before cleaning up; otherwise files
        #  could get removed before they get processed.

        try:
            # TODO: Just post run ID, fetch rest from the WES service?
            r = requests.post(f"http+unix://{NGINX_INTERNAL_SOCKET}{c_workflow_ingestion_path}",
                              json=run_results, timeout=INGEST_POST_TIMEOUT)
            return states.STATE_COMPLETE if r.status_code < 400 else states.STATE_SYSTEM_ERROR

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            # Ingestion failed due to a network error, or was too slow.
            # TODO: Retry a few times...
            # TODO: Report error somehow
            return states.STATE_SYSTEM_ERROR

    # TODO: Change based on workflow type / what's supported
    backend: WESBackend = ToilWDLBackend(current_app.config["SERVICE_TEMP"], chord_mode, logger, event_bus,
                                         chord_callback)

    try:
        backend.perform_run(run, self.request.id)
    except Exception as e:
        # Intercept any uncaught exceptions and finish with an error state
        finish_run(db, c, event_bus, run, states.STATE_SYSTEM_ERROR)
        raise e
