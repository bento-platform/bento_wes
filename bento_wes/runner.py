import bento_lib.workflows as w
import os
import requests
import uuid

from celery.utils.log import get_task_logger
from bento_lib.events.types import EVENT_WES_RUN_FINISHED
from flask import current_app, json
from typing import List, Optional
from urllib.parse import urlparse


from . import states
from .backends import WESBackend
# from .backends.toil_wdl import ToilWDLBackend
from .backends.cromwell_local import CromwellLocalBackend
from .celery import celery
from .constants import SERVICE_ARTIFACT
from .db import get_db, get_run_details, finish_run
from .events import get_new_event_bus
from .workflows import parse_workflow_host_allow_list


logger = get_task_logger(__name__)


def ingest_in_drs(path: str, otts: List[str]):
    # TODO: Not compliant with "standard" DRS
    #  - document how this has to work or provide an alternative
    url = f"{current_app.config['DRS_URL']}/private/ingest"
    params = {"path": path, **({"deduplicate": True} if current_app.config["DRS_DEDUPLICATE"] else {})}

    # Include the next one-time-use token if we have one
    next_token = otts.pop() if otts else None

    try:
        cert_verify = current_app.config["BENTO_VALIDATE_SSL"]

        logger.info(f"Attempting DRS ingestion request to {url}:\n"
                    f"cert verify: {cert_verify}\n"
                    f"       body: {json.dumps(params)}")

        r = requests.post(
            url,
            headers={"X-OTT": next_token} if next_token else {},
            json=params,
            timeout=current_app.config["INGEST_POST_TIMEOUT"],
            verify=cert_verify,
        )
        r.raise_for_status()

    except requests.RequestException as e:
        if hasattr(e, "response"):
            # noinspection PyUnresolvedReferences
            logger.error(f"Encountered DRS request exception: {e.response.json()}")
        return None

    data = r.json()
    logger.info(f"Ingested {path} as {data['self_uri']}")

    return data["self_uri"]


def should_ingest_to_drs(path: str) -> bool:
    return current_app.config["WRITE_OUTPUT_TO_DRS"] and not \
        any(path.endswith(t) for t in current_app.config["DRS_SKIP_TYPES"])


def return_drs_url_or_full_path(full_path: str, otts: List[str]) -> str:
    # TODO: As it stands, ingest_in_drs will return None in case of DRS ingest failure
    drs_url = ingest_in_drs(full_path, otts) if should_ingest_to_drs(full_path) else None
    return drs_url or full_path


def build_workflow_outputs(run_dir, workflow_id, workflow_params: dict, c_workflow_metadata: dict, c_otts: List[str]):
    logger.info(f"Building workflow outputs for workflow ID {workflow_id} "
                f"(WRITE_OUTPUT_TO_DRS={current_app.config['WRITE_OUTPUT_TO_DRS']})")
    output_params = w.make_output_params(workflow_id, workflow_params, c_workflow_metadata["inputs"])

    workflow_outputs = {}
    for output in c_workflow_metadata["outputs"]:
        fo = w.formatted_output(output, output_params)

        # Skip optional outputs resulting from optional inputs
        if fo is None:
            continue

        # Rewrite file outputs to include full path to temporary location, or ingested DRS object URI
        # TODO: Ideally we shouldn't need one DRS request per file -- bundles would maybe be better.

        if output["type"] == w.WORKFLOW_TYPE_FILE:
            workflow_outputs[output["id"]] = return_drs_url_or_full_path(
                os.path.abspath(os.path.join(run_dir, "output", fo)), c_otts)
            logger.info(f"Setting workflow output {output['id']} to {workflow_outputs[output['id']]}")

        elif output["type"] == w.WORKFLOW_TYPE_FILE_ARRAY:
            workflow_outputs[output["id"]] = [
                return_drs_url_or_full_path(os.path.abspath(os.path.join(run_dir, wo)), c_otts)
                for wo in fo
            ]
            logger.info(f"Setting workflow output {output['id']} to [{', '.join(workflow_outputs[output['id']])}]")

        else:
            workflow_outputs[output["id"]] = fo
            logger.info(f"Setting workflow output {output['id']} to {workflow_outputs[output['id']]}")

    return workflow_outputs


@celery.task(bind=True)
def run_workflow(self, run_id: uuid.UUID, chord_mode: bool, c_workflow_metadata: dict,
                 c_workflow_ingestion_url: Optional[str], c_table_id: Optional[str], c_otts: List[str],
                 c_use_otts_for_drs: bool):
    db = get_db()
    c = db.cursor()
    event_bus = get_new_event_bus()

    # Checks ------------------------------------------------------------------

    # Check that workflow ingestion URL is set if CHORD mode is on
    if chord_mode and c_workflow_ingestion_url is None:
        logger.error("An ingestion URL must be set when chord_mode is enabled.")
        return

    # TODO: Check c_workflow_ingestion_url is valid?

    # Check that the run and its associated objects exist
    run, err = get_run_details(c, run_id)
    if run is None:
        logger.error(f"Cannot find run {run_id} ({err})")
        return

    # Pass to workflow execution backend---------------------------------------

    def chord_callback(b: WESBackend):
        run_dir = b.run_dir(run)
        workflow_name = b.get_workflow_name(b.workflow_path(run))
        workflow_params: dict = run["request"]["workflow_params"]

        # TODO: Verify ingestion URL (vulnerability??)

        workflow_outputs = build_workflow_outputs(
            run_dir, workflow_name, workflow_params, c_workflow_metadata, c_otts if c_use_otts_for_drs else [])

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

        headers = {"Host": urlparse(current_app.config["CHORD_URL"] or "").netloc or ""}
        if c_otts:
            # If we have OTTs
            # TODO: Should validate scope instead of relying on order
            headers["X-OTT"] = c_otts.pop()

        try:
            cert_verify = current_app.config["BENTO_VALIDATE_SSL"]

            logger.info(
                f"Calling ingestion callback\n"
                f"      cert verify: {cert_verify}"
                f"    ingestion URL: {c_workflow_ingestion_url}\n"
                f"    JSON contents: {json.dumps(run_results)}\n")

            # TODO: Just post run ID, fetch rest from the WES service results?
            # TODO: In the future, allow localhost requests to chord_metadata_service so we don't need to manually
            #  set the Host header?

            # TODO: Refactor:
            # TEMP: avoid calling ingestion callback when using Gohan
            if "gohan" not in c_workflow_ingestion_url:
                r = requests.post(
                    c_workflow_ingestion_url,
                    headers=headers,
                    json=run_results,
                    timeout=current_app.config["INGEST_POST_TIMEOUT"],
                    verify=cert_verify,
                )

                if not r.ok:
                    # An error occurred, do some logging
                    logger.error(
                        f"Encountered error while POSTing to ingestion URL\n"
                        f"           URL: {c_workflow_ingestion_url}\n"
                        f"        Status: {r.status_code}\n"
                        f"      Response: {r.content}\n"
                        f"  Req. Headers: {headers}")
                    return states.STATE_SYSTEM_ERROR

            return states.STATE_COMPLETE

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as ee:
            # Ingestion failed due to a network error, or was too slow.
            logger.error(f"Encountered ConnectionError or Timeout: {type(ee).__name__} {ee}")
            # TODO: Retry a few times...
            # TODO: Report error somehow
            return states.STATE_SYSTEM_ERROR

    # TODO: Change based on workflow type / what's supported - get first runner
    #  'enabled' (somehow) which supports the type
    logger.info("Initializing backend")
    backend: WESBackend = CromwellLocalBackend(
        tmp_dir=current_app.config["SERVICE_TEMP"],
        workflow_timeout=current_app.config["WORKFLOW_TIMEOUT"],
        logger=logger,
        event_bus=event_bus,

        # Get list of allowed workflow hosts from configuration for any checks inside the runner
        workflow_host_allow_list=parse_workflow_host_allow_list(current_app.config["WORKFLOW_HOST_ALLOW_LIST"]),

        # Bento-specific stuff
        chord_mode=chord_mode,
        chord_callback=chord_callback,
        chord_url=(current_app.config["CHORD_URL"] or None),

        validate_ssl=current_app.config["BENTO_VALIDATE_SSL"],
        debug=current_app.config["BENTO_DEBUG"],
    )

    # Obtain access token for use inside workflow to ingest data
    try:
        logger.info("Obtaining access token")
        # TODO: cache OpenID config
        # TODO: handle errors more elegantly/precisely

        # TODO: somehow get an access token which is only able to ingest into a specific dataset, not everything.
        #  - perhaps exchange the user's token for some type of limited-scope token (ingest only) which lasts 24 hours,
        #    given out by the authorization service?

        openid_config = requests.get(current_app.config["BENTO_OPENID_CONFIG_URL"]).json()
        token_res = requests.post(openid_config["token_endpoint"], data={
            "grant_type": "client_credentials",
            "client_id": current_app.config["WES_CLIENT_ID"],
            "client_secret": current_app.config["WES_CLIENT_SECRET"],
        })
        access_token = token_res.json()["access_token"]
    except Exception as e:
        # Intercept any uncaught exceptions and finish with an error state
        logger.error(f"Uncaught exception while obtaining access token: {type(e).__name__} {e}")
        finish_run(db, c, event_bus, run, states.STATE_SYSTEM_ERROR, logger=logger)
        raise e

    # Perform the run
    try:
        logger.info("Starting workflow execution...")
        backend.perform_run(run, self.request.id, access_token)
    except Exception as e:
        # Intercept any uncaught exceptions and finish with an error state
        logger.error(f"Uncaught exception while performing run: {type(e).__name__} {e}")
        finish_run(db, c, event_bus, run, states.STATE_SYSTEM_ERROR, logger=logger)
        raise e
