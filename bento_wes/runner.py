import bento_lib.workflows as w
import os
import requests
import uuid

from celery.utils.log import get_task_logger
from flask import current_app

from . import states
from .backends import WESBackend
from .backends.cromwell_local import CromwellLocalBackend
from .celery import celery
from .db import get_db, get_run_details, finish_run
from .events import get_new_event_bus
from .workflows import parse_workflow_host_allow_list


logger = get_task_logger(__name__)


def build_workflow_outputs(run_dir, workflow_id: str, workflow_params: dict, c_workflow_metadata: dict):
    logger.info(f"Building workflow outputs for workflow ID {workflow_id} "
                f"(WRITE_OUTPUT_TO_DRS={current_app.config['WRITE_OUTPUT_TO_DRS']})")
    output_params = w.make_output_params(workflow_id, workflow_params, c_workflow_metadata["inputs"])

    workflow_outputs = {}
    for output in c_workflow_metadata["outputs"]:
        fo = w.formatted_output(output, output_params)

        # Skip optional outputs resulting from optional inputs
        if fo is None:
            continue

        # Rewrite file outputs to include full path to temporary location
        if output["type"] == w.WORKFLOW_TYPE_FILE:
            workflow_outputs[output["id"]] = os.path.abspath(os.path.join(run_dir, "output", fo))

        elif output["type"] == w.WORKFLOW_TYPE_FILE_ARRAY:
            workflow_outputs[output["id"]] = [os.path.abspath(os.path.join(run_dir, wo)) for wo in fo]
            logger.info(f"Setting workflow output {output['id']} to [{', '.join(workflow_outputs[output['id']])}]")

        else:
            workflow_outputs[output["id"]] = fo
            logger.info(f"Setting workflow output {output['id']} to {workflow_outputs[output['id']]}")

    return workflow_outputs


@celery.task(bind=True)
def run_workflow(self, run_id: uuid.UUID):
    db = get_db()
    c = db.cursor()
    event_bus = get_new_event_bus()

    # Checks ------------------------------------------------------------------

    # Check that the run and its associated objects exist
    run, err = get_run_details(c, run_id)
    if run is None:
        logger.error(f"Cannot find run {run_id} ({err})")
        return

    # Pass to workflow execution backend---------------------------------------

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
        chord_url=(current_app.config["BENTO_URL"] or None),

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
