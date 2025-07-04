import requests
import uuid

from celery.utils.log import get_task_logger
from flask import current_app

from . import states
from .backends.cromwell_local import CromwellLocalBackend
from .backends.wes_backend import WESBackend
from .celery import celery
from .db import Database, get_db
from .events import get_new_event_bus
from .workflows import parse_workflow_host_allow_list


@celery.task(bind=True)
def run_workflow(self, run_id: uuid.UUID):
    logger = get_task_logger(__name__)

    db: Database = get_db()
    c = db.cursor()
    event_bus = get_new_event_bus()

    # Checks ------------------------------------------------------------------

    # Check that the run and its associated objects exist
    run = db.get_run_with_details(c, run_id, stream_content=False)
    if run is None:
        logger.error(f"Cannot find run {run_id}")
        return

    # Pass to workflow execution backend---------------------------------------

    # TODO: Change based on workflow type / what's supported - get first runner
    #  'enabled' (somehow) which supports the type
    logger.info("Initializing backend")
    validate_ssl = current_app.config["BENTO_VALIDATE_SSL"]
    backend: WESBackend = CromwellLocalBackend(
        tmp_dir=current_app.config["SERVICE_TEMP"],
        data_dir=current_app.config["SERVICE_DATA"],
        workflow_timeout=current_app.config["WORKFLOW_TIMEOUT"],
        # Dependencies
        logger=logger,
        event_bus=event_bus,
        # Get list of allowed workflow hosts from configuration for any checks inside the runner
        workflow_host_allow_list=parse_workflow_host_allow_list(current_app.config["WORKFLOW_HOST_ALLOW_LIST"]),
        # Bento-specific stuff
        bento_url=(current_app.config["BENTO_URL"] or None),
        # Debug/production flags (validate SSL must be ON in production; debug must be OFF)
        validate_ssl=validate_ssl,
        debug=current_app.config["BENTO_DEBUG"],
    )

    secrets: dict[str, str] = {"access_token": ""}

    # If we have credentials, obtain access token for use inside workflow to ingest data
    try:
        if (client_id := current_app.config["WES_CLIENT_ID"]) and (
            client_secret := current_app.config["WES_CLIENT_SECRET"]
        ):
            logger.info("Obtaining access token")
            # TODO: cache OpenID config
            # TODO: handle errors more elegantly/precisely

            # TODO: somehow get an access token which is only able to ingest into a specific dataset, not everything.
            #  - perhaps exchange the user's token for some type of limited-scope token (ingest only) which lasts
            #    48 hours, given out by the authorization service?

            openid_config = requests.get(current_app.config["BENTO_OPENID_CONFIG_URL"], verify=validate_ssl).json()
            token_res = requests.post(
                openid_config["token_endpoint"],
                verify=validate_ssl,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
            )
            secrets["access_token"] = token_res.json()["access_token"]
        else:
            logger.warning(
                "Missing WES credentials: WES_CLIENT_ID and/or WES_CLIENT_SECRET; setting job access token to ''"
            )
    except Exception as e:
        # Intercept any uncaught exceptions and finish with an error state
        logger.error(f"Uncaught exception while obtaining access token: {type(e).__name__} {e}")
        db.finish_run(event_bus, run, states.STATE_SYSTEM_ERROR, cursor=c, logger=logger)
        raise e

    # Perform the run
    try:
        logger.info("Starting workflow execution...")
        backend.perform_run(run, self.request.id, secrets)
    except Exception as e:
        # Intercept any uncaught exceptions and finish with an error state
        logger.error(f"Uncaught exception while performing run: {type(e).__name__} {e}")
        db.finish_run(event_bus, run, states.STATE_SYSTEM_ERROR, cursor=c, logger=logger)
        raise e
