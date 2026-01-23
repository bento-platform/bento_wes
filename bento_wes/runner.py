import asyncio
import httpx
import uuid

from bento_lib.events import EventBus
from bento_lib.service_info.manager import ServiceManager
from celery.utils.log import get_task_logger
from structlog import wrap_logger
from structlog.stdlib import BoundLogger
from typing import cast

from . import states
from .backends.cromwell_local import CromwellLocalBackend
from .backends.wes_backend import WESBackend
from .celery import celery
from .db import Database, get_db_with_event_bus
from .events import get_worker_event_bus, close_worker_event_bus
from .config import get_settings, Settings
from .service_registry import get_service_manager
from .workflows import WorkflowManager, get_workflow_manager


@celery.task(bind=True)
def run_workflow(self, run_id: uuid.UUID):
    # Initialize dependencies  ------------------------------------------------

    settings: Settings = get_settings()
    logger: BoundLogger = cast(BoundLogger, wrap_logger(get_task_logger(__name__))).bind(
        run_id=run_id, celery_id=self.request.id
    )

    event_bus: EventBus = get_worker_event_bus(logger, settings.bento_event_redis_url)
    service_manager: ServiceManager = get_service_manager(settings, logger)
    workflow_manager: WorkflowManager = get_workflow_manager(settings, logger)

    _db_gen = get_db_with_event_bus(logger, event_bus)
    db: Database = next(_db_gen)

    # Checks ------------------------------------------------------------------

    # Check that the run and its associated objects exist
    run = db.get_run_with_details(run_id, stream_content=False)
    if run is None:
        logger.error("cannot find run")
        return

    # Pass to workflow execution backend---------------------------------------

    # TODO: Change based on workflow type / what's supported - get first runner
    #  'enabled' (somehow) which supports the type
    logger.info("Initializing backend")
    backend: WESBackend = CromwellLocalBackend(
        event_bus=event_bus,
        logger=logger,  # run_id already bound
        service_manager=service_manager,
        settings=settings,
        workflow_manager=workflow_manager,
    )

    secrets: dict[str, str] = {"access_token": ""}

    # If we have credentials, obtain access token for use inside workflow to ingest data
    try:
        if (client_id := settings.wes_client_id) and (client_secret := settings.wes_client_secret.get_secret_value()):
            logger.info("Obtaining access token")
            # TODO: cache OpenID config
            # TODO: handle errors more elegantly/precisely

            # TODO: somehow get an access token which is only able to ingest into a specific dataset, not everything.
            #  - perhaps exchange the user's token for some type of limited-scope token (ingest only) which lasts
            #    48 hours, given out by the authorization service?

            with httpx.Client(verify=settings.bento_validate_ssl) as client:
                openid_config = client.get(settings.bento_openid_config_url).json()
                token_res = client.post(
                    openid_config["token_endpoint"],
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
        logger.exception("uncaught exception while obtaining access token", exc_info=e)
        db.finish_run(run, states.STATE_SYSTEM_ERROR)
        raise e

    # Perform the run
    try:
        logger.info("starting workflow execution")
        asyncio.run(backend.perform_run(run, self.request.id, secrets))
    except Exception as e:
        # Intercept any uncaught exceptions and finish with an error state
        logger.exception("uncaught exception while performing run", exc_info=e)
        db.finish_run(run, states.STATE_SYSTEM_ERROR)
        raise e
    finally:
        try:
            next(_db_gen)
        except StopIteration:
            pass
        asyncio.run(close_worker_event_bus(logger))
        backend.close()
