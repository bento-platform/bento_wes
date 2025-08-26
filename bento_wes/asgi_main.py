from bento_lib.apps.fastapi import BentoFastAPI
from contextlib import asynccontextmanager

from .authz import authz_middleware
from .config import config, BENTO_EXTRA_SERVICE_INFO
from .constants import  SERVICE_TYPE
from .db import setup_database_on_startup, repair_database_on_startup
from .logger import logger
from . import __version__
from .routers.runs.runs import runs_router
from .events import init_event_bus, shutdown_event_bus

@asynccontextmanager
async def lifespan():
    logger.info("Starting up database...")
    try: 
        init_event_bus()
        setup_database_on_startup()
        repair_database_on_startup()
        yield
    finally:
        shutdown_event_bus()
    logger.info("Shutting down database...")
    logger.info("Finished shutting down database.")


app = BentoFastAPI(
    authz_middleware,
    config,
    logger,
    BENTO_EXTRA_SERVICE_INFO,
    SERVICE_TYPE,
    __version__,
    configure_structlog_access_logger=True,
    lifespan = lifespan
)

app.include_router(runs_router)