from bento_lib.apps.fastapi import BentoFastAPI
from contextlib import asynccontextmanager

from .authz import get_authz_middleware
from .config import get_settings, BENTO_EXTRA_SERVICE_INFO
from .constants import SERVICE_TYPE
from .db import setup_database_on_startup, repair_database_on_startup
from .logger import get_logger
from . import __version__
from .events import init_event_bus, shutdown_event_bus


@asynccontextmanager
async def lifespan(app: BentoFastAPI):
    logger = get_logger()
    logger.info("Starting up database...")
    try:
        init_event_bus(logger)
        setup_database_on_startup(logger)
        repair_database_on_startup(logger)
        yield
    finally:
        await shutdown_event_bus(logger)


def create_app():
    settings = get_settings()
    logger = get_logger()
    authz_middleware = get_authz_middleware(settings, logger)

    app = BentoFastAPI(
        authz_middleware,
        settings,
        logger,
        BENTO_EXTRA_SERVICE_INFO,
        SERVICE_TYPE,
        __version__,
        configure_structlog_access_logger=True,
        lifespan=lifespan,
    )

    from .routers.runs.runs import runs_router

    app.include_router(runs_router)

    return app
