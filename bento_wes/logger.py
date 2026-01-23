import logging
import structlog.stdlib
from bento_lib.logging.structured.configure import configure_structlog_from_bento_config, configure_structlog_uvicorn
from fastapi import Depends
from functools import lru_cache
from logging import Logger
from typing import Annotated

from .config import SettingsDep

__all__ = ["get_logger", "LoggerDep"]


# quiet one noisy celery logger in debug mode
# - we can't put this in the logger config below, since it doesn't run in time for initial startup spam.
logging.getLogger("celery.utils.functional").setLevel(logging.INFO)


@lru_cache
def get_logger(settings: SettingsDep) -> structlog.stdlib.BoundLogger:
    configure_structlog_from_bento_config(settings)
    configure_structlog_uvicorn()

    # TODO
    # quiet noisy libs in dev
    # "asyncio": {"level": "INFO"},
    # "celery.app.trace": {"level": "INFO"},
    # "python_multipart.multipart": {
    #     "level": "WARNING",
    #     "handlers": ["console"],
    #     "propagate": False,
    # },

    return structlog.stdlib.get_logger("wes")


LoggerDep = Annotated[structlog.stdlib.BoundLogger, Depends(get_logger)]
