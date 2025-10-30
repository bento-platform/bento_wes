import logging
import logging.config
from typing import Annotated
from fastapi import Depends
from logging import Logger
from functools import lru_cache

import os

from bento_lib.logging import log_level_from_str

__all__ = ["get_logger", "LoggerDep"]


@lru_cache
def get_logger() -> Logger:
    logger = logging.getLogger(__name__)

    LOG_LEVEL = log_level_from_str(os.environ.get("LOG_LEVEL", "info").strip().lower())

    LOGGING = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {"format": "%(levelname)s:%(name)s:%(message)s"},
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
            }
        },
        "root": {
            "level": LOG_LEVEL,
            "handlers": ["console"],
        },
        "loggers": {
            # quiet noisy libs in dev
            "asyncio": {"level": "INFO"},
            "celery.utils.functional": {"level": "WARNING"},
            "celery.app.trace": {"level": "INFO"},
            "python_multipart.multipart": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False,
            },
        },
    }

    logging.config.dictConfig(LOGGING)
    return logger


LoggerDep = Annotated[Logger, Depends(get_logger)]
