import logging
import logging.config
from typing import Annotated
from fastapi import Depends
from logging import Logger
from functools import lru_cache

import os

from bento_lib.logging import log_level_from_str

__all__ = ["get_logger", "LoggerDep"]


# quiet one noisy celery logger in debug mode
# - we can't put this in the logger config below, since it doesn't run in time for initial startup spam.
logging.getLogger("celery.utils.functional").setLevel(logging.INFO)


@lru_cache
def get_logger() -> Logger:
    logger = logging.getLogger("wes")

    log_level = log_level_from_str(os.environ.get("LOG_LEVEL", "info").strip().lower())

    logging.config.dictConfig(
        {
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
                "level": log_level,
                "handlers": ["console"],
            },
            "loggers": {
                # quiet noisy libs in dev
                "asyncio": {"level": "INFO"},
                "celery.app.trace": {"level": "INFO"},
                "python_multipart.multipart": {
                    "level": "WARNING",
                    "handlers": ["console"],
                    "propagate": False,
                },
            },
        }
    )

    return logger


LoggerDep = Annotated[Logger, Depends(get_logger)]
