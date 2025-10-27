import logging
import logging.config

import os

from bento_lib.logging import log_level_from_str

__all__ = [
    "logger",
]

logging.basicConfig(level=logging.NOTSET)

# The following disables unecessary logs from the packages which then are printed repetitevly in dev mode
logging.getLogger("asyncio").setLevel(logging.INFO)
logging.getLogger("celery.utils.functional").setLevel(logging.WARNING)
logging.getLogger("celery.app.trace").setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(log_level_from_str(os.environ.get("LOG_LEVEL", "info").lower().strip()))


logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "loggers": {"python_multipart.multipart": {"level": "WARNING", "handlers": ["console"], "propagate": False}},
        "handlers": {"console": {"class": "logging.StreamHandler", "formatter": "default"}},
        "formatters": {"default": {"format": "%(levelname)s:%(name)s:%(message)s"}},
    }
)
