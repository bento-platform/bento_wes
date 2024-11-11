import logging
import os

from bento_lib.logging import log_level_from_str

__all__ = [
    "logger",
]

logging.basicConfig(level=logging.NOTSET)

# Suppress asyncio debug logs
logging.getLogger("asyncio").setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(log_level_from_str(os.environ.get("LOG_LEVEL", "info").lower().strip()))
