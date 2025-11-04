from __future__ import annotations

import os

from bento_lib.events import EventBus, types as et
from fastapi import Depends
from logging import Logger
from typing import Annotated, Optional

from .config import get_settings
from .logger import LoggerDep

__all__ = [
    "init_event_bus",
    "shutdown_event_bus",
    "get_event_bus",
    "get_worker_event_bus",
    "close_worker_event_bus",
    "EventBusDep",
]

# ---------- Singleton state ----------
_BUS: Optional[EventBus] = None


# ---------- Construction ----------
def _create_event_bus(logger: Logger) -> EventBus:
    """
    Create and configure the EventBus instance (no I/O side effects here).
    """
    settings = get_settings()
    bus = EventBus(url=settings.bento_event_redis_url, allow_fake=True, logger=logger)

    # Register all event types here
    bus.register_service_event_type(et.EVENT_WES_RUN_UPDATED, et.EVENT_WES_RUN_UPDATED_SCHEMA)
    bus.register_service_event_type(et.EVENT_WES_RUN_FINISHED, et.EVENT_WES_RUN_FINISHED_SCHEMA)
    bus.register_service_event_type(et.EVENT_CREATE_NOTIFICATION, et.EVENT_CREATE_NOTIFICATION_SCHEMA)
    return bus


async def _close_event_bus(bus: EventBus, logger: Logger) -> None:
    try:
        bus.stop_event_loop()
    except Exception as e:
        logger.exception("Error while shutting down EventBus", exc_info=e)


# ---------- Lifecycle ----------
def init_event_bus(logger: Logger) -> EventBus:
    """
    Initialize the global EventBus singleton if not already created.
    Safe to call multiple times.
    """
    global _BUS
    if _BUS is None:
        logger.info("Initializing EventBus")
        _BUS = _create_event_bus(logger)
    return _BUS


async def shutdown_event_bus(logger: Logger) -> None:
    """
    Shut down the global EventBus singleton, if it exists.
    """
    global _BUS
    if _BUS is None:
        return
    logger.info("Shutting down EventBus")
    await _close_event_bus(_BUS, logger)
    _BUS = None


# ---------- Dependency ----------
def get_event_bus(logger: LoggerDep) -> EventBus:
    """
    Retrieve the global EventBus singleton.
    creates if not initialized.
    """
    if _BUS is None:
        return init_event_bus(logger)
    return _BUS


EventBusDep = Annotated[EventBus, Depends(get_event_bus)]

# ---------- For Celery Workers ----------

_WORKER_BUS: Optional[EventBus] = None
_WORKER_PID: Optional[int] = None


def get_worker_event_bus(logger: Logger) -> EventBus:
    """
    Lazily create and return a per-process EventBus for Celery workers.
    Safe to call inside tasks; initializes after fork.
    """
    global _WORKER_BUS, _WORKER_PID
    pid = os.getpid()

    if _WORKER_BUS is None or _WORKER_PID != pid:
        logger.debug("Initializing EventBus for Celery worker process (pid=%s)", pid)
        _WORKER_BUS = _create_event_bus(logger)
        _WORKER_PID = pid

    return _WORKER_BUS


async def close_worker_event_bus(logger: Logger) -> None:
    """
    Close the per-process Celery worker EventBus, if present.
    """
    global _WORKER_BUS
    if _WORKER_BUS is None:
        return
    logger.debug("Shutting down EventBus for Celery worker process (pid=%s)", os.getpid())
    await _close_event_bus(_WORKER_BUS, logger)
    _WORKER_BUS = None
