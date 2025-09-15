from fastapi import Depends
from bento_lib.events import EventBus, types as et
from typing import Annotated, Optional

from .config import get_settings

__all__ = [
    "create_event_bus",
    "init_event_bus",
    "shutdown_event_bus",
    "EventBusDep"
]

_BUS: Optional[EventBus] = None

def create_event_bus() -> EventBus:
    settings = get_settings()
    bus = EventBus(url=settings.bento_event_redis_url, allow_fake=True)
    bus.register_service_event_type(et.EVENT_WES_RUN_UPDATED,   et.EVENT_WES_RUN_UPDATED_SCHEMA)
    bus.register_service_event_type(et.EVENT_WES_RUN_FINISHED,  et.EVENT_WES_RUN_FINISHED_SCHEMA)
    bus.register_service_event_type(et.EVENT_CREATE_NOTIFICATION, et.EVENT_CREATE_NOTIFICATION_SCHEMA)
    return bus

def init_event_bus() -> EventBus:
    global _BUS
    if _BUS is None:
        _BUS = create_event_bus()
    return _BUS

def shutdown_event_bus() -> None:
    global _BUS
    if _BUS is not None:
        close = getattr(_BUS, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
    _BUS = None

def get_event_bus() -> EventBus:
    if _BUS is None:
        raise RuntimeError("EventBus not initialized. Call init_event_bus() at startup.")
    return _BUS

EventBusDep = Annotated[EventBus, Depends(get_event_bus)]
