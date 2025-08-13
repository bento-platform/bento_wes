from typing import Generator

from fastapi import Depends, Request
from bento_lib.events import EventBus, types as et

from .config import BENTO_EVENT_REDIS_URL

__all__ = [
    "create_event_bus",
    "startup_event_bus",
    "shutdown_event_bus",
    "get_event_bus",
    "get_event_bus_per_request",  # optional alternative
]

def create_event_bus() -> EventBus:
    bus = EventBus(url=BENTO_EVENT_REDIS_URL, allow_fake=True)
    bus.register_service_event_type(et.EVENT_WES_RUN_UPDATED,   et.EVENT_WES_RUN_UPDATED_SCHEMA)
    bus.register_service_event_type(et.EVENT_WES_RUN_FINISHED,  et.EVENT_WES_RUN_FINISHED_SCHEMA)
    bus.register_service_event_type(et.EVENT_CREATE_NOTIFICATION, et.EVENT_CREATE_NOTIFICATION_SCHEMA)
    return bus

# --- App-scoped (recommended): one EventBus for the whole app ---

def startup_event_bus(app) -> None:
    """Call this in your FastAPI lifespan startup."""
    app.state.event_bus = create_event_bus()

def shutdown_event_bus(app) -> None:
    """Call this in your FastAPI lifespan shutdown."""
    # Add any explicit close/cleanup if EventBus supports it.
    if hasattr(app.state, "event_bus"):
        del app.state.event_bus

def get_event_bus(request: Request) -> EventBus:
    """Dependency to access the app-scoped EventBus."""
    return request.app.state.event_bus

# Usage in routes/services:
# def handler(event_bus: EventBus = Depends(get_event_bus)): ...

# --- Optional: per-request bus (if you really want isolation) ---

def get_event_bus_per_request() -> Generator[EventBus, None, None]:
    """Creates & tears down an EventBus per request (usually not needed)."""
    bus = create_event_bus()
    try:
        yield bus
    finally:
        # bus.close() if the SDK provides it
        pass
