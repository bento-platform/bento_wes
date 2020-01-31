from chord_lib.events import EventBus
from chord_lib.events.types import *
from flask import g


__all__ = [
    "get_new_event_bus",
    "get_flask_event_bus",
    "close_flask_event_bus",
]


def get_new_event_bus() -> EventBus:
    event_bus = EventBus(allow_fake=True)
    event_bus.register_service_event_type(EVENT_WES_RUN_UPDATED, EVENT_WES_RUN_UPDATED_SCHEMA)
    event_bus.register_service_event_type(EVENT_WES_RUN_FINISHED, EVENT_WES_RUN_FINISHED_SCHEMA)
    event_bus.register_service_event_type(EVENT_CREATE_NOTIFICATION, EVENT_CREATE_NOTIFICATION_SCHEMA)
    return event_bus


def get_flask_event_bus() -> EventBus:
    if "event_bus" not in g:
        g.event_bus = get_new_event_bus()
    return g.event_bus


def close_flask_event_bus(_e=None):
    # TODO: More closing stuff?
    g.pop("event_bus", None)
