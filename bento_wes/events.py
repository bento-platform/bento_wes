from bento_lib.events import EventBus, types as et
from flask import g

from .config import BENTO_EVENT_REDIS_HOST, BENTO_EVENT_REDIS_PORT


__all__ = [
    "get_new_event_bus",
    "get_flask_event_bus",
    "close_flask_event_bus",
]


def get_new_event_bus() -> EventBus:
    event_bus = EventBus(connection_data=dict(
        host=BENTO_EVENT_REDIS_HOST,
        port=BENTO_EVENT_REDIS_PORT
    ), allow_fake=True)
    event_bus.register_service_event_type(et.EVENT_WES_RUN_UPDATED, et.EVENT_WES_RUN_UPDATED_SCHEMA)
    event_bus.register_service_event_type(et.EVENT_WES_RUN_FINISHED, et.EVENT_WES_RUN_FINISHED_SCHEMA)
    event_bus.register_service_event_type(et.EVENT_CREATE_NOTIFICATION, et.EVENT_CREATE_NOTIFICATION_SCHEMA)
    return event_bus


def get_flask_event_bus() -> EventBus:
    if "event_bus" not in g:
        g.event_bus = get_new_event_bus()
    return g.event_bus


def close_flask_event_bus(_e=None):
    # TODO: More closing stuff?
    g.pop("event_bus", None)
