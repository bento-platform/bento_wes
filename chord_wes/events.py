from chord_lib.events import EventBus
from chord_lib.events.types import *


__all__ = [
    "event_bus",
]


event_bus = EventBus(allow_fake=True)
event_bus.register_service_event_type(EVENT_WES_RUN_UPDATED, EVENT_WES_RUN_UPDATED_SCHEMA)
event_bus.register_service_event_type(EVENT_WES_RUN_FINISHED, EVENT_WES_RUN_FINISHED_SCHEMA)
event_bus.register_service_event_type(EVENT_CREATE_NOTIFICATION, EVENT_CREATE_NOTIFICATION_SCHEMA)
