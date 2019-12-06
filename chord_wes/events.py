from chord_lib.events import EventBus
from typing import Optional


__all__ = [
    "EVENT_WES_RUN_UPDATED",
    "EVENT_WES_RUN_UPDATED_SCHEMA",
    "EVENT_WES_RUN_FINISHED",
    "EVENT_WES_RUN_FINISHED_SCHEMA",
    "event_bus"
]


EVENT_WES_RUN_UPDATED = "wes_run_updated"
EVENT_WES_RUN_UPDATED_SCHEMA = {
    "type": "object",
    # TODO
}

EVENT_WES_RUN_FINISHED = "wes_run_finished"
EVENT_WES_RUN_FINISHED_SCHEMA = {
    "type": "object",
    # TODO
}


event_bus = EventBus(allow_fake=True)
event_bus.register_service_event_type(EVENT_WES_RUN_UPDATED, EVENT_WES_RUN_UPDATED_SCHEMA)
event_bus.register_service_event_type(EVENT_WES_RUN_FINISHED, EVENT_WES_RUN_FINISHED_SCHEMA)
