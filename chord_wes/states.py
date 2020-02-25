# TODO: Wrap with a NewType

STATE_UNKNOWN = "UNKNOWN"
STATE_QUEUED = "QUEUED"
STATE_INITIALIZING = "INITIALIZING"
STATE_RUNNING = "RUNNING"
STATE_PAUSED = "PAUSED"
STATE_COMPLETE = "COMPLETE"
STATE_EXECUTOR_ERROR = "EXECUTOR_ERROR"
STATE_SYSTEM_ERROR = "SYSTEM_ERROR"
STATE_CANCELED = "CANCELED"
STATE_CANCELING = "CANCELING"

FAILURE_STATES = [STATE_EXECUTOR_ERROR, STATE_SYSTEM_ERROR]
SUCCESS_STATES = [STATE_COMPLETE]
