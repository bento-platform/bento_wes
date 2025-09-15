from bento_wes import states

CHUNK_SIZE = 1024 * 16

RUN_CANCEL_BAD_REQUEST_STATES = (
    ((states.STATE_CANCELING, states.STATE_CANCELED), "Run already canceled"),
    (states.FAILURE_STATES, "Run already terminated with error"),
    (states.SUCCESS_STATES, "Run already completed"),
)
