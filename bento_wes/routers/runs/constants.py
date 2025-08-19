from bento_wes import states

CHUNK_SIZE = 1024 * 16

PUBLIC_RUN_DETAILS_SHAPE = {
    "request": {
        "workflow_type": True,
        "tags": {
            "workflow_id": True,
            "workflow_metadata": {
                "data_type": True,
            },
            "project_id": True,
            "dataset_id": True,
        },
    },
    "run_log": {
        "start_time": True,
        "end_time": True,
    },
}

PRIVATE_RUN_DETAILS_SHAPE = {
    "request": True,
    "run_log": True,
    "task_logs": True,
    "outputs": True,
}

RUN_CANCEL_BAD_REQUEST_STATES = (
    ((states.STATE_CANCELING, states.STATE_CANCELED), "Run already canceled"),
    (states.FAILURE_STATES, "Run already terminated with error"),
    (states.SUCCESS_STATES, "Run already completed"),
)