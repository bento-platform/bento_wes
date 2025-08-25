import bento_wes
import os

from bento_lib.service_info.helpers import build_bento_service_type


__all__ = [
    "BENTO_SERVICE_KIND",
    "GIT_REPOSITORY",
    "SERVICE_ARTIFACT",
    "SERVICE_TYPE",
    "SERVICE_ID",
    "SERVICE_NAME",
]

BENTO_SERVICE_KIND = "wes"
SERVICE_ARTIFACT = BENTO_SERVICE_KIND
SERVICE_TYPE = build_bento_service_type(SERVICE_ARTIFACT, bento_wes.__version__)
SERVICE_ID = os.environ.get("SERVICE_ID", ":".join(SERVICE_TYPE.values()))
SERVICE_NAME = "Bento WES"
GIT_REPOSITORY = "https://github.com/bento-platform/bento_wes"

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