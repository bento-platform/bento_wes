import bento_wes
import os

from typing import Literal


__all__ = [
    "BENTO_SERVICE_KIND",
    "SERVICE_ARTIFACT",
    "SERVICE_TYPE",
    "SERVICE_ID",
    "SERVICE_NAME",
]

BENTO_SERVICE_KIND = "wes"
SERVICE_ARTIFACT = BENTO_SERVICE_KIND
SERVICE_TYPE = {
    "group": "ca.c3g.bento",
    "artifact": SERVICE_ARTIFACT,
    "version": bento_wes.__version__,
}
SERVICE_ID = os.environ.get("SERVICE_ID", ":".join(SERVICE_TYPE.values()))
SERVICE_NAME = "Bento WES"
