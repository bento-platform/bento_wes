import bento_wes
import os


__all__ = [
    "SERVICE_ARTIFACT",
    "SERVICE_TYPE",
    "SERVICE_ID",
    "SERVICE_NAME",
]

SERVICE_ARTIFACT = "wes"
SERVICE_TYPE = {
    "group": "ca.c3g.bento",
    "artifact": SERVICE_ARTIFACT,
    "version": bento_wes.__version__,
}
SERVICE_ID = os.environ.get("SERVICE_ID", ":".join(SERVICE_TYPE.values()))
SERVICE_NAME = "Bento WES"
