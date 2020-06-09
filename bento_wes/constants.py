import bento_wes
import os


__all__ = [
    "SERVICE_ARTIFACT",
    "SERVICE_TYPE",
    "SERVICE_ID",
    "SERVICE_NAME",
]

SERVICE_ARTIFACT = "wes"
SERVICE_TYPE = f"ca.c3g.bento:{SERVICE_ARTIFACT}:{bento_wes.__version__}"
SERVICE_ID = os.environ.get("SERVICE_ID", SERVICE_TYPE)
SERVICE_NAME = "Bento WES"
