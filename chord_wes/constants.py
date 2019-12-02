import chord_wes
import os


__all__ = [
    "SERVICE_ARTIFACT",
    "SERVICE_TYPE",
    "SERVICE_ID",
]


SERVICE_ARTIFACT = "wes"
SERVICE_TYPE = f"ca.c3g.chord:{SERVICE_ARTIFACT}:{chord_wes.__version__}"
SERVICE_ID = os.environ.get("SERVICE_ID", SERVICE_TYPE)
