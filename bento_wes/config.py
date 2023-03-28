import os

from typing import Optional, Tuple

from .constants import SERVICE_ID


__all__ = [
    "BENTO_EVENT_REDIS_URL",
    "Config",
]

TRUTH_VALUES = ("true", "1")

BENTO_EVENT_REDIS_URL = os.environ.get("BENTO_EVENT_REDIS_URL", "redis://localhost:6379")


class Config:
    CHORD_URL: str = os.environ.get("CHORD_URL", "http://127.0.0.1:5000/")

    BENTO_DEBUG: bool = os.environ.get(
        "BENTO_DEBUG",
        os.environ.get(
            "CHORD_DEBUG",
            os.environ.get("FLASK_DEBUG", "false"))).strip().lower() in TRUTH_VALUES
    BENTO_VALIDATE_SSL: bool = os.environ.get(
        "BENTO_VALIDATE_SSL", str(not BENTO_DEBUG)).strip().lower() in TRUTH_VALUES

    IS_RUNNING_DEV: bool = os.environ.get("FLASK_DEBUG", "false").strip().lower() in TRUTH_VALUES

    DATABASE: str = os.environ.get("DATABASE", "bento_wes.db")
    SERVICE_ID = SERVICE_ID
    SERVICE_TEMP: str = os.environ.get("SERVICE_TEMP", "tmp")
    SERVICE_URL_BASE_PATH: str = os.environ.get("SERVICE_URL_BASE_PATH", "/")

    # WDL-file-related configuration
    WOM_TOOL_LOCATION: Optional[str] = os.environ.get("WOM_TOOL_LOCATION")
    WORKFLOW_HOST_ALLOW_LIST: Optional[str] = os.environ.get("WORKFLOW_HOST_ALLOW_LIST")

    # Backend configuration
    CROMWELL_LOCATION: str = os.environ.get("CROMWELL_LOCATION", "/cromwell.jar")

    # OTT-related configuration
    OTT_ENDPOINT_NAMESPACE: str = os.environ.get("OTT_ENDPOINT_NAMESPACE", f"{CHORD_URL}api/auth/ott/")

    # TT (temporary token)-related config
    TT_ENDPOINT_NAMESPACE: str = os.environ.get("TT_ENDPOINT_NAMESPACE", f"{CHORD_URL}api/auth/tt/")

    # DRS-related configuration
    DRS_URL: str = os.environ.get("DRS_URL", f"{CHORD_URL}api/drs").strip().rstrip("/")
    WRITE_OUTPUT_TO_DRS: bool = os.environ.get("WRITE_OUTPUT_TO_DRS", "false").lower().strip() in TRUTH_VALUES
    DRS_DEDUPLICATE: bool = os.environ.get("DRS_DEDUPLICATE", "true").lower().strip() in TRUTH_VALUES
    DRS_SKIP_TYPES: Tuple[str, ...] = tuple(
        t.strip() for t in os.environ.get("DRS_SKIP_TYPES", "").split(",") if t.strip())

    # Other services, used for interpolating workflow variables
    METADATA_URL: str = os.environ.get("METADATA_URL", f"{CHORD_URL}api/metadata").strip().rstrip("/")

    # VEP-related configuration
    VEP_CACHE_DIR: Optional[str] = os.environ.get("VEP_CACHE_DIR")

    INGEST_POST_TIMEOUT: int = 60 * 60  # 1 hour
