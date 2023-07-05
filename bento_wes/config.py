import os

from typing import Optional

from .constants import SERVICE_ID
from .logger import logger


__all__ = [
    "BENTO_AUTHZ_SERVICE_URL",
    "AUTHZ_ENABLED",
    "BENTO_DEBUG",
    "BENTO_EVENT_REDIS_URL",
    "Config",
]

TRUTH_VALUES = ("true", "1")

BENTO_AUTHZ_SERVICE_URL = os.environ.get("BENTO_AUTHZ_SERVICE_URL", "")

if BENTO_AUTHZ_SERVICE_URL == "":
    logger.critical("BENTO_AUTHZ_SERVICE_URL must be set")
    exit(1)

AUTHZ_ENABLED = os.environ.get("AUTHZ_ENABLED", "true").strip().lower() in TRUTH_VALUES

BENTO_DEBUG: bool = os.environ.get(
        "BENTO_DEBUG",
        os.environ.get(
            "CHORD_DEBUG",
            os.environ.get("FLASK_DEBUG", "false"))).strip().lower() in TRUTH_VALUES

BENTO_EVENT_REDIS_URL = os.environ.get("BENTO_EVENT_REDIS_URL", "redis://localhost:6379")


class Config:
    CHORD_URL: str = os.environ.get("CHORD_URL", "http://127.0.0.1:5000/")

    BENTO_DEBUG: bool = BENTO_DEBUG
    BENTO_VALIDATE_SSL: bool = os.environ.get(
        "BENTO_VALIDATE_SSL", str(not BENTO_DEBUG)).strip().lower() in TRUTH_VALUES

    DATABASE: str = os.environ.get("DATABASE", "bento_wes.db")
    SERVICE_ID = SERVICE_ID
    SERVICE_TEMP: str = os.environ.get("SERVICE_TEMP", "tmp")
    SERVICE_URL_BASE_PATH: str = os.environ.get("SERVICE_URL_BASE_PATH", "/")

    # WDL-file-related configuration
    WOM_TOOL_LOCATION: Optional[str] = os.environ.get("WOM_TOOL_LOCATION")
    WORKFLOW_HOST_ALLOW_LIST: Optional[str] = os.environ.get("WORKFLOW_HOST_ALLOW_LIST")

    # Backend configuration
    CROMWELL_LOCATION: str = os.environ.get("CROMWELL_LOCATION", "/cromwell.jar")

    # DRS-related configuration
    DRS_URL: str = os.environ.get("DRS_URL", f"{CHORD_URL}api/drs").strip().rstrip("/")

    # Other services, used for interpolating workflow variables
    METADATA_URL: str = os.environ.get("METADATA_URL", f"{CHORD_URL}api/metadata").strip().rstrip("/")

    # VEP-related configuration
    VEP_CACHE_DIR: Optional[str] = os.environ.get("VEP_CACHE_DIR")

    INGEST_POST_TIMEOUT: int = 60 * 60  # 1 hour
    # Timeout for workflow runs themselves, in seconds - default to 48 hours
    WORKFLOW_TIMEOUT: int = int(os.environ.get("WORKFLOW_TIMEOUT", str(60 * 60 * 48)))

    # Auth-related config for WES itself
    BENTO_OPENID_CONFIG_URL: str = os.environ.get(
        "BENTO_OPENID_CONFIG_URL", "https://bentov2auth.local/realms/bentov2/.well-known/openid-configuration")
    WES_CLIENT_ID: str = os.environ.get("WES_CLIENT_ID", "bento_wes")
    WES_CLIENT_SECRET: str = os.environ.get("WES_CLIENT_SECRET", "")
