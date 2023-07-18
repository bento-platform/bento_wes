import os

from typing import Optional

from .constants import SERVICE_ID
from .logger import logger


__all__ = [
    "AUTHZ_URL",
    "AUTHZ_ENABLED",
    "BENTO_DEBUG",
    "BENTO_EVENT_REDIS_URL",
    "Config",
]


def _get_from_environ_or_fail(var: str) -> str:
    if (val := os.environ.get(var, "")) == "":
        logger.critical(f"{var} must be set")
        exit(1)
    return val


TRUTH_VALUES = ("true", "1")

AUTHZ_ENABLED = os.environ.get("AUTHZ_ENABLED", "true").strip().lower() in TRUTH_VALUES

BENTO_DEBUG: bool = os.environ.get(
    "BENTO_DEBUG",
    os.environ.get("FLASK_DEBUG", "false")
).strip().lower() in TRUTH_VALUES

AUTHZ_URL: str = _get_from_environ_or_fail("BENTO_AUTHZ_SERVICE_URL").strip().rstrip("/")
SERVICE_REGISTRY_URL: str = _get_from_environ_or_fail("SERVICE_REGISTRY_URL").strip().rstrip("/")

BENTO_EVENT_REDIS_URL = os.environ.get("BENTO_EVENT_REDIS_URL", "redis://localhost:6379")


class Config:
    BENTO_URL: str = os.environ.get("BENTO_URL", "http://127.0.0.1:5000/")

    BENTO_DEBUG: bool = BENTO_DEBUG
    BENTO_VALIDATE_SSL: bool = os.environ.get(
        "BENTO_VALIDATE_SSL", str(not BENTO_DEBUG)).strip().lower() in TRUTH_VALUES

    DATABASE: str = os.environ.get("DATABASE", "bento_wes.db")
    SERVICE_ID = SERVICE_ID
    SERVICE_TEMP: str = os.environ.get("SERVICE_TEMP", "tmp")
    SERVICE_BASE_URL: str = os.environ.get("SERVICE_BASE_URL", "http://127.0.0.1:5000/")

    # WDL-file-related configuration
    WOM_TOOL_LOCATION: Optional[str] = os.environ.get("WOM_TOOL_LOCATION")
    WORKFLOW_HOST_ALLOW_LIST: Optional[str] = os.environ.get("WORKFLOW_HOST_ALLOW_LIST")

    # Backend configuration
    CROMWELL_LOCATION: str = os.environ.get("CROMWELL_LOCATION", "/cromwell.jar")

    # CORS
    CORS_ORIGINS: list[str] | str = os.environ.get("CORS_ORIGINS", "").split(";") or "*"

    # Authn/z-related configuration
    AUTHZ_URL: str = AUTHZ_URL
    AUTHZ_ENABLED: bool = AUTHZ_ENABLED
    #  - ... for WES itself:
    BENTO_OPENID_CONFIG_URL: str = os.environ.get(
        "BENTO_OPENID_CONFIG_URL", "https://bentov2auth.local/realms/bentov2/.well-known/openid-configuration")
    WES_CLIENT_ID: str = os.environ.get("WES_CLIENT_ID", "bento_wes")
    WES_CLIENT_SECRET: str = os.environ.get("WES_CLIENT_SECRET", "")

    # Other services, used for interpolating workflow variables and (
    DRS_URL: str = os.environ.get("DRS_URL", "").strip().rstrip("/")
    METADATA_URL: str = os.environ.get("METADATA_URL", "").strip().rstrip("/")
    SERVICE_REGISTRY_URL: str = SERVICE_REGISTRY_URL

    # VEP-related configuration
    VEP_CACHE_DIR: Optional[str] = os.environ.get("VEP_CACHE_DIR")

    INGEST_POST_TIMEOUT: int = 60 * 60  # 1 hour
    # Timeout for workflow runs themselves, in seconds - default to 48 hours
    WORKFLOW_TIMEOUT: int = int(os.environ.get("WORKFLOW_TIMEOUT", str(60 * 60 * 48)))
