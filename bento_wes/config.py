import os

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


def _to_bool(val: str) -> bool:
    return val.strip().lower() in TRUTH_VALUES


TRUTH_VALUES = ("true", "1")

AUTHZ_ENABLED = os.environ.get("AUTHZ_ENABLED", "true").strip().lower() in TRUTH_VALUES

BENTO_DEBUG: bool = _to_bool(os.environ.get("BENTO_DEBUG", os.environ.get("FLASK_DEBUG", "false")))
CELERY_DEBUG: bool = _to_bool(os.environ.get("CELERY_DEBUG", ""))
BENTO_CONTAINER_LOCAL: bool = _to_bool(os.environ.get("BENTO_CONTAINER_LOCAL", "false"))
BENTO_VALIDATE_SSL: bool = _to_bool(os.environ.get("BENTO_VALIDATE_SSL", str(not BENTO_DEBUG)))

if not BENTO_VALIDATE_SSL:
    # If we've turned off SSL validation, suppress insecure connection warnings
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

AUTHZ_URL: str = _get_from_environ_or_fail("BENTO_AUTHZ_SERVICE_URL").strip().rstrip("/")
SERVICE_REGISTRY_URL: str = _get_from_environ_or_fail("SERVICE_REGISTRY_URL").strip().rstrip("/")

BENTO_EVENT_REDIS_URL = os.environ.get("BENTO_EVENT_REDIS_URL", "redis://localhost:6379")

SERVICE_BASE_URL: str = os.environ.get("SERVICE_BASE_URL", "http://127.0.0.1:5000/")
if not SERVICE_BASE_URL.endswith("/"):
    SERVICE_BASE_URL += "/"


class Config:
    BENTO_URL: str = os.environ.get("BENTO_URL", "http://127.0.0.1:5000/")

    BENTO_DEBUG: bool = BENTO_DEBUG
    BENTO_CONTAINER_LOCAL: bool = BENTO_CONTAINER_LOCAL
    BENTO_VALIDATE_SSL: bool = BENTO_VALIDATE_SSL

    SERVICE_ID = SERVICE_ID
    SERVICE_DATA: str = os.environ.get("SERVICE_DATA", "data")
    DATABASE: str = os.environ.get("DATABASE", f"{SERVICE_DATA}/bento_wes.db")
    SERVICE_TEMP: str = os.environ.get("SERVICE_TEMP", "tmp")
    SERVICE_BASE_URL: str = SERVICE_BASE_URL

    # WDL-file-related configuration
    WOM_TOOL_LOCATION: str | None = os.environ.get("WOM_TOOL_LOCATION")
    WORKFLOW_HOST_ALLOW_LIST: str | None = os.environ.get("WORKFLOW_HOST_ALLOW_LIST")

    # Backend configuration
    CROMWELL_LOCATION: str = os.environ.get("CROMWELL_LOCATION", "/cromwell.jar")

    # CORS
    CORS_ORIGINS: list[str] | str = [x for x in os.environ.get("CORS_ORIGINS", "").split(";") if x] or "*"

    # Authn/z-related configuration
    AUTHZ_URL: str = AUTHZ_URL
    AUTHZ_ENABLED: bool = AUTHZ_ENABLED
    #  - ... for WES itself:
    BENTO_OPENID_CONFIG_URL: str = os.environ.get(
        "BENTO_OPENID_CONFIG_URL", "https://bentov2auth.local/realms/bentov2/.well-known/openid-configuration")
    WES_CLIENT_ID: str = os.environ.get("WES_CLIENT_ID", "bento_wes")
    WES_CLIENT_SECRET: str = os.environ.get("WES_CLIENT_SECRET", "")

    # Service registry URL, used for looking up service kinds to inject as workflow input
    SERVICE_REGISTRY_URL: str = SERVICE_REGISTRY_URL

    # VEP-related configuration
    VEP_CACHE_DIR: str | None = os.environ.get("VEP_CACHE_DIR")

    INGEST_POST_TIMEOUT: int = 60 * 60  # 1 hour
    # Timeout for workflow runs themselves, in seconds - default to 48 hours
    WORKFLOW_TIMEOUT: int = int(os.environ.get("WORKFLOW_TIMEOUT", str(60 * 60 * 48)))

    # Enables interactive debug of Celery tasks locally, not possible with worker threads otherwise
    CELERY_ALWAYS_EAGER: bool = CELERY_DEBUG
