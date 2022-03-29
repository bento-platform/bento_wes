import os
from urllib.parse import quote

from .constants import SERVICE_ID


__all__ = [
    "NGINX_INTERNAL_SOCKET",
    "Config",
]


NGINX_INTERNAL_SOCKET = quote(os.environ.get("NGINX_INTERNAL_SOCKET", "/chord/tmp/nginx_internal.sock"), safe="")


class Config:
    CHORD_URL = os.environ.get("CHORD_URL", "http://127.0.0.1:5000/")
    BENTO_DEBUG = os.environ.get("CHORD_DEBUG", os.environ.get("FLASK_ENV", "production")).strip().lower() in (
        "true", "1", "development")

    DATABASE = os.environ.get("DATABASE", "bento_wes.db")
    SERVICE_ID = SERVICE_ID
    SERVICE_TEMP = os.environ.get("SERVICE_TEMP", "tmp")
    SERVICE_URL_BASE_PATH = os.environ.get("SERVICE_URL_BASE_PATH", "/")

    # WDL-file-related configuration
    WOM_TOOL_LOCATION = os.environ.get("WOM_TOOL_LOCATION")
    WORKFLOW_HOST_ALLOW_LIST = os.environ.get("WORKFLOW_HOST_ALLOW_LIST")

    # OTT-related configuration
    OTT_ENDPOINT_NAMESPACE = os.environ.get("OTT_ENDPOINT_NAMESPACE", f"{CHORD_URL}api/auth/ott/")

    # TT (temporary token)-related config
    TT_ENDPOINT_NAMESPACE = os.environ.get("TT_ENDPOINT_NAMESPACE", f"{CHORD_URL}api/auth/tt/")

    # DRS-related configuration
    DRS_URL = os.environ.get("DRS_URL", f"http+unix://{NGINX_INTERNAL_SOCKET}/api/drs").strip().rstrip("/")
    WRITE_OUTPUT_TO_DRS = os.environ.get("WRITE_OUTPUT_TO_DRS", "false").lower().strip() == "true"
    DRS_DEDUPLICATE = os.environ.get("DRS_DEDUPLICATE", "true").lower().strip() == "true"
    DRS_SKIP_TYPES = tuple(t.strip() for t in os.environ.get("DRS_SKIP_TYPES", "").split(",") if t.strip())

    # VEP-related configuration
    VEP_CACHE_DIR = os.environ.get("VEP_CACHE_DIR")

    NGINX_INTERNAL_SOCKET = NGINX_INTERNAL_SOCKET
    INGEST_POST_TIMEOUT = 60 * 60  # 1 hour
