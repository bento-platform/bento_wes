import os
from urllib.parse import quote

from .constants import SERVICE_ID


__all__ = [
    "NGINX_INTERNAL_SOCKET",
    "Config",
]


NGINX_INTERNAL_SOCKET = quote(os.environ.get("NGINX_INTERNAL_SOCKET", "/chord/tmp/nginx_internal.sock"), safe="")


class Config:
    CHORD_SERVICES = os.environ.get("CHORD_SERVICES", "chord_services.json")
    CHORD_URL = os.environ.get("CHORD_URL", "http://127.0.0.1:5000/")
    DATABASE = os.environ.get("DATABASE", "bento_wes.db")
    SERVICE_ID = SERVICE_ID
    SERVICE_TEMP = os.environ.get("SERVICE_TEMP", "tmp")
    SERVICE_URL_BASE_PATH = os.environ.get("SERVICE_URL_BASE_PATH", "/")
    WOM_TOOL_LOCATION = os.environ.get("WOM_TOOL_LOCATION", "womtool.jar")

    DRS_URL = os.environ.get("DRS_URL", f"http+unix://{NGINX_INTERNAL_SOCKET}/api/drs").strip().rstrip("/")
    WRITE_OUTPUT_TO_DRS = os.environ.get("WRITE_OUTPUT_TO_DRS", "false").lower().strip() == "true"
    DRS_DEDUPLICATE = os.environ.get("DRS_DEDUPLICATE", "true").lower().strip() == "true"
    DRS_SKIP_TYPES = tuple(t.strip() for t in os.environ.get("DRS_SKIP_TYPES", "").split(",") if t.strip())

    NGINX_INTERNAL_SOCKET = NGINX_INTERNAL_SOCKET
    INGEST_POST_TIMEOUT = 60 * 10  # 10 minutes
