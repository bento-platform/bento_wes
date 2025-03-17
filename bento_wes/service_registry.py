import requests
from datetime import datetime
from flask import current_app

__all__ = [
    "get_bento_services",
]


# TODO: this will need to be re-done without a global cache for any async implementation

_bento_services_cache: dict | None = None
_bento_services_last_updated: datetime | None = None

_cache_ttl: int = 30  # seconds


def get_bento_services() -> dict:
    global _bento_services_cache
    global _bento_services_last_updated

    if not (
            _bento_services_cache and
            _bento_services_last_updated and
            (datetime.now() - _bento_services_last_updated).total_seconds() < _cache_ttl
    ):
        validate_ssl = current_app.config["BENTO_VALIDATE_SSL"]
        res = requests.get(
            current_app.config["SERVICE_REGISTRY_URL"].rstrip("/") + "/bento-services", verify=validate_ssl)
        res.raise_for_status()
        _bento_services_cache = {v["service_kind"]: v for v in res.json().values()}
        _bento_services_last_updated = datetime.now()

    return _bento_services_cache


def get_bento_service_kind_url(kind: str) -> str | None:
    services = get_bento_services()
    service_details: dict | None = services.get(kind)
    if service_details:
        return service_details.get("url")
