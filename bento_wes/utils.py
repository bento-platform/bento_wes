from datetime import datetime, timezone
from typing import Literal

from bento_wes.service_registry import get_bento_service_kind_url


__all__ = ["iso_now", "get_drop_box_resource_url"]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO date format


def get_drop_box_resource_url(path: str, resource: Literal["objects", "tree"] = "objects") -> str:
    drop_box_url = get_bento_service_kind_url("drop-box")
    clean_path = path.lstrip("/")
    return f"{drop_box_url}/{resource}/{clean_path}"
