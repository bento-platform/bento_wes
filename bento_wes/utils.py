from datetime import datetime, timezone

from bento_wes.service_registry import get_bento_service_kind_url


__all__ = ["iso_now"]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO date format


def get_object_drop_box_url(path: str) -> str:
    drop_box_url = get_bento_service_kind_url("drop-box")
    clean_path = path.lstrip("/")
    return f"{drop_box_url}/objects/{clean_path}"
