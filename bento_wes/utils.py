from datetime import datetime, timezone


__all__ = ["iso_now"]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO date format
