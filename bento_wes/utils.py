from datetime import datetime


__all__ = ["iso_now"]


def iso_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO date format
