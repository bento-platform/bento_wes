import os
import re
from datetime import UTC, datetime

__all__ = [
    "iso_now",
    "subprocess_env",
]


def iso_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO date format


SENSITIVE_ENV_VAR_PATTERN = re.compile(r"SECRET|TOKEN|PASSWORD|PASSWD|CREDENTIAL|PRIVATE_KEY|API_KEY", re.IGNORECASE)


def subprocess_env() -> dict[str, str]:
    """
    Environment for child processes (Cromwell, WOMtool). Workflow task commands run as shell processes under Cromwell
    and inherit its environment, so strip anything that looks like a credential (e.g. WES_CLIENT_SECRET.)
    """
    return {k: v for k, v in os.environ.items() if not SENSITIVE_ENV_VAR_PATTERN.search(k)}
