from fastapi import Request
from typing import Iterator

from bento_wes.authz import authz_middleware
from bento_wes.models import RunRequest
from bento_wes.config import config

def check_runs_permission(run_requests: list[RunRequest], permission: str, request: Request) -> Iterator[bool]:
    if not config.authz_enabled:
        yield from [True] * len(run_requests)  # Assume we have permission for everything if authz disabled
        return

    # /policy/evaluate returns a matrix of booleans of row: resource, col: permission. Thus, we can
    # return permission booleans by resource by flattening it, since there is only one column.
    yield from (
        r[0]
        for r in authz_middleware.evaluate(
            request,
            [run_request.get_authz_resource for run_request in run_requests],
            [permission],
        )
    )