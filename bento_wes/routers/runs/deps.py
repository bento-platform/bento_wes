from fastapi import Depends, Request
from fastapi.exceptions import HTTPException
from fastapi import status
from starlette.datastructures import Headers
from typing import Awaitable, Callable, FrozenSet, Annotated, Iterable, Iterator
from uuid import UUID

from bento_lib.auth.permissions import Permission, P_VIEW_RUNS

from bento_wes import states
from bento_wes.db import DatabaseDep
from bento_wes.models import RunWithDetails
from bento_wes.types import RunStream, AuthHeaderModel
from bento_wes.authz import AuthzMiddlewareDep
from bento_wes.config import SettingsDep


# TODO: middleware just to check if run_id is valid
def stash_run_or_404(
    request: Request,
    run_id: UUID,
    db: DatabaseDep,
) -> None:
    run = db.get_run_with_details(run_id, stream_content=False)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    request.state.run = run


def get_run_from_state(request: Request) -> RunWithDetails:
    try:
        return request.state.run  # set by stash_run_or_404
    except AttributeError:
        raise RuntimeError("Run not initialized for this request")


RunDep = Annotated[RunWithDetails, Depends(get_run_from_state)]


StreamData = tuple[str, dict[str, str]]  # (content, headers)


def get_stream_data(db: DatabaseDep, stream: RunStream, run_id: UUID) -> StreamData:
    """Dependency function that returns stream content and cache control headers."""
    run = db.get_run_with_details(run_id, stream_content=True)
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Stream {stream} not found for run {run_id}")

    # If we've finished, we allow long-term (24h) caching of the stdout/stderr responses.
    # Otherwise, no caching allowed!
    cache_control = (
        "private, max-age=86400"
        if run.state in states.TERMINATED_STATES
        else "no-cache, no-store, must-revalidate, max-age=0"
    )

    content = run.run_log.stdout if stream == "stdout" else run.run_log.stderr

    return content, {"Cache-Control": cache_control}


StreamDataDep = Annotated[StreamData, Depends(get_stream_data)]


AuthzCallable = Callable[[Permission, dict], Awaitable[None]]


def evaluate_run_permissions_function(
    request: Request, settings: SettingsDep, authz_middleware: AuthzMiddlewareDep
) -> AuthzCallable:
    async def _inner(permission: Permission, resource: dict) -> None:
        if not settings.authz_enabled:
            return None

        p: FrozenSet[Permission] = frozenset({permission})
        return await authz_middleware.async_check_authz_evaluate(request, p, resource, set_authz_flag=True)

    return _inner


AuthzDep = Annotated[AuthzCallable, Depends(evaluate_run_permissions_function)]


def evaluate_run_permissions_function_from_form(
    request: Request,
    settings: SettingsDep,
    authz_middleware: AuthzMiddlewareDep,
    auth: Annotated[AuthHeaderModel, Depends(AuthHeaderModel.from_form)],
) -> AuthzCallable:
    async def _inner(permission: Permission, resource: dict) -> None:
        if not settings.authz_enabled:
            return None

        # Create a modified request with authorization from form data
        if auth.Authorization:
            new_headers = dict(request.headers) | auth.as_dict()
            request._headers = Headers(new_headers)

        p: FrozenSet[Permission] = frozenset({permission})
        return await authz_middleware.async_check_authz_evaluate(request, p, resource, set_authz_flag=True)

    return _inner


AuthzDepFromForm = Annotated[AuthzCallable, Depends(evaluate_run_permissions_function_from_form)]

AuthzCompletionCallable = Callable[[], Awaitable[None]]


def mark_authz_done(authz_middleware: AuthzMiddlewareDep, request: Request):
    async def _inner():
        authz_middleware.mark_authz_done(request)

    return _inner


AuthzCompletionDep = Annotated[AuthzCompletionCallable, Depends(mark_authz_done)]


AuthzViewRunsEvaluateCallable = Callable[[Iterable[dict]], Iterator[bool]]


def authz_evaluate_view(
    authz_middleware: AuthzMiddlewareDep, request: Request, settings: SettingsDep
) -> AuthzViewRunsEvaluateCallable:
    def _inner(resources: Iterable[dict]) -> Iterator[bool]:
        if not settings.authz_enabled:
            yield from [True] * len(resources)
        yield from [r[0] for r in authz_middleware.evaluate(request, resources, [P_VIEW_RUNS])]

    return _inner


AuthzViewRunsEvaluateDep = Annotated[AuthzViewRunsEvaluateCallable, Depends(authz_evaluate_view)]
