from fastapi import Request, HTTPException
from typing import Iterator

from bento_lib.workflows.utils import namespaced_input
from bento_lib.auth.resources import RESOURCE_EVERYTHING, build_resource
from bento_lib.workflows.models import WorkflowProjectDatasetInput

from bento_wes.authz import authz_middleware
from bento_wes.models import RunRequest
from bento_wes.config import config
from bento_wes.types import AuthHeaderModel

def _get_resource_for_run_request(run_req: RunRequest) -> dict:
    wi, wm = run_req.tags.workflow_id, run_req.tags.workflow_metadata
    resource = RESOURCE_EVERYTHING

    inp = next((i for i in wm.inputs if isinstance(i, WorkflowProjectDatasetInput)), None)
    if inp and (val := run_req.workflow_params.get(namespaced_input(wi, inp.id))):
        project, dataset = val.split(":")
        resource = build_resource(project, dataset, data_type=wm.data_type)

    return resource

def check_single_run_permission_and_mark(run_req: RunRequest, permission: str, request: Request, authz_header_from_form: AuthHeaderModel = None) -> bool:
    if config.authz_enabled:
        if not authz_middleware.evaluate_one(
            request,
            _get_resource_for_run_request(run_req),
            permission,
            headers_getter=authz_header_from_form,
            mark_authz_done=True,
        ):
            raise HTTPException(status_code=403, detail="Forbidden: insufficient permissions.")

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
            [_get_resource_for_run_request(run_request) for run_request in run_requests],
            [permission],
        )
    )