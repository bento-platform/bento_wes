from fastapi import Depends, Request
from fastapi.responses import PlainTextResponse
from fastapi.exceptions import HTTPException
from typing import Annotated
from uuid import UUID

from bento_wes import states
from bento_wes.db import Database, DatabaseDep
from bento_wes.models import RunWithDetails  
from bento_wes.types import RunStream

# TODO: middleware just to check if run_id is valid
def stash_run_or_404(
    request: Request,
    run_id: UUID,
    db: DatabaseDep,
) -> None:
    run = db.get_run_with_details(db.c, run_id, stream_content=False)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    request.state.run = run


def get_run_from_state(request: Request) -> RunWithDetails:
    try:
        return request.state.run  # set by stash_run_or_404
    except AttributeError:
        raise RuntimeError("Run not initialized for this request")

RunDep = Annotated[RunWithDetails, Depends(get_run_from_state)]


def get_stream(db: Database, stream: RunStream, run_id: UUID):
    run = db.get_run_with_details(db.c, run_id, stream_content=True)
    if run is None:
            raise HTTPException(f"Stream {stream} not found for run {run_id}")
    
    cache_control = (
        "private, max-age=86400"
        if run.state in states.TERMINATED_STATES
        else "no-cache, no-store, must-revalidate, max-age=0"
    )

    content = run.run_log.stdout if stream == "stdout" else run.run_log.stderr

    return PlainTextResponse(
        content,
        status_code=200,
        headers={
            "Cache-Control": cache_control
        }
    )