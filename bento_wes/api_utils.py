from fastapi.responses import PlainTextResponse
from fastapi.exceptions import HTTPException
from uuid import UUID

from .types import RunStream
from .db import Database
from . import states

def get_stream(db: Database, stream: RunStream, run_id: UUID):
    c = db.cursor()
    run = db.get_run_with_details(c, run_id, stream_content=True)
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