from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse, PlainTextResponse
from typing import Annotated
import uuid
import json

from bento_wes.db import Database, get_db
from bento_wes.types import RunStream

from .deps import get_run_or_404, get_stream

detail_router = APIRouter(prefix="/{run_id}")
detail_router.dependencies.append(Depends(get_run_or_404))

@detail_router.get("")
def get_run(run_id: uuid.UUID, db: Annotated[Database, Depends(get_db)]):
    run_details = db.get_run_with_details(db.cursor(), run_id, stream_content=False)
    return JSONResponse(json.loads(run_details.model_dump_json()))

@detail_router.post("/download-artifact")
def run_download_artifact(run_id: uuid.UUID):
    # TODO
    pass

@detail_router.get(
    "/{stream}",
    response_class=PlainTextResponse
)
def run_stream(
    run_id: uuid.UUID,
    stream: RunStream,
    db: Annotated[Database, Depends(get_db)],
):
    # TODO: add auth
    # TODO: validate run_id 
    return get_stream(db, stream, run_id)


@detail_router.post("/cancel")
def cancel_run(run_id: uuid.UUID, db: Annotated[Database, Depends(get_db)]):
    pass

@detail_router.get("/status")
def run_status(run_id: uuid.UUID):
    pass