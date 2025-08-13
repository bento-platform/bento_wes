from fastapi import Depends, HTTPException
from typing import Annotated
from uuid import UUID
from bento_wes.db import Database, get_db
from bento_wes.models import RunWithDetails  

def get_run_or_404(
    run_id: UUID,
    db: Annotated[Database, Depends(get_db)],
) -> "RunWithDetails":
    c = db.cursor()
    run = db.get_run_with_details(c, run_id, stream_content=False)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run