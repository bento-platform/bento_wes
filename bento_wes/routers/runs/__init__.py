from .runs import runs_router
from .run_details import detail_router

runs_router.include_router(detail_router)

__all__ = ["runs_router"]
