from functools import lru_cache
from typing import Annotated
from fastapi import Depends

from bento_lib.auth.middleware.fastapi import FastApiAuthMiddleware
from .config import SettingsDep
from .logger import LoggerDep


@lru_cache
def get_authz_middleware(settings: SettingsDep, logger: LoggerDep):
    return FastApiAuthMiddleware.build_from_fastapi_pydantic_config(settings, logger)


AuthzMiddlewareDep = Annotated[FastApiAuthMiddleware, Depends(get_authz_middleware)]
