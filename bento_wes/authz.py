from bento_lib.auth.middleware.fastapi import FastApiAuthMiddleware
from .config import config
from .logger import logger

authz_middleware = FastApiAuthMiddleware.build_from_fastapi_pydantic_config(
    config, logger
)