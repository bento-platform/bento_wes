from bento_lib.auth.middleware.flask import FlaskAuthMiddleware
from bento_lib.auth.middleware.fastapi import FastApiAuthMiddleware
from .config import config
from .logger import logger

__all__ = [
    "authz_middleware",
    "authz_middleware_flask"
]

authz_middleware = FastApiAuthMiddleware.build_from_fastapi_pydantic_config(
    config, logger
)

authz_middleware_flask = FlaskAuthMiddleware(
    config.authz_url,
    debug_mode=config.bento_debug,
    enabled=config.authz_url,
)