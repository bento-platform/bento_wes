from bento_lib.auth.middleware.flask import FlaskAuthMiddleware
from . import config

__all__ = [
    "authz_middleware",
]

authz_middleware = FlaskAuthMiddleware(
    config.BENTO_AUTHZ_SERVICE_URL,
    debug_mode=config.BENTO_DEBUG,
    enabled=config.AUTHZ_ENABLED,
)
