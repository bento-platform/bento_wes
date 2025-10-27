from functools import lru_cache
from typing import Annotated
from fastapi import Depends

from bento_lib.service_info.manager import ServiceManager

from .config import get_settings
from .logger import get_logger

__all__ = [
    "get_service_manager",
    "get_service_url"
]

@lru_cache
def get_service_manager() -> ServiceManager:
    settings = get_settings()
    logger = get_logger()
    return ServiceManager(logger, 60, settings.service_registry_url, settings.bento_validate_ssl) # type: ignore

ServiceManagerDep = Annotated[ServiceManager, Depends(get_service_manager)]

async def get_service_url(service_kind) -> str | None:
    return await get_service_manager().get_bento_service_url_by_kind(service_kind)
