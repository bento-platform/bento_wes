from functools import lru_cache
from typing import Annotated
from fastapi import Depends

from bento_lib.service_info.manager import ServiceManager

from .config import SettingsDep
from .logger import LoggerDep

__all__ = ["get_service_manager", "ServiceManagerDep"]


@lru_cache
def get_service_manager(settings: SettingsDep, logger: LoggerDep) -> ServiceManager:
    return ServiceManager(logger, 60, settings.service_registry_url, settings.bento_validate_ssl)  # type: ignore


ServiceManagerDep = Annotated[ServiceManager, Depends(get_service_manager)]
