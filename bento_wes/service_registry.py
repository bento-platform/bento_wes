from typing import Annotated
from fastapi import Depends

from bento_lib.service_info.manager import ServiceManager

from .config import SettingsDep
from .logger import LoggerDep

__all__ = ["get_service_manager", "ServiceManagerDep"]


def get_service_manager(settings: SettingsDep, logger: LoggerDep) -> ServiceManager:
    # BoundLogger isn't hashable; cannot memoize with this
    return ServiceManager(logger, 60, settings.service_registry_url, settings.bento_validate_ssl)  # type: ignore


ServiceManagerDep = Annotated[ServiceManager, Depends(get_service_manager)]
