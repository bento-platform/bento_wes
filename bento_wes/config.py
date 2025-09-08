import os
from pathlib import Path
from pydantic import field_validator, ValidationError, Field, AliasChoices
from bento_lib.config.pydantic import BentoFastAPIBaseConfig
from bento_lib.service_info.types import BentoExtraServiceInfo

from .constants import SERVICE_ID, SERVICE_NAME, BENTO_SERVICE_KIND, GIT_REPOSITORY
from .logger import logger


__all__ = [
    "BENTO_EVENT_REDIS_URL",
    "flask_config"
    "config"
]


def _get_from_environ_or_fail(var: str) -> str:
    if (val := os.environ.get(var, "")) == "":
        logger.critical(f"{var} must be set")
        exit(1)
    return val


def _to_bool(val: str) -> bool:
    return val.strip().lower() in TRUTH_VALUES


TRUTH_VALUES = ("true", "1")

AUTHZ_ENABLED = os.environ.get("AUTHZ_ENABLED", "true").strip().lower() in TRUTH_VALUES

BENTO_DEBUG: bool = _to_bool(os.environ.get("BENTO_DEBUG", os.environ.get("FLASK_DEBUG", "false")))
CELERY_DEBUG: bool = _to_bool(os.environ.get("CELERY_DEBUG", ""))
BENTO_CONTAINER_LOCAL: bool = _to_bool(os.environ.get("BENTO_CONTAINER_LOCAL", "false"))
BENTO_VALIDATE_SSL: bool = _to_bool(os.environ.get("BENTO_VALIDATE_SSL", str(not BENTO_DEBUG)))

if not BENTO_VALIDATE_SSL:
    # If we've turned off SSL validation, suppress insecure connection warnings
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

AUTHZ_URL: str = _get_from_environ_or_fail("BENTO_AUTHZ_SERVICE_URL").strip().rstrip("/")
SERVICE_REGISTRY_URL: str = _get_from_environ_or_fail("SERVICE_REGISTRY_URL").strip().rstrip("/")

BENTO_EVENT_REDIS_URL = os.environ.get("BENTO_EVENT_REDIS_URL", "redis://localhost:6379")

SERVICE_BASE_URL: str = os.environ.get("SERVICE_BASE_URL", "http://127.0.0.1:5000/")
if not SERVICE_BASE_URL.endswith("/"):
    SERVICE_BASE_URL += "/"



BENTO_EXTRA_SERVICE_INFO: BentoExtraServiceInfo = {
    "serviceKind": BENTO_SERVICE_KIND,
    "dataService": False,
    "workflowProvider": True,
    "gitRepository": GIT_REPOSITORY
}

class Config(BentoFastAPIBaseConfig):
    bento_url: str = "http://127.0.0.1:5000/"

    bento_debug: bool = Field(False, validation_alias=AliasChoices("BENTO_DEBUG", "FLASK_DEBUG"))
    bento_container_local: bool = False
    bento_validate_ssl: bool = not bento_debug

    service_id: str = SERVICE_ID
    service_name: str = SERVICE_NAME
    service_data: Path = Path("data")
    database: Path = service_data / "bento_wes.db"
    service_temp: Path = Path("tmp")
    service_base_url: str = SERVICE_BASE_URL

    # WDL-file-related configuration
    wom_tool_location: str | None
    workflow_host_allow_list: str | None
    
    # Backend configuration
    cromwell_location: str = "/cromwell.jar"
    
    # CORS
    cors_origins: list[str] | str = "*"

    # Authn/z-related configuration
    authz_url: str = Field(..., validation_alias="BENTO_AUTHZ_SERVICE_URL")
    authz_enabled: bool = True
    bento_authz_enabled: bool = authz_enabled # for authz middlware to self recognize
    

    #  - ... for WES itself:
    bento_openid_config_url: str = "https://bentov2auth.local/realms/bentov2/.well-known/openid-configuration"
    wes_client_id: str = "bento_wes"
    wes_client_secret: str = ""

    # Service registry URL, used for looking up service kinds to inject as workflow input
    service_registry_url: str

    # VEP-related configuration
    vep_cache_dir: str | None = None

    ingest_post_timeout: int = 60 * 60  # 1 hour
    workflow_timeout: int = 60 * 60 * 48 # 2 days

    # Enables interactive debug of Celery tasks locally, not possible with worker threads otherwise
    celery_always_eager: bool = Field(False, validation_alias="CELERY_DEBUG")

    @field_validator("authz_url", "service_registry_url", mode="before")
    def required_and_skip_trailing_slash(cls, v:str, info) -> str:
        if not v or not v.strip():
            raise ValidationError(f"{info.field_name.upper()} must not be empty")
        return v.strip().rstrip("/")



config = Config()
