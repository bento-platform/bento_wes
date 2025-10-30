from __future__ import annotations

from fastapi import Depends
from functools import lru_cache
from datetime import timedelta
from pathlib import Path
from typing import Literal, Annotated, Tuple

from pydantic import (
    AnyHttpUrl,
    Field,
    AliasChoices,
    model_validator,
    field_validator,
    SecretStr,
)
from pydantic.networks import RedisDsn
from pydantic_settings import SettingsConfigDict

from bento_lib.config.pydantic import BentoFastAPIBaseConfig
from bento_lib.service_info.types import BentoExtraServiceInfo

from .constants import SERVICE_ID, SERVICE_NAME, BENTO_SERVICE_KIND, GIT_REPOSITORY

__all__ = ["Settings", "get_settings", "SettingsDep"]

BENTO_EXTRA_SERVICE_INFO: BentoExtraServiceInfo = {
    "serviceKind": BENTO_SERVICE_KIND,
    "dataService": False,
    "workflowProvider": True,
    "gitRepository": GIT_REPOSITORY,
}


# Even though hashable, the class isn't detected as hashable by the type checker requiring ignore comments
class Settings(BentoFastAPIBaseConfig):
    """
    Centralized application configuration.

    Extends pydantic's BaseSettings.
    Loads from environment variables (optionally .env), provides type-safety,
    and normalizes values (e.g., URLs without trailing slashes, base URL with trailing slash).
    """

    # --- Pydantic/Settings config ---
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        frozen=True,
    )

    # --- Core / Bento ---
    bento_url: AnyHttpUrl = AnyHttpUrl("http://127.0.0.1:5000/")
    bento_debug: bool = Field(
        default=False,
        validation_alias=AliasChoices("BENTO_DEBUG", "FLASK_DEBUG"),
        description="Debug mode (BENTO_DEBUG takes precedence over FLASK_DEBUG).",
    )
    bento_container_local: bool = Field(default=False, alias="BENTO_CONTAINER_LOCAL")

    @model_validator(mode="after")
    def _derive_bento_validate_ssl_if_unset(self):
        if "bento_validate_ssl" not in self.model_fields_set:
            object.__setattr__(self, "bento_validate_ssl", not self.bento_debug)
        return self

    # --- Service identity & paths ---
    service_id: str = SERVICE_ID
    service_name: str = SERVICE_NAME
    service_data: Path = Path("data")
    database: Path = service_data / "bento_wes.db"
    service_temp: Path = Path("tmp")

    service_base_url: str = Field(
        "http://127.0.0.1:5000/",
        alias="SERVICE_BASE_URL",
        description="Public base URL of this service (normalized to include trailing slash).",
    )

    @field_validator("service_base_url", mode="after")
    def _ensure_trailing_slash(cls, v: str) -> str:
        return v if v.endswith("/") else v + "/"

    # --- Event bus / Redis ---
    bento_event_redis_url: RedisDsn | str = Field(
        "redis://localhost:6379",
        alias="BENTO_EVENT_REDIS_URL",
    )

    # --- AuthN/Z + Service registry ---
    authz_url: str = Field(..., validation_alias="BENTO_AUTHZ_SERVICE_URL")
    authz_enabled: bool = Field(True, alias="AUTHZ_ENABLED")
    bento_authz_enabled: bool = True  # consumed by middleware

    service_registry_url: str = Field(..., alias="SERVICE_REGISTRY_URL")

    # OIDC / WES client
    bento_openid_config_url: str = "https://bentov2auth.local/realms/bentov2/.well-known/openid-configuration"
    wes_client_id: str = "bento_wes"
    wes_client_secret: SecretStr = SecretStr("")

    # --- Workflow backend / WDL ---
    cromwell_location: Path = Path("/cromwell.jar")
    wom_tool_location: str | None = None
    workflow_host_allow_list: str | None = None

    # --- CORS ---
    cors_origins: Tuple[str, ...] | Literal["*"] = "*"

    # --- VEP / optional data ---
    vep_cache_dir: Path | None = None

    # --- Timeouts as timedeltas for semantics ---
    ingest_post_timeout: timedelta = timedelta(hours=1)
    workflow_timeout: timedelta = timedelta(days=2)

    @field_validator("ingest_post_timeout", "workflow_timeout", mode="before")
    @classmethod
    def _coerce_timeout(cls, v):
        if v is None or isinstance(v, timedelta):
            return v
        # ints/floats -> seconds
        if isinstance(v, (int, float)):
            return timedelta(seconds=float(v))
        s = str(v).strip()
        # numeric string -> seconds
        if s.isdigit():
            return timedelta(seconds=int(s))
        # HH:MM:SS
        try:
            parts = s.split(":")
            if len(parts) == 3:
                h, m, sec = parts
                return timedelta(hours=int(h), minutes=int(m), seconds=float(sec))
        except Exception:
            pass
        # You can add ISO-8601 parsing here if you want (e.g., via isodate)
        raise ValueError("Invalid timeout; use timedelta, seconds (int), 'HH:MM:SS', or ISO-8601 like 'P2D'.")

    # --- Celery / local debug ---
    celery_always_eager: bool = Field(False, validation_alias="CELERY_DEBUG")

    # --- Normalizers / guards ---
    @field_validator("authz_url", "service_registry_url", mode="before")
    @classmethod
    def _require_non_empty_and_strip(cls, v: str) -> str:
        if not v or not str(v).strip():
            raise ValueError("This URL must not be empty")
        return str(v).strip().rstrip("/")

    @field_validator("bento_event_redis_url", mode="before")
    @classmethod
    def _normalize_redis(cls, v: str) -> str:
        return str(v).strip()


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    return settings


SettingsDep = Annotated[Settings, Depends(get_settings)]
