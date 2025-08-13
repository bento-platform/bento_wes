from pydantic_settings import BaseSettings
from celery import Celery


class CeleryConfig(BaseSettings):
    celery_result_backend: str = "redis://"
    celery_broker_url: str = "redis://"

    class Config:
        env_prefix = "CELERY_"
        case_sensitive = False


config = CeleryConfig()

celery = Celery(
    "bento_wes",
    backend=config.celery_result_backend,
    broker=config.celery_broker_url,
)
