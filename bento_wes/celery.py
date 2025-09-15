from pydantic_settings import BaseSettings
from pydantic import ConfigDict
from celery import Celery


class CeleryConfig(BaseSettings):
    celery_result_backend: str = "redis://"
    celery_broker_url: str = "redis://"

    model_config = ConfigDict(
        env_prefix="CELERY_",
        case_sensitive=False
    )


config = CeleryConfig()

celery = Celery(
    "bento_wes",
    backend=config.celery_result_backend,
    broker=config.celery_broker_url,
)

celery.conf.update(
    imports=[
        "bento_wes.runner",
    ]
)