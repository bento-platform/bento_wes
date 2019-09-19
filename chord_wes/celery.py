import os
from celery import Celery

CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "redis://")
CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://")

celery = Celery("chord_wes", backend=CELERY_RESULT_BACKEND, broker=CELERY_BROKER_URL)
