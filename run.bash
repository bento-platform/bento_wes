#!/bin/bash

# Set default internal port to 5000
: "${INTERNAL_PORT:=5000}"

# Start Celery worker with log level dependent on BENTO_DEBUG
echo "[bento_wes] [entrypoint] Starting celery worker"
celery_log_level="INFO"
if [[
  "${BENTO_DEBUG}" == "true" ||
  "${BENTO_DEBUG}" == "True" ||
  "${BENTO_DEBUG}" == "1"
]]; then
  celery_log_level="DEBUG"
fi
celery --app bento_wes.app worker --loglevel="${celery_log_level}" &

# Start API server
echo "[bento_wes] [entrypoint] Starting gunicorn"
# using 1 worker, multiple threads
# see https://stackoverflow.com/questions/38425620/gunicorn-workers-and-threads
gunicorn bento_wes.app:application \
  --workers 1 \
  --threads "$(( 2 * $(nproc --all) + 1))" \
  --bind "0.0.0.0:${INTERNAL_PORT}"
