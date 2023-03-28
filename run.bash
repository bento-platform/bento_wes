#!/bin/bash

if [ -z "${INTERNAL_PORT}" ]; then
  # Set default internal port to 5000
  export INTERNAL_PORT=5000
fi

# Clean up after any crashed previous container runs
job_store_path="${SERVICE_TEMP:-tmp}/toil_job_store"
if [ -d "${job_store_path}" ]; then
  echo "[bento_wes] [entrypoint] Cleaning Toil job store"
  toil clean "file:${SERVICE_TEMP:-tmp}/toil_job_store"
fi

# Start Celery worker with log level dependent on BENTO_DEBUG
echo "[bento_wes] [entrypoint] Starting celery worker"
celery_log_level="INFO"
if [[
  "${BENTO_DEBUG}" == "true" ||
  "${BENTO_DEBUG}" == "True" ||
  "${BENTO_DEBUG}" == "1" ||
  "${CHORD_DEBUG}" == "true" ||
  "${CHORD_DEBUG}" == "True" ||
  "${CHORD_DEBUG}" == "1"
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
