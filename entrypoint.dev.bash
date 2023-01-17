#!/bin/bash

export FLASK_APP=bento_wes.app:application

if [ -z "${INTERNAL_PORT}" ]; then
  # Set default internal port to 5000
  export INTERNAL_PORT=5000
fi

# Install any dependency changes if needed
python -m poetry install

# Clean up after any crashed previous container runs
job_store_path="${SERVICE_TEMP:-tmp}/toil_job_store"
if [ -d "${job_store_path}" ]; then
  echo "[ENTRYPOINT] Cleaning Toil job store"
  toil clean "file:${SERVICE_TEMP:-tmp}/toil_job_store"
fi

# Start Celery worker with log level dependent on BENTO_DEBUG
echo "[ENTRYPOINT] Starting celery worker"
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
echo "[ENTRYPOINT] Starting Flask server"
python -m debugpy --listen 0.0.0.0:5678 -m flask run \
  --host 0.0.0.0 \
  --port "${INTERNAL_PORT}"
