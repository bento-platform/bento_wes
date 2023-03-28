#!/bin/bash

# Update dependencies and install module locally
/poetry_user_install_dev.bash

export FLASK_APP="bento_wes.app:application"

# Create temporary directory if needed
mkdir -p /wes/tmp

# Clean up after any crashed previous container runs
job_store_path="${SERVICE_TEMP:-tmp}/toil_job_store"
if [[ -d "${job_store_path}" ]]; then
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

# Set default internal port to 5000
: "${INTERNAL_PORT:=5000}"

# Set internal debug port, falling back to debugpy default
: "${DEBUGGER_PORT:=5680}"

# Start API server
echo "[bento_wes] [entrypoint] Starting Flask server"
python -m debugpy --listen 0.0.0.0:${DEBUGGER_PORT} -m flask run \
  --host 0.0.0.0 \
  --port "${INTERNAL_PORT}"