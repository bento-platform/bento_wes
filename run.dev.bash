#!/bin/bash

# Set .gitconfig for development
/set_gitconfig.bash

# Update dependencies and install module locally
poetry export -f requirements.txt --with dev --output requirements.txt
pip install --user -r requirements.txt
rm requirements.txt
pip install -e .

export FLASK_APP="bento_wes.app:application"

if [[ -z "${INTERNAL_PORT}" ]]; then
  # Set default internal port to 5000
  export INTERNAL_PORT=5000
fi

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

# Start API server
echo "[bento_wes] [entrypoint] Starting Flask server"
python -m debugpy --listen 0.0.0.0:5678 -m flask run \
  --host 0.0.0.0 \
  --port "${INTERNAL_PORT}"
