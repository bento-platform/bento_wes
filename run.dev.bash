#!/bin/bash

# # Update dependencies and install module locally
# /poetry_user_install_dev.bash
poetry install

export FLASK_APP="bento_wes.app:application"

# Create temporary directory if needed
mkdir -p /wes/tmp

# Start Celery worker with log level dependent on BENTO_DEBUG
echo "[bento_wes] [entrypoint] Starting celery worker"
celery_log_level="INFO"
if [[ "${BENTO_DEBUG}" == "true" || "${BENTO_DEBUG}" == "True" || "${BENTO_DEBUG}" == "1" ]]; then
  celery_log_level="DEBUG"
fi
celery --app bento_wes.app worker --loglevel="${celery_log_level}" &

# Set default internal port to 5000
: "${INTERNAL_PORT:=5000}"

# Set internal debug port, falling back to debugpy default
: "${DEBUGGER_PORT:=5680}"

# Start ASGI API server using Uvicorn
echo "[bento_wes] [entrypoint] Starting Uvicorn ASGI server (FastAPI + Flask wrapper)"
poetry run python -Xfrozen_modules=off -m debugpy --listen 0.0.0.0:${DEBUGGER_PORT} -m uvicorn \
  bento_wes.asgi_main:app \
  --host 0.0.0.0 \
  --port "${INTERNAL_PORT}" \
  --reload
