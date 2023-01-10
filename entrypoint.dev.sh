#!/bin/bash

export FLASK_DEBUG=false
export FLASK_APP=bento_wes.app:application

if [ -z "${INTERNAL_PORT}" ]; then
  # Set default internal port to 5000
  export INTERNAL_PORT=5000
fi

python -m poetry install

echo "[ENTRYPOINT] Starting celery worker"
celery --app bento_wes.app worker --loglevel=INFO &

echo "[ENTRYPOINT] Starting Flask server"
python -m debugpy --listen 0.0.0.0:5678 -m flask run \
  --host 0.0.0.0 \
  --port "${INTERNAL_PORT}"