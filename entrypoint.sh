#!/bin/bash

export FLASK_DEBUG=false
if [ -z "${INTERNAL_PORT}" ]; then
  # Set default internal port to 5000
  export INTERNAL_PORT=5000
fi

echo "[ENTRYPOINT] Starting celery worker"
celery --app bento_wes.app worker --loglevel=INFO &

echo "[ENTRYPOINT] Starting gunicorn"
# using 1 worker, multiple threads
# see https://stackoverflow.com/questions/38425620/gunicorn-workers-and-threads
gunicorn bento_wes.app:application \
  --workers 1 \
  --threads $(expr 2 \* $(nproc --all) + 1) \
  --bind "0.0.0.0:${INTERNAL_PORT}"
