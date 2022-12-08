#!/bin/bash

celery --loglevel=INFO --app bento_wes.app worker &> ./celery.log &

# using 1 worker, multiple threads
# see https://stackoverflow.com/questions/38425620/gunicorn-workers-and-threads
gunicorn bento_wes.app:application \
  --workers 1 \
  --threads $(expr 2 \* $(nproc --all) + 1) \
  --bind "0.0.0.0:${INTERNAL_PORT}"
