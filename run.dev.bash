#!/bin/bash
set -euo pipefail

# ---------- Dev install ----------
# /poetry_user_install_dev.bash
poetry install

# ---------- Defaults ----------
: "${INTERNAL_PORT:=5000}"                               # API port
: "${HOST:=0.0.0.0}"                                     # Bind host
: "${DEBUGGER_PORT:=5680}"                               # debugpy port
: "${APP_FACTORY:=bento_wes.app_factory:create_app}"     # "module.path:factory_fn"
: "${UVICORN_EXTRA:=}"                                   # e.g. "--lifespan on"

# ---------- Temp dir ----------
mkdir -p /wes/tmp

# ---------- Log levels ----------
celery_log_level="INFO"
uvicorn_log_level="info"
if [[ "${BENTO_DEBUG:-}" == "true" || "${BENTO_DEBUG:-}" == "True" || "${BENTO_DEBUG:-}" == "1" ]]; then
  celery_log_level="DEBUG"
  uvicorn_log_level="debug"
fi

# ---------- Celery worker ----------
echo "[bento_wes] [entrypoint-dev] Starting Celery worker"
poetry run watchfiles \
  --filter python \
  --ignore-path .venv \
  --ignore-path /wes/tmp \
  --target-type command \
  "celery -A bento_wes.celery worker --loglevel=${celery_log_level} --pool=solo" \
  /wes \
& CELERY_PID=$!

# ---------- Graceful shutdown ----------
terminate() {
  echo "[bento_wes] [entrypoint-dev] Terminating..."
  if kill -0 "${CELERY_PID}" 2>/dev/null; then
    kill -TERM "${CELERY_PID}" || true
    wait "${CELERY_PID}" || true
  fi
  exit 143
}
trap terminate TERM INT

# ---------- ASGI server (Uvicorn --factory + debugpy) ----------
echo "[bento_wes] [entrypoint-dev] Starting Uvicorn (factory: ${APP_FACTORY}, reload enabled)"
exec poetry run python -Xfrozen_modules=off -m debugpy --listen "0.0.0.0:${DEBUGGER_PORT}" -m uvicorn \
  "${APP_FACTORY}" \
  --factory \
  --host "${HOST}" \
  --port "${INTERNAL_PORT}" \
  --workers 1 \
  --reload \
  --log-level "${uvicorn_log_level}" \
  --timeout-keep-alive 65 \
  --proxy-headers \
  ${UVICORN_EXTRA}
