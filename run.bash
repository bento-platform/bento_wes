#!/bin/bash

# ---------- Defaults ----------
: "${INTERNAL_PORT:=5000}"                # Container port
: "${HOST:=0.0.0.0}"                      # Bind host
: "${APP_IMPORT:=bento_wes.app_factory:create_app}"      # ASGI app import path (module:var)
: "${WEB_CONCURRENCY:=}"                  # If empty, we auto-calc below
: "${UVICORN_EXTRA:=}"                             # Extra flags, e.g. "--http h11" or "--lifespan on"

echo "[bento_wes] [entrypoint] Starting services"

# ---------- Log levels ----------
celery_log_level="INFO"
uvicorn_log_level="info"
dev_reload_flag=""
if [[ "${BENTO_DEBUG:-}" == "true" || "${BENTO_DEBUG:-}" == "True" || "${BENTO_DEBUG:-}" == "1" ]]; then
  celery_log_level="DEBUG"
  uvicorn_log_level="debug"
  dev_reload_flag="--reload"
fi

# ---------- Celery worker ----------
echo "[bento_wes] [entrypoint] Starting Celery worker"
poetry run celery --app bento_wes.celery worker --loglevel="${celery_log_level}" &
CELERY_PID=$!

# ---------- Worker count (ASGI) ----------
if [[ -z "${WEB_CONCURRENCY}" ]]; then
  # Common heuristic: 2 * CPU + 1
  CPU_COUNT="$(nproc --all || echo 1)"
  WEB_CONCURRENCY="$(( 2 * CPU_COUNT + 1 ))"
fi

# ---------- Graceful shutdown ----------
terminate() {
  echo "[bento_wes] [entrypoint] Terminating..."
  if kill -0 "${CELERY_PID}" 2>/dev/null; then
    kill -TERM "${CELERY_PID}" || true
    wait "${CELERY_PID}" || true
  fi
  exit 143
}
trap terminate TERM INT

# ---------- FastAPI (ASGI) with Uvicorn ----------
echo "[bento_wes] [entrypoint] Starting Uvicorn (factory: ${APP_FACTORY})"
exec poetry run python -Xfrozen_modules=off -m uvicorn "${APP_FACTORY}" \
  --factory \
  --host "${HOST}" \
  --port "${INTERNAL_PORT}" \
  --workers "${WEB_CONCURRENCY}" \
  --log-level "${uvicorn_log_level}" \
  --timeout-keep-alive 65 \
  --proxy-headers \
  ${dev_reload_flag} \
  ${UVICORN_EXTRA}
 
