#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
BACKEND_DIR="$PROJECT_DIR/backend"
FRONTEND_DIR="$PROJECT_DIR/frontend"
VENV_DIR="$BACKEND_DIR/.venv"
PYTHON="$VENV_DIR/bin/python"
STATE_DIR="$PROJECT_DIR/.engine"
BACKEND_PID_FILE="$STATE_DIR/backend.pid"
FRONTEND_PID_FILE="$STATE_DIR/frontend.pid"
BACKEND_LOG="$STATE_DIR/backend.log"
FRONTEND_LOG="$STATE_DIR/frontend.log"

# Project-specific configuration, verified from dev.sh and backend/app/main.py.
SERVICE_KIND="web"
BACKEND_HOST="${ENGINE_BACKEND_HOST:-127.0.0.1}"
BACKEND_PORT="${ENGINE_BACKEND_PORT:-3018}"
FRONTEND_HOST="${ENGINE_FRONTEND_HOST:-127.0.0.1}"
FRONTEND_PORT="${ENGINE_FRONTEND_PORT:-3011}"
HEALTHCHECK_URL=""

usage() {
  cat <<'EOF'
Usage: ./engine.sh {setup|start|stop|restart|status}

  setup    Create backend/.venv with uv and install frontend dependencies.
  start    Start the FastAPI backend and Vite frontend in the background.
  stop     Stop only services recorded by this project.
  restart  Stop, then start both services.
  status   Show both service states, IPs, ports, and log locations.
EOF
}

is_running() {
  local pid_file="$1"
  [[ -s "$pid_file" ]] || return 1
  local pid
  pid="$(<"$pid_file")"
  [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null
}

clear_stale_pid() {
  local pid_file="$1"
  if [[ -e "$pid_file" ]] && ! is_running "$pid_file"; then
    rm -f -- "$pid_file"
  fi
}

listener_pid() {
  local port="$1"
  lsof -nP -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | head -n 1 || true
}

setup() {
  command -v uv >/dev/null || { echo 'uv is required.' >&2; return 1; }
  command -v pnpm >/dev/null || { echo 'pnpm is required.' >&2; return 1; }

  (cd "$BACKEND_DIR" && uv sync --frozen)
  [[ -x "$PYTHON" ]] || { echo "Missing backend Python: $PYTHON" >&2; return 1; }
  (cd "$FRONTEND_DIR" && pnpm install)
  printf 'Setup complete. Backend Python: %s\n' "$PYTHON"
}

start_one() {
  local name="$1" pid_file="$2" log_file="$3" port="$4"
  shift 4
  clear_stale_pid "$pid_file"
  if is_running "$pid_file"; then
    printf '%s is already running (PID %s).\n' "$name" "$(<"$pid_file")"
    return 0
  fi
  local occupied_pid
  occupied_pid="$(listener_pid "$port")"
  if [[ -n "$occupied_pid" ]]; then
    echo "$name port $port is already occupied by PID $occupied_pid." >&2
    return 1
  fi

  mkdir -p "$STATE_DIR"
  (
    cd "$PROJECT_DIR"
    setsid nohup "$@" >>"$log_file" 2>&1 < /dev/null &
    printf '%s\n' "$!" >"$pid_file"
  )
  sleep 1
  if ! is_running "$pid_file"; then
    echo "$name failed to start; inspect $log_file" >&2
    return 1
  fi
  printf 'Started %s (PID %s).\n' "$name" "$(<"$pid_file")"
}

start() {
  if [[ ! -x "$PYTHON" || ! -d "$FRONTEND_DIR/node_modules" ]]; then
    echo 'Dependencies are missing; running setup first.'
    setup
  fi

  local backend_args=("$PYTHON" -m uvicorn app.main:app --host "$BACKEND_HOST" --port "$BACKEND_PORT")
  if [[ -f "$PROJECT_DIR/.env" ]]; then
    backend_args+=(--env-file "$PROJECT_DIR/.env")
  fi
  start_one backend "$BACKEND_PID_FILE" "$BACKEND_LOG" "$BACKEND_PORT" "${backend_args[@]}"
  start_one frontend "$FRONTEND_PID_FILE" "$FRONTEND_LOG" "$FRONTEND_PORT" \
    pnpm --dir "$FRONTEND_DIR" dev --host "$FRONTEND_HOST" --port "$FRONTEND_PORT"
  status
}

stop_one() {
  local name="$1" pid_file="$2"
  clear_stale_pid "$pid_file"
  if ! is_running "$pid_file"; then
    printf '%s is not running.\n' "$name"
    return 0
  fi

  local pid
  pid="$(<"$pid_file")"
  kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid"
  for _ in {1..30}; do
    is_running "$pid_file" || break
    sleep 0.1
  done
  if is_running "$pid_file"; then
    kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid"
  fi
  rm -f -- "$pid_file"
  printf 'Stopped %s.\n' "$name"
}

stop() {
  stop_one frontend "$FRONTEND_PID_FILE"
  stop_one backend "$BACKEND_PID_FILE"
}

restart() {
  stop
  start
}

status() {
  clear_stale_pid "$BACKEND_PID_FILE"
  clear_stale_pid "$FRONTEND_PID_FILE"
  if is_running "$BACKEND_PID_FILE"; then
    printf 'backend running (PID %s) [IP: %s, port: %s]\n' "$(<"$BACKEND_PID_FILE")" "$BACKEND_HOST" "$BACKEND_PORT"
  else
    printf 'backend stopped [IP: %s, port: %s]\n' "$BACKEND_HOST" "$BACKEND_PORT"
  fi
  if is_running "$FRONTEND_PID_FILE"; then
    printf 'frontend running (PID %s) [IP: %s, port: %s]\n' "$(<"$FRONTEND_PID_FILE")" "$FRONTEND_HOST" "$FRONTEND_PORT"
  else
    printf 'frontend stopped [IP: %s, port: %s]\n' "$FRONTEND_HOST" "$FRONTEND_PORT"
  fi
  printf 'Logs: %s\n' "$STATE_DIR"
}

case "${1:-}" in
  setup) setup ;;
  start) start ;;
  stop) stop ;;
  restart) restart ;;
  status) status ;;
  -h|--help|help|'') usage ;;
  *) echo "Unknown command: $1" >&2; usage >&2; exit 2 ;;
esac
