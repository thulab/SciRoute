#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=service_common.sh
source "${SCRIPT_DIR}/service_common.sh"

main() {
  local root host port
  root="$(resolve_mount_point)"
  host="$(resolve_web_host)"
  port="$(resolve_web_port)"
  if [[ $# -gt 0 && "$1" != -* ]]; then
    root="$1"
    shift
  fi

  ensure_executable "$WEB_BIN" "start_web"
  ensure_runtime_dirs

  if pid_is_running "$WEB_PID_FILE"; then
    echo "[start_web] Existing process pid $(read_pid "$WEB_PID_FILE")"
    return 0
  fi

  local log_file
  log_file="$(resolve_log_dir)/iotdb_fs_web.out"
  export IOTDB_CONFIG_FILE="$CONF_FILE"
  echo "[start_web] Starting $WEB_BIN --host $host --port $port --root $root"
  nohup "$WEB_BIN" --host "$host" --port "$port" --root "$root" "$@" \
    >>"$log_file" 2>&1 &
  local pid=$!
  echo "$pid" >"$WEB_PID_FILE"

  local probe_host="$host"
  if [[ "$probe_host" == "0.0.0.0" || "$probe_host" == "::" ]]; then
    probe_host="127.0.0.1"
  fi

  if wait_for_http "$probe_host" "$port" "$pid"; then
    echo "[start_web] Started pid $pid"
    echo "[start_web] URL: http://${host}:${port}/"
  else
    echo "[start_web] Failed to start; last logs:" >&2
    tail -n 80 "$log_file" >&2 || true
    exit 1
  fi
}

main "$@"
