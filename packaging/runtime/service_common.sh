#!/usr/bin/env bash
# Shared helpers for IoTDB-FS release service scripts.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONF_FILE="${IOTDB_CONFIG_FILE:-${ROOT_DIR}/resources/config.properties}"
FS_BIN="${IOTDB_FS_BIN:-${ROOT_DIR}/bin/iotdb_fs}"
WEB_BIN="${IOTDB_FS_WEB_BIN:-${ROOT_DIR}/bin/iotdb_fs_web}"
RUN_DIR="${IOTDB_FS_RUN_DIR:-${ROOT_DIR}/run}"
FS_PID_FILE="${RUN_DIR}/iotdb_fs.pid"
WEB_PID_FILE="${RUN_DIR}/iotdb_fs_web.pid"

read_prop() {
  local key="$1"
  [[ -f "$CONF_FILE" ]] || return 0
  awk -F'=' -v key="$key" '
    /^[[:space:]]*(#|;|\/\/)/ {next}
    NF < 2 {next}
    {
      k=$1; gsub(/^[[:space:]]+|[[:space:]]+$/, "", k);
      if (k==key) {
        $1=""; sub(/^=/, "");
        value=$0;
        gsub(/[[:space:]]*(#|;|\/\/).*$/, "", value);
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", value);
        print value;
        exit;
      }
    }
  ' "$CONF_FILE"
}

resolve_path() {
  local value="$1"
  local fallback="$2"
  [[ -z "$value" ]] && value="$fallback"
  if [[ "$value" = /* ]]; then
    printf '%s\n' "$value"
  else
    printf '%s\n' "${ROOT_DIR}/${value}"
  fi
}

resolve_mount_point() {
  local mp="${IOTDB_MOUNT_POINT:-}"
  [[ -z "$mp" ]] && mp="$(read_prop "mount.point" || true)"
  [[ -z "$mp" ]] && mp="/mnt/dfs"
  printf '%s\n' "$mp"
}

resolve_log_dir() {
  local dir="${IOTDB_LOG_DIR:-}"
  [[ -z "$dir" ]] && dir="$(read_prop "log.dir" || true)"
  resolve_path "$dir" "logs"
}

resolve_web_host() {
  local host="${IOTDB_WEB_HOST:-}"
  [[ -z "$host" ]] && host="$(read_prop "web.host" || true)"
  [[ -z "$host" ]] && host="127.0.0.1"
  printf '%s\n' "$host"
}

resolve_web_port() {
  local port="${IOTDB_WEB_PORT:-}"
  [[ -z "$port" ]] && port="$(read_prop "web.port" || true)"
  [[ -z "$port" ]] && port="8080"
  printf '%s\n' "$port"
}

ensure_runtime_dirs() {
  mkdir -p "$RUN_DIR" "$(resolve_log_dir)"
}

ensure_executable() {
  local path="$1"
  local name="$2"
  if [[ ! -x "$path" ]]; then
    echo "[$name] Executable not found: $path" >&2
    exit 1
  fi
}

pid_is_running() {
  local pid_file="$1"
  [[ -f "$pid_file" ]] || return 1
  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" >/dev/null 2>&1
}

read_pid() {
  local pid_file="$1"
  [[ -f "$pid_file" ]] || return 0
  cat "$pid_file" 2>/dev/null || true
}

is_mounted() {
  local mp="$1"
  if command -v mountpoint >/dev/null 2>&1; then
    mountpoint -q "$mp"
    return
  fi
  awk -v mp="$mp" '$2 == mp {found=1} END {exit found ? 0 : 1}' /proc/mounts
}

wait_for_mount() {
  local mp="$1"
  local pid="$2"
  local i
  for i in $(seq 1 50); do
    if is_mounted "$mp"; then
      return 0
    fi
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      return 1
    fi
    sleep 0.2
  done
  return 1
}

wait_for_http() {
  local host="$1"
  local port="$2"
  local pid="$3"
  local url="http://${host}:${port}/"
  local i
  command -v curl >/dev/null 2>&1 || return 0
  for i in $(seq 1 50); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      return 1
    fi
    sleep 0.2
  done
  return 1
}

terminate_pid_file() {
  local pid_file="$1"
  local label="$2"
  local pid
  pid="$(read_pid "$pid_file")"
  if [[ -z "$pid" ]]; then
    rm -f "$pid_file"
    echo "[$label] No pid file"
    return 0
  fi
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    rm -f "$pid_file"
    echo "[$label] Process $pid is not running"
    return 0
  fi

  echo "[$label] Stopping pid $pid"
  kill "$pid" >/dev/null 2>&1 || true
  local i
  for i in $(seq 1 50); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      rm -f "$pid_file"
      return 0
    fi
    sleep 0.2
  done
  echo "[$label] Process $pid did not exit after SIGTERM" >&2
  return 1
}
