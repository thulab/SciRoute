#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=service_common.sh
source "${SCRIPT_DIR}/service_common.sh"

main() {
  local mount_point
  mount_point="$(resolve_mount_point)"
  if [[ $# -gt 0 && "$1" != -* ]]; then
    mount_point="$1"
    shift
  fi

  ensure_executable "$FS_BIN" "start_fs"
  ensure_runtime_dirs
  mkdir -p "$mount_point"

  if is_mounted "$mount_point"; then
    echo "[start_fs] Already mounted: $mount_point"
    return 0
  fi
  if pid_is_running "$FS_PID_FILE"; then
    echo "[start_fs] Existing process pid $(read_pid "$FS_PID_FILE")"
    return 0
  fi

  local log_file
  log_file="$(resolve_log_dir)/iotdb_fs.out"
  export IOTDB_CONFIG_FILE="$CONF_FILE"
  echo "[start_fs] Starting $FS_BIN -> $mount_point"
  nohup "$FS_BIN" "$mount_point" "$@" >>"$log_file" 2>&1 &
  local pid=$!
  echo "$pid" >"$FS_PID_FILE"

  if wait_for_mount "$mount_point" "$pid"; then
    echo "[start_fs] Started pid $pid"
  else
    echo "[start_fs] Failed to start; last logs:" >&2
    tail -n 80 "$log_file" >&2 || true
    exit 1
  fi
}

main "$@"
