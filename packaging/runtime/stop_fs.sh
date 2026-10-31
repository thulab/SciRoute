#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=service_common.sh
source "${SCRIPT_DIR}/service_common.sh"

unmount_fs() {
  local mount_point="$1"
  if ! is_mounted "$mount_point"; then
    echo "[stop_fs] Not mounted: $mount_point"
    return 0
  fi
  echo "[stop_fs] Unmounting $mount_point"
  if command -v fusermount3 >/dev/null 2>&1; then
    fusermount3 -u "$mount_point"
  elif command -v fusermount >/dev/null 2>&1; then
    fusermount -u "$mount_point"
  else
    umount "$mount_point"
  fi
}

main() {
  local mount_point
  mount_point="$(resolve_mount_point)"
  if [[ $# -gt 0 && "$1" != -* ]]; then
    mount_point="$1"
    shift
  fi

  unmount_fs "$mount_point"
  if pid_is_running "$FS_PID_FILE"; then
    terminate_pid_file "$FS_PID_FILE" "stop_fs"
  else
    rm -f "$FS_PID_FILE"
  fi
}

main "$@"
