#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=service_common.sh
source "${SCRIPT_DIR}/service_common.sh"

main() {
  if pid_is_running "$WEB_PID_FILE"; then
    terminate_pid_file "$WEB_PID_FILE" "stop_web"
  else
    rm -f "$WEB_PID_FILE"
    echo "[stop_web] Web process is not running"
  fi
}

main "$@"
