#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

clean_logs() {
  local logs_dir="$ROOT_DIR/logs"
  if [[ -d "$logs_dir" ]]; then
    find "$logs_dir" -mindepth 1 -delete
    echo "[clean.sh] Cleared logs in $logs_dir"
  else
    echo "[clean.sh] Logs directory $logs_dir does not exist"
  fi
}

clean_build_artifacts() {
  if [[ "${CLEAN_BUILD:-0}" != "0" ]]; then
    rm -rf "$ROOT_DIR/build"
    echo "[clean.sh] Removed build directory"
  fi
}

main() {
  clean_logs
  clean_build_artifacts
}

main "$@"
