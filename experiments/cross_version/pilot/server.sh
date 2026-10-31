#!/usr/bin/env bash
set -euo pipefail

# Reuse the multi-file dependencies; never install or rebuild a second runtime.
HERE=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
CROSS=$(cd -- "$HERE/.." && pwd)
BASE=$(cd -- "$CROSS/../.." && pwd)
PY="$BASE/experiments/single_file/work/session-20261012/venv/bin/python"
ENVIRONMENT="$BASE/experiments/multi_file/work/runner-20261013/environment.json"
BASE_WORK="$BASE/experiments/multi_file/work/small-query-20261013"
WRITER_LOCK=/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock
cd -- "$BASE"

# Preserve corpus/report/work arguments relative to cross_version, as before.
cross_path() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s/%s\n' "$CROSS" "$1" ;;
  esac
}

case "${1:-help}" in
  prepare)
    exec env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" -m experiments.cross_version.pilot.corpus prepare \
      --base-work "$BASE_WORK" --output "$(cross_path "${2:-work/pilot-20261015}")"
    ;;
  publish|resume|verify)
    exec env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" -m experiments.cross_version.pilot.publish "$1" \
      --input "$(cross_path "${2:-work/pilot-20261015}")" --report "$(cross_path "${3:-results/pilot-20261015/publication.json}")" \
      --work "$(cross_path "${4:-work/$1-$(date +%s)-$$}")" --environment "$ENVIRONMENT" \
      --writer-lock "$WRITER_LOCK" --source-pid "${IOTDBFS_CONNECTION_SOURCE_PID:-1001546}"
    ;;
  verify-raw)
    exec env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" -m experiments.cross_version.pilot.corpus verify \
      --input "$(cross_path "${2:-work/pilot-20261015}")"
    ;;
  test)
    exec env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" -m unittest experiments.cross_version.pilot.tests.test_corpus -v
    ;;
  *)
    echo 'Usage: bash experiments/cross_version/pilot/server.sh prepare [CORPUS]'
    echo '       bash experiments/cross_version/pilot/server.sh publish|resume|verify [CORPUS [REPORT [NEW_WORK]]]'
    echo '       bash experiments/cross_version/pilot/server.sh verify-raw [CORPUS]'
    echo '       bash experiments/cross_version/pilot/server.sh test'
    ;;
esac
