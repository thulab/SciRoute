#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
export PATH="$PWD/experiments/single_file/work/session-20261012/venv/bin:$PATH"
exec env -u LD_LIBRARY_PATH -u LD_PRELOAD PYTHONDONTWRITEBYTECODE=1 python -u -m experiments.fixed_matrix.grib_render "$@"
