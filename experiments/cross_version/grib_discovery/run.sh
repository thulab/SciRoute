#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
exec env -u LD_LIBRARY_PATH -u LD_PRELOAD PYTHONDONTWRITEBYTECODE=1 python3 -u -m experiments.fixed_matrix.grib_catalog "$@"
