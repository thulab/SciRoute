#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

SRC_FILE="${SRC_FILE:-/root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_105_2024031800_GLB_1_2_0P25.grib2}"
# SRC_FILE="${SRC_FILE:-/root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2}"
MOUNT_DIR="${MOUNT_DIR:-/mnt/dfs}"
LOCAL_DIR="${LOCAL_DIR:-/mnt/local}"
ORACLE_DIR="${ORACLE_DIR:-/mnt/oracle/staging_area_2}"
TASKS="${TASKS:-3}"
CONCURRENCY="${CONCURRENCY:-1}"
PREFIX="${PREFIX:-paper_check_}"
WRITE_PERF_BIN="${WRITE_PERF_BIN:-${ROOT_DIR}/build/bin/write_perf}"

run_write_perf() {
  local label="$1"
  local target_dir="$2"
  local mode="${3:-cp}"

  echo "$label"
  "$WRITE_PERF_BIN" \
    --src="$SRC_FILE" \
    --target-dir="$target_dir" \
    --tasks="$TASKS" \
    --concurrency="$CONCURRENCY" \
    --prefix="$PREFIX" \
    --mode="$mode"
}

run_write_perf "在挂载目录测试写入性能" "$MOUNT_DIR" "cp"

run_write_perf "在本地目录测试写入性能(强制落盘)" "$LOCAL_DIR" "dd_fsync"

run_write_perf "在 Oracle 目录测试写入性能(强制落盘)" "$ORACLE_DIR" "cp"
