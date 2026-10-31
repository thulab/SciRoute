#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

MOUNT_DIR="${MOUNT_DIR:-/mnt/dfs}"
LOCAL_DIR="${LOCAL_DIR:-/mnt/local}"
ORACLE_DIR="${ORACLE_DIR:-/mnt/oracle/staging_area_2}"
PREFIX="${PREFIX:-paper_check_}"
# FILE_NAME="${FILE_NAME:-KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25_0.grib2}"
FILE_NAME="${FILE_NAME:-KWBCGFS_CMRA_105_2024031800_GLB_1_2_0P25_0.grib2}"

# QUERY_SUFFIX="${QUERY_SUFFIX:-?function=read&level=500&dtype=f64}"
# QUERY_SUFFIX="${QUERY_SUFFIX:-?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64}"

QUERY_SUFFIX="${QUERY_SUFFIX:-?function=read&sr=72??function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64}"

FULL_TASKS="${FULL_TASKS:-10}"
FULL_CONCURRENCY="${FULL_CONCURRENCY:-4}"
PARTIAL_TASKS="${PARTIAL_TASKS:-10}"
PARTIAL_CONCURRENCY="${PARTIAL_CONCURRENCY:-2}"

READ_PERF_BIN="${READ_PERF_BIN:-${ROOT_DIR}/build/bin/read_perf}"
READ_PERF_ORIGIN_BIN="${READ_PERF_ORIGIN_BIN:-${ROOT_DIR}/build/bin/read_perf_origin}"

MOUNT_FILE="${MOUNT_DIR}/${PREFIX}${FILE_NAME}"
LOCAL_FILE="${LOCAL_DIR}/${PREFIX}${FILE_NAME}"
ORACLE_FILE="${ORACLE_DIR}/${PREFIX}${FILE_NAME}"
MOUNT_QUERY="${MOUNT_FILE}${QUERY_SUFFIX}"
LOCAL_QUERY="${LOCAL_FILE}${QUERY_SUFFIX}"
ORACLE_QUERY="${ORACLE_FILE}${QUERY_SUFFIX}"

run_read_perf() {
  local label="$1"
  local bin="$2"
  local src="$3"
  local tasks="$4"
  local concurrency="$5"

  echo "$label"
  "$bin" \
    --src="$src" \
    --tasks="$tasks" \
    --concurrency="$concurrency"
}

run_read_perf "测试挂载目录部分数据读取性能" \
  "$READ_PERF_BIN" \
  "$MOUNT_QUERY" \
  "$PARTIAL_TASKS" \
  "$PARTIAL_CONCURRENCY"

run_read_perf "测试本地目录部分数据读取性能" \
  "$READ_PERF_ORIGIN_BIN" \
  "$LOCAL_QUERY" \
  "$PARTIAL_TASKS" \
  "$PARTIAL_CONCURRENCY"

run_read_perf "测试Oracle目录部分文件读取性能" \
  "$READ_PERF_ORIGIN_BIN" \
  "$ORACLE_QUERY" \
  "$PARTIAL_TASKS" \
  "$PARTIAL_CONCURRENCY"
