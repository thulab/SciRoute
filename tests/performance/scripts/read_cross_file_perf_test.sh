#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

MOUNT_DIR="${MOUNT_DIR:-/mnt/dfs}"
LOCAL_DIR="${LOCAL_DIR:-/mnt/local}"
ORACLE_DIR="${ORACLE_DIR:-/mnt/oracle/staging_area_2}"
PREFIX="${PREFIX:-paper_check_}"
BASE_FILE_NAME="${BASE_FILE_NAME:-KWBCGFS_CMRA_105_2024031800_GLB_1_2_0P25}"
FILE_INDEXES="${FILE_INDEXES:-0 1 2}"
QUERY_SUFFIX="${QUERY_SUFFIX:-?function=read&sr=72&dtype=f64}"
TASKS="${TASKS:-10}"
CONCURRENCY="${CONCURRENCY:-2}"

READ_PERF_BIN="${READ_PERF_BIN:-${ROOT_DIR}/build/bin/read_perf}"
READ_PERF_ORIGIN_BIN="${READ_PERF_ORIGIN_BIN:-${ROOT_DIR}/build/bin/read_perf_origin}"

read -r -a FILE_INDEX_ARRAY <<< "$FILE_INDEXES"

append_src_args() {
  local dir="$1"
  local -n out_args_ref="$2"
  local idx

  for idx in "${FILE_INDEX_ARRAY[@]}"; do
    out_args_ref+=(
      "--src=${dir}/${PREFIX}${BASE_FILE_NAME}_${idx}.grib2${QUERY_SUFFIX}"
    )
  done
}

run_cross_file_read_perf() {
  local label="$1"
  local bin="$2"
  local dir="$3"
  local -a src_args=()

  append_src_args "$dir" src_args

  echo "$label"
  "$bin" \
    "${src_args[@]}" \
    --tasks="$TASKS" \
    --concurrency="$CONCURRENCY"
}

run_cross_file_read_perf "测试挂载目录跨文件部分数据读取性能" \
  "$READ_PERF_BIN" \
  "$MOUNT_DIR"

run_cross_file_read_perf "测试本地目录跨文件部分数据读取性能" \
  "$READ_PERF_ORIGIN_BIN" \
  "$LOCAL_DIR"

run_cross_file_read_perf "测试Oracle目录跨文件部分数据读取性能" \
  "$READ_PERF_ORIGIN_BIN" \
  "$ORACLE_DIR"
