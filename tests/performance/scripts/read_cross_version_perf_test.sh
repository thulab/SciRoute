#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

MOUNT_DIR="${MOUNT_DIR:-/mnt/dfs}"
LOCAL_DIR="${LOCAL_DIR:-/mnt/local}"
ORACLE_DIR="${ORACLE_DIR:-/mnt/oracle/staging_area_2}"

MOUNT_FILES="${MOUNT_FILES:-.paper_check_KWBCGFS_CMRA_105_2024031800_GLB_1_2_0P25_0_1774609942564.grib2 .paper_check_KWBCGFS_CMRA_105_2024031800_GLB_1_2_0P25_0_1774609988379.grib2 .paper_check_KWBCGFS_CMRA_105_2024031800_GLB_1_2_0P25_0_1774609995659.grib2}"
LOCAL_FILES="${LOCAL_FILES:-paper_check_KWBCGFS_CMRA_105_2024031800_GLB_1_2_0P25_0.grib2 paper_check_KWBCGFS_CMRA_105_2024031800_GLB_1_2_0P25_1.grib2 paper_check_KWBCGFS_CMRA_105_2024031800_GLB_1_2_0P25_2.grib2}"
ORACLE_FILES="${ORACLE_FILES:-$LOCAL_FILES}"

QUERY_SUFFIX="${QUERY_SUFFIX:-?function=read&sr=72&dtype=f64}"
TASKS="${TASKS:-10}"
CONCURRENCY="${CONCURRENCY:-2}"

READ_PERF_BIN="${READ_PERF_BIN:-${ROOT_DIR}/build/bin/read_perf}"
READ_PERF_ORIGIN_BIN="${READ_PERF_ORIGIN_BIN:-${ROOT_DIR}/build/bin/read_perf_origin}"

append_src_args() {
  local dir="$1"
  local files_text="$2"
  local -n out_args_ref="$3"
  local -a files=()
  local file_name

  read -r -a files <<< "$files_text"
  for file_name in "${files[@]}"; do
    out_args_ref+=("--src=${dir}/${file_name}${QUERY_SUFFIX}")
  done
}

run_cross_version_read_perf() {
  local label="$1"
  local bin="$2"
  local dir="$3"
  local files_text="$4"
  local -a src_args=()

  append_src_args "$dir" "$files_text" src_args

  echo "$label"
  "$bin" \
    "${src_args[@]}" \
    --tasks="$TASKS" \
    --concurrency="$CONCURRENCY"
}

run_cross_version_read_perf "测试IoTDB-FS跨版本部分数据读取性能" \
  "$READ_PERF_BIN" \
  "$MOUNT_DIR" \
  "$MOUNT_FILES"

run_cross_version_read_perf "测试Ext4跨文件部分数据读取性能" \
  "$READ_PERF_ORIGIN_BIN" \
  "$LOCAL_DIR" \
  "$LOCAL_FILES"

run_cross_version_read_perf "测试Oracle目录跨文件部分数据读取性能" \
  "$READ_PERF_ORIGIN_BIN" \
  "$ORACLE_DIR" \
  "$ORACLE_FILES"
