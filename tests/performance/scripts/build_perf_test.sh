#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
BUILD_BIN_DIR="${ROOT_DIR}/build/bin"

echo "清理 /mnt/ 目录下的文件..."
"${SCRIPT_DIR}/cleanAndRestart.sh"
rm -rf /mnt/local/*
rm -rf /mnt/oracle/staging_area_2/*
mkdir -p "$BUILD_BIN_DIR"

echo "编译写入性能测试程序..."
g++ -std=c++17 -O2 -pthread \
  "${ROOT_DIR}/tests/performance/write_perf.cpp" \
  -I"${ROOT_DIR}/include" \
  -o "${BUILD_BIN_DIR}/write_perf"

echo "编译读取性能测试程序..."
g++ -std=c++17 -O2 -pthread \
  "${ROOT_DIR}/tests/performance/read_perf.cpp" \
  -I"${ROOT_DIR}/include" \
  -o "${BUILD_BIN_DIR}/read_perf"

echo "编译原始读取性能测试程序..."
g++ -std=c++17 -O2 -pthread \
  "${ROOT_DIR}/tests/performance/read_perf_origin.cpp" \
  "${ROOT_DIR}/src/formats/grib/gribUtils.cpp" \
  "${ROOT_DIR}/src/Logging.cpp" \
  -I"${ROOT_DIR}/include" \
  $(pkg-config --cflags eccodes) \
  $(pkg-config --libs eccodes) \
  -o "${BUILD_BIN_DIR}/read_perf_origin"
