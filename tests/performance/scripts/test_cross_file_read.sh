#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "第一次跨文件读取性能测试..."
"${SCRIPT_DIR}/read_cross_file_perf_test.sh"
echo "第二次跨文件读取性能测试..."
"${SCRIPT_DIR}/read_cross_file_perf_test.sh"
echo "第三次跨文件读取性能测试..."
"${SCRIPT_DIR}/read_cross_file_perf_test.sh"
