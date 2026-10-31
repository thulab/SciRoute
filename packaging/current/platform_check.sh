#!/usr/bin/env bash

# Verify platform-specific configuration without claiming target-host runtime validation.
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PLATFORM=""
STATIC_ONLY=0
RUNTIME=0

usage() {
  cat <<'EOF'
Usage: platform_check.sh --platform macos|wsl2 [--root PATH] [--static|--runtime]

  --static   Check repository configuration and resources only (default).
  --runtime  Also require that the current host matches the selected platform;
             this checks prerequisites but does not run a full build.
EOF
}

die() { printf '[platform-check][ERROR] %s\n' "$*" >&2; exit 1; }
require_file() { [[ -f "$1" ]] || die "required file is missing: $1"; }
require_executable() { [[ -x "$1" ]] || die "required executable is missing: $1"; }
contains() {
  local needle="$1" file="$2"
  grep -Fq -- "$needle" "$file" || die "${file} does not contain required configuration: ${needle}"
}

check_common() {
  local cmake_file="${ROOT_DIR}/CMakeLists.txt"
  require_file "$cmake_file"
  require_executable "${ROOT_DIR}/packaging/current/build_release.sh"
  require_executable "${ROOT_DIR}/packaging/current/verify_release.sh"
  require_file "${ROOT_DIR}/packaging/scripts/package_common.sh"
  contains 'pkg_check_modules(FUSE REQUIRED fuse)' "$cmake_file"
  contains 'pkg_check_modules(ECCODES REQUIRED eccodes)' "$cmake_file"
  contains 'BUILD_TESTS=OFF' "${ROOT_DIR}/packaging/current/build_release.sh"
}

check_macos() {
  local cmake_file="${ROOT_DIR}/CMakeLists.txt"
  local schema_file="${ROOT_DIR}/mac/schema.xml"
  require_file "$schema_file"
  contains 'if(APPLE)' "$cmake_file"
  contains 'find_package(Boost REQUIRED COMPONENTS date_time)' "$cmake_file"
  contains 'install(FILES mac/schema.xml DESTINATION share/mdimport)' "$cmake_file"
  for attr in com_iotdbfs_shortName com_iotdbfs_globalDomain com_iotdbfs_numberOfDataPoints; do
    grep -Fq "name=\"${attr}\"" "$schema_file" || die "mac/schema.xml is missing Finder attribute ${attr}"
  done
}

check_wsl2() {
  local cmake_file="${ROOT_DIR}/CMakeLists.txt"
  contains 'Native Windows builds are not supported yet; use WSL2/Linux' "$cmake_file"
  contains 'if(CMAKE_SYSTEM_NAME STREQUAL "Windows")' "$cmake_file"
  require_file "${ROOT_DIR}/packaging/runtime/self_check.sh"
  require_file "${ROOT_DIR}/tests/integration/generic_fs_test.cpp"
}

check_runtime_host() {
  case "$PLATFORM" in
    macos)
      [[ "$(uname -s)" == "Darwin" ]] || die "--runtime macos requires a Darwin host (static checks remain available here)"
      command -v pkg-config >/dev/null 2>&1 || die "pkg-config is required for macOS runtime checks"
      ;;
    wsl2)
      [[ "$(uname -s)" == "Linux" ]] || die "--runtime wsl2 requires a Linux host"
      grep -Eiq '(microsoft|wsl)' /proc/version 2>/dev/null || die "current Linux host is not identified as WSL2"
      command -v pkg-config >/dev/null 2>&1 || die "pkg-config is required for WSL2 runtime checks"
      ;;
  esac
}

while (($#)); do
  case "$1" in
    --platform)
      (($# >= 2)) || die "--platform requires a value"
      PLATFORM="$2"
      shift 2
      ;;
    --root)
      (($# >= 2)) || die "--root requires a path"
      ROOT_DIR="$(cd "$2" && pwd)"
      shift 2
      ;;
    --static) STATIC_ONLY=1; shift ;;
    --runtime) RUNTIME=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

[[ -n "$PLATFORM" ]] || { usage >&2; die "--platform is required"; }
[[ "$PLATFORM" == "macos" || "$PLATFORM" == "wsl2" ]] || die "unsupported platform: $PLATFORM"
if ((STATIC_ONLY && RUNTIME)); then die "--static and --runtime are mutually exclusive"; fi

check_common
case "$PLATFORM" in
  macos) check_macos ;;
  wsl2) check_wsl2 ;;
esac

if ((RUNTIME)); then
  check_runtime_host
  printf '%s runtime prerequisite checks passed; full build and mount validation remain to be run on the target host.\n' "$PLATFORM"
else
  printf '%s static checks passed; runtime validation was not performed.\n' "$PLATFORM"
fi
