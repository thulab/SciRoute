#!/usr/bin/env bash
# Verify the release package built for the current host system.

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/packaging/scripts/package_common.sh"

PROJECT_SLUG="${PROJECT_SLUG:-iotdb-fs-c}"
PKG_VERSION="${PKG_VERSION:-$(read_project_version "${ROOT_DIR}")}"
PLATFORM="${PLATFORM:-$(detect_current_platform)}"
ARCH="${ARCH:-$(normalize_arch "$(uname -m)")}"
LINKAGE="${LINKAGE:-native}"
PKG_NAME="${PKG_NAME:-$(make_package_name "${PROJECT_SLUG}" "${PKG_VERSION}" "${PLATFORM}" "${ARCH}" "${LINKAGE}")}"
DIST_DIR="${ROOT_DIR}/build/dist/${PKG_NAME}"
REQUIRE_STATIC="${REQUIRE_STATIC:-0}"

BINARIES=(
    "bin/iotdb_fs"
    "bin/iotdb_fs_web"
    "bin/grib_client"
)

REQUIRED_FILES=(
    "bin/iotdb_fs"
    "bin/iotdb_fs_web"
    "bin/grib_client"
    "resources/config.properties"
    "scripts/start.sh"
    "scripts/stop.sh"
    "scripts/self_check.sh"
    "README.md"
)

log() { printf '\e[1;32m[current-verify]\e[0m %s\n' "$*"; }
error() { printf '\e[1;31m[current-verify][ERROR]\e[0m %s\n' "$*" >&2; exit 1; }

[[ -d "${DIST_DIR}" ]] || error "Package directory not found: ${DIST_DIR}. Please run packaging/current/build_release.sh first."

log "Verifying ${PKG_NAME}"

for f in "${REQUIRED_FILES[@]}"; do
    [[ -f "${DIST_DIR}/${f}" ]] || error "Missing required file: ${DIST_DIR}/${f}"
done
log "Package layout is complete."

for bin in "${BINARIES[@]}"; do
    bin_path="${DIST_DIR}/${bin}"
    [[ -x "${bin_path}" ]] || error "Binary not executable: ${bin_path}"

    if [[ "${REQUIRE_STATIC}" == "1" ]]; then
        ldd_out=$(ldd "${bin_path}" 2>&1 || true)
        if [[ ! "${ldd_out}" =~ "not a dynamic executable" ]]; then
            echo "${ldd_out}"
            error "${bin} is not fully static."
        fi
    else
        ldd_out=$(ldd "${bin_path}" 2>&1 || true)
        if [[ "${ldd_out}" =~ "not found" ]]; then
            echo "${ldd_out}"
            error "${bin} has missing runtime dependencies."
        fi
    fi

    if "${bin_path}" --help >/dev/null 2>&1; then
        log "${bin} executed successfully with --help."
    else
        log "${bin} returned non-zero for --help, but dependency loading passed."
    fi
done

[[ -f "${ROOT_DIR}/build/dist/${PKG_NAME}.tar.gz" ]] || error "Archive not found: ${ROOT_DIR}/build/dist/${PKG_NAME}.tar.gz"
log "Release package verified: build/dist/${PKG_NAME}.tar.gz"
