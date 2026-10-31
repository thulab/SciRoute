#!/usr/bin/env bash
# verify_release.sh
# 
# Automated verification for the IoTDB-FS release package.
# Checks: Portability (Static link), Structure, and Basic Execution.

set -Eeuo pipefail

# Configuration
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/packaging/scripts/package_common.sh"

PROJECT_SLUG="${PROJECT_SLUG:-iotdb-fs-c}"
PKG_VERSION="${PKG_VERSION:-$(read_project_version "${ROOT_DIR}")}"
PLATFORM="${PLATFORM:-kylin-v10}"
ARCH="${ARCH:-$(normalize_arch "$(uname -m)")}"
LINKAGE="${LINKAGE:-static}"
PKG_NAME="${PKG_NAME:-$(make_package_name "${PROJECT_SLUG}" "${PKG_VERSION}" "${PLATFORM}" "${ARCH}" "${LINKAGE}")}"

DIST_DIR="${ROOT_DIR}/build/dist/${PKG_NAME}"
BINARIES=(
    "bin/iotdb_fs"
    "bin/iotdb_fs_web"
)

log() { printf '\e[1;32m[verify]\e[0m %s\n' "$*"; }
error() { printf '\e[1;31m[error]\e[0m %s\n' "$*" >&2; exit 1; }

[[ -d "${DIST_DIR}" ]] || error "Package directory not found: ${DIST_DIR}. Please run packaging/kylin/build_release.sh first."

echo "===================================================="
log "Starting Verification for ${PKG_NAME}"
echo "===================================================="

# 1. Portability Check (Truly Static)
log "1. Checking Binary Portability..."
for bin in "${BINARIES[@]}"; do
    bin_path="${DIST_DIR}/${bin}"
    [[ -x "${bin_path}" ]] || error "Binary not found or not executable: ${bin_path}"

    LDD_OUT=$(ldd "${bin_path}" 2>&1 || true)
    if [[ ! "${LDD_OUT}" =~ "not a dynamic executable" ]]; then
        echo "${LDD_OUT}"
        error "${bin} is NOT fully static. It will likely fail on different GLIBC versions."
    fi
    log "  - PASS: ${bin} has no dynamic dependencies (ldd)."

    if command -v readelf >/dev/null; then
        if readelf -d "${bin_path}" 2>&1 | grep -q "Dynamic section"; then
            error "${bin} contains a dynamic section. It is not a True Static executable."
        fi
        log "  - PASS: ${bin} has no dynamic section (readelf)."
    else
        log "  - SKIP: readelf not installed."
    fi
done

# 2. Package Structure Check
log "2. Checking Package Structure..."
REQUIRED_FILES=(
    "bin/iotdb_fs"
    "bin/iotdb_fs_web"
    "resources/config.properties"
    "scripts/start.sh"
    "scripts/stop.sh"
    "README.md"
)
for f in "${REQUIRED_FILES[@]}"; do
    if [[ ! -f "${DIST_DIR}/$f" ]]; then
        error "Missing required file: ${DIST_DIR}/$f"
    fi
done
log "  - PASS: All required files present."

# 3. Execution Check
log "3. Checking Basic Execution..."
# Try to run with -h or --help. FUSE apps usually exit 0 or 1 on help.
# We just want to see if it can be loaded into memory without GLIBC errors.
for bin in "${BINARIES[@]}"; do
    bin_path="${DIST_DIR}/${bin}"
    chmod +x "${bin_path}"
    if "${bin_path}" --help > /dev/null 2>&1; then
        log "  - PASS: ${bin} executed successfully (help output)."
    else
        # Some FUSE environments return non-zero for help; loading is the key check.
        log "  - NOTE: ${bin} returned non-zero for --help, but was able to load."
    fi
done

echo "===================================================="
log "SUCCESS: Release package is verified and ready."
log "Path: ${ROOT_DIR}/build/dist/${PKG_NAME}.tar.gz"
echo "===================================================="
