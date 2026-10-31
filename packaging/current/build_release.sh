#!/usr/bin/env bash
# Build and package a release for the current host system.

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/packaging/scripts/package_common.sh"

PROJECT_SLUG="${PROJECT_SLUG:-iotdb-fs-c}"
PKG_VERSION="${PKG_VERSION:-$(read_project_version "${ROOT_DIR}")}"
PLATFORM="${PLATFORM:-$(detect_current_platform)}"
ARCH="${ARCH:-$(normalize_arch "$(uname -m)")}"
LINKAGE="${LINKAGE:-native}"
PKG_NAME="${PKG_NAME:-$(make_package_name "${PROJECT_SLUG}" "${PKG_VERSION}" "${PLATFORM}" "${ARCH}" "${LINKAGE}")}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
JOBS="${JOBS:-$(nproc)}"
IOTDBFS_STATIC_WEB="${IOTDBFS_STATIC_WEB:-ON}"

DIST_ROOT="${ROOT_DIR}/build/dist"
DIST_DIR="${DIST_ROOT}/${PKG_NAME}"
BUILD_DIR="${BUILD_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/iotdb-fs-c-current-build.XXXXXX")}"
KEEP_BUILD_DIR="${KEEP_BUILD_DIR:-0}"

log() { printf '\e[1;32m[current-release]\e[0m %s\n' "$*"; }
die() { printf '\e[1;31m[current-release][ERROR]\e[0m %s\n' "$*" >&2; exit 1; }

cleanup() {
    if [[ "${KEEP_BUILD_DIR}" != "1" && -n "${BUILD_DIR:-}" && "${BUILD_DIR}" == "${TMPDIR:-/tmp}"/iotdb-fs-c-current-build.* ]]; then
        rm -rf "${BUILD_DIR}"
    fi
}
trap cleanup EXIT

ensure_dirs() {
    mkdir -p "${DIST_ROOT}"
    rm -rf "${DIST_DIR}"
    mkdir -p "${DIST_DIR}/bin" "${DIST_DIR}/resources" "${DIST_DIR}/scripts" "${DIST_DIR}/logs"
}

do_build() {
    mkdir -p "${BUILD_DIR}/.ccache" "${BUILD_DIR}/.ccache-tmp"
    export CCACHE_DIR="${CCACHE_DIR:-${BUILD_DIR}/.ccache}"
    export CCACHE_TEMPDIR="${CCACHE_TEMPDIR:-${BUILD_DIR}/.ccache-tmp}"

    log "Configuring current-system build in ${BUILD_DIR}"
    cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" \
        -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
        -DBUILD_TESTS=OFF \
        -DIOTDBFS_STATIC_WEB="${IOTDBFS_STATIC_WEB}"

    log "Building release binaries"
    cmake --build "${BUILD_DIR}" --target iotdb_fs iotdb_fs_web grib_client -j "${JOBS}"
}

copy_binary() {
    local name="$1"
    local src="${BUILD_DIR}/bin/${name}"
    [[ -x "${src}" ]] || die "Expected binary not found: ${src}"
    install -m 0755 "${src}" "${DIST_DIR}/bin/${name}"
}

copy_hdf5_notices() {
    # CMake resolves the exact shared-library dependency; retain its upstream
    # notices even while the H02 driver has no registered filesystem adapter.
    local config_dir prefix notice
    config_dir="$(sed -n 's/^HDF5_DIR:[^=]*=//p' "${BUILD_DIR}/CMakeCache.txt" | head -n1)"
    [[ -n "${config_dir}" ]] || die "Build did not record the locked HDF5_DIR"
    prefix="$(cd "${config_dir}/../../.." && pwd)"
    mkdir -p "${DIST_DIR}/share/doc/hdf5"
    for notice in COPYING COPYING_LBNL_HDF5; do
        [[ -f "${prefix}/share/doc/hdf5/${notice}" ]] || die "Missing HDF5 notice: ${notice}"
        cp "${prefix}/share/doc/hdf5/${notice}" "${DIST_DIR}/share/doc/hdf5/"
    done
}

do_package() {
    log "Assembling package ${PKG_NAME}"
    copy_binary iotdb_fs
    copy_binary iotdb_fs_web
    copy_binary grib_client
    copy_hdf5_notices

    cp "${ROOT_DIR}/resources/config.properties" "${DIST_DIR}/resources/"
    cp "${ROOT_DIR}/packaging/runtime/"*.sh "${DIST_DIR}/scripts/"
    if [[ -d "${ROOT_DIR}/third_party/eccodes-static/share/eccodes" ]]; then
        mkdir -p "${DIST_DIR}/share"
        cp -a "${ROOT_DIR}/third_party/eccodes-static/share/eccodes" "${DIST_DIR}/share/"
    fi

    cat > "${DIST_DIR}/README.md" << EOF
# IoTDB-FS-C Release (${PKG_VERSION})
Target: ${PLATFORM} (${ARCH}, ${LINKAGE})

## Quick Start
1. Extract: tar -xf ${PKG_NAME}.tar.gz
2. Configure: Edit resources/config.properties
3. Self-Check: ./scripts/self_check.sh
4. Launch FS: ./scripts/start_fs.sh
5. Launch Web: ./scripts/start_web.sh
6. Launch both: ./scripts/start_all.sh
7. Stop both: ./scripts/stop_all.sh

## Web Explorer
Run: ./scripts/start_web.sh

## GRIB Client
Run: ./bin/grib_client --help

## Native shared dependencies
This package uses host-provided shared libraries. Install the locked HDF5
1.14.4 thread-safe serial C library (with DEFLATE), along with the other build
runtime dependencies. HDF5 notices are retained in share/doc/hdf5. The bounded HDF5 source driver, ingestion and query adapters are registered.
EOF

    chmod +x "${DIST_DIR}/scripts/"*.sh

    (
        cd "${DIST_ROOT}"
        tar -czf "${PKG_NAME}.tar.gz" "${PKG_NAME}"
    )
    log "Package ready: build/dist/${PKG_NAME}.tar.gz"
}

main() {
    ensure_dirs
    do_build
    do_package
}

main "$@"
