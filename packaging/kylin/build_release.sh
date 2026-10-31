#!/usr/bin/env bash
# build_release_kylin.sh
#
# A consolidated script to build, verify, and package IoTDB-FS for Kylin V10.
# Result: A standardized .tar.gz distribution package in build/dist/

set -Eeuo pipefail

# --- Configuration ---
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/packaging/scripts/package_common.sh"

PROJECT_SLUG="${PROJECT_SLUG:-iotdb-fs-c}"
PKG_VERSION="${PKG_VERSION:-$(read_project_version "${ROOT_DIR}")}"
PLATFORM="${PLATFORM:-kylin-v10}"
ARCH="${ARCH:-$(normalize_arch "$(uname -m)")}"
LINKAGE="${LINKAGE:-static}"
PKG_NAME="${PKG_NAME:-$(make_package_name "${PROJECT_SLUG}" "${PKG_VERSION}" "${PLATFORM}" "${ARCH}" "${LINKAGE}")}"

THIRD_PARTY_DIR="${ROOT_DIR}/third_party"
DIST_ROOT="${ROOT_DIR}/build/dist"
DIST_DIR="${DIST_ROOT}/${PKG_NAME}"
OUTPUT_BIN_NAME="iotdb_fs"
WEB_OUTPUT_BIN_NAME="iotdb_fs_web"

# Dependency Versions/Paths
FUSE_VERSION="2.9.9"
FUSE_SRC_DIR="${THIRD_PARTY_DIR}/fuse-${FUSE_VERSION}"
FUSE_INSTALL_DIR="${THIRD_PARTY_DIR}/fuse-static"
ECCODES_DIR="${THIRD_PARTY_DIR}/eccodes-static"
IOTDB_CLIENT_CPP_DIR="${THIRD_PARTY_DIR}/iotdb-client-cpp-src"
IOTDB_SESSION_BUILD_DIR="${IOTDB_CLIENT_CPP_DIR}/target/build/main-static"
IOTDB_GENERATED_INCLUDE_DIR="${IOTDB_CLIENT_CPP_DIR}/target/build/main/generated-sources-cpp"
THRIFT_DIR="${IOTDB_CLIENT_CPP_DIR}/target/thrift"

# --- Logging Helpers ---
log() { printf '\e[1;32m[release-build]\e[0m %s\n' "$*"; }
warn() { printf '\e[1;33m[release-build][WARN]\e[0m %s\n' "$*" >&2; }
die() { printf '\e[1;31m[release-build][ERROR]\e[0m %s\n' "$*" >&2; exit 1; }

ensure_dirs() {
    mkdir -p "${THIRD_PARTY_DIR}" "${DIST_ROOT}"
    rm -rf "${DIST_DIR}"
    mkdir -p "${DIST_DIR}/bin" "${DIST_DIR}/resources" "${DIST_DIR}/scripts" "${DIST_DIR}/logs"
}

# 1. Build FUSE static library (with naming conflict patch)
build_fuse_static() {
    if [[ -f "${FUSE_INSTALL_DIR}/lib/libfuse.a" ]]; then
        log "FUSE static library already exists."
        return 0
    fi

    log "Building FUSE ${FUSE_VERSION} static library..."
    local tarball="${THIRD_PARTY_DIR}/fuse-${FUSE_VERSION}.tar.gz"
    if [[ ! -f "${tarball}" ]]; then
        curl -L "https://github.com/libfuse/libfuse/releases/download/fuse-${FUSE_VERSION}/fuse-${FUSE_VERSION}.tar.gz" -o "${tarball}"
    fi
    
    rm -rf "${FUSE_SRC_DIR}"
    tar -xf "${tarball}" -C "${THIRD_PARTY_DIR}"

    log "Patching FUSE closefrom conflict..."
    sed -i 's/static int closefrom/static int fuse_closefrom/g' "${FUSE_SRC_DIR}/util/ulockmgr_server.c"
    sed -i 's/closefrom(5)/fuse_closefrom(5)/g' "${FUSE_SRC_DIR}/util/ulockmgr_server.c"

    cd "${FUSE_SRC_DIR}"
    ./configure --prefix="${FUSE_INSTALL_DIR}" --enable-static --disable-shared
    make -j$(nproc)
    make install
    log "FUSE static library ready."
}

# 2. Build other dependencies
init_deps() {
    log "Initializing ecCodes and IoTDB session..."
    export BUILD_ECCODES=1
    export BUILD_IOTDB_SESSION=1
    "${ROOT_DIR}/packaging/scripts/init_third_party.sh"
}

# 3. Final Static Compilation
do_build() {
    log "Starting True Static compilation..."
    
    local sources=(
        "${ROOT_DIR}/src/main.cpp"
        "${ROOT_DIR}/src/CrossFileClient.cpp"
        "${ROOT_DIR}/src/formats/grib/EcCodesRuntime.cpp"
        "${ROOT_DIR}/src/formats/grib/GribExtensionService.cpp"
        "${ROOT_DIR}/src/formats/grib/GribWriteStreamer.cpp"
        "${ROOT_DIR}/src/InodeCache.cpp"
        "${ROOT_DIR}/src/IoTDBClient.cpp"
        "${ROOT_DIR}/src/IoTDBConnector.cpp"
        "${ROOT_DIR}/src/SemanticGrepClient.cpp"
        "${ROOT_DIR}/src/formats/grib/gribUtils.cpp"
        "${ROOT_DIR}/src/Logging.cpp"
        "${ROOT_DIR}/src/formats/grib/GribFileWriter.cpp"
    )

    local filesystem_lib=""
    [[ $(g++ -dumpversion | cut -d. -f1) -lt 9 ]] && filesystem_lib="-lstdc++fs"

    local eccodes_deps=""
    for p in /usr/lib/x86_64-linux-gnu /usr/lib64 /usr/lib; do
        [[ -f "${p}/libopenjp2.a" ]] && eccodes_deps="${eccodes_deps} ${p}/libopenjp2.a"
        [[ -f "${p}/libaec.a" ]] && eccodes_deps="${eccodes_deps} ${p}/libaec.a"
    done

    local sys_libs=""
    for lib in libz.a libssl.a libcrypto.a libm.a libdl.a librt.a; do
        for p in /usr/lib/x86_64-linux-gnu /usr/lib64 /usr/lib; do
            if [[ -f "${p}/${lib}" ]]; then sys_libs="${sys_libs} ${p}/${lib}"; break; fi
        done
    done

    local output_bin="${DIST_DIR}/bin/${OUTPUT_BIN_NAME}"
    local web_output_bin="${DIST_DIR}/bin/${WEB_OUTPUT_BIN_NAME}"

    log "Building static ${OUTPUT_BIN_NAME}..."
    g++ -std=c++17 -O2 -static -static-libstdc++ -static-libgcc \
        -D_FILE_OFFSET_BITS=64 \
        "${sources[@]}" \
        -I"${ROOT_DIR}/include" \
        -I"${FUSE_INSTALL_DIR}/include" \
        -I"${ECCODES_DIR}/include" \
        -I"${IOTDB_GENERATED_INCLUDE_DIR}" \
        -I"${THRIFT_DIR}/include" \
        -L"${FUSE_INSTALL_DIR}/lib" \
        -L"${ECCODES_DIR}/lib" \
        -L"${IOTDB_SESSION_BUILD_DIR}" \
        -L"${THRIFT_DIR}/lib" \
        -Wl,--start-group -lfuse -leccodes -liotdb_session -lthrift ${eccodes_deps} ${sys_libs} -Wl,--end-group \
        ${filesystem_lib} -lpthread \
        -o "${output_bin}"

    strip "${output_bin}"
    log "Build successful: ${output_bin}"

    log "Building static ${WEB_OUTPUT_BIN_NAME}..."
    g++ -std=c++17 -O2 -static -static-libstdc++ -static-libgcc \
        -D_FILE_OFFSET_BITS=64 \
        "${ROOT_DIR}/src/iotdb_fs_web.cpp" \
        -I"${ROOT_DIR}/include" \
        ${filesystem_lib} -lpthread \
        -o "${web_output_bin}"

    strip "${web_output_bin}"
    log "Build successful: ${web_output_bin}"
}

# 4. Verification
do_verify() {
    local bins=(
        "${DIST_DIR}/bin/${OUTPUT_BIN_NAME}"
        "${DIST_DIR}/bin/${WEB_OUTPUT_BIN_NAME}"
    )
    log "Verifying portability..."
    
    local bin
    for bin in "${bins[@]}"; do
        local ldd_out
        ldd_out=$(ldd "${bin}" 2>&1 || true)
        if [[ ! "${ldd_out}" =~ "not a dynamic executable" ]]; then
            die "$(basename "${bin}") is NOT fully static! Output: ${ldd_out}"
        fi

        if command -v readelf >/dev/null; then
            if ! readelf -d "${bin}" 2>&1 | grep -q "There is no dynamic section"; then
                die "$(basename "${bin}") contains a dynamic section!"
            fi
        fi
        log "  - PASS: $(basename "${bin}") is static."
    done
    log "Verification passed."
}

# 5. Packaging
do_package() {
    log "Finalizing distribution package..."
    
    # Copy config and self-check
    cp "${ROOT_DIR}/resources/config.properties" "${DIST_DIR}/resources/"
    cp "${ROOT_DIR}/packaging/runtime/"*.sh "${DIST_DIR}/scripts/"
    mkdir -p "${DIST_DIR}/share"
    cp -a "${ECCODES_DIR}/share/eccodes" "${DIST_DIR}/share/"

    # Generate README
    cat > "${DIST_DIR}/README.md" << EOF
# IoTDB-FS-C Release (${PKG_VERSION})
Target: ${PLATFORM} (${ARCH}, ${LINKAGE})

## Quick Start
1. Transfer and extract: tar -xf ${PKG_NAME}.tar.gz
2. Configure: Edit resources/config.properties (Use IP for iotdb.host)
3. Self-Check: ./scripts/self_check.sh (Verify environment & IoTDB connectivity without mounting)
4. Launch FS: ./scripts/start_fs.sh
5. Launch Web: ./scripts/start_web.sh
6. Launch both: ./scripts/start_all.sh
7. Stop both: ./scripts/stop_all.sh

## Web Explorer
Run: ./scripts/start_web.sh
EOF

    chmod +x "${DIST_DIR}/scripts/"*.sh
    
    cd "${DIST_ROOT}"
    tar -czf "${PKG_NAME}.tar.gz" "${PKG_NAME}"
    log "Package ready: build/dist/${PKG_NAME}.tar.gz"
}

main() {
    ensure_dirs
    build_fuse_static
    init_deps
    do_build
    do_verify
    do_package
    log "All steps completed successfully."
}

main "$@"
