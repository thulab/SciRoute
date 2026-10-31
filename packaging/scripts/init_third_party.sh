#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
THIRD_PARTY_DIR="${ROOT_DIR}/third_party"

ECCODES_VERSION="${ECCODES_VERSION:-2.44.0}"
ECCODES_URL="${ECCODES_URL:-https://github.com/ecmwf/eccodes/archive/refs/tags/${ECCODES_VERSION}.tar.gz}"
ECCODES_SRC_DIR="${THIRD_PARTY_DIR}/eccodes-src"
ECCODES_INSTALL_DIR="${THIRD_PARTY_DIR}/eccodes-static"

IOTDB_CLIENT_CPP_SOURCE="${IOTDB_CLIENT_CPP_SOURCE:-/root/auto/iotdb/iotdb-client/client-cpp}"
IOTDB_CLIENT_CPP_DIR="${THIRD_PARTY_DIR}/iotdb-client-cpp-src"
IOTDB_SESSION_STATIC_BUILD_DIR="${IOTDB_CLIENT_CPP_DIR}/target/build/main-static"

FORCE="${FORCE:-0}"
BUILD_ECCODES="${BUILD_ECCODES:-1}"
BUILD_IOTDB_SESSION="${BUILD_IOTDB_SESSION:-1}"

log() {
  printf '[init-third-party] %s\n' "$*"
}

die() {
  printf '[init-third-party][ERROR] %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

ensure_tools() {
  require_cmd cmake
  require_cmd c++
  require_cmd cp
  require_cmd mkdir
  require_cmd perl
  require_cmd tar
  if [[ "${BUILD_ECCODES}" == "1" && ! -f "${ECCODES_INSTALL_DIR}/lib/libeccodes.a" ]]; then
    require_cmd curl
  fi
}

patch_eccodes_pkgconfig() {
  local pc_file="${ECCODES_INSTALL_DIR}/lib/pkgconfig/eccodes.pc"
  [[ -f "${pc_file}" ]] || return 0

  if [[ -f /usr/lib/x86_64-linux-gnu/libopenjp2.a && \
        -f /usr/lib/x86_64-linux-gnu/libaec.a && \
        -f /usr/lib/x86_64-linux-gnu/libm.a ]]; then
    perl -0pi -e \
      's|^libs_private=.*$|libs_private=/usr/lib/x86_64-linux-gnu/libopenjp2.a /usr/lib/x86_64-linux-gnu/libaec.a /usr/lib/x86_64-linux-gnu/libm.a|m' \
      "${pc_file}"
  else
    log "static OpenJPEG/AEC/libm archives were not all found; leaving ${pc_file} unchanged"
  fi
}

init_eccodes() {
  [[ "${BUILD_ECCODES}" == "1" ]] || {
    log "skipping ecCodes initialization"
    return 0
  }

  if [[ "${FORCE}" == "1" ]]; then
    log "removing existing ecCodes third_party directories"
    rm -rf "${ECCODES_SRC_DIR}" "${ECCODES_INSTALL_DIR}"
  fi

  if [[ -f "${ECCODES_INSTALL_DIR}/lib/libeccodes.a" ]]; then
    log "ecCodes static library already exists: ${ECCODES_INSTALL_DIR}/lib/libeccodes.a"
    patch_eccodes_pkgconfig
    return 0
  fi

  mkdir -p "${ECCODES_SRC_DIR}" "${ECCODES_INSTALL_DIR}"

  local tarball="${ECCODES_SRC_DIR}/eccodes-${ECCODES_VERSION}.tar.gz"
  if [[ ! -f "${tarball}" ]]; then
    log "downloading ecCodes ${ECCODES_VERSION}"
    curl -L --fail --output "${tarball}" "${ECCODES_URL}"
  fi

  if [[ ! -d "${ECCODES_SRC_DIR}/eccodes-${ECCODES_VERSION}" ]]; then
    log "extracting ecCodes ${ECCODES_VERSION}"
    tar xf "${tarball}" -C "${ECCODES_SRC_DIR}"
  fi

  log "configuring ecCodes static build"
  cmake -S "${ECCODES_SRC_DIR}/eccodes-${ECCODES_VERSION}" \
    -B "${ECCODES_SRC_DIR}/eccodes-${ECCODES_VERSION}/build-static" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${ECCODES_INSTALL_DIR}" \
    -DBUILD_SHARED_LIBS=OFF \
    -DENABLE_FORTRAN=OFF \
    -DENABLE_NETCDF=OFF \
    -DENABLE_JPG=ON \
    -DENABLE_AEC=ON

  log "building ecCodes static library"
  cmake --build "${ECCODES_SRC_DIR}/eccodes-${ECCODES_VERSION}/build-static" -j

  log "installing ecCodes into ${ECCODES_INSTALL_DIR}"
  cmake --install "${ECCODES_SRC_DIR}/eccodes-${ECCODES_VERSION}/build-static"

  patch_eccodes_pkgconfig
  [[ -f "${ECCODES_INSTALL_DIR}/lib/libeccodes.a" ]] || die "ecCodes static library was not created"
}

patch_iotdb_cmake() {
  local cmake_file="$1"
  [[ -f "${cmake_file}" ]] || die "missing CMake file: ${cmake_file}"

  perl -0pi -e \
    's/IF\(CMAKE_CXX_COMPILER_ID MATCHES "GNU\|Clang" AND NOT MSVC\)\n\s*add_compile_options\(-fsanitize=address -fno-omit-frame-pointer\)\n\s*add_link_options\(-fsanitize=address\)\nENDIF\(\)/OPTION(IOTDB_SESSION_ENABLE_ASAN "Build IoTDB C++ session with AddressSanitizer" OFF)\nIF(IOTDB_SESSION_ENABLE_ASAN AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang" AND NOT MSVC)\n    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)\n    add_link_options(-fsanitize=address)\nENDIF()/s' \
    "${cmake_file}"

  perl -0pi -e \
    's/IF\(MSVC\)\n\s*ADD_LIBRARY\(iotdb_session \$\{SESSION_SRCS\}\)\nELSE\(\)\n\s*ADD_LIBRARY\(iotdb_session SHARED \$\{SESSION_SRCS\}\)\nENDIF\(\)/IF(MSVC)\n    ADD_LIBRARY(iotdb_session \${SESSION_SRCS})\nELSEIF(BUILD_SHARED_LIBS)\n    ADD_LIBRARY(iotdb_session SHARED \${SESSION_SRCS})\nELSE()\n    ADD_LIBRARY(iotdb_session STATIC \${SESSION_SRCS})\nENDIF()/s' \
    "${cmake_file}"
}

init_iotdb_session() {
  [[ "${BUILD_IOTDB_SESSION}" == "1" ]] || {
    log "skipping IoTDB C++ session initialization"
    return 0
  }

  [[ -d "${IOTDB_CLIENT_CPP_SOURCE}" ]] || die "IoTDB C++ client source directory not found: ${IOTDB_CLIENT_CPP_SOURCE}"
  [[ -d "${IOTDB_CLIENT_CPP_SOURCE}/src" ]] || die "missing source dir: ${IOTDB_CLIENT_CPP_SOURCE}/src"
  [[ -d "${IOTDB_CLIENT_CPP_SOURCE}/target/thrift/include" ]] || die "missing generated thrift include dir; build IoTDB client-cpp once first"
  [[ -d "${IOTDB_CLIENT_CPP_SOURCE}/target/thrift/lib" ]] || die "missing thrift lib dir; build IoTDB client-cpp once first"
  [[ -d "${IOTDB_CLIENT_CPP_SOURCE}/target/build/main" ]] || die "missing generated CMake build/main dir; build IoTDB client-cpp once first"

  if [[ "${FORCE}" == "1" ]]; then
    log "removing existing IoTDB C++ client third_party directory"
    rm -rf "${IOTDB_CLIENT_CPP_DIR}"
  fi

  mkdir -p "${IOTDB_CLIENT_CPP_DIR}/target/build" \
    "${IOTDB_CLIENT_CPP_DIR}/target/thrift" \
    "${IOTDB_CLIENT_CPP_DIR}/target/protocols"

  log "copying IoTDB C++ client source into ${IOTDB_CLIENT_CPP_DIR}"
  cp -a "${IOTDB_CLIENT_CPP_SOURCE}/src" "${IOTDB_CLIENT_CPP_DIR}/"
  cp -a "${IOTDB_CLIENT_CPP_SOURCE}/pom.xml" "${IOTDB_CLIENT_CPP_DIR}/"
  cp -a "${IOTDB_CLIENT_CPP_SOURCE}/README.md" "${IOTDB_CLIENT_CPP_DIR}/"
  cp -a "${IOTDB_CLIENT_CPP_SOURCE}/target/thrift/include" "${IOTDB_CLIENT_CPP_DIR}/target/thrift/"
  cp -a "${IOTDB_CLIENT_CPP_SOURCE}/target/thrift/lib" "${IOTDB_CLIENT_CPP_DIR}/target/thrift/"
  cp -a "${IOTDB_CLIENT_CPP_SOURCE}/target/protocols/." "${IOTDB_CLIENT_CPP_DIR}/target/protocols/"

  rm -rf "${IOTDB_CLIENT_CPP_DIR}/target/build/main"
  cp -a "${IOTDB_CLIENT_CPP_SOURCE}/target/build/main" "${IOTDB_CLIENT_CPP_DIR}/target/build/"

  patch_iotdb_cmake "${IOTDB_CLIENT_CPP_DIR}/src/main/CMakeLists.txt"
  patch_iotdb_cmake "${IOTDB_CLIENT_CPP_DIR}/target/build/main/CMakeLists.txt"

  rm -rf "${IOTDB_SESSION_STATIC_BUILD_DIR}"

  log "configuring IoTDB C++ session static build"
  cmake -S "${IOTDB_CLIENT_CPP_DIR}/target/build/main" \
    -B "${IOTDB_SESSION_STATIC_BUILD_DIR}" \
    -DBUILD_SHARED_LIBS=OFF \
    -DIOTDB_SESSION_ENABLE_ASAN=OFF

  log "building libiotdb_session.a"
  cmake --build "${IOTDB_SESSION_STATIC_BUILD_DIR}" -j

  [[ -f "${IOTDB_SESSION_STATIC_BUILD_DIR}/libiotdb_session.a" ]] || die "libiotdb_session.a was not created"
}

main() {
  cd "${ROOT_DIR}"
  mkdir -p "${THIRD_PARTY_DIR}"
  ensure_tools
  init_eccodes
  init_iotdb_session

  log "third_party initialization completed"
  log "ecCodes: ${ECCODES_INSTALL_DIR}/lib/libeccodes.a"
  log "IoTDB session: ${IOTDB_SESSION_STATIC_BUILD_DIR}/libiotdb_session.a"
}

main "$@"
