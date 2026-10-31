cmake_minimum_required(VERSION 3.13)

# Reject native Windows before CMake probes the host compiler. This keeps the
# first-phase WSL2 boundary explicit even when no Windows toolchain is present.
if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
  message(FATAL_ERROR "Native Windows builds are not supported yet; use WSL2/Linux or provide a future WinFsp/Dokany backend.")
endif()

project(IoTDBFuseGrib VERSION 1.0.0 LANGUAGES CXX)

if(WIN32)
  message(FATAL_ERROR "Native Windows builds are not supported yet; use WSL2/Linux or provide a future WinFsp/Dokany backend.")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

add_definitions(-D_FILE_OFFSET_BITS=64)

find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)
find_package(OpenSSL REQUIRED COMPONENTS Crypto)
# Boost is a transitive dependency of the IoTDB C++ session client. On Linux
# it is typically picked up implicitly from system paths, so we keep the
# Linux build untouched. On macOS the IoTDB session headers (e.g. via
# boost/date_time/gregorian/gregorian.hpp) require an explicit find/link.
if(APPLE)
  find_package(Boost REQUIRED COMPONENTS date_time)
endif()
pkg_check_modules(FUSE REQUIRED fuse)
pkg_check_modules(ECCODES REQUIRED eccodes)
find_package(Arrow 21.0.0 EXACT CONFIG REQUIRED)
if(NOT TARGET Arrow::arrow_shared OR NOT ARROW_IPC OR NOT ARROW_WITH_LZ4 OR NOT ARROW_WITH_ZSTD)
  message(FATAL_ERROR "Arrow 21.0.0 must provide arrow_shared with IPC, LZ4, and ZSTD enabled")
endif()
# H03 registers HDF5 ingestion on the bounded source driver. Use the locked
# shared C library, not a system/version fallback.
find_package(HDF5 1.14.4 EXACT CONFIG REQUIRED COMPONENTS C shared)
if(NOT TARGET hdf5-shared OR NOT HDF5_ENABLE_THREADSAFE OR
    HDF5_ENABLE_PARALLEL OR NOT HDF5_ENABLE_Z_LIB_SUPPORT)
  message(FATAL_ERROR "HDF5 1.14.4 must provide thread-safe serial hdf5-shared with DEFLATE enabled")
endif()

# Optional native scientific-file index; standalone build: cmake -S fs_index ...
option(IOTDBFS_BUILD_FS_INDEX "Build the standalone FS-index component" OFF)
if(IOTDBFS_BUILD_FS_INDEX)
  add_subdirectory(fs_index)
endif()

set(IOTDB_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/third_party/iotdb-client-cpp-2.0.5-SNAPSHOT" CACHE PATH "IoTDB C++ client root")
set(IOTDB_INCLUDE_DIR "${IOTDB_ROOT}/include")
set(IOTDB_LIBRARY_DIR "${IOTDB_ROOT}/lib")
find_library(IOTDB_SESSION_LIB NAMES iotdb_session HINTS "${IOTDB_LIBRARY_DIR}" REQUIRED)

set(IOTDBFS_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")
include(cmake/QueryModules.cmake)
set(IOTDBFS_VERSIONING_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/storage/NativeFileStore.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/storage/versioning/ContentArtifacts.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/storage/versioning/PayloadArtifacts.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/storage/versioning/VersionDirectory.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/storage/versioning/OwnerJournal.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/storage/versioning/VersionNamespaceRecord.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/storage/versioning/NamespaceSelection.cpp
)
set(IOTDBFS_CORE_SOURCES
  ${IOTDBFS_VERSIONING_SOURCES}
  src/storage/versioning/IoTDBVersionStore.cpp
  src/CrossFileClient.cpp
  src/Platform.cpp
  src/MetadataConfig.cpp
  src/MetadataConfigStore.cpp
  src/MetadataSerialization.cpp
  src/MetadataProjector.cpp
  src/MetadataSchemaCapability.cpp
  src/MetadataCache.cpp
  src/FormatAdapter.cpp
  src/FormatRegistry.cpp
  src/Ingestion.cpp
  src/DatasetCatalog.cpp
  src/DatasetSubstructureIndex.cpp
  src/migration/ReingestionService.cpp
  src/migration/MigrationPolicy.cpp
  src/migration/ArrowReingestionPolicy.cpp
  src/migration/Hdf5ReingestionPolicy.cpp
  src/migration/MigrationJournal.cpp
  src/migration/MigrationCli.cpp
  src/StructuredBlobStorage.cpp
  src/IoTDBStructuredBlobBackend.cpp
  src/NamespaceWriterLock.cpp
  src/MetadataQueryEvaluator.cpp
  ${IOTDBFS_QUERY_CORE_SOURCES}
  ${IOTDBFS_QUERY_ENCODING_SOURCES}
  ${IOTDBFS_QUERY_FRONTEND_SOURCES}
  ${IOTDBFS_TRAINING_MODEL_SOURCES}
  ${IOTDBFS_ARROW_TRAINING_SOURCES}
  ${IOTDBFS_ARROW_ADAPTER_SOURCES}
  ${IOTDBFS_GRIB_ADAPTER_SOURCES}
  ${IOTDBFS_HDF5_ADAPTER_SOURCES}
  ${IOTDBFS_GRIB_RUNTIME_SOURCES}
  src/InodeCache.cpp
  src/IoTDBClient.cpp
  src/IoTDBConnector.cpp
  src/SemanticGrepClient.cpp
  src/Logging.cpp
)

# CF scale and offset are two specified Float64 operations. Keep contraction
# disabled only for the HDF5 value kernel, in both production and unit builds.
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  set_source_files_properties(src/formats/hdf5/Hdf5QueryKernel.cpp
    PROPERTIES COMPILE_OPTIONS "-ffp-contract=off")
endif()

link_directories(${FUSE_LIBRARY_DIRS} ${ECCODES_LIBRARY_DIRS} "${IOTDB_LIBRARY_DIR}")

add_library(iotdb_fs_core STATIC ${IOTDBFS_CORE_SOURCES})
target_include_directories(iotdb_fs_core
  PUBLIC
    "${IOTDBFS_INCLUDE_DIR}"
    ${ECCODES_INCLUDE_DIRS}
    "${IOTDB_INCLUDE_DIR}"
  PRIVATE
    ${FUSE_INCLUDE_DIRS}
)
target_link_libraries(iotdb_fs_core
  PUBLIC
    ${FUSE_LIBRARIES}
    ${ECCODES_LIBRARIES}
    Arrow::arrow_shared
    hdf5-shared ZLIB::ZLIB
    ${IOTDB_SESSION_LIB}
    Threads::Threads
    OpenSSL::Crypto
)
if(APPLE)
  target_include_directories(iotdb_fs_core PUBLIC ${Boost_INCLUDE_DIRS})
  target_link_libraries(iotdb_fs_core PUBLIC Boost::date_time)
endif()

add_executable(iotdb_fs src/main.cpp)
target_include_directories(iotdb_fs PRIVATE
  ${FUSE_INCLUDE_DIRS}
  ${ECCODES_INCLUDE_DIRS}
  "${IOTDB_INCLUDE_DIR}"
)
if(APPLE)
  target_include_directories(iotdb_fs PRIVATE ${Boost_INCLUDE_DIRS})
endif()
target_link_libraries(iotdb_fs PRIVATE iotdb_fs_core)

add_executable(v01_store_probe EXCLUDE_FROM_ALL
  tests/benchmarks/versioning/v01_store_probe.cpp)
target_link_libraries(v01_store_probe PRIVATE iotdb_fs_core)

add_executable(native_file_probe EXCLUDE_FROM_ALL
  tests/benchmarks/versioning/native_file_probe.cpp)
target_link_libraries(native_file_probe PRIVATE iotdb_fs_core)

add_executable(v02_cache_probe EXCLUDE_FROM_ALL
  tests/benchmarks/versioning/v02_cache_probe.cpp)
target_link_libraries(v02_cache_probe PRIVATE iotdb_fs_core)

add_executable(v02_content_probe EXCLUDE_FROM_ALL
  tests/benchmarks/versioning/v02_content_probe.cpp)
target_link_libraries(v02_content_probe PRIVATE iotdb_fs_core)

add_executable(v03_context_probe EXCLUDE_FROM_ALL
  tests/benchmarks/versioning/v03_context_probe.cpp)
target_link_libraries(v03_context_probe PRIVATE iotdb_fs_core)

add_executable(global_hot_cache_probe EXCLUDE_FROM_ALL
  tests/benchmarks/versioning/global_hot_cache_probe.cpp)
target_link_libraries(global_hot_cache_probe PRIVATE iotdb_fs_core)

add_executable(grib_selector_handle_probe EXCLUDE_FROM_ALL
  tests/benchmarks/versioning/grib_selector_handle_probe.cpp)
target_link_libraries(grib_selector_handle_probe PRIVATE iotdb_fs_core)

add_executable(shared_hot_query_probe EXCLUDE_FROM_ALL
  tests/benchmarks/versioning/shared_hot_query_probe.cpp)
target_link_libraries(shared_hot_query_probe PRIVATE iotdb_fs_core)

add_executable(v04_payload_probe EXCLUDE_FROM_ALL
  tests/benchmarks/versioning/v04_payload_probe.cpp)
target_link_libraries(v04_payload_probe PRIVATE iotdb_fs_core)

add_executable(v04_range_probe EXCLUDE_FROM_ALL
  tests/benchmarks/versioning/v04_range_probe.cpp)
target_link_libraries(v04_range_probe PRIVATE iotdb_fs_core)

add_executable(owner_reader_pool_probe EXCLUDE_FROM_ALL
  tests/benchmarks/versioning/owner_reader_pool_probe.cpp)
target_link_libraries(owner_reader_pool_probe PRIVATE iotdb_fs_core)

# Public-data A04 oracle, built explicitly by experiment validation.
add_executable(arrow_query_probe EXCLUDE_FROM_ALL
  tests/benchmarks/arrow_training/a04_query_probe.cpp)
target_link_libraries(arrow_query_probe PRIVATE iotdb_fs_core)

# Read-only, randomized IoTDB payload shape probe. Built explicitly for
# byte-versus-range profiling; never creates or changes a source version.
add_executable(arrow_read_shape_probe EXCLUDE_FROM_ALL
  tests/benchmarks/arrow_training/read_shape_probe.cpp)
target_link_libraries(arrow_read_shape_probe PRIVATE iotdb_fs_core)

# Experiment-owned socketpair transport; validates immutable source once.
add_executable(arrow_derived_range_bridge EXCLUDE_FROM_ALL
  tests/benchmarks/arrow_training/derived_range_bridge.cpp)
target_link_libraries(arrow_derived_range_bridge PRIVATE iotdb_fs_core)

# Read-only A/B/C/D non-training Arrow attribution experiment.
add_executable(arrow_query_paths_probe EXCLUDE_FROM_ALL
  tests/benchmarks/arrow_training/arrow_query_paths_probe.cpp)
target_link_libraries(arrow_query_paths_probe PRIVATE iotdb_fs_core)

# H00 is an opt-in, local-file HDF5 spike; no format adapter is registered.
# CONFIG imported targets let the production project remain CXX-only.
option(IOTDBFS_BUILD_HDF5_H00_PROBE "Build the locked HDF5 H00 validation probe" OFF)
if(IOTDBFS_BUILD_HDF5_H00_PROBE)
  find_package(ZLIB REQUIRED)
  add_executable(h00_hdf5_probe EXCLUDE_FROM_ALL
    tests/benchmarks/hdf5_query/h00_hdf5_probe.cpp)
  target_link_libraries(h00_hdf5_probe PRIVATE hdf5-shared ZLIB::ZLIB)
  target_compile_options(h00_hdf5_probe PRIVATE -Wall -Wextra -Wpedantic)
endif()

# H02 diagnostics use the same production source driver through a bounded
# callback. This executable is explicit validation work, never a mounted route.
add_executable(h02_source_probe EXCLUDE_FROM_ALL
  tests/benchmarks/hdf5_query/h02_source_probe.cpp)
target_link_libraries(h02_source_probe PRIVATE iotdb_fs_core)

add_executable(h03_ingestion_probe EXCLUDE_FROM_ALL
  tests/benchmarks/hdf5_query/h03_ingestion_probe.cpp)
target_link_libraries(h03_ingestion_probe PRIVATE iotdb_fs_core)

add_executable(h04_query_probe EXCLUDE_FROM_ALL
  tests/benchmarks/hdf5_query/h04_query_probe.cpp)
target_link_libraries(h04_query_probe PRIVATE iotdb_fs_core)

add_executable(h06_query_probe EXCLUDE_FROM_ALL tests/benchmarks/hdf5_query/h06_query_probe.cpp)
target_link_libraries(h06_query_probe PRIVATE iotdb_fs_core)

add_executable(h07_query_probe EXCLUDE_FROM_ALL tests/benchmarks/hdf5_query/h07_query_probe.cpp)
target_link_libraries(h07_query_probe PRIVATE iotdb_fs_core)

add_executable(h09_migration_fixture EXCLUDE_FROM_ALL
  tests/benchmarks/hdf5_query/h09_migration_fixture.cpp)
target_link_libraries(h09_migration_fixture PRIVATE iotdb_fs_core)

add_executable(h10_profile_probe EXCLUDE_FROM_ALL
  tests/benchmarks/hdf5_query/h10_profile_probe.cpp)
target_link_libraries(h10_profile_probe PRIVATE iotdb_fs_core)

add_executable(h11_query_probe EXCLUDE_FROM_ALL
  tests/benchmarks/hdf5_query/h11_query_probe.cpp)
target_link_libraries(h11_query_probe PRIVATE iotdb_fs_core hdf5-shared)

# Explicit A12 live fixture: publishes a legacy pass-through Arrow stream in
# an isolated test database, so the migration CLI can exercise real IoTDB rows.
add_executable(a12_legacy_seed EXCLUDE_FROM_ALL
  tests/benchmarks/arrow_training/a12_legacy_seed.cpp)
target_link_libraries(a12_legacy_seed PRIVATE iotdb_fs_core)

add_executable(iotdb_fs_web src/iotdb_fs_web.cpp src/MetadataConfig.cpp src/MetadataSerialization.cpp)
target_include_directories(iotdb_fs_web PRIVATE
  "${IOTDBFS_INCLUDE_DIR}"
)
target_link_libraries(iotdb_fs_web PRIVATE Threads::Threads)
option(IOTDBFS_STATIC_WEB "Build iotdb_fs_web with static libstdc++/libgcc and static system linkage" OFF)
if(IOTDBFS_STATIC_WEB)
  target_link_options(iotdb_fs_web PRIVATE -static -static-libstdc++ -static-libgcc)
endif()

add_executable(grib_client src/grib_client_main.cpp)
target_include_directories(grib_client PRIVATE
  ${ECCODES_INCLUDE_DIRS}
  "${IOTDB_INCLUDE_DIR}"
)
if(APPLE)
  target_include_directories(grib_client PRIVATE ${Boost_INCLUDE_DIRS})
endif()
target_link_libraries(grib_client PRIVATE iotdb_fs_core)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)
  target_link_libraries(iotdb_fs_core PUBLIC stdc++fs)
  target_link_libraries(iotdb_fs_web PRIVATE stdc++fs)
endif()

add_executable(iotdbfs_hdf5_scale_worker src/hdf5_scale_worker_main.cpp)
target_link_libraries(iotdbfs_hdf5_scale_worker PRIVATE iotdb_fs_core)
add_dependencies(iotdb_fs iotdbfs_hdf5_scale_worker)

install(TARGETS iotdb_fs iotdb_fs_web grib_client iotdbfs_hdf5_scale_worker
  RUNTIME DESTINATION bin
)
install(DIRECTORY include/
  DESTINATION include
  FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp"
)
install(DIRECTORY resources/
  DESTINATION resources
  FILES_MATCHING PATTERN "*.properties"
)
if(APPLE)
  install(FILES mac/schema.xml DESTINATION share/mdimport)
endif()

# ---- Testing (optional) ----
option(BUILD_TESTS "Build unit tests" ON)
if(BUILD_TESTS)
  enable_testing()
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
  add_test(NAME query_module_boundaries
    COMMAND "${Python3_EXECUTABLE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/ci/check_query_module_dependencies.py")
  set_tests_properties(query_module_boundaries
    PROPERTIES LABELS "unit" TIMEOUT 30)
  add_test(NAME hdf5_profile_runner_validation
    COMMAND "${Python3_EXECUTABLE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmarks/hdf5_query/test_h10_runner.py")
  set_tests_properties(hdf5_profile_runner_validation
    PROPERTIES LABELS "unit" TIMEOUT 30)
  add_test(NAME hdf5_benchmark_runner_validation
    COMMAND "${Python3_EXECUTABLE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmarks/hdf5_query/test_h11_runner.py")
  set_tests_properties(hdf5_benchmark_runner_validation
    PROPERTIES LABELS "unit" TIMEOUT 30)
  # Linux: try the system GTest first (existing fast path), fall back to
  # FetchContent if it is missing. macOS: skip find_package — system GTest
  # versions on Homebrew/Xcode have proven incompatible, so always fetch.
  if(APPLE)
    set(GTest_FOUND FALSE)
  else()
    find_package(GTest QUIET)
  endif()
  if(NOT GTest_FOUND)
    include(FetchContent)
    FetchContent_Declare(
      googletest
      GIT_REPOSITORY https://github.com/google/googletest.git
      GIT_TAG        v1.14.0
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
  endif()
  add_subdirectory(tests/unit)
  add_subdirectory(tests/integration)
endif()

# The former strict/substructure catalog ablation probe is archived under
# experiments/legacy/hdf5-catalog-strict and uses its frozen historical build.

add_executable(sc_semantic_probe EXCLUDE_FROM_ALL tests/benchmarks/hdf5_query/sc_semantic_probe.cpp)
target_link_libraries(sc_semantic_probe PRIVATE iotdb_fs_core hdf5-shared)
add_dependencies(sc_semantic_probe iotdbfs_hdf5_scale_worker)

# Matched OISST scientific-query baselines. These opt-in probes emit raw
# per-request timing and value/mask digests; they are not production binaries.
add_executable(sc_perf_native EXCLUDE_FROM_ALL tests/benchmarks/hdf5_query/sc_perf_native.cpp)
target_link_libraries(sc_perf_native PRIVATE hdf5-shared)
add_executable(sc_perf_direct EXCLUDE_FROM_ALL tests/benchmarks/hdf5_query/sc_perf_direct.cpp)
target_link_libraries(sc_perf_direct PRIVATE iotdb_fs_core hdf5-shared)
add_dependencies(sc_perf_direct iotdbfs_hdf5_scale_worker)

# GEOS physical-value pilot uses the same bounded direct-query harness.
add_executable(geos_pilot_direct EXCLUDE_FROM_ALL
  tests/benchmarks/hdf5_query/geos_pilot_direct.cpp)
target_link_libraries(geos_pilot_direct PRIVATE iotdb_fs_core hdf5-shared)
add_dependencies(geos_pilot_direct iotdbfs_hdf5_scale_worker)

add_executable(geos_time_ingest_probe EXCLUDE_FROM_ALL
  tests/benchmarks/hdf5_query/geos_time_ingest_probe.cpp)
target_link_libraries(geos_time_ingest_probe PRIVATE iotdb_fs_core hdf5-shared)
add_dependencies(geos_time_ingest_probe iotdbfs_hdf5_scale_worker)

add_executable(published_catalog_probe EXCLUDE_FROM_ALL
  tests/benchmarks/hdf5_query/published_catalog_probe.cpp)
target_link_libraries(published_catalog_probe PRIVATE iotdb_fs_core)

add_executable(publication_payload_verify EXCLUDE_FROM_ALL
  tests/benchmarks/hdf5_query/publication_payload_verify.cpp)
target_link_libraries(publication_payload_verify PRIVATE iotdb_fs_core)
