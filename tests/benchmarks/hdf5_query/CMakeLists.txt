cmake_minimum_required(VERSION 3.19)
project(iotdbfs_hdf5_h00 LANGUAGES C CXX)
find_package(HDF5 1.14.4 EXACT CONFIG REQUIRED COMPONENTS C shared)
if(NOT TARGET hdf5-shared OR NOT HDF5_ENABLE_THREADSAFE OR
    HDF5_ENABLE_PARALLEL OR NOT HDF5_ENABLE_Z_LIB_SUPPORT)
  message(FATAL_ERROR "HDF5 1.14.4 must provide thread-safe serial hdf5-shared with DEFLATE enabled")
endif()
find_package(ZLIB REQUIRED)
find_package(OpenSSL REQUIRED COMPONENTS Crypto)
add_executable(h00_hdf5_probe h00_hdf5_probe.cpp)
target_compile_features(h00_hdf5_probe PRIVATE cxx_std_17)
target_link_libraries(h00_hdf5_probe PRIVATE hdf5-shared ZLIB::ZLIB)
target_compile_options(h00_hdf5_probe PRIVATE -Wall -Wextra -Wpedantic)
find_package(Threads REQUIRED)
add_executable(h00_hdf5_concurrency h00_hdf5_concurrency.cpp)
target_compile_features(h00_hdf5_concurrency PRIVATE cxx_std_17)
target_link_libraries(h00_hdf5_concurrency PRIVATE hdf5-shared Threads::Threads)
target_compile_options(h00_hdf5_concurrency PRIVATE -Wall -Wextra -Wpedantic)

# H02 exercises the actual source driver and its model/core dependencies while
# keeping this diagnostic build independent of IoTDB, FUSE, Arrow C++ and GRIB.
# ArrowQueryModel is a dependency-free request-contract implementation needed
# by QueryPlanning; no Arrow layout or decoder enters this standalone target.
get_filename_component(IOTDBFS_SOURCE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)
add_executable(h02_source_probe
  h02_source_probe.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5SourceDriver.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5Layout.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5QueryModel.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5SemanticBinding.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/arrow/ArrowQueryModel.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/query/core/QueryModel.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/query/core/QueryPlanning.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/DatasetCatalog.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/DatasetSubstructureIndex.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/query/core/DatasetCatalogQuery.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5ScaleWorker.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/StructuredBlobStorage.cpp)
target_include_directories(h02_source_probe PRIVATE ${IOTDBFS_SOURCE_ROOT}/include)
target_compile_features(h02_source_probe PRIVATE cxx_std_17)
target_link_libraries(h02_source_probe PRIVATE hdf5-shared Threads::Threads OpenSSL::Crypto)
target_compile_options(h02_source_probe PRIVATE -Wall -Wextra -Wpedantic)

add_executable(h03_ingestion_probe
  h03_ingestion_probe.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5SourceDriver.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5Layout.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5FormatAdapter.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5QueryModel.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5SemanticBinding.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/arrow/ArrowQueryModel.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/query/core/QueryModel.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/query/core/QueryPlanning.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/DatasetCatalog.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/DatasetSubstructureIndex.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/query/core/DatasetCatalogQuery.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5ScaleWorker.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/StructuredBlobStorage.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/Ingestion.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/FormatAdapter.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/FormatRegistry.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/MetadataConfig.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/MetadataSerialization.cpp)
target_include_directories(h03_ingestion_probe PRIVATE ${IOTDBFS_SOURCE_ROOT}/include)
target_compile_features(h03_ingestion_probe PRIVATE cxx_std_17)
target_link_libraries(h03_ingestion_probe PRIVATE hdf5-shared Threads::Threads OpenSSL::Crypto)
target_compile_options(h03_ingestion_probe PRIVATE -Wall -Wextra -Wpedantic)

# H04 uses the real QueryModule without requiring a mounted service.
get_target_property(H03_SOURCE_FILES h03_ingestion_probe SOURCES)
list(REMOVE_ITEM H03_SOURCE_FILES h03_ingestion_probe.cpp)
add_executable(h04_query_probe h04_query_probe.cpp ${H03_SOURCE_FILES}
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5FormatQueryAdapter.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5FragmentProvider.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5ChunkDecoder.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5RangeReader.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/formats/hdf5/Hdf5QueryKernel.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/query/core/DecodedFragmentCache.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/query/core/QueryRuntime.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/query/core/FairWorkScheduler.cpp
  ${IOTDBFS_SOURCE_ROOT}/src/query/core/QueryModule.cpp)
target_include_directories(h04_query_probe PRIVATE ${IOTDBFS_SOURCE_ROOT}/include)
target_compile_features(h04_query_probe PRIVATE cxx_std_17)
target_link_libraries(h04_query_probe PRIVATE ZLIB::ZLIB hdf5-shared Threads::Threads OpenSSL::Crypto)
target_compile_options(h04_query_probe PRIVATE -Wall -Wextra -Wpedantic)

get_target_property(H04_SOURCE_FILES h04_query_probe SOURCES)
list(REMOVE_ITEM H04_SOURCE_FILES h04_query_probe.cpp)
add_executable(h06_query_probe h06_query_probe.cpp ${H04_SOURCE_FILES})
target_include_directories(h06_query_probe PRIVATE ${IOTDBFS_SOURCE_ROOT}/include)
target_compile_features(h06_query_probe PRIVATE cxx_std_17)
target_link_libraries(h06_query_probe PRIVATE ZLIB::ZLIB hdf5-shared Threads::Threads OpenSSL::Crypto)
target_compile_options(h06_query_probe PRIVATE -Wall -Wextra -Wpedantic)

add_executable(h07_query_probe h07_query_probe.cpp ${H04_SOURCE_FILES})
target_include_directories(h07_query_probe PRIVATE ${IOTDBFS_SOURCE_ROOT}/include)
target_compile_features(h07_query_probe PRIVATE cxx_std_17)
target_link_libraries(h07_query_probe PRIVATE ZLIB::ZLIB hdf5-shared Threads::Threads OpenSSL::Crypto)
target_compile_options(h07_query_probe PRIVATE -Wall -Wextra -Wpedantic)

add_executable(iotdbfs_hdf5_scale_worker ${IOTDBFS_SOURCE_ROOT}/src/hdf5_scale_worker_main.cpp ${H03_SOURCE_FILES})
target_include_directories(iotdbfs_hdf5_scale_worker PRIVATE ${IOTDBFS_SOURCE_ROOT}/include)
target_compile_features(iotdbfs_hdf5_scale_worker PRIVATE cxx_std_17)
target_link_libraries(iotdbfs_hdf5_scale_worker PRIVATE hdf5-shared Threads::Threads OpenSSL::Crypto)
foreach(probe h02_source_probe h03_ingestion_probe h04_query_probe h06_query_probe h07_query_probe)
  add_dependencies(${probe} iotdbfs_hdf5_scale_worker)
endforeach()
