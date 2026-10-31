set(UNIT_TEST_SOURCES
  ${CMAKE_SOURCE_DIR}/src/Platform.cpp
  ${CMAKE_SOURCE_DIR}/src/MetadataConfig.cpp
  ${CMAKE_SOURCE_DIR}/src/MetadataConfigStore.cpp
  ${CMAKE_SOURCE_DIR}/src/MetadataSerialization.cpp
  ${CMAKE_SOURCE_DIR}/src/MetadataSchemaCapability.cpp
  ${CMAKE_SOURCE_DIR}/src/MetadataCache.cpp
  ${CMAKE_SOURCE_DIR}/src/MetadataProjector.cpp
  ${CMAKE_SOURCE_DIR}/src/FormatAdapter.cpp
  ${CMAKE_SOURCE_DIR}/src/FormatRegistry.cpp
  ${CMAKE_SOURCE_DIR}/src/Ingestion.cpp
  ${CMAKE_SOURCE_DIR}/src/DatasetCatalog.cpp
  ${CMAKE_SOURCE_DIR}/src/DatasetSubstructureIndex.cpp
  ${CMAKE_SOURCE_DIR}/src/migration/ReingestionService.cpp
  ${CMAKE_SOURCE_DIR}/src/migration/MigrationPolicy.cpp
  ${CMAKE_SOURCE_DIR}/src/migration/ArrowReingestionPolicy.cpp
  ${CMAKE_SOURCE_DIR}/src/migration/Hdf5ReingestionPolicy.cpp
  ${CMAKE_SOURCE_DIR}/src/migration/MigrationJournal.cpp
  ${CMAKE_SOURCE_DIR}/src/StructuredBlobStorage.cpp
  ${CMAKE_SOURCE_DIR}/src/NamespaceWriterLock.cpp
  ${IOTDBFS_VERSIONING_SOURCES}
  ${CMAKE_SOURCE_DIR}/src/MetadataQueryEvaluator.cpp
  ${IOTDBFS_QUERY_CORE_SOURCES}
  ${IOTDBFS_QUERY_ENCODING_SOURCES}
  ${IOTDBFS_QUERY_FRONTEND_SOURCES}
  ${IOTDBFS_TRAINING_MODEL_SOURCES}
  ${IOTDBFS_ARROW_TRAINING_SOURCES}
  ${IOTDBFS_ARROW_ADAPTER_SOURCES}
  ${IOTDBFS_GRIB_ADAPTER_SOURCES}
  ${IOTDBFS_HDF5_ADAPTER_SOURCES}
  ${CMAKE_SOURCE_DIR}/src/Logging.cpp
  ${CMAKE_SOURCE_DIR}/src/InodeCache.cpp
)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  set_source_files_properties(${CMAKE_SOURCE_DIR}/src/formats/hdf5/Hdf5QueryKernel.cpp
    PROPERTIES COMPILE_OPTIONS "-ffp-contract=off")
endif()

add_executable(unit_tests
  test_PathUtils.cpp
  test_FuseSemantics.cpp
  test_FeatureConfig.cpp
  test_MetadataConfig.cpp
  test_MetadataConfigStore.cpp
  test_MetadataSerialization.cpp
  test_MetadataSchemaCapability.cpp
  test_MetadataCache.cpp
  test_MetadataProjector.cpp
  test_FormatAdapter.cpp
  test_FormatRegistry.cpp
  test_Ingestion.cpp
  test_StructuredBlobStorage.cpp
  test_DatasetSubstructureIndex.cpp
  test_NamespaceWriterLock.cpp
  test_HotQueryCache.cpp
  formats/hdf5/test_Hdf5QueryMode.cpp
  storage/versioning/test_VersionDirectory.cpp
  storage/versioning/test_BoundedReaderPool.cpp
  storage/versioning/test_OwnerReadTiming.cpp
  storage/versioning/test_PayloadBlockFlights.cpp
  storage/versioning/test_PayloadReadCoalescing.cpp
  storage/versioning/test_ReaderPoolRuntime.cpp
  storage/test_NativeFileStore.cpp
  storage/versioning/test_NativeFileNamespace.cpp
  storage/versioning/test_NamespaceSelection.cpp
  storage/versioning/test_ContentArtifacts.cpp
  storage/versioning/test_PayloadArtifacts.cpp
  migration/test_ReingestionService.cpp
  migration/test_Hdf5ReingestionPolicy.cpp
  migration/test_MigrationJournal.cpp
  formats/grib/test_GribFormatAdapter.cpp
  test_MetadataQueryEvaluator.cpp
  query/core/test_QueryModel.cpp
  query/training/test_TrainingQueryModel.cpp
  query/training/arrow/test_DatasetSnapshot.cpp
  query/training/arrow/test_TrainingQueryModule.cpp
  formats/hdf5/test_Hdf5QueryModel.cpp
  formats/hdf5/test_Hdf5QueryBinding.cpp
  formats/hdf5/test_Hdf5SourceDriver.cpp
  formats/hdf5/test_Hdf5Layout.cpp
  formats/hdf5/test_Hdf5Inventory.cpp
  formats/hdf5/test_Hdf5Metadata.cpp
  formats/hdf5/test_Hdf5SemanticBinding.cpp
  formats/hdf5/test_Hdf5FormatAdapter.cpp
  formats/hdf5/test_Hdf5FormatQueryAdapter.cpp
  formats/hdf5/test_Hdf5ChunkDecoder.cpp
  formats/hdf5/test_Hdf5RangeReader.cpp
  formats/hdf5/test_Hdf5SharedCache.cpp
  formats/arrow/test_ArrowQueryModel.cpp
  formats/arrow/test_ArrowLayout.cpp
  formats/arrow/test_ArrowFormatAdapter.cpp
  formats/arrow/test_ArrowFormatQueryAdapter.cpp
  query/core/test_DecodedFragmentCache.cpp
  query/core/test_QueryProfile.cpp
  query/core/test_FairWorkScheduler.cpp
  query/core/test_QueryTaskScheduler.cpp
  query/encoding/test_QueryBatchIpc.cpp
  query/encoding/test_WindowIpcEncoder.cpp
  query/frontend/test_TypedQueryFrontend.cpp
  query/frontend/test_QueryRegistrationSocket.cpp
  query/frontend/test_BoundedResultSender.cpp
  query/frontend/test_PreparedSelectorRegistry.cpp
  query/frontend/test_FormatCatalogRegistration.cpp
  query/frontend/test_Hdf5Registration.cpp
  query/frontend/test_TypedResultStore.cpp
  query/training/arrow/test_TrainingViewTransform.cpp
  query/core/test_QueryPlanning.cpp
  query/core/test_CatalogSelection.cpp
  query/core/test_QueryModule.cpp
  query/core/test_FormatCatalog.cpp
  formats/grib/test_GribQueryFoundation.cpp
  formats/grib/test_GribFormatQueryAdapter.cpp
  test_QueryUtils.cpp
  test_MetadataSetIndex.cpp
  formats/grib/test_gribUtils.cpp
  formats/grib/test_GribExtensionService.cpp
  formats/grib/test_GribWriteStreamer.cpp
  formats/grib/test_GribFileWriter.cpp
  formats/hdf5/test_Hdf5ScaleWorker.cpp
  formats/hdf5/test_Hdf5CfSemantics.cpp
  query/core/test_DatasetCatalogQuery.cpp
  formats/hdf5/test_Hdf5GlobalAttributeIndex.cpp
  query/core/test_QueryMetadataCacheTrace.cpp
  query/core/test_ImmutableEqualityIndex.cpp
  query/core/test_QueryDomainCache.cpp
  query/core/test_QueryModuleDomainCache.cpp
  test_Logging.cpp
  test_InodeCache.cpp
  test_StagedFile.cpp
  test_IoTDBClientBoundaries.cpp
  test_PublicationProfile.cpp
  test_InodeRowParser.cpp
  test_Platform.cpp
  test_EnvironmentPreflight.cpp
  ${UNIT_TEST_SOURCES}
)

target_include_directories(unit_tests PRIVATE
  ${CMAKE_SOURCE_DIR}/include
  "${IOTDB_INCLUDE_DIR}"
  ${ECCODES_INCLUDE_DIRS}
)

target_link_libraries(unit_tests PRIVATE
  GTest::gtest
  GTest::gtest_main
  OpenSSL::Crypto
  GTest::gmock
  ${ECCODES_LIBRARIES}
  Arrow::arrow_shared
  hdf5-shared ZLIB::ZLIB
  Threads::Threads
)

target_sources(unit_tests PRIVATE
  ${CMAKE_SOURCE_DIR}/src/EnvironmentPreflight.cpp
)

# Exercise error handling against deterministic syscall failures. Wrappers are
# confined to this test executable and forward unarmed calls to the real OS.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_sources(unit_tests PRIVATE test_PlatformFaults.cpp)
  target_link_options(unit_tests PRIVATE
    -Wl,--wrap=pread64,--wrap=pwrite64,--wrap=ftruncate64
    -Wl,--wrap=mkstemp64,--wrap=unlink,--wrap=fsync,--wrap=readlink
    -Wl,--wrap=__pread64_chk,--wrap=__readlink_chk
  )
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)
  target_link_libraries(unit_tests PRIVATE stdc++fs)
endif()

include(GoogleTest)
gtest_discover_tests(unit_tests
  PROPERTIES LABELS "unit" TIMEOUT 30
)

add_dependencies(unit_tests iotdbfs_hdf5_scale_worker)
