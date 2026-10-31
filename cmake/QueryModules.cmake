# Query ownership is declared once for both the library and focused unit tests.
# Paths are absolute because tests/unit is a separate CMake directory.
set(IOTDBFS_QUERY_CORE_SOURCES
  ${PROJECT_SOURCE_DIR}/src/query/core/QueryModel.cpp
  ${PROJECT_SOURCE_DIR}/src/query/core/DatasetCatalogQuery.cpp
  ${PROJECT_SOURCE_DIR}/src/query/core/DecodedFragmentCache.cpp
  ${PROJECT_SOURCE_DIR}/src/query/core/FairWorkScheduler.cpp
  ${PROJECT_SOURCE_DIR}/src/query/core/QueryTaskScheduler.cpp
  ${PROJECT_SOURCE_DIR}/src/query/core/QueryRuntime.cpp
  ${PROJECT_SOURCE_DIR}/src/query/core/QueryPlanning.cpp
  ${PROJECT_SOURCE_DIR}/src/query/core/QueryModule.cpp
)

set(IOTDBFS_QUERY_ENCODING_SOURCES
  ${PROJECT_SOURCE_DIR}/src/query/encoding/ArrowResultEncoder.cpp
  ${PROJECT_SOURCE_DIR}/src/query/encoding/WindowIpcEncoder.cpp
)

set(IOTDBFS_QUERY_FRONTEND_SOURCES
  ${PROJECT_SOURCE_DIR}/src/query/frontend/PreparedSelectorRegistry.cpp
  ${PROJECT_SOURCE_DIR}/src/query/frontend/PreparedTrainingRegistry.cpp
  ${PROJECT_SOURCE_DIR}/src/query/frontend/TypedQueryFrontend.cpp
  ${PROJECT_SOURCE_DIR}/src/query/frontend/TypedResultStore.cpp
  ${PROJECT_SOURCE_DIR}/src/query/frontend/BoundedResultSender.cpp
  ${PROJECT_SOURCE_DIR}/src/query/frontend/TypedQueryService.cpp
  ${PROJECT_SOURCE_DIR}/src/query/frontend/FuseTypedQueryBridge.cpp
  ${PROJECT_SOURCE_DIR}/src/query/frontend/QueryRegistrationSocket.cpp
)

set(IOTDBFS_TRAINING_MODEL_SOURCES
  ${PROJECT_SOURCE_DIR}/src/query/training/TrainingQueryModel.cpp
)

# A07/A08 execution currently binds Arrow layout and fragments explicitly.
set(IOTDBFS_ARROW_TRAINING_SOURCES
  ${PROJECT_SOURCE_DIR}/src/query/training/arrow/DatasetSnapshot.cpp
  ${PROJECT_SOURCE_DIR}/src/query/training/arrow/WindowPlanner.cpp
  ${PROJECT_SOURCE_DIR}/src/query/training/arrow/WindowAssembler.cpp
  ${PROJECT_SOURCE_DIR}/src/query/training/arrow/TrainingQueryModule.cpp
)

set(IOTDBFS_ARROW_ADAPTER_SOURCES
  ${PROJECT_SOURCE_DIR}/src/formats/arrow/ArrowQueryModel.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/arrow/ArrowLayout.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/arrow/ArrowFormatAdapter.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/arrow/ArrowRangeReader.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/arrow/ArrowFragmentProvider.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/arrow/ArrowFormatQueryAdapter.cpp
)

# HDF5 contracts, bounded ingestion, typed execution and explicit scientific bindings.
set(IOTDBFS_HDF5_ADAPTER_SOURCES
  ${PROJECT_SOURCE_DIR}/src/formats/hdf5/Hdf5GlobalAttributeIndex.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/hdf5/Hdf5QueryModel.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/hdf5/Hdf5SemanticBinding.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/hdf5/Hdf5ScaleWorker.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/hdf5/Hdf5SourceDriver.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/hdf5/Hdf5Layout.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/hdf5/Hdf5FormatAdapter.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/hdf5/Hdf5FormatQueryAdapter.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/hdf5/Hdf5FragmentProvider.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/hdf5/Hdf5RangeReader.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/hdf5/Hdf5ChunkDecoder.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/hdf5/Hdf5QueryKernel.cpp
)

set(IOTDBFS_GRIB_ADAPTER_SOURCES
  ${PROJECT_SOURCE_DIR}/src/formats/grib/GribLayout.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/grib/GribQueryModel.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/grib/GribQueryKernel.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/grib/GribFormatQueryAdapter.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/grib/GribFormatAdapter.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/grib/GribExtensionService.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/grib/GribWriteStreamer.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/grib/GribFileWriter.cpp
  ${PROJECT_SOURCE_DIR}/src/formats/grib/gribUtils.cpp
)

# The unit-test executable supplies ecCodes fixtures without this runtime TU.
set(IOTDBFS_GRIB_RUNTIME_SOURCES
  ${PROJECT_SOURCE_DIR}/src/formats/grib/EcCodesRuntime.cpp
)
