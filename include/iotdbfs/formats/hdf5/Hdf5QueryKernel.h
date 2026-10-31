#pragma once
#include "iotdbfs/formats/hdf5/Hdf5FragmentProvider.h"
#include "iotdbfs/formats/hdf5/Hdf5QueryMode.h"
namespace iotdbfs {
QueryResult<std::unique_ptr<QueryBatchCursor>> make_hdf5_query_cursor(
  std::shared_ptr<const QuerySource> source, Hdf5QueryBinding binding,
  Hdf5DatasetLayout dataset, const QueryExecutionOptions& options,
  Hdf5QueryKernelConfig config, std::shared_ptr<QueryRuntime> runtime,
  std::string identity,
  std::optional<Hdf5PreparedSelection> prepared = std::nullopt,
  Hdf5QueryMode mode = Hdf5QueryMode::IoTDBIndexed);
} // namespace iotdbfs
