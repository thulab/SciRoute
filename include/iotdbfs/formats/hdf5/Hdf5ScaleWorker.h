#pragma once

#include "iotdbfs/FormatAdapter.h"
#include "iotdbfs/query/core/QueryModel.h"

namespace iotdbfs {
struct Hdf5DimensionAssociation {
  std::string dataset_id;
  std::uint32_t axis{0};
  std::string scale_id;
};
struct Hdf5ScaleWorkerLimits {
  std::uint64_t max_read_bytes{64ULL * 1024 * 1024};
  std::uint64_t max_read_calls{65536};
  std::uint64_t max_associations{4096};
  std::uint64_t address_space_bytes{512ULL * 1024 * 1024};
  std::uint32_t cpu_seconds{5};
  std::uint32_t wall_ms{10000};
};

// Only the helper calls the native implementation: H5Aread's variable-length
// allocations are constrained by the child process, never the mount process.
QueryResult<std::vector<Hdf5DimensionAssociation>>
inspect_hdf5_dimension_associations_native(const FormatSource&, std::uint64_t max_associations);

// Starts a fresh executable (no post-fork HDF5 calls in a multithreaded daemon).
// The child has only a bounded read RPC, never a source path or writable source.
// wall_ms bounds RPC waits/native execution; an in-progress backend callback
// remains subject to the backend's own I/O timeout, and is checked on return.
QueryResult<std::vector<Hdf5DimensionAssociation>>
read_hdf5_dimension_associations_isolated(const FormatSource&,
  const Hdf5ScaleWorkerLimits& = {});
int hdf5_scale_worker_main();
} // namespace iotdbfs
