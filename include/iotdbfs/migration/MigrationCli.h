#pragma once
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/migration/MigrationPolicy.h"
#include <memory>
namespace iotdbfs {
// Caller has loaded configuration, but has not initialized FUSE.
int run_migration_cli(int argc, char* argv[], const IoTDBConfig& config,
  std::size_t pool_size, std::shared_ptr<const ReingestionPolicy> policy);
} // namespace iotdbfs
