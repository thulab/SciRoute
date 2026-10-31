#pragma once
#include "iotdbfs/migration/MigrationPolicy.h"
#include <memory>
namespace iotdbfs {
std::shared_ptr<const ReingestionPolicy> make_arrow_reingestion_policy();
} // namespace iotdbfs
