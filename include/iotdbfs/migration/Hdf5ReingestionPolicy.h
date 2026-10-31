#pragma once

#include "iotdbfs/migration/MigrationPolicy.h"
#include <memory>

namespace iotdbfs {
// Latest pass-through HDF5 and obsolete /1-/3 layouts are rebuilt from original
// payload into a new /4 version. Historical versions remain immutable.
std::shared_ptr<const ReingestionPolicy> make_hdf5_reingestion_policy();
} // namespace iotdbfs
