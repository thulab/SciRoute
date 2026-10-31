#pragma once

#include "iotdbfs/query/core/QueryPlanning.h"
#include "iotdbfs/storage/versioning/VersionDirectory.h"
#include "iotdbfs/storage/versioning/VersionNamespaceRecord.h"

#include <functional>
#include <map>
#include <optional>
#include <utility>
#include <vector>

namespace iotdbfs::versioning {

struct NamespaceSelection {
  std::map<StructuredBlobVersionKey, NamespaceRecord> sources;
  std::map<std::string, std::optional<NamespaceRecord>> parents;
};

// Reads only identity/permission facts from one immutable view. The observer
// runs before decoding each record actually read, including index duplicates
// and predicate misses. Owner retains its normal page/object budget charges.
// file_predicate is residual; this helper never reads content to evaluate it.
NamespaceSelection select_namespace(
  Owner&, const View&, const SourceSelector&,
  const std::function<void(Table, const std::string&, const Bytes&)>& observe);

// Project an already authenticated same-view Exact identity snapshot without
// decoding or observing its records again. The caller must charge/replay the
// snapshot's complete evidence and directory fees before calling. Newly read
// current ancestors and their Exact membership still use the observer.
NamespaceSelection project_namespace(
  Owner&, const View&, const SourceSelector&,
  const std::map<StructuredBlobVersionKey, NamespaceRecord>& parsed_exact,
  const std::function<void(Table, const std::string&, const Bytes&)>& observe);

// Once at namespace startup, after the caller's existing Exact closure and
// content-profile verification. Reuse that same-view Exact scan; expected
// Current values borrow its bytes rather than retaining another copy.
void validate_current_index(Owner&, const View&,
  const std::vector<std::pair<std::string, Bytes>>& exact_records);

} // namespace iotdbfs::versioning
