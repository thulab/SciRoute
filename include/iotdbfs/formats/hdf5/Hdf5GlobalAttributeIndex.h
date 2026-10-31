#pragma once

#include "iotdbfs/DatasetCatalog.h"
#include "iotdbfs/query/core/QueryModel.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace iotdbfs {

// HDF5-specific cross-file attribute index, shared by both query engines.
// This is derived from sealed catalogs, not libhdf5's in-file index.
// Immutable derived metadata for one already-authorized frozen source domain.
// This object neither selects versions nor grants permissions. It owns no ROI
// values or query answers. Caller supplies the complete domain in source order.
class Hdf5GlobalAttributeIndex {
public:
  struct Match {
    std::size_t source_ordinal{0}, record_ordinal{0};
    friend bool operator==(const Match& a, const Match& b) {
      return a.source_ordinal == b.source_ordinal && a.record_ordinal == b.record_ordinal;
    }
  };

  // Decline (nullptr) on any nonstandard domain record or insufficient resident
  // budget. Caller must then use its unchanged generic evaluator/error order.
  // A dataset path scopes the domain; source slots, including empty ones, remain.
  static std::shared_ptr<const Hdf5GlobalAttributeIndex> build(
      const std::vector<std::shared_ptr<const DatasetCatalogArtifact>>& catalogs,
      std::uint64_t max_estimated_bytes,
      std::optional<std::string> dataset_path = std::nullopt);
  static bool supports(const QueryExpressionPtr& predicate);
  // nullopt means unsupported syntax/types, never a successful empty result.
  // Matches follow input source order, then object_id order within a source.
  std::optional<std::vector<Match>> lookup(const QueryExpressionPtr& predicate) const;
  // Conservative charge includes own capacities and every retained catalog's
  // allocation estimate (duplicates overcharged intentionally). Pins remain
  // charged by the caller until the final shared owner releases the object.
  std::uint64_t estimated_bytes() const;
  const std::vector<std::shared_ptr<const DatasetCatalogArtifact>>& catalogs() const;
  const std::vector<std::size_t>& candidate_counts() const;

private:
  struct Impl;
  explicit Hdf5GlobalAttributeIndex(std::shared_ptr<const Impl> impl) : impl_(std::move(impl)) {}
  std::shared_ptr<const Impl> impl_;
};
} // namespace iotdbfs
