#pragma once

#include "iotdbfs/formats/arrow/ArrowLayout.h"
#include "iotdbfs/query/core/QueryPlanning.h"
#include "iotdbfs/query/core/QueryReadContext.h"
#include "iotdbfs/query/training/TrainingQueryModel.h"

#include <memory>
#include <mutex>
#include <map>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

namespace iotdbfs {

// Dataset semantics are declarations, separate from the anonymous Arrow
// layout. A logical row may map to any physical row (including a repeated
// physical row in a Hugging Face indices view).
struct SnapshotShard {
  std::string filename; // Exact filename in HF state.json or dataset manifest.
  StructuredBlobVersionKey key;
};

struct SnapshotChannelBinding {
  std::string field_id;
  std::uint64_t original_ordinal{0};
  std::optional<std::uint64_t> physical_ordinal; // absent = missing channel
  std::string label;
  std::optional<std::string> unit;
  std::uint64_t length{0}; // Logical time length, validated against the declared layout.
};

// StepMajorFlat is explicit training semantics over a physical List<Float32>:
// leaf offset = step * declared_n_channels + physical_ordinal.
enum class SnapshotValueLayout { ChannelMajorLists, StepMajorFlat };

struct SnapshotRowBinding {
  std::uint64_t logical_row{0};
  std::size_t shard_index{0};
  std::uint64_t batch_index{0};
  std::uint64_t row_index{0};
  std::string series_id; // Only equal explicit IDs may continue.
  std::string entity_id;
  StepInterval steps; // Explicit global positions; never inferred from shard order.
  TrainingTimeAxis time;
  std::string target_field{"target"};
  std::optional<std::uint64_t> declared_n_channels;
  std::optional<std::uint64_t> declared_length;
  SnapshotValueLayout value_layout{SnapshotValueLayout::ChannelMajorLists};
  std::vector<SnapshotChannelBinding> channels;
  // Exactly one representation is permitted; shared tables avoid per-row strings.
  std::shared_ptr<const std::vector<SnapshotChannelBinding>> channel_table;
  const std::vector<SnapshotChannelBinding>& channel_bindings() const {
    return channel_table ? *channel_table : channels;
  }
};

struct DatasetSnapshotRequest {
  std::string dataset_id;
  SourceSelector selector;
  std::vector<SnapshotShard> shards; // Dataset/HF state order, not pathname order.
  std::vector<SnapshotRowBinding> rows; // Logical order after any indices map.
  // When supplied, _data_files filenames must exactly match shards above.
  // Nonempty _indices_data_files are rejected until their mapping can be verified.
  std::optional<std::string> hf_state_json;
};

struct DatasetSnapshot {
  std::string dataset_id;
  std::string fingerprint; // Stable content identity for views and sample IDs.
  std::string process_identity; // Runtime guard; excluded from fingerprint.
  std::uint64_t storage_namespace{0}; // Backend instance identity, not persistent.
  std::vector<SnapshotShard> shards;
  std::vector<StructuredBlobManifest> manifests;
  std::vector<SnapshotRowBinding> rows;
  std::shared_ptr<const QueryReadContext> read_context;
};

// Clone shared channel tables once, severing any retained mutable aliases.
DatasetSnapshot deep_copy_dataset_snapshot(const DatasetSnapshot& snapshot);
// Channel allocation charge only; shared tables are charged once.
std::uint64_t snapshot_channel_metadata_bytes(const DatasetSnapshot& snapshot) noexcept;

// A preparation freezes a catalog selection to exact published versions.
// Neither training epochs nor coverage lookups invoke latest selection again.
QueryResult<DatasetSnapshot> prepare_dataset_snapshot(
  const DatasetSnapshotRequest& request,
  const PublishedVersionCatalog& catalog,
  std::shared_ptr<const StructuredBlobStorage> storage,
  std::size_t max_sources = 1024,
  const std::optional<QueryPrincipal>& principal = std::nullopt);

// Checks the stable content fingerprint plus process/backend binding without
// opening every shard; suitable for lazy window preparation.
QueryStatus validate_dataset_snapshot_runtime(const DatasetSnapshot& snapshot,
  std::shared_ptr<const StructuredBlobStorage> storage);

// A read handle pins a manifest, not deletion. Administrative delete/rename
// must be disabled while a snapshot is in use; verify reports SourceNotFound
// for a deleted exact version and never substitutes a newer version.
QueryStatus verify_dataset_snapshot_sources(const DatasetSnapshot& snapshot,
  std::shared_ptr<const StructuredBlobStorage> storage);

struct DatasetCoverageSpan {
  std::string series_id;
  std::string field_id;
  std::uint64_t original_ordinal{0};
  std::uint64_t logical_row{0}; // Resolves time/entity metadata in DatasetSnapshot.
  std::size_t shard_index{0};
  std::uint64_t batch_index{0};
  std::uint64_t row_index{0};
  std::optional<std::uint64_t> physical_ordinal;
  StepInterval steps; // Clipped by lookup.
  std::uint64_t source_step_begin{0}; // First step of the physical List row.
  bool null_series{false};
  bool null_channel{false};
  bool missing_channel{false};
};

class DatasetCoverageIndex {
public:
  DatasetCoverageIndex() = default;
  DatasetCoverageIndex(const DatasetCoverageIndex& other);
  DatasetCoverageIndex& operator=(const DatasetCoverageIndex& other);
  DatasetCoverageIndex(DatasetCoverageIndex&&) noexcept = default;
  DatasetCoverageIndex& operator=(DatasetCoverageIndex&&) noexcept = default;
  const std::string& snapshot_fingerprint() const { return fingerprint_; }
  const std::string& process_identity() const { return process_identity_; }
  std::uint64_t storage_namespace() const { return storage_namespace_; }
  // Diagnostic full expansion; lookups retain the compact representation.
  const std::vector<DatasetCoverageSpan>& spans() const;
  // Conservative registry budget charge, not process RSS. Includes dynamic
  // capacity and map nodes; arithmetic overflow saturates to UINT64_MAX.
  std::uint64_t estimated_metadata_bytes() const noexcept;

  // Returns clipped spans in step order. A hole or an unavailable channel is
  // represented explicitly; the caller's missing policy decides what to do.
  QueryResult<std::vector<DatasetCoverageSpan>> lookup(
    const std::string& series_id, const std::string& field_id,
    std::uint64_t original_ordinal, StepInterval steps) const;
  // Return only intersecting declared spans. Gaps and absent channels are
  // represented by the caller, so Mask can retain their original positions.
  QueryResult<std::vector<DatasetCoverageSpan>> lookup_overlaps(
    const std::string& series_id, const std::string& field_id,
    std::uint64_t original_ordinal, StepInterval steps) const;

private:
  friend QueryResult<DatasetCoverageIndex> prepare_dataset_coverage_index(
    const DatasetSnapshot&, std::shared_ptr<const StructuredBlobStorage>,
    const std::optional<std::string>&);
  std::string fingerprint_;
  std::string process_identity_;
  std::uint64_t storage_namespace_{0};
  mutable std::vector<DatasetCoverageSpan> spans_;
  mutable std::shared_ptr<std::mutex> spans_mutex_{std::make_shared<std::mutex>()};
  struct CompactRow {
    DatasetCoverageSpan common;
    std::shared_ptr<const std::vector<SnapshotChannelBinding>> channels;
    std::vector<unsigned char> null_channels;
  };
  struct CompactRef { std::uint32_t row; std::uint32_t channel; };
  std::vector<CompactRow> compact_rows_;
  using LookupKey = std::tuple<std::string, std::string, std::uint64_t>;
  std::map<LookupKey, std::pair<std::size_t, std::size_t>> ranges_;
  std::map<LookupKey, std::vector<CompactRef>> compact_ranges_;
  DatasetCoverageSpan expand(CompactRef ref) const;
  void compact(const DatasetSnapshot& snapshot);
  void rebuild_ranges();
};

// Sidecar is optional but preparation is explicit. A missing/corrupt sidecar
// rebuilds here, never inside a window lookup. The file includes a protocol,
// runtime sidecar binding (content fingerprint + process + storage instance),
// and SHA-256 checksum. Publication failures leave the rebuilt index usable.
// A completed temporary file is atomically renamed into place.
QueryResult<DatasetCoverageIndex> prepare_dataset_coverage_index(
  const DatasetSnapshot& snapshot,
  std::shared_ptr<const StructuredBlobStorage> storage,
  const std::optional<std::string>& sidecar_path = std::nullopt);

// Preflight the view's selected channel identities against every named series.
// Coverage gaps/null values remain window-level decisions for A08.
QueryStatus validate_snapshot_view(const DatasetSnapshot& snapshot,
  const DatasetCoverageIndex& index, const TrainingViewSpec& view);

} // namespace iotdbfs
