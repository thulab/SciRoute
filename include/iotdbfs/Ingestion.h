#pragma once

#include "iotdbfs/FormatAdapter.h"
#include "iotdbfs/FormatRegistry.h"
#include "iotdbfs/StructuredBlobStorage.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>

namespace iotdbfs {

struct IngestionOutcome {
  std::optional<std::string> format_id;
  MetadataMap metadata;
  std::uint64_t payload_size{ 0 };
  std::uint64_t layout_size{ 0 };
};

enum class IngestionError {
  None,
  InvalidSource,
  SourceReadFailure,
  AdapterFailure,
  StorageFailure,
  PublicationPreparationFailure,
};

class IngestionResult {
public:
  static IngestionResult success(IngestionOutcome outcome);
  static IngestionResult failure(IngestionError error, std::string message);

  bool ok() const { return error_ == IngestionError::None; }
  IngestionError error() const { return error_; }
  const std::string& message() const { return message_; }
  const IngestionOutcome& outcome() const { return *outcome_; }

private:
  IngestionError error_{ IngestionError::None };
  std::string message_;
  std::optional<IngestionOutcome> outcome_;
};

// Called after B, L, Sigma, and format identity are durable but before the
// publication marker becomes visible. The callback persists namespace/POSIX
// fields for the same version and must be idempotent.
using PrepareIngestionPublication =
  std::function<bool(const IngestionOutcome& outcome)>;

// Orchestrates one complete ingestion while StructuredBlobStorage remains the
// owner of lifecycle legality and visibility. A source must be stable and
// support repeatable reads for the duration of this synchronous call.
class IngestionModule {
public:
  struct Config {
    std::size_t chunk_size{ 1024 * 1024 };
    // Keep transfer/adapter chunk boundaries independent from the durable
    // manifest checkpoint. Each bounded batch advances its frontier once.
    std::size_t payload_batch_max_bytes{ 8 * 1024 * 1024 };
  };

  IngestionModule(StructuredBlobStorage& storage,
    const FormatRegistry& formats, Config config);

  IngestionResult ingest(const StructuredBlobVersionKey& key,
    const FormatSource& source,
    PrepareIngestionPublication prepare_publication) const;

private:
  StructuredBlobStorage& storage_;
  const FormatRegistry& formats_;
  Config config_;
};

} // namespace iotdbfs
