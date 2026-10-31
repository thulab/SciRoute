#include "iotdbfs/Ingestion.h"
#include "iotdbfs/PublicationProfile.h"

#include <algorithm>
#include <exception>
#include <limits>
#include <memory>
#include <utility>

namespace iotdbfs {
namespace {

template <typename F>
decltype(auto) publication_stage(const char* name, F&& action) {
  PublicationStageScope stage(name);
  return action();
}

IngestionResult storage_failure(const char* operation,
  const StructuredBlobStatus& status) {
  return IngestionResult::failure(IngestionError::StorageFailure,
    std::string(operation) + ": " + status.message());
}

enum class SourceReadIssue { None, OutOfRange, ShortOrFailed };

IngestionResult source_read_failure(SourceReadIssue issue) {
  if (issue == SourceReadIssue::OutOfRange) {
    return IngestionResult::failure(IngestionError::AdapterFailure,
      "format adapter requested an out-of-range source read");
  }
  return IngestionResult::failure(IngestionError::SourceReadFailure,
    "ingestion source returned a short or failed read");
}

} // namespace

IngestionResult IngestionResult::success(IngestionOutcome outcome) {
  IngestionResult result;
  result.outcome_ = std::move(outcome);
  return result;
}

IngestionResult IngestionResult::failure(IngestionError error,
  std::string message) {
  IngestionResult result;
  result.error_ = error;
  result.message_ = std::move(message);
  return result;
}

IngestionModule::IngestionModule(StructuredBlobStorage& storage,
  const FormatRegistry& formats, Config config)
  : storage_(storage), formats_(formats), config_(config) {}

IngestionResult IngestionModule::ingest(
  const StructuredBlobVersionKey& key, const FormatSource& source,
  PrepareIngestionPublication prepare_publication) const {
  if (config_.chunk_size == 0) {
    return IngestionResult::failure(IngestionError::InvalidSource,
      "ingestion chunk size must be greater than zero");
  }
  if (config_.payload_batch_max_bytes < config_.chunk_size ||
      config_.payload_batch_max_bytes > kMaxPayloadWriteBytesPerBatch) {
    return IngestionResult::failure(IngestionError::InvalidSource,
      "payload batch budget must contain one chunk and not exceed 8 MiB");
  }
  if (source.size != 0 && !source.read) {
    return IngestionResult::failure(IngestionError::InvalidSource,
      "non-empty ingestion source has no reader");
  }
  if (!prepare_publication) {
    return IngestionResult::failure(IngestionError::InvalidSource,
      "ingestion publication preparation is not configured");
  }

  bool began = false;
  const auto abort = [&]() {
    if (!began) return;
    const auto ignored = publication_stage("abort", [&] { return storage_.abort(key); });
    (void)ignored;
  };

  try {
    SourceReadIssue read_issue = SourceReadIssue::None;
    const auto lifetime = std::make_shared<int>(0);
    const std::weak_ptr<int> live_source = lifetime;
    // The same bounded, exact reader is used for detection, streaming copy,
    // and finish-time adapter lookups. It always reads the original stable
    // source; an unpublished backend version is not a valid source.
    const FormatSource checked_source{ source.size,
      [&, live_source](std::uint64_t offset, std::uint64_t length)
          -> std::optional<std::vector<std::uint8_t>> {
        // An accidentally retained callback must not dereference source or
        // read_issue after this synchronous ingest call has returned.
        if (live_source.expired()) return std::nullopt;
        if (offset > source.size || length > source.size - offset ||
            length > static_cast<std::uint64_t>(
              std::numeric_limits<std::size_t>::max())) {
          if (read_issue == SourceReadIssue::None)
            read_issue = SourceReadIssue::OutOfRange;
          return std::nullopt;
        }
        if (length == 0) return std::vector<std::uint8_t>{};
        if (!source.read) {
          read_issue = SourceReadIssue::ShortOrFailed;
          return std::nullopt;
        }
        auto bytes = source.read(offset, length);
        if (!bytes || bytes->size() != static_cast<std::size_t>(length)) {
          // A real source failure takes precedence over an earlier probe
          // over-read by this same candidate.
          read_issue = SourceReadIssue::ShortOrFailed;
          return std::nullopt;
        }
        return bytes;
      } };

    // An out-of-range read during detection means only that this candidate
    // cannot match the source. Check it after each candidate, even if a buggy
    // matcher claims success after the failed read. Real source failures stay
    // fatal. The registry's ids preserve registration order.
    std::shared_ptr<const FormatAdapter> adapter;
    std::unique_ptr<FormatIngestionSession> format_session;
    {
      PublicationStageScope detection_stage("format_detection");
      for (const auto& id : formats_.ids()) {
        const auto candidate = formats_.resolve_by_id(id);
        if (!candidate)
          return IngestionResult::failure(IngestionError::AdapterFailure,
            "format registry changed during content detection");
        read_issue = SourceReadIssue::None;
        const bool matched = candidate->matches(checked_source);
        if (read_issue == SourceReadIssue::ShortOrFailed)
          return source_read_failure(read_issue);
        if (read_issue == SourceReadIssue::OutOfRange) {
          read_issue = SourceReadIssue::None;
          continue;
        }
        if (matched) {
          adapter = candidate;
          break;
        }
      }
      if (adapter) {
        format_session = adapter->begin_ingestion(checked_source);
        if (read_issue != SourceReadIssue::None)
          return source_read_failure(read_issue);
        if (!format_session) {
          return IngestionResult::failure(IngestionError::AdapterFailure,
            "selected format adapter did not create an ingestion session");
        }
      }
    }

    auto status = publication_stage("begin", [&] { return storage_.begin(key); });
    if (!status.ok()) return storage_failure("begin ingestion", status);
    began = true;

    for (std::uint64_t offset = 0; offset < source.size;) {
      PublicationStageScope payload_stage("payload");
      std::vector<StructuredBlobBytes> chunks;
      chunks.reserve(kMaxPayloadWriteChunksPerBatch);
      std::uint64_t batch_bytes = 0;
      while (chunks.size() < kMaxPayloadWriteChunksPerBatch &&
          batch_bytes < source.size - offset) {
        const auto wanted = std::min<std::uint64_t>(
          source.size - offset - batch_bytes, config_.chunk_size);
        if (wanted > config_.payload_batch_max_bytes - batch_bytes) break;
        auto bytes = [&] {
          PublicationDetailTimer read_timer("payload_source_read");
          auto result = checked_source.read(offset + batch_bytes, wanted);
          read_timer.finish(result.has_value() && read_issue == SourceReadIssue::None);
          return result;
        }();
        if (read_issue != SourceReadIssue::None) {
          abort();
          return source_read_failure(read_issue);
        }
        chunks.push_back(std::move(*bytes));
        batch_bytes += wanted;
      }

      status = storage_.write_payload_batch(key, offset, chunks);
      if (!status.ok()) {
        abort();
        return storage_failure("write ingestion payload", status);
      }
      if (format_session) {
        auto chunk_offset = offset;
        for (const auto& bytes : chunks) {
          auto consumed = [&] {
            PublicationDetailTimer consume_timer("payload_adapter_consume");
            auto result = format_session->consume_chunk(
              chunk_offset, bytes.data(), bytes.size());
            consume_timer.finish(result.ok() && read_issue == SourceReadIssue::None);
            return result;
          }();
          if (read_issue != SourceReadIssue::None) {
            abort();
            return source_read_failure(read_issue);
          }
          if (!consumed.ok()) {
            abort();
            return IngestionResult::failure(IngestionError::AdapterFailure,
              consumed.error());
          }
          chunk_offset += bytes.size();
        }
      }
      offset += batch_bytes;
    }

    FormatIngestionArtifacts artifacts;
    if (format_session) {
      PublicationStageScope finish_stage("format_finish");
      auto derived = format_session->finish();
      if (read_issue != SourceReadIssue::None) {
        abort();
        return source_read_failure(read_issue);
      }
      if (!derived.ok()) {
        abort();
        return IngestionResult::failure(IngestionError::AdapterFailure,
          derived.error());
      }
      artifacts = std::move(derived.value());
    }

    status = publication_stage("seal", [&] { return storage_.seal_payload(key); });
    if (!status.ok()) {
      abort();
      return storage_failure("seal ingestion payload", status);
    }
    status = publication_stage("layout", [&] { return storage_.write_layout(key, artifacts.layout); });
    if (!status.ok()) {
      abort();
      return storage_failure("write ingestion layout", status);
    }

    IngestionOutcome outcome;
    if (adapter) outcome.format_id = adapter->id();
    outcome.metadata = artifacts.metadata;
    if (artifacts.dataset_catalog) {
      outcome.metadata["__iotdbfs.dataset_catalog.protocol"] = MetadataValue(artifacts.dataset_catalog->protocol);
      outcome.metadata["__iotdbfs.dataset_catalog.layout_digest"] = MetadataValue(artifacts.dataset_catalog->layout_digest);
    }
    outcome.payload_size = source.size;
    outcome.layout_size = artifacts.layout.size();
    status = publication_stage("description", [&] {
      return storage_.write_metadata_and_format_identity(key,
        outcome.format_id, outcome.metadata, std::move(artifacts.dataset_catalog),
        &outcome.metadata);
    });
    if (!status.ok()) {
      abort();
      return storage_failure("write ingestion description", status);
    }
    if (!publication_stage("namespace_prepare", [&] { return prepare_publication(outcome); })) {
      abort();
      return IngestionResult::failure(
        IngestionError::PublicationPreparationFailure,
        "ingestion publication preparation failed");
    }
    status = publication_stage("publish", [&] { return storage_.publish(key); });
    if (!status.ok()) {
      abort();
      return storage_failure("publish ingestion", status);
    }
    began = false;
    return IngestionResult::success(std::move(outcome));
  }
  catch (const std::exception& error) {
    abort();
    return IngestionResult::failure(IngestionError::SourceReadFailure,
      std::string("ingestion raised an exception: ") + error.what());
  }
  catch (...) {
    abort();
    return IngestionResult::failure(IngestionError::SourceReadFailure,
      "ingestion raised an unknown exception");
  }
}

} // namespace iotdbfs
