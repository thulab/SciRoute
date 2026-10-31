#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5SemanticBinding.h"
#include "iotdbfs/MetadataSerialization.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <limits>
#include <openssl/sha.h>
#include <stdexcept>

namespace iotdbfs {
namespace {

Hdf5LayoutDocument unavailable(std::uint64_t size) {
  Hdf5LayoutDocument result;
  result.source_size = size;
  result.directory_complete = false;
  result.index_unavailable_reason = "resource_limit";
  return result;
}

MetadataValue count_value(std::uint64_t value) {
  if (value >
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
    throw std::runtime_error(
        "HDF5 metadata count exceeds signed representation");
  return MetadataValue(static_cast<std::int64_t>(value));
}

class Hdf5IngestionSession final : public FormatIngestionSession {
public:
  Hdf5IngestionSession(FormatSource source, Hdf5LayoutLimits limits,
                       Hdf5SourceOptions source_options)
      : source_(std::move(source)), limits_(limits),
        source_options_(std::move(source_options)) {}

  FormatStatus consume_chunk(std::uint64_t offset, const std::uint8_t *data,
                             std::size_t length) override {
    if (!error_.empty())
      return FormatStatus::failure(error_);
    if (finished_)
      return fail("HDF5 ingestion session is already finished");
    if (offset != consumed_ || consumed_ > source_.size ||
        length > source_.size - consumed_ || (length && !data))
      return fail(
          "HDF5 ingestion received a nonsequential or invalid source chunk");
    consumed_ += length;
    return FormatStatus::success();
  }

  FormatResult<FormatIngestionArtifacts> finish() override {
    using Result = FormatResult<FormatIngestionArtifacts>;
    if (!error_.empty())
      return Result::failure(error_);
    if (finished_)
      return Result::failure("HDF5 ingestion session is already finished");
    finished_ = true;
    if (consumed_ != source_.size) {
      error_ = "HDF5 ingestion stream ended before the complete source";
      return Result::failure(error_);
    }
    try {
      Hdf5LayoutDocument document;
      std::uint64_t inventory_read_bytes = 0;
      // Count at the source boundary, including library opening reads before
      // the inventory callback. An opening-budget failure can still publish
      // an unavailable marker and must not claim those reads were zero.
      FormatSource counted_source{
          source_.size,
          [&](std::uint64_t offset,
              std::uint64_t length) -> std::optional<BinaryValue> {
            if (!source_.read)
              return std::nullopt;
            auto bytes = source_.read(offset, length);
            if (bytes) {
              if (bytes->size() > std::numeric_limits<std::uint64_t>::max() -
                                      inventory_read_bytes)
                throw std::runtime_error(
                    "HDF5 inventory read counter overflow");
              inventory_read_bytes += bytes->size();
            }
            return bytes;
          }};
      auto options = source_options_;
      options.max_read_bytes =
          std::min(options.max_read_bytes, limits_.max_read_bytes);
      auto status =
          with_hdf5_ingestion_source(counted_source, std::move(options),
                                     [&](const Hdf5SourceDriver &driver) {
                                       auto inventory =
                                           driver.inventory(limits_);
                                       if (!inventory.ok())
                                         return inventory.status();
                                       document = std::move(inventory.value());
                                       return QueryStatus::success();
                                     });
      if (!status.ok()) {
        if (status.code() != QueryError::ResourceExhausted)
          throw std::runtime_error(status.message());
        document = unavailable(source_.size);
      }
      auto encoded = serialize_hdf5_layout(document, limits_);
      if (!encoded.ok() &&
          encoded.status().code() == QueryError::ResourceExhausted) {
        document = unavailable(source_.size);
        encoded = serialize_hdf5_layout(document, limits_);
      }
      if (!encoded.ok())
        throw std::runtime_error(encoded.status().message());
      FormatIngestionArtifacts artifacts;
      artifacts.layout = std::move(encoded.value());

      DatasetCatalogArtifact catalog;
      catalog.format_id = "hdf5";
      catalog.layout_protocol = kHdf5LayoutMagic;
      catalog.directory_complete = document.directory_complete;
      unsigned char hash[SHA256_DIGEST_LENGTH];
      SHA256(artifacts.layout.data(), artifacts.layout.size(), hash);
      constexpr char hex[] = "0123456789abcdef";
      for (auto b : hash) {
        catalog.layout_digest += hex[b >> 4];
        catalog.layout_digest += hex[b & 15];
      }
      const auto utc_second = verified_hdf5_geos_utc_second(document);
      for (const auto &o : document.objects)
        if (o.dataset)
          catalog.records.push_back(
              hdf5_dataset_catalog_record(o, o.dataset->descriptor.path));
      artifacts.dataset_catalog = std::move(catalog);
      artifacts.metadata = {
          {"hdf5.container", MetadataValue("hdf5")},
          {"hdf5.profile", MetadataValue(document.file_profile)},
          {"hdf5.layout_version", MetadataValue(std::int64_t(5))},
          {"hdf5.directory_complete",
           MetadataValue(document.directory_complete)},
          {"hdf5.inventory_read_bytes", count_value(inventory_read_bytes)},
      };
      if (utc_second) {
        artifacts.metadata.emplace("__iotdbfs.time_coverage.protocol",
                                   MetadataValue("utc-point/1"));
        artifacts.metadata.emplace("__iotdbfs.time_coverage.dataset_path",
                                   MetadataValue("/T"));
        artifacts.metadata.emplace("__iotdbfs.time_coverage.utc_second",
                                   MetadataValue(*utc_second));
        artifacts.metadata.emplace("__iotdbfs.time_coverage.layout_digest",
                                   MetadataValue(artifacts.dataset_catalog->layout_digest));
      }
      if (document.directory_complete) {
        std::uint64_t datasets = 0, queryable = 0;
        bool indexes_complete = true;
        for (const auto &object : document.objects) {
          if (!object.dataset)
            continue;
          ++datasets;
          const auto &dataset = *object.dataset;
          if (dataset.descriptor.supported && dataset.index_complete)
            ++queryable;
          if (dataset.descriptor.supported && !dataset.index_complete)
            indexes_complete = false;
        }
        artifacts.metadata.emplace("hdf5.datasets", count_value(datasets));
        artifacts.metadata.emplace("hdf5.queryable_datasets",
                                   count_value(queryable));
        artifacts.metadata.emplace("hdf5.index_complete",
                                   MetadataValue(indexes_complete));
        std::uint64_t attributes_total = 0, attributes_retained = 0,
                      scale_candidates = 0;
        bool counts_known = true, attributes_complete = true,
             values_complete = true, scales_complete = true;
        for (const auto &object : document.objects) {
          if (!object.attribute_count_known ||
              object.attribute_count >
                  static_cast<std::uint64_t>(INT64_MAX) - attributes_total)
            counts_known = false;
          else
            attributes_total += object.attribute_count;
          attributes_retained += object.attributes.size();
          scale_candidates += object.dimension_scales.size();
          attributes_complete =
              attributes_complete && object.attributes_complete;
          values_complete = values_complete && object.attributes_complete;
          scales_complete = scales_complete && object.dimension_scales_complete;
          for (const auto &attribute : object.attributes)
            values_complete = values_complete &&
                              attribute.state == Hdf5AttributeState::Complete;
        }
        // File-level aggregate facts remain distinct from typed object
        // attributes.
        if (counts_known)
          artifacts.metadata.emplace("hdf5.attributes_total",
                                     count_value(attributes_total));
        artifacts.metadata.emplace("hdf5.attribute_counts_known",
                                   MetadataValue(counts_known));
        artifacts.metadata.emplace("hdf5.attributes_retained",
                                   count_value(attributes_retained));
        artifacts.metadata.emplace("hdf5.attributes_complete",
                                   MetadataValue(attributes_complete));
        artifacts.metadata.emplace("hdf5.attribute_values_complete",
                                   MetadataValue(values_complete));
        artifacts.metadata.emplace("hdf5.dimension_scale_candidates",
                                   count_value(scale_candidates));
        artifacts.metadata.emplace("hdf5.dimension_scales_complete",
                                   MetadataValue(scales_complete));
      } else {
        // Absence of counts means unknown; a truncated inventory must never
        // become an exact negative dataset filter with count=0.
        artifacts.metadata.emplace("hdf5.index_complete", MetadataValue(false));
        artifacts.metadata.emplace("hdf5.index_status",
                                   MetadataValue("resource_limit"));
        artifacts.metadata.emplace("hdf5.attribute_counts_known",
                                   MetadataValue(false));
        artifacts.metadata.emplace("hdf5.attributes_complete",
                                   MetadataValue(false));
        artifacts.metadata.emplace("hdf5.attribute_values_complete",
                                   MetadataValue(false));
        artifacts.metadata.emplace("hdf5.dimension_scales_complete",
                                   MetadataValue(false));
      }
      return Result::success(std::move(artifacts));
    } catch (const std::exception &exception) {
      error_ = exception.what();
    } catch (...) {
      error_ = "HDF5 ingestion inventory callback threw";
    }
    return Result::failure(error_);
  }

private:
  FormatStatus fail(std::string message) {
    error_ = std::move(message);
    return FormatStatus::failure(error_);
  }
  FormatSource source_;
  Hdf5LayoutLimits limits_;
  Hdf5SourceOptions source_options_;
  std::uint64_t consumed_{0};
  bool finished_{false};
  std::string error_;
};

} // namespace

DatasetCatalogRecord
hdf5_dataset_catalog_record(const Hdf5LayoutObject &object,
                            const std::string &canonical_path) {
  DatasetCatalogRecord record;
  record.object_id = object.id;
  record.canonical_path = canonical_path;
  record.attributes_complete = object.attributes_complete;
  for (const auto &a : object.attributes) {
    DatasetCatalogAttribute out;
    out.name = a.name;
    out.shape = a.shape;
    out.reason = a.reason;
    out.state = a.state == Hdf5AttributeState::TooLarge
                    ? DatasetAttributeState::TooLarge
                : a.state == Hdf5AttributeState::InvalidValue
                    ? DatasetAttributeState::Invalid
                    : DatasetAttributeState::Unsupported;
    if (a.state == Hdf5AttributeState::Complete && !a.null_dataspace) {
      if (a.type == LogicalTypeId::Utf8 && a.strings.size() == 1)
        out.value = a.strings[0];
      else if (a.values.size() == 8) {
        std::uint64_t bits = 0;
        for (unsigned i = 0; i < 8; ++i)
          bits |= std::uint64_t(a.values[i]) << (8 * i);
        if (a.type == LogicalTypeId::UInt64)
          out.value = bits;
        else if (a.type == LogicalTypeId::Int64) {
          std::int64_t v;
          std::memcpy(&v, &bits, 8);
          out.value = v;
        } else if (a.type == LogicalTypeId::Float64) {
          double v;
          std::memcpy(&v, &bits, 8);
          if (std::isfinite(v))
            out.value = v;
        }
      }
      if (out.value) {
        out.state = DatasetAttributeState::Complete;
        out.reason.clear();
      } else
        out.reason = "non_scalar_or_nonfinite_attribute";
    }
    record.attributes.push_back(std::move(out));
  }
  return record;
}

Hdf5FormatAdapter::Hdf5FormatAdapter(Hdf5LayoutLimits limits,
                                     Hdf5SourceOptions source_options)
    : limits_(limits), source_options_(std::move(source_options)) {
  // An impossible artifact budget is configuration failure, not an index
  // overflow. Reject it before IngestionModule can begin copying the payload;
  // all real resource-limit fallbacks still obey the configured hard cap.
  auto marker = serialize_hdf5_layout(unavailable(0), limits_);
  if (!marker.ok())
    throw std::invalid_argument(
        "HDF5 layout limits cannot hold the unavailable marker: " +
        marker.status().message());
}

const std::string &Hdf5FormatAdapter::id() const {
  static const std::string value = "hdf5";
  return value;
}

const std::vector<std::string> &Hdf5FormatAdapter::extensions() const {
  static const std::vector<std::string> value{"h5", "hdf5", "he5"};
  return value;
}

bool Hdf5FormatAdapter::matches(const FormatSource &source) const {
  static constexpr std::array<std::uint8_t, 8> signature{
      {0x89, 'H', 'D', 'F', '\r', '\n', 0x1a, '\n'}};
  if (source.size < signature.size() || !source.read)
    return false;
  std::uint64_t offset = 0;
  while (offset <= source.size && signature.size() <= source.size - offset) {
    auto bytes = source.read(offset, signature.size());
    // Ingestion's checked source makes true short/failed reads fatal even
    // though this bool detection interface reports no candidate match.
    if (!bytes || bytes->size() != signature.size())
      return false;
    if (std::equal(signature.begin(), signature.end(), bytes->begin()))
      return true;
    if (!offset)
      offset = 512;
    else {
      if (offset > std::numeric_limits<std::uint64_t>::max() / 2)
        break;
      offset *= 2;
    }
  }
  return false;
}

std::unique_ptr<FormatIngestionSession>
Hdf5FormatAdapter::begin_ingestion(const FormatSource &source) const {
  return std::make_unique<Hdf5IngestionSession>(source, limits_,
                                                source_options_);
}

FormatStatus Hdf5FormatAdapter::validate_layout_artifact(
    const std::vector<std::uint8_t>& layout, std::uint64_t payload_size) const {
  try {
    if (layout.size() > limits_.max_layout_bytes)
      return FormatStatus::failure("HDF5 layout exceeds validation byte limit");
    if (layout.size() < kHdf5LayoutHeaderBytes ||
        !std::equal(std::begin(kHdf5LayoutMagic), std::end(kHdf5LayoutMagic) - 1, layout.begin()))
      return FormatStatus::failure("HDF5 publication requires layout protocol /5");
    const Hdf5LayoutRead read = [&](std::uint64_t offset, std::uint64_t length) {
      if (offset > layout.size() || length > layout.size() - offset)
        return QueryResult<BinaryValue>::failure(QueryStatus::failure(
          QueryError::CorruptLayout, QueryPhase::Planning, "HDF5 layout validation range exceeds artifact"));
      return QueryResult<BinaryValue>::success(
        BinaryValue(layout.begin() + offset, layout.begin() + offset + length));
    };
    auto parsed = read_hdf5_layout_directory(read, layout.size(), payload_size, limits_);
    if (!parsed.ok()) return FormatStatus::failure(parsed.status().message());
    auto document = parsed.value().document();
    Hdf5LayoutReadBudget budget;
    for (auto& object : document.objects) {
      if (!object.dataset || object.dataset->storage != Hdf5StorageLayout::Chunked ||
          !object.dataset->index_complete || !object.dataset->allocated_chunk_count)
        continue;
      auto& dataset = *object.dataset;
      // A full-domain selection verifies every referenced page's digest, records,
      // offsets and dataset-relative bounds. No source payload callback exists.
      auto chunks = read_hdf5_layout_chunks(read, parsed.value(), object.id,
        std::vector<std::uint64_t>(dataset.descriptor.shape.size(), 0),
        dataset.descriptor.shape, budget, limits_);
      if (!chunks.ok()) return FormatStatus::failure(chunks.status().message());
      dataset.chunks = std::move(chunks.value());
    }
    const auto validated = validate_hdf5_layout(document, limits_);
    return validated.ok() ? FormatStatus::success()
                          : FormatStatus::failure(validated.message());
  } catch (const std::bad_alloc&) {
    return FormatStatus::failure("HDF5 layout validation allocation failed");
  }
}

FormatXattrList Hdf5FormatAdapter::list_xattrs(long long, const std::string &,
                                               const Inode &inode) const {
  if (inode.format_id != id())
    return {};
  auto metadata = deserialize_metadata(inode.metadata);
  if (!metadata.ok())
    return {};
  FormatXattrList result;
  for (const char *name :
       {"hdf5.container", "hdf5.profile", "hdf5.layout_version",
        "hdf5.directory_complete", "hdf5.inventory_read_bytes", "hdf5.datasets",
        "hdf5.queryable_datasets", "hdf5.index_complete", "hdf5.index_status",
        "hdf5.attributes_total", "hdf5.attribute_counts_known",
        "hdf5.attributes_retained", "hdf5.attributes_complete",
        "hdf5.attribute_values_complete", "hdf5.dimension_scale_candidates",
        "hdf5.dimension_scales_complete"}) {
    const auto found = metadata.value().find(name);
    if (found != metadata.value().end())
      result.emplace_back(std::string("user.") + name,
                          metadata_value_text(found->second));
  }
  return result;
}

std::optional<std::string>
Hdf5FormatAdapter::get_xattr(long long row_time, const std::string &path,
                             const Inode &inode,
                             const std::string &name) const {
  for (const auto &attribute : list_xattrs(row_time, path, inode))
    if (attribute.first == name)
      return attribute.second;
  return std::nullopt;
}

} // namespace iotdbfs
