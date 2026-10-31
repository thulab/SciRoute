#include "iotdbfs/migration/Hdf5ReingestionPolicy.h"
#include "iotdbfs/formats/hdf5/Hdf5ChunkDecoder.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"

#include <algorithm>
#include <limits>
#include <sstream>
#include <stdexcept>

namespace iotdbfs {
namespace {
constexpr std::uint64_t kStreamBytes = 1024 * 1024;

template<class T> const T* metadata(const MetadataMap& values, const char* key) {
  const auto found = values.find(key);
  return found == values.end() ? nullptr : std::get_if<T>(&found->second.value());
}
template<class T> bool equals(const MetadataMap& values, const char* key, const T& expected) {
  const auto* actual = metadata<T>(values, key);
  return actual && *actual == expected;
}
void require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}
const char* storage_name(Hdf5StorageLayout value) {
  switch (value) {
    case Hdf5StorageLayout::Contiguous: return "contiguous";
    case Hdf5StorageLayout::Chunked: return "chunked";
    case Hdf5StorageLayout::Compact: return "compact";
    case Hdf5StorageLayout::Virtual: return "virtual";
    default: return "other";
  }
}
void numbers(std::ostream& out, const std::vector<std::uint64_t>& values) {
  out << '[';
  for (std::size_t i = 0; i < values.size(); ++i) { if (i) out << ','; out << values[i]; }
  out << ']';
}

void validate_identity(const MetadataMap& m, const Hdf5LayoutDocument& d) {
  require(equals(m, "hdf5.container", std::string("hdf5")) &&
          equals(m, "hdf5.layout_version", std::int64_t(5)) &&
          equals(m, "hdf5.profile", d.file_profile) && d.file_profile == "HDF5" &&
          equals(m, "hdf5.directory_complete", d.directory_complete),
          "HDF5 manifest identity disagrees with layout /5");
  const auto* bytes = metadata<std::int64_t>(m, "hdf5.inventory_read_bytes");
  require(bytes && *bytes >= 0, "HDF5 inventory read-byte summary is missing or invalid");
  if (!d.directory_complete) {
    require(equals(m, "hdf5.index_complete", false) &&
            equals(m, "hdf5.index_status", std::string("resource_limit")) &&
            !m.count("hdf5.datasets") && !m.count("hdf5.queryable_datasets") &&
            equals(m, "hdf5.attribute_counts_known", false) &&
            equals(m, "hdf5.attributes_complete", false) &&
            equals(m, "hdf5.attribute_values_complete", false) &&
            equals(m, "hdf5.dimension_scales_complete", false) &&
            !m.count("hdf5.attributes_total") && !m.count("hdf5.attributes_retained") &&
            !m.count("hdf5.dimension_scale_candidates"),
            "HDF5 incomplete-directory summary is inconsistent");
    return;
  }
  std::int64_t datasets = 0, supported = 0, attrs = 0, retained = 0, scales = 0;
  bool indexes = true, counts_known = true, names = true, values = true, candidates = true;
  for (const auto& object : d.objects) {
    if (object.dataset) {
      ++datasets;
      supported += object.dataset->descriptor.supported && object.dataset->index_complete;
      if (object.dataset->descriptor.supported && !object.dataset->index_complete) indexes = false;
    }
    if (!object.attribute_count_known || object.attribute_count >
        static_cast<std::uint64_t>(INT64_MAX - attrs)) counts_known = false;
    else attrs += static_cast<std::int64_t>(object.attribute_count);
    retained += static_cast<std::int64_t>(object.attributes.size());
    scales += static_cast<std::int64_t>(object.dimension_scales.size());
    names &= object.attributes_complete;
    values &= object.attributes_complete;
    candidates &= object.dimension_scales_complete;
    for (const auto& attribute : object.attributes)
      values &= attribute.state == Hdf5AttributeState::Complete;
  }
  require(equals(m, "hdf5.datasets", datasets) &&
          equals(m, "hdf5.queryable_datasets", supported) &&
          equals(m, "hdf5.index_complete", indexes) &&
          equals(m, "hdf5.attribute_counts_known", counts_known) &&
          equals(m, "hdf5.attributes_retained", retained) &&
          equals(m, "hdf5.attributes_complete", names) &&
          equals(m, "hdf5.attribute_values_complete", values) &&
          equals(m, "hdf5.dimension_scale_candidates", scales) &&
          equals(m, "hdf5.dimension_scales_complete", candidates) &&
          (counts_known ? equals(m, "hdf5.attributes_total", attrs) : !m.count("hdf5.attributes_total")),
          "HDF5 manifest summary disagrees with layout contents");
}

MigrationInspection inspect_layout(const BinaryValue& layout, std::uint64_t source_size,
    const MetadataMap& metadata, bool indexed) {
  MigrationInspection result;
  Hdf5LayoutLimits limits;
  require(layout.size() <= limits.max_layout_bytes, "HDF5 layout exceeds migration byte limit");
  Hdf5LayoutRead read = [&](std::uint64_t offset, std::uint64_t size) {
    if (offset > layout.size() || size > layout.size() - offset)
      return QueryResult<BinaryValue>::failure(QueryStatus::failure(
        QueryError::ReadFailure, QueryPhase::Storage, "HDF5 migration layout read exceeds bytes"));
    return QueryResult<BinaryValue>::success(BinaryValue(layout.begin() + offset, layout.begin() + offset + size));
  };
  auto parsed = read_hdf5_layout_directory(read, layout.size(), source_size, limits);
  require(parsed.ok(), parsed.ok() ? "" : "invalid HDF5 layout: " + parsed.status().message());
  const auto& directory = parsed.value();
  const auto& document = directory.document();
  validate_identity(metadata, document);
  Hdf5LayoutReadBudget budget;
  std::size_t datasets = 0, queryable = 0;
  std::ostringstream profiles;
  profiles << "{\"directory_complete\":" << (document.directory_complete ? "true" : "false")
    << ",\"directory_reason\":" << migration_json_quote(document.index_unavailable_reason)
    << ",\"datasets\":[";
  for (const auto& object : document.objects) {
    if (!object.dataset) continue;
    const auto& dataset = *object.dataset;
    // Every indexed page is checked, including datasets outside the typed
    // profile. A directory-only check cannot certify persisted layout bytes.
    if (dataset.storage == Hdf5StorageLayout::Chunked && dataset.index_complete) {
      auto records = read_hdf5_layout_chunks(read, directory, object.id,
        std::vector<std::uint64_t>(dataset.descriptor.shape.size(), 0),
        dataset.descriptor.shape, budget, limits);
      require(records.ok(), records.ok() ? "" : "invalid HDF5 chunk pages: " + records.status().message());
      require(records.value().size() == dataset.allocated_chunk_count,
        "HDF5 complete migration chunk scan lost records");
    }
    bool usable = document.directory_complete && dataset.descriptor.supported && dataset.index_complete;
    std::string reason = !document.directory_complete ? document.index_unavailable_reason :
      !dataset.descriptor.supported ? dataset.descriptor.unsupported_reason :
      !dataset.index_complete ? dataset.index_unavailable_reason : "";
    if (usable && dataset.storage == Hdf5StorageLayout::Chunked) {
      auto decoder = Hdf5ChunkDecoder::compile(dataset);
      if (!decoder.ok()) { usable = false; reason = decoder.status().message(); }
    }
    if (usable) ++queryable;
    if (datasets++) profiles << ',';
    profiles << "{\"object_id\":" << migration_json_quote(object.id)
      << ",\"path\":" << migration_json_quote(dataset.descriptor.path)
      << ",\"native_type_class\":" << dataset.native_type_class
      << ",\"element_bytes\":" << dataset.element_bytes
      << ",\"logical_type\":" << migration_json_quote(dataset.descriptor.supported ?
           (dataset.descriptor.value_type == LogicalTypeId::Float32 ? "float32" :
            dataset.descriptor.value_type == LogicalTypeId::Float64 ? "float64" : "int32") : "unsupported")
      << ",\"shape\":";
    numbers(profiles, dataset.descriptor.shape);
    profiles << ",\"storage\":" << migration_json_quote(storage_name(dataset.storage)) << ",\"chunk_shape\":";
    numbers(profiles, dataset.chunk_shape);
    profiles << ",\"filters\":[";
    for (std::size_t i = 0; i < dataset.filters.size(); ++i) {
      if (i) profiles << ',';
      profiles << "{\"id\":" << dataset.filters[i].id << ",\"flags\":" << dataset.filters[i].flags << ",\"parameters\":[";
      for (std::size_t p = 0; p < dataset.filters[i].parameters.size(); ++p) {
        if (p) profiles << ',';
        profiles << dataset.filters[i].parameters[p];
      }
      profiles << "]}";
    }
    profiles << "],\"index_complete\":" << (dataset.index_complete ? "true" : "false")
      << ",\"allocated_chunks\":" << dataset.allocated_chunk_count
      << ",\"queryable\":" << (usable ? "true" : "false")
      << ",\"reason\":" << migration_json_quote(reason) << '}';
  }
  profiles << "],\"dataset_count\":" << datasets << ",\"queryable_dataset_count\":" << queryable << '}';
  result.profile_json = profiles.str();
  result.partially_queryable = queryable > 0 && queryable < datasets;
  result.layout_sha256 = migration_sha256(layout);
  result.kind = indexed ? MigrationKind::AlreadyIndexed : queryable ? MigrationKind::Candidate : MigrationKind::Unsupported;
  result.detail = "HDF5 layout /5; " + std::to_string(queryable) + "/" + std::to_string(datasets) + " datasets queryable";
  if (!document.directory_complete) result.detail += "; directory unavailable: " + document.index_unavailable_reason;
  return result;
}

class Hdf5ReingestionPolicy final : public ReingestionPolicy {
public:
  std::string adapter_id() const override { return "hdf5"; }
  std::string layout_protocol() const override { return "IOTDBFS-HDF5-LAYOUT/5"; }
  bool supports_indexed_reingestion() const override { return true; }
  MigrationInspection inspect(const StructuredBlobReadHandle& source, const FormatRegistry& formats) const override {
    MigrationInspection result;
    try {
      const auto& manifest = source.manifest();
      if (manifest.format_id && *manifest.format_id != "hdf5") {
        result.kind = MigrationKind::OtherFormat;
        result.detail = "published format identity: " + *manifest.format_id;
        return result;
      }
      if (manifest.format_id) {
        const auto* version = metadata<std::int64_t>(manifest.metadata, "hdf5.layout_version");
        require(equals(manifest.metadata, "hdf5.container", std::string("hdf5")) && version,
          "HDF5 published identity is missing its container/layout version");
        const auto expected = "IOTDBFS-HDF5-LAYOUT/" + std::to_string(*version) + "\n";
        require(manifest.layout_size >= expected.size(), "HDF5 layout header is absent or truncated");
        auto prefix = source.read_layout_range(0, expected.size());
        require(prefix.ok(), prefix.ok() ? "" : "cannot read HDF5 layout header: " + prefix.status().message());
        require(std::string(prefix.value().begin(), prefix.value().end()) == expected,
          "HDF5 layout magic and published metadata disagree");
        require(*version >= 1 && *version <= 5, "unsupported HDF5 layout version");
        if (*version == 5) {
          require(manifest.layout_size <= Hdf5LayoutLimits{}.max_layout_bytes,
            "HDF5 layout exceeds migration byte limit");
          auto bytes = source.read_layout_range(0, manifest.layout_size);
          require(bytes.ok(), bytes.ok() ? "" : "cannot read HDF5 layout: " + bytes.status().message());
          require(bytes.value().size() == manifest.layout_size, "short HDF5 layout read");
          return inspect_layout(bytes.value(), manifest.payload_size, manifest.metadata, true);
        }
        // Obsolete private indexes are not interpreted or upgraded in place.
        // Derive the new layout from original B, exactly like pass-through.
      }
      const auto adapter = formats.resolve_by_id("hdf5");
      require(static_cast<bool>(adapter), "HDF5 adapter is not registered");
      bool read_failed = false;
      FormatSource input{manifest.payload_size, [&](std::uint64_t offset, std::uint64_t length) -> std::optional<BinaryValue> {
        if (offset > manifest.payload_size || length > manifest.payload_size - offset) {
          read_failed = true; return std::nullopt;
        }
        auto bytes = source.read_payload_range(offset, length);
        if (!bytes.ok() || bytes.value().size() != length) { read_failed = true; return std::nullopt; }
        return std::move(bytes.value());
      }};
      const bool recognized = adapter->matches(input);
      require(!read_failed, "source read failed during HDF5 content detection");
      if (!recognized) { result.kind = MigrationKind::NotFormat; result.detail = "content is not HDF5"; return result; }
      auto session = adapter->begin_ingestion(input);
      require(static_cast<bool>(session), "HDF5 adapter could not start inspection");
      for (std::uint64_t offset = 0; offset < input.size;) {
        const auto length = std::min(kStreamBytes, input.size - offset);
        auto bytes = input.read(offset, length);
        require(bytes.has_value(), "source read failed during HDF5 inspection");
        auto status = session->consume_chunk(offset, bytes->data(), bytes->size());
        require(status.ok(), "HDF5 inspection failed: " + status.error());
        offset += length;
      }
      auto artifacts = session->finish();
      require(!read_failed, "source read failed during HDF5 metadata inventory");
      require(artifacts.ok(), artifacts.ok() ? "" : "HDF5 metadata inventory failed: " + artifacts.error());
      return inspect_layout(artifacts.value().layout, input.size, artifacts.value().metadata, false);
    } catch (const std::exception& error) { result.detail = error.what(); }
      catch (...) { result.detail = "HDF5 migration inspection failed"; }
    return result;
  }
};
} // namespace

std::shared_ptr<const ReingestionPolicy> make_hdf5_reingestion_policy() {
  return std::make_shared<const Hdf5ReingestionPolicy>();
}
} // namespace iotdbfs
