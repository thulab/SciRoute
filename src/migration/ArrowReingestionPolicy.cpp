#include "iotdbfs/migration/ArrowReingestionPolicy.h"
#include "iotdbfs/formats/arrow/ArrowLayout.h"
#include <algorithm>
#include <set>
#include <sstream>

namespace iotdbfs {
namespace {
constexpr std::uint64_t kChunk = 1024 * 1024;
bool expected_profile(const StructuredBlobManifest& manifest) {
  const auto container = manifest.metadata.find("arrow.container");
  const auto version = manifest.metadata.find("arrow.layout_version");
  if (container == manifest.metadata.end() || version == manifest.metadata.end()) return false;
  const auto* text = std::get_if<std::string>(&container->second.value());
  const auto* number = std::get_if<std::int64_t>(&version->second.value());
  return text && *text == "stream" && number && *number == 1;
}
// These summaries participate in file-metadata filtering. Matching manifest
// and inode metadata is insufficient unless their counts also match the layout.
bool expected_summary(const MetadataMap& metadata, const ArrowLayoutDocument& layout) {
  const auto count_matches = [&](const char* name, std::uint64_t expected) {
    const auto found = metadata.find(name);
    if (found == metadata.end()) return false;
    const auto* value = std::get_if<std::int64_t>(&found->second.value());
    return value && *value >= 0 && static_cast<std::uint64_t>(*value) == expected;
  };
  return count_matches("arrow.record_batches", layout.batches.size()) &&
    count_matches("arrow.rows", layout.total_rows) &&
    count_matches("arrow.fields", layout.fields.size());
}
void describe(const ArrowLayoutDocument& layout, MigrationInspection& result) {
  std::ostringstream out;
  out << "{\"container\":\"stream\",\"batches\":" << layout.batches.size()
      << ",\"rows\":" << layout.total_rows << ",\"fields\":[";
  bool first = true; std::size_t supported = 0;
  for (const auto& field : layout.fields) {
    if (!first) out << ','; first = false;
    out << "{\"name\":" << migration_json_quote(field.name)
        << ",\"type\":" << migration_json_quote(field.native_type)
        << ",\"list_depth\":" << field.list_depth
        << ",\"unsupported_reason\":" << migration_json_quote(field.unsupported_reason) << '}';
    if ((field.list_depth == 1 || field.list_depth == 2) && field.unsupported_reason.empty()) ++supported;
  }
  out << "],\"compression\":["; first = true;
  std::set<ArrowBufferCompression> codecs;
  for (const auto& message : layout.messages)
    if (message.kind == ArrowMessageKind::RecordBatch) codecs.insert(message.compression);
  for (auto codec : codecs) {
    if (!first) out << ','; first = false;
    out << migration_json_quote(codec == ArrowBufferCompression::None ? "none" :
      codec == ArrowBufferCompression::Lz4 ? "lz4" : "zstd");
  }
  out << "],\"queryable_fields\":" << supported << '}';
  result.profile_json = out.str();
  result.partially_queryable = supported > 0 && supported < layout.fields.size();
  result.detail = "stream; " + std::to_string(layout.batches.size()) + " batches; " +
    std::to_string(layout.total_rows) + " rows; " + std::to_string(layout.fields.size()) + " fields";
}
class ArrowPolicy final : public ReingestionPolicy {
public:
  std::string adapter_id() const override { return "arrow"; }
  std::string layout_protocol() const override { return "IOTDBFS-ARROW-LAYOUT/1"; }
  MigrationInspection inspect(const StructuredBlobReadHandle& source,
      const FormatRegistry& formats) const override {
    MigrationInspection result;
    const auto& manifest = source.manifest();
    if (manifest.format_id) {
      if (*manifest.format_id != adapter_id()) {
        result.kind = MigrationKind::OtherFormat; result.detail = "published format identity: " + *manifest.format_id; return result;
      }
      if (!expected_profile(manifest) || !manifest.layout_size || manifest.layout_size > 64ULL*1024*1024) {
        result.detail = "Arrow identity has no valid bounded stream/layout profile"; return result;
      }
      auto bytes = source.read_layout_range(0, manifest.layout_size);
      if (!bytes.ok()) {result.detail = bytes.status().message(); return result;}
      auto layout = parse_arrow_layout(bytes.value(), manifest.payload_size);
      if (!layout.ok()) {result.detail = "invalid Arrow layout: " + layout.status().message(); return result;}
      if (!expected_summary(manifest.metadata, layout.value())) {
        result.detail = "Arrow manifest summary disagrees with layout contents"; return result;
      }
      describe(layout.value(), result); result.layout_sha256 = migration_sha256(bytes.value());
      result.kind = MigrationKind::AlreadyIndexed; return result;
    }
    auto adapter = formats.resolve_by_id(adapter_id());
    if (!adapter) {result.detail = "Arrow adapter is not registered"; return result;}
    bool read_failed = false;
    FormatSource input{manifest.payload_size,
      [&](std::uint64_t offset, std::uint64_t length) -> std::optional<StructuredBlobBytes> {
        if (offset > manifest.payload_size || length > manifest.payload_size - offset) return std::nullopt;
        auto bytes = source.read_payload_range(offset, length);
        if (!bytes.ok() || bytes.value().size() != length) {read_failed = true; return std::nullopt;}
        return std::move(bytes.value());
      }};
    if (!adapter->matches(input)) {
      bool recognized = false;
      if (!read_failed && manifest.payload_size >= 6) {
        auto magic = input.read(0, 6); recognized = magic && *magic == StructuredBlobBytes{'A','R','R','O','W','1'};
      }
      if (!read_failed && manifest.payload_size >= 4) {
        auto magic = input.read(0, 4); recognized |= magic && *magic == StructuredBlobBytes{0xff,0xff,0xff,0xff};
      }
      result.kind = read_failed ? MigrationKind::Error : recognized ? MigrationKind::Unsupported : MigrationKind::NotFormat;
      result.detail = read_failed ? "source read failed during Arrow detection" : recognized ?
        "Arrow IPC outside the supported stream profile" : "content is not an Arrow IPC stream";
      return result;
    }
    auto session = adapter->begin_ingestion(input);
    if (!session) {result.detail = "Arrow inspection could not start"; return result;}
    for (std::uint64_t offset = 0; offset < manifest.payload_size;) {
      auto bytes = input.read(offset, std::min(kChunk, manifest.payload_size-offset));
      if (!bytes) {result.detail = "source read failed during Arrow inspection"; return result;}
      auto consumed = session->consume_chunk(offset, bytes->data(), bytes->size());
      if (!consumed.ok()) {
        result.kind = read_failed ? MigrationKind::Error : MigrationKind::Unsupported;
        result.detail = consumed.error(); return result;
      }
      offset += bytes->size();
    }
    auto artifacts = session->finish();
    if (!artifacts.ok() || read_failed) {
      result.kind = read_failed ? MigrationKind::Error : MigrationKind::Unsupported;
      result.detail = read_failed ? "source read failed during Arrow inspection" : artifacts.error(); return result;
    }
    auto layout = parse_arrow_layout(artifacts.value().layout, manifest.payload_size);
    if (!layout.ok()) {result.detail = layout.status().message(); return result;}
    if (!expected_summary(artifacts.value().metadata, layout.value())) {
      result.detail = "derived Arrow summary disagrees with layout contents"; return result;
    }
    describe(layout.value(), result);
    if (!std::any_of(layout.value().fields.begin(), layout.value().fields.end(), [](const auto& field) {
        return (field.list_depth == 1 || field.list_depth == 2) && field.unsupported_reason.empty();
      })) {result.kind = MigrationKind::Unsupported; result.detail = "no supported Float32 List field"; return result;}
    result.layout_sha256 = migration_sha256(artifacts.value().layout);
    result.kind = MigrationKind::Candidate; return result;
  }
};
} // namespace
std::shared_ptr<const ReingestionPolicy> make_arrow_reingestion_policy() {
  return std::make_shared<ArrowPolicy>();
}
} // namespace iotdbfs
