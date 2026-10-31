#include "iotdbfs/DatasetSubstructureIndex.h"
#include "iotdbfs/PathUtils.h"

#include <openssl/evp.h>
#include <algorithm>
#include <limits>
#include <memory>

namespace iotdbfs {
namespace {
constexpr const char* prefix = "__iotdbfs.dataset_index.";
StructuredBlobStatus invalid(const std::string& message) {
  return StructuredBlobStatus::failure(StructuredBlobError::InvalidState, message);
}
const MetadataValue* field(const StructuredBlobManifest& manifest, const char* name) {
  const auto found = manifest.metadata.find(std::string(prefix) + name);
  return found == manifest.metadata.end() ? nullptr : &found->second;
}
bool text(const StructuredBlobManifest& manifest, const char* name, std::string& value) {
  const auto* f = field(manifest, name);
  if (!f || f->type() != MetadataValueType::String) return false;
  value = std::get<std::string>(f->value());
  return true;
}
bool number(const StructuredBlobManifest& manifest, const char* name, std::uint64_t& value) {
  const auto* f = field(manifest, name);
  if (!f || f->type() != MetadataValueType::Int64) return false;
  const auto n = std::get<std::int64_t>(f->value());
  if (n < 0) return false;
  value = static_cast<std::uint64_t>(n);
  return true;
}
StructuredBlobResult<DatasetSubstructureSeal> compute(
    const StructuredBlobManifest& manifest, const DatasetCatalogArtifact& artifact) {
  using Result = StructuredBlobResult<DatasetSubstructureSeal>;
  if (manifest.key.path.empty() || manifest.key.path.front() != '/' ||
      !manifest.format_id || *manifest.format_id != artifact.format_id)
    return Result::failure(StructuredBlobError::InvalidState,
      "dataset index source or format identity is invalid");
  auto bytes = serialize_dataset_catalog(artifact);
  if (!bytes.ok()) return Result::failure(StructuredBlobError::InvalidState, bytes.error());
  DatasetSubstructureSeal seal;
  seal.bytes = bytes.value().size(); seal.records = artifact.records.size();
  for (const auto& r : artifact.records) seal.attributes += r.attributes.size();

  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> ctx(EVP_MD_CTX_new(), EVP_MD_CTX_free);
  bool okay = ctx && EVP_DigestInit_ex(ctx.get(), EVP_sha256(), nullptr) == 1;
  const auto update = [&](const void* data, std::size_t n) {
    if (okay && EVP_DigestUpdate(ctx.get(), data, n) != 1) okay = false;
  };
  const auto integer = [&](std::uint64_t n) {
    unsigned char encoded[8];
    for (unsigned i = 0; i != 8; ++i) encoded[i] = static_cast<unsigned char>(n >> (8 * i));
    update(encoded, sizeof(encoded));
  };
  const auto string = [&](const std::string& s) { integer(s.size()); update(s.data(), s.size()); };
  string(kDatasetSubstructureIndexProtocol);
  string(path_utils::norm(manifest.key.path));
  integer(static_cast<std::uint64_t>(manifest.key.version));
  string(*manifest.format_id);
  integer(manifest.payload_size); integer(manifest.layout_size);
  integer(seal.bytes); update(bytes.value().data(), bytes.value().size());
  unsigned char digest[EVP_MAX_MD_SIZE]; unsigned length = 0;
  if (!okay || EVP_DigestFinal_ex(ctx.get(), digest, &length) != 1 || length != 32)
    return Result::failure(StructuredBlobError::BackendFailure, "cannot hash dataset substructure index");
  constexpr char hex[] = "0123456789abcdef";
  for (unsigned i = 0; i != length; ++i) {
    seal.digest += hex[digest[i] >> 4]; seal.digest += hex[digest[i] & 15];
  }
  return Result::success(std::move(seal));
}
} // namespace

StructuredBlobStatus seal_dataset_substructure_index(
    StructuredBlobManifest& manifest, const DatasetCatalogArtifact& artifact) {
  auto computed = compute(manifest, artifact);
  if (!computed.ok()) return computed.status();
  const auto& seal = computed.value();
  manifest.metadata[std::string(prefix) + "protocol"] = MetadataValue(kDatasetSubstructureIndexProtocol);
  manifest.metadata[std::string(prefix) + "sha256"] = MetadataValue(seal.digest);
  manifest.metadata[std::string(prefix) + "bytes"] = MetadataValue(static_cast<std::int64_t>(seal.bytes));
  manifest.metadata[std::string(prefix) + "records"] = MetadataValue(static_cast<std::int64_t>(seal.records));
  manifest.metadata[std::string(prefix) + "attributes"] = MetadataValue(static_cast<std::int64_t>(seal.attributes));
  return StructuredBlobStatus::success();
}

StructuredBlobResult<DatasetSubstructureSeal> read_dataset_substructure_seal(
    const StructuredBlobManifest& manifest) {
  using Result = StructuredBlobResult<DatasetSubstructureSeal>;
  DatasetSubstructureSeal seal;
  std::string protocol;
  if (!text(manifest, "protocol", protocol) || protocol != kDatasetSubstructureIndexProtocol ||
      !text(manifest, "sha256", seal.digest) || seal.digest.size() != 64 ||
      !std::all_of(seal.digest.begin(), seal.digest.end(), [](char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
      }) || !number(manifest, "bytes", seal.bytes) || !seal.bytes || seal.bytes > kMaxDatasetCatalogBytes ||
      !number(manifest, "records", seal.records) || !number(manifest, "attributes", seal.attributes) ||
      seal.records > seal.bytes || seal.attributes > seal.bytes)
    return Result::failure(StructuredBlobError::InvalidState,
      "dataset substructure publication seal is missing or invalid; reingest the source");
  return Result::success(std::move(seal));
}

StructuredBlobStatus validate_dataset_substructure_index(
    const StructuredBlobManifest& manifest, const DatasetCatalogArtifact& artifact) {
  auto expected = read_dataset_substructure_seal(manifest);
  if (!expected.ok()) return expected.status();
  auto observed = compute(manifest, artifact);
  if (!observed.ok()) return observed.status();
  const auto& a = expected.value(); const auto& b = observed.value();
  if (a.digest != b.digest || a.bytes != b.bytes || a.records != b.records || a.attributes != b.attributes)
    return invalid("dataset substructure index differs from its sealed source version");
  return StructuredBlobStatus::success();
}
} // namespace iotdbfs
