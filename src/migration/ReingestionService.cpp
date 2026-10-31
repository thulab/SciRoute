#include "iotdbfs/migration/ReingestionService.h"
#include "iotdbfs/MetadataConfig.h"
#include "iotdbfs/PathUtils.h"
#include <openssl/evp.h>
#include <algorithm>
#include <array>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace iotdbfs {
namespace {
constexpr std::uint64_t kChunk = 1024 * 1024;
template<class T> StructuredBlobResult<T> failure(std::string message) {
  return StructuredBlobResult<T>::failure(StructuredBlobError::BackendFailure, std::move(message));
}
std::string hex_digest(const unsigned char* bytes, unsigned int size) {
  std::ostringstream out; out << std::hex << std::setfill('0');
  for (unsigned int i=0; i<size; ++i) out << std::setw(2) << static_cast<unsigned>(bytes[i]);
  return out.str();
}
bool inode_matches(const std::optional<Inode>& inode, const StructuredBlobVersionKey& key,
    std::uint64_t bytes, const std::string& format) {
  return inode && !inode->is_dir && inode->path == key.path && inode->row_time == key.version &&
    inode->size >= 0 && static_cast<std::uint64_t>(inode->size) == bytes && inode->format_id == format;
}
} // namespace
ReingestionService::ReingestionService(StructuredBlobStorage& storage,
    const FormatRegistry& formats, ReingestionNamespace name_space,
    std::shared_ptr<const ReingestionPolicy> policy)
  : storage_(storage), formats_(formats), namespace_(std::move(name_space)), policy_(std::move(policy)) {
  if (!policy_ || policy_->adapter_id().empty() || policy_->layout_protocol().empty())
    throw std::invalid_argument("reingestion requires an explicit format policy");
}
StructuredBlobResult<std::string> ReingestionService::hash_payload(
    const StructuredBlobReadHandle& source) const {
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> digest(
    EVP_MD_CTX_new(), EVP_MD_CTX_free);
  if (!digest || EVP_DigestInit_ex(digest.get(), EVP_sha256(), nullptr) != 1)
    return failure<std::string>("cannot initialize SHA-256");
  const auto size = source.manifest().payload_size;
  for (std::uint64_t offset = 0; offset < size;) {
    const auto length = std::min(kChunk, size - offset);
    auto bytes = source.read_payload_range(offset, length);
    if (!bytes.ok() || bytes.value().size() != length)
      return failure<std::string>("cannot read exact published payload for SHA-256");
    if (EVP_DigestUpdate(digest.get(), bytes.value().data(),
          bytes.value().size()) != 1)
      return failure<std::string>("cannot update SHA-256");
    offset += length;
  }
  std::array<unsigned char, EVP_MAX_MD_SIZE> value{};
  unsigned int length = 0;
  if (EVP_DigestFinal_ex(digest.get(), value.data(), &length) != 1)
    return failure<std::string>("cannot finalize SHA-256");
  return StructuredBlobResult<std::string>::success(
    hex_digest(value.data(), length));
}

MigrationInventoryEntry ReingestionService::inspect(const StructuredBlobManifest& manifest) const {
  MigrationInventoryEntry entry; entry.source = manifest.key; entry.bytes = manifest.payload_size;
  if (manifest.format_id && *manifest.format_id != policy_->adapter_id()) {
    entry.kind = MigrationKind::OtherFormat;
    entry.detail = "published format identity: " + *manifest.format_id; return entry;
  }
  auto opened = storage_.open_published_version(manifest.key);
  if (!opened.ok()) {entry.detail = opened.status().message(); return entry;}
  auto inspection = policy_->inspect(opened.value(), formats_);
  entry.kind = inspection.kind; entry.detail = std::move(inspection.detail);
  entry.profile_json = std::move(inspection.profile_json);
  entry.partially_queryable = inspection.partially_queryable;
  entry.layout_sha256 = std::move(inspection.layout_sha256);
  if (entry.kind == MigrationKind::Candidate || entry.kind == MigrationKind::AlreadyIndexed) {
    if (!migration_valid_sha256(entry.layout_sha256)) {
      entry.kind = MigrationKind::Error; entry.detail = "inspection did not verify layout digest"; return entry;
    }
    auto digest = hash_payload(opened.value());
    if (!digest.ok()) {entry.kind = MigrationKind::Error; entry.detail = digest.status().message();}
    else entry.sha256 = std::move(digest.value());
  }
  return entry;
}
StructuredBlobResult<std::vector<MigrationInventoryEntry>>
ReingestionService::inventory(const std::optional<std::string>& path) const {
  if (!namespace_.published_manifests || !namespace_.latest_inode)
    return failure<std::vector<MigrationInventoryEntry>>(
      "migration namespace inventory operations are unavailable");
  auto listed = namespace_.published_manifests();
  if (!listed.ok())
    return failure<std::vector<MigrationInventoryEntry>>(
      listed.status().message());
  std::map<std::string, StructuredBlobManifest> latest;
  std::vector<StructuredBlobManifest> selected;
  for (const auto& manifest : listed.value()) {
    if (path && manifest.key.path != *path) continue;
    selected.push_back(manifest);
    auto at = latest.find(manifest.key.path);
    if (at == latest.end() || at->second.key.version < manifest.key.version)
      latest[manifest.key.path] = manifest;
  }
  std::vector<MigrationInventoryEntry> entries;
  entries.reserve(selected.size());
  for (const auto& manifest : selected) {
    const auto& current = latest.at(manifest.key.path);
    if (manifest.key.version == current.key.version) continue;
    MigrationInventoryEntry entry;
    entry.source = manifest.key;
    entry.bytes = manifest.payload_size;
    entry.kind = MigrationKind::HistoricalVersion;
    entry.detail = "historical version retained; latest is " +
      std::to_string(current.key.version);
    entries.push_back(std::move(entry));
  }
  for (const auto& [name, manifest] : latest) {
    auto inode = namespace_.latest_inode(name);
    if (!inode || inode->row_time != manifest.key.version) {
      MigrationInventoryEntry entry;
      entry.source = manifest.key;
      entry.bytes = manifest.payload_size;
      entry.detail = "latest inode changed during inventory";
      entries.push_back(std::move(entry));
      continue;
    }
    entries.push_back(inspect(manifest));
  }
  std::sort(entries.begin(), entries.end(),
    [](const MigrationInventoryEntry& a,
       const MigrationInventoryEntry& b) {
      return a.source < b.source;
    });
  return StructuredBlobResult<std::vector<MigrationInventoryEntry>>::success(
    std::move(entries));
}

MigrationResult ReingestionService::migrate(const MigrationInventoryEntry& entry,
    BeforeIngest before_ingest) const {
  MigrationResult result;
  result.source = entry.source; result.source_sha256 = entry.sha256;
  result.layout_sha256 = entry.layout_sha256; result.bytes = entry.bytes;
  result.adapter_id = policy_->adapter_id(); result.layout_version = policy_->layout_protocol();
  result.state = "failed";
  auto fail = [&](std::string state, std::string message) {
    result.state = std::move(state); result.error = std::move(message); return result;
  };
  if (entry.kind != MigrationKind::Candidate || !migration_valid_sha256(entry.sha256) ||
      !migration_valid_sha256(entry.layout_sha256))
    return fail("skipped", "inventory entry is not a verified candidate");
  if (!before_ingest || !namespace_.latest_inode || !namespace_.exact_inode ||
      !namespace_.prepare_inode || !namespace_.next_version)
    return fail("failed", "migration publication operations are unavailable");
  auto exact = namespace_.exact_inode(entry.source);
  const auto source_format = exact ? exact->format_id : std::string{};
  if (!source_format.empty() && (!policy_->supports_indexed_reingestion() ||
      source_format != policy_->adapter_id()))
    return fail("conflict", "source indexed format is not eligible for this policy");
  const auto expected_format = source_format.empty() ? std::optional<std::string>{} :
    std::optional<std::string>{source_format};
  auto source_is_latest = [&] {
    return inode_matches(namespace_.latest_inode(entry.source.path), entry.source, entry.bytes, source_format) &&
      inode_matches(namespace_.exact_inode(entry.source), entry.source, entry.bytes, source_format);
  };
  if (!inode_matches(exact, entry.source, entry.bytes, source_format) || !source_is_latest())
    return fail("conflict", "source is no longer the exact latest eligible version");
  auto manifest = storage_.load_manifest(entry.source);
  if (!manifest.ok() || manifest.value().format_id != expected_format || manifest.value().payload_size != entry.bytes)
    return fail("conflict", "source manifest changed after inventory");
  auto current = inspect(manifest.value());
  if (current.kind != MigrationKind::Candidate || current.sha256 != entry.sha256 ||
      current.layout_sha256 != entry.layout_sha256)
    return fail("conflict", "source bytes or derived layout changed after inventory");
  auto opened = storage_.open_published_version(entry.source);
  if (!opened.ok()) return fail("conflict", "source version is no longer readable");
  auto source = std::make_shared<StructuredBlobReadHandle>(std::move(opened.value()));
  const auto next = namespace_.next_version(entry.source.version);
  if (next <= entry.source.version) return fail("failed", "version allocator did not advance");
  result.destination = {entry.source.path, next}; result.state = "started";
  if (!before_ingest(result)) return fail("failed", "could not durably journal destination before ingestion");
  // The journal callback may block; recheck after it and before creating B/L.
  if (!source_is_latest()) return fail("conflict", "source or latest changed before ingestion");
  auto current_hash = hash_payload(*source);
  if (!current_hash.ok() || current_hash.value() != entry.sha256)
    return fail("conflict", "source changed before ingestion");
  FormatSource input{entry.bytes, [source](std::uint64_t offset, std::uint64_t length)
      -> std::optional<StructuredBlobBytes> {
    auto bytes = source->read_payload_range(offset, length);
    if (!bytes.ok() || bytes.value().size() != length) return std::nullopt;
    return std::move(bytes.value());
  }};
  IngestionModule ingestion(storage_, formats_, {});
  bool conflict = false;
  auto ingested = ingestion.ingest(result.destination, input, [&](const IngestionOutcome& outcome) {
    if (!source_is_latest()) {conflict = true; return false;}
    auto now = storage_.load_manifest(entry.source);
    if (!now.ok() || now.value().format_id != expected_format || now.value().payload_size != entry.bytes ||
        now.value().revision != source->manifest().revision) {conflict = true; return false;}
    if (outcome.format_id != std::optional<std::string>(policy_->adapter_id())) return false;
    auto hash = hash_payload(*source);
    if (!hash.ok() || hash.value() != entry.sha256) {conflict = true; return false;}
    auto encoded = serialize_metadata(outcome.metadata, 65536);
    if (!encoded.ok()) return false;
    Inode destination = *exact;
    destination.row_time = next; destination.size = static_cast<long long>(entry.bytes);
    destination.format_id = *outcome.format_id; destination.metadata = std::move(encoded.value());
    destination.ctime = destination.mtime = next;
    return namespace_.prepare_inode(destination);
  });
  if (!ingested.ok()) return fail(conflict ? "conflict" : "failed", conflict ?
    "source or latest changed during reingestion" : "reingestion failed: " + ingested.message());
  result.layout_bytes = ingested.outcome().layout_size; result.state = "published_unverified";
  return reconcile(result);
}

MigrationResult ReingestionService::reconcile(const MigrationResult& journal) const {
  MigrationResult result = journal; result.state = "published_unverified"; result.error.clear();
  auto fail = [&](std::string message) {result.error = std::move(message); return result;};
  if (result.adapter_id != policy_->adapter_id() || result.layout_version != policy_->layout_protocol() ||
      result.source.path.empty() || result.source.path != result.destination.path || result.source.version <= 0 ||
      result.destination.version <= result.source.version || !migration_valid_sha256(result.source_sha256) ||
      !migration_valid_sha256(result.layout_sha256)) return fail("journal identity or format policy mismatch");
  auto source = storage_.open_published_version(result.source);
  auto destination = storage_.open_published_version(result.destination);
  if (!source.ok() || !destination.ok()) return fail("source or destination exact published version is absent or unreadable");
  const auto source_format = source.value().manifest().format_id;
  if ((source_format && (!policy_->supports_indexed_reingestion() ||
       *source_format != policy_->adapter_id())) ||
      destination.value().manifest().format_id != std::optional<std::string>(policy_->adapter_id()) ||
      source.value().manifest().payload_size != result.bytes || destination.value().manifest().payload_size != result.bytes)
    return fail("source/destination manifest or format identity mismatches journal");
  auto old_hash = hash_payload(source.value()); auto new_hash = hash_payload(destination.value());
  if (!old_hash.ok() || !new_hash.ok() || old_hash.value() != result.source_sha256 ||
      new_hash.value() != result.source_sha256) return fail("source or destination payload SHA-256 mismatch");
  auto original = policy_->inspect(source.value(), formats_);
  auto indexed = policy_->inspect(destination.value(), formats_);
  if (original.kind != MigrationKind::Candidate || indexed.kind != MigrationKind::AlreadyIndexed ||
      original.layout_sha256 != result.layout_sha256 || indexed.layout_sha256 != result.layout_sha256)
    return fail("source-derived or destination layout is invalid or differs from journal");
  if (!namespace_.exact_inode ||
      !inode_matches(namespace_.exact_inode(result.source), result.source, result.bytes, source_format.value_or("")) ||
      !inode_matches(namespace_.exact_inode(result.destination), result.destination, result.bytes, policy_->adapter_id()))
    return fail("source/destination namespace inode is absent or inconsistent");
  auto inode = namespace_.exact_inode(result.destination);
  auto metadata = serialize_metadata(destination.value().manifest().metadata, 65536);
  if (!inode || !metadata.ok() || inode->metadata != metadata.value())
    return fail("destination namespace metadata differs from manifest");
  result.destination_sha256 = new_hash.value();
  result.layout_bytes = destination.value().manifest().layout_size;
  result.state = "verified"; return result;
}
} // namespace iotdbfs
