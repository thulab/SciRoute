// Independent, read-only inspection. Does not open an owner journal or recover
// an owner, create tables, initialize namespaces, or clean staging data.
#include "iotdbfs/storage/versioning/PayloadArtifacts.h"
#include "iotdbfs/storage/versioning/ContentArtifacts.h"
#include "iotdbfs/storage/versioning/IoTDBVersionStore.h"
#include "iotdbfs/storage/versioning/VersionNamespaceRecord.h"
#include "TableSession.h"
#include <openssl/evp.h>
#include <algorithm>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>

using namespace iotdbfs;
using namespace iotdbfs::versioning;
namespace {
void check(bool value, const char* message) {
  if (!value) throw Error(ErrorCode::Corrupt, message);
}
std::string environment(const char* name) {
  const auto* value = std::getenv(name);
  if (!value || !*value) throw Error(ErrorCode::Invalid, "connection environment missing");
  return value;
}
std::string quote(const std::string& value) {
  std::ostringstream out; out << '"'; constexpr char hex[] = "0123456789abcdef";
  for (unsigned char c : value) {
    if (c == '"' || c == '\\') out << '\\' << c;
    else if (c < 32) out << "\\u00" << hex[c >> 4] << hex[c & 15];
    else out << c;
  }
  out << '"'; return out.str();
}
std::string hex(const unsigned char* bytes, unsigned size) {
  constexpr char digits[] = "0123456789abcdef"; std::string result;
  for (unsigned i = 0; i < size; ++i) { result += digits[bytes[i] >> 4]; result += digits[bytes[i] & 15]; }
  return result;
}
struct File {
  std::string path, format, sha256;
  std::int64_t version;
  std::uint64_t size;
  ContentId descriptor, seal, map;
  std::vector<PayloadExtent> extents;
  std::uint64_t payload_batch_queries{0}, payload_block_cells{0}, payload_returned_bytes{0};
};
class Inspector {
public:
  IoTDBVersionStore store;
  Head head;
  Bytes initial_head;
  std::uint64_t metadata_bytes{0}, logical_payload_bytes{0};
  std::map<std::pair<std::string, ContentId>, Bytes> controls;
  std::map<ContentId, std::uint64_t> unique_blocks;
  std::set<ContentId> unique_maps;
  explicit Inspector(const IoTDBClientConfig& config, const std::string& prefix) : store(config, prefix) {
    auto raw = store.read_head(); check(raw.has_value(), "namespace head missing");
    initial_head = *raw; head = decode_head(*raw); metadata_bytes = raw->size();
  }
  const Bytes& get(const std::string& type, const ContentId& id) {
    auto found = controls.find({type, id}); if (found != controls.end()) return found->second;
    check(controls.size() < kScanEntries, "inspect object bound exceeded");
    auto bytes = store.get_object(head.identity, {type, "iotdbfs-" + type + (type == "file-descriptor" ? "/2" : "/1"), id});
    check(bytes.size() <= kMetadataBytes - metadata_bytes, "inspect metadata byte bound exceeded");
    metadata_bytes += bytes.size();
    return controls.emplace(std::make_pair(type, id), std::move(bytes)).first->second;
  }
  void map_page(const ContentId& id, unsigned level, std::uint64_t begin, std::uint64_t end,
      std::uint64_t count, std::uint64_t encoded_size, std::vector<PayloadExtent>& extents,
      std::set<ContentId>& ancestors) {
    check(level < kTreeHeight && ancestors.insert(id).second, "extent cycle or height invalid");
    const auto& bytes = get("extent-page", id);
    check(bytes.size() == encoded_size, "extent encoded size differs");
    const auto page = decode_extent_page(bytes);
    check(page.level == level && page.begin == begin && page.end == end && page.extent_count == count,
          "extent parent/child binding differs");
    if (!level) {
      check(extents.size() <= kScanEntries && page.extents.size() <= kScanEntries - extents.size(),
            "inspect extent count bound exceeded");
      extents.insert(extents.end(), page.extents.begin(), page.extents.end());
    } else for (const auto& child : page.children)
      map_page(child.page, level - 1, child.begin, child.end, child.extent_count, child.encoded_size, extents, ancestors);
    ancestors.erase(id);
  }
  File file(const NamespaceRecord& record) {
    const auto descriptor = decode_file_descriptor(get("file-descriptor", record.descriptor_id));
    const auto seal = decode_version_seal(get("version-seal", record.seal_id));
    check(seal.identity == head.identity && seal.key == record.manifest.key &&
          seal.descriptor_id == record.descriptor_id && seal.proof_id == descriptor.proof_id &&
          seal.payload_size == record.manifest.payload_size && seal.format == record.inode.format_id,
          "version seal or exact binding differs");
    check(descriptor.payload_size == record.manifest.payload_size && descriptor.format == record.inode.format_id &&
          descriptor.layout_size == record.manifest.layout_size, "descriptor differs from namespace");
    const auto map = decode_payload_map(get("payload-map", descriptor.payload_root));
    check(map.payload_size == descriptor.payload_size && map.payload_size <= kPayloadRequestBytes,
          "payload map size differs or exceeds probe bound");
    File result{record.inode.path, record.inode.format_id, {}, record.inode.row_time,
      map.payload_size, record.descriptor_id, record.seal_id, descriptor.payload_root, {}};
    std::set<ContentId> ancestors;
    if (map.payload_size) map_page(map.page, map.height - 1, 0, map.payload_size,
      map.extent_count, map.page_bytes, result.extents, ancestors);
    check(result.extents.size() == map.extent_count, "payload extent aggregate differs");
    std::uint64_t offset = 0;
    for (const auto& extent : result.extents) {
      check(extent.logical_offset == offset && extent.content_offset == 0 &&
        extent.length == std::min<std::uint64_t>(kPayloadBlockBytes, map.payload_size - offset) &&
        extent.content_size == extent.length, "noncanonical payload block coverage");
      auto inserted = unique_blocks.emplace(extent.content_id, extent.content_size);
      check(inserted.second || inserted.first->second == extent.content_size, "block ID has conflicting sizes");
      offset += extent.length;
    }
    check(offset == map.payload_size, "payload map has a hole");
    unique_maps.insert(result.map);
    check(result.size <= kPayloadRequestBytes - logical_payload_bytes, "inspect cumulative payload bound exceeded");
    logical_payload_bytes += result.size;
    using Digest = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;
    Digest digest(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    check(bool(digest) && EVP_DigestInit_ex(digest.get(), EVP_sha256(), nullptr) == 1, "digest initialization failed");
    const auto before_payload = store.stats();
    for (std::size_t at = 0; at < result.extents.size();) {
      const auto end = std::min(result.extents.size(), at + 8);
      std::vector<ObjectReadRequest> requests;
      std::map<ContentId, std::size_t> positions;
      for (auto i = at; i < end; ++i) {
        const auto& extent = result.extents[i];
        if (positions.emplace(extent.content_id, requests.size()).second)
          requests.push_back({{"payload-block", "iotdbfs-payload-block/1", extent.content_id}, extent.content_size});
      }
      const auto blocks = store.get_objects(head.identity, requests);
      check(blocks.size() == requests.size(), "payload batch count differs");
      for (std::size_t i = 0; i < blocks.size(); ++i)
        check(blocks[i].size() == requests[i].size &&
          content_id(requests[i].ref.type, requests[i].ref.protocol, blocks[i]) == requests[i].ref.id,
          "payload block length or digest differs");
      for (auto i = at; i < end; ++i) {
        const auto& block = blocks[positions.at(result.extents[i].content_id)];
        check(EVP_DigestUpdate(digest.get(), block.data(), block.size()) == 1, "payload hash update failed");
      }
      at = end;
    }
    unsigned char hash[EVP_MAX_MD_SIZE]; unsigned hash_size = 0;
    check(EVP_DigestFinal_ex(digest.get(), hash, &hash_size) == 1 && hash_size == 32, "payload hash failed");
    const auto after_payload = store.stats();
    result.payload_batch_queries = after_payload.batch_query_attempts - before_payload.batch_query_attempts;
    result.payload_block_cells = after_payload.batch_object_reads - before_payload.batch_object_reads;
    result.payload_returned_bytes = after_payload.returned_blob_bytes - before_payload.returned_blob_bytes;
    result.sha256 = hex(hash, hash_size); return result;
  }
};

std::uint64_t staging_payload_rows(const IoTDBClientConfig& config, const std::string& prefix) {
  AbstractSessionBuilder builder;
  builder.host = config.host; builder.rpcPort = config.port;
  builder.username = config.user; builder.password = config.pass; builder.sqlDialect = "table";
  builder.enableAutoFetch = false; builder.enableRedirections = false;
  builder.maxRetries = 0; builder.retryDelayMs = 0; builder.connectTimeoutMs = 5000; builder.fetchSize = 1;
  TableSession session(std::make_shared<Session>(&builder));
  std::unique_ptr<SessionDataSet> rows;
  std::uint64_t count = 0;
  try {
    session.open(false);
    session.executeNonQueryStatement("USE " + config.db); // Session selection; no data mutation.
    rows = session.executeQueryStatement("SELECT time FROM " + prefix + "_blob_payload LIMIT 65537", 5000);
    check(bool(rows), "staging payload row query failed");
    while (rows->hasNext()) {
      check(count < 65536, "staging row count exceeds inspect bound");
      check(bool(rows->next()), "staging row is malformed"); ++count;
    }
    rows->closeOperationHandle(); rows.reset(); session.close();
  } catch (...) {
    try { if (rows) rows->closeOperationHandle(); session.close(); } catch (...) {}
    throw;
  }
  return count;
}

void inspect(const IoTDBClientConfig& config, const std::string& prefix) {
  Inspector scan(config, prefix);
  const auto roots = decode_roots(scan.get("catalog-root", scan.head.root));
  std::vector<File> files;
  std::set<ContentId> pages;
  std::function<void(const ContentId&, std::optional<unsigned>)> visit;
  visit = [&](const ContentId& id, std::optional<unsigned> level) {
    if (id.empty()) return;
    check(pages.insert(id).second, "catalog page cycle or duplicate");
    const auto page = decode_page(scan.get("catalog-page", id));
    check(!level || page.level == *level, "catalog level differs");
    for (const auto& entry : page.entries) {
      if (page.level) visit(entry.value, page.level - 1);
      else {
        const auto record = decode_namespace_record(scan.get("catalog-record", entry.value));
        check(entry.key == exact_key(record.inode.path, record.inode.row_time), "catalog exact key differs");
        check(files.size() < kScanEntries, "inspect source bound exceeded");
        if (!record.inode.is_dir) files.push_back(scan.file(record));
      }
    }
  };
  // The new descriptor codec rejects old V03 profiles: no upgrade or fallback.
  visit(roots[static_cast<unsigned>(Table::Exact)], {});
  check(scan.store.read_head() == std::optional<Bytes>(scan.initial_head), "head changed during read-only inspection");
  std::uint64_t unique_bytes = 0;
  for (const auto& block : scan.unique_blocks) unique_bytes += block.second;
  const auto staged = staging_payload_rows(config, prefix);
  std::cout << "{\"completed\":true,\"protocol\":\"iotdbfs-v04-payload-inspect/1\",\"prefix\":" << quote(prefix)
    << ",\"namespace\":" << quote(scan.head.identity.namespace_id) << ",\"incarnation\":" << quote(scan.head.identity.incarnation)
    << ",\"head_sequence\":" << scan.head.sequence << ",\"logical_payload_bytes\":" << scan.logical_payload_bytes
    << ",\"unique_payload_blocks\":" << scan.unique_blocks.size() << ",\"unique_payload_block_bytes\":" << unique_bytes
    << ",\"unique_payload_maps\":" << scan.unique_maps.size() << ",\"metadata_bytes\":" << scan.metadata_bytes
    << ",\"staging_payload_rows\":" << staged
    << ",\"head_unchanged\":true,\"files\":[";
  for (std::size_t i = 0; i < files.size(); ++i) {
    const auto& file = files[i];
    std::cout << (i ? "," : "") << "{\"path\":" << quote(file.path) << ",\"version\":" << file.version
      << ",\"format\":" << quote(file.format) << ",\"size\":" << file.size << ",\"sha256\":" << quote(file.sha256)
      << ",\"descriptor_id\":" << quote(file.descriptor) << ",\"seal_id\":" << quote(file.seal)
      << ",\"payload_root\":" << quote(file.map) << ",\"payload_batch_queries\":" << file.payload_batch_queries
      << ",\"payload_block_cells\":" << file.payload_block_cells << ",\"payload_returned_bytes\":" << file.payload_returned_bytes
      << ",\"payload_counter_scope\":\"independent full-file graph verification only; excludes metadata queries\",\"extents\":[";
    for (std::size_t j = 0; j < file.extents.size(); ++j) {
      const auto& extent = file.extents[j];
      std::cout << (j ? "," : "") << "{\"logical_offset\":" << extent.logical_offset << ",\"length\":" << extent.length
        << ",\"content_offset\":" << extent.content_offset << ",\"content_size\":" << extent.content_size
        << ",\"content_id\":" << quote(extent.content_id) << '}';
    }
    std::cout << "]}";
  }
  std::cout << "]}\n";
}
} // namespace

int main(int argc, char** argv) {
  try {
    check(argc == 4 && std::string(argv[1]) == "inspect" && std::string(argv[2]) == "--prefix", "usage differs");
    const std::string prefix = argv[3];
    check(prefix.rfind("vd_v04_", 0) == 0, "only a new V04 namespace is accepted");
    IoTDBClientConfig config;
    config.host = environment("IOTDB_HOST"); config.user = environment("IOTDB_USER");
    config.pass = environment("IOTDB_PASS"); config.db = environment("IOTDB_DB");
    const auto port = environment("IOTDB_PORT");
    check(port == "33667" && config.db == "h09probe1790939889dbfc1f", "original experiment endpoint required");
    check(config.host == "localhost" || config.host == "127.0.0.1", "loopback endpoint required");
    config.host = "127.0.0.1"; config.port = 33667; config.auto_create_db = false;
    inspect(config, prefix); return 0;
  } catch (const std::exception&) {
    std::cout << "{\"completed\":false,\"error\":\"read-only V04 payload graph inspection rejected\"}\n";
    return 1;
  }
}
