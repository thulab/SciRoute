// H09-only live fixture writer. Refuses production database/path names.
#include "iotdbfs/FormatRegistry.h"
#include "iotdbfs/Ingestion.h"
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/MetadataSerialization.h"
#include "iotdbfs/NamespaceWriterLock.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/migration/MigrationPolicy.h"
#include "iotdbfs/migration/ReingestionService.h"
#include "iotdbfs/migration/MigrationCli.h"
#include "iotdbfs/migration/MigrationJournal.h"
#include "iotdbfs/migration/Hdf5ReingestionPolicy.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <unistd.h>

namespace {
using namespace iotdbfs;
std::string env(const char* key, const char* fallback) {
  const char* value = std::getenv(key); return value ? value : fallback;
}
StructuredBlobBytes load_file(const std::string& path) {
  std::ifstream file(path, std::ios::binary | std::ios::ate);
  if (!file || file.tellg() < 0 || file.tellg() > 64 * 1024 * 1024)
    throw std::runtime_error("fixture must be a readable file of at most 64 MiB");
  const auto size = static_cast<std::size_t>(file.tellg()); file.seekg(0);
  StructuredBlobBytes bytes(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>{});
  if (bytes.size() != size) throw std::runtime_error("fixture read was short");
  return bytes;
}
std::int64_t next_version(std::int64_t previous) {
  if (previous == std::numeric_limits<std::int64_t>::max())
    throw std::runtime_error("fixture version exhausted");
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
  return std::max<std::int64_t>(now, previous + 1);
}

// Synthetic old-protocol markers exercise classification only. The file bytes
// remain real HDF5; no claim is made that the marker's body is a historical /1
// or /2 encoding. Production code must reject these before parsing that body.
class MarkerSession final : public FormatIngestionSession {
public:
  MarkerSession(std::unique_ptr<FormatIngestionSession> real, std::string mode)
    : real_(std::move(real)), mode_(std::move(mode)) {}
  FormatStatus consume_chunk(std::uint64_t o, const std::uint8_t* p, std::size_t n) override {
    return real_ ? real_->consume_chunk(o, p, n) : FormatStatus::success();
  }
  FormatResult<FormatIngestionArtifacts> finish() override {
    if (!real_) return FormatResult<FormatIngestionArtifacts>::success({{}, {'o','t','h','e','r'}});
    auto result = real_->finish(); if (!result.ok()) return result;
    const std::string prefix = "IOTDBFS-HDF5-LAYOUT/";
    if (result.value().layout.size() <= prefix.size())
      return FormatResult<FormatIngestionArtifacts>::failure("fixture layout has no protocol marker");
    const auto version = mode_ == "layout1" ? 1 : 2;
    result.value().layout[prefix.size()] = static_cast<std::uint8_t>('0' + version);
    result.value().metadata["hdf5.layout_version"] = MetadataValue(std::int64_t(version));
    return result;
  }
private:
  std::unique_ptr<FormatIngestionSession> real_;
  std::string mode_;
};
class MarkerAdapter final : public FormatAdapter {
public:
  explicit MarkerAdapter(std::string mode) : mode_(std::move(mode)), id_(mode_ == "other" ? "h09_other" : "hdf5") {}
  const std::string& id() const override { return id_; }
  const std::vector<std::string>& extensions() const override { static const std::vector<std::string> none; return none; }
  bool matches(const FormatSource&) const override { return true; }
  std::unique_ptr<FormatIngestionSession> begin_ingestion(const FormatSource& source) const override {
    return std::make_unique<MarkerSession>(mode_ == "other" ? nullptr : real_.begin_ingestion(source), mode_);
  }
private:
  std::string mode_, id_;
  Hdf5FormatAdapter real_;
};
void emit(const StructuredBlobReadHandle& source, const StructuredBlobBytes& bytes,
    const StructuredBlobBytes& layout) {
  const auto& m = source.manifest();
  const auto newline = std::find(layout.begin(), layout.end(), '\n');
  std::string protocol;
  if (newline != layout.end() && std::distance(layout.begin(), newline) < 128)
    protocol.assign(layout.begin(), newline);
  std::cout << "{\"path\":" << migration_json_quote(m.key.path)
    << ",\"version\":" << m.key.version << ",\"bytes\":" << bytes.size()
    << ",\"sha256\":" << migration_json_quote(migration_sha256(bytes))
    << ",\"format_id\":" << (m.format_id ? migration_json_quote(*m.format_id) : "null")
    << ",\"layout_protocol\":" << migration_json_quote(protocol)
    << ",\"layout_sha256\":" << migration_json_quote(migration_sha256(layout)) << "}\n";
}
}

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cerr << "usage: h09_migration_fixture seed /h09-name file [pass|indexed|layout1|layout2|other]\n"
      "       h09_migration_fixture verify /h09-name version file\n"
      "       h09_migration_fixture pause-migrate /h09-name marker report-directory\n";
    return 2;
  }
  try {
    const std::string command = argv[1], path = argv[2];
    const auto database = env("IOTDB_DB", "");
    if (database.rfind("h09", 0) != 0 ||
        !std::all_of(database.begin(), database.end(), [](unsigned char c) {return std::isalnum(c) || c == '_';}) ||
        path.rfind("/h09-", 0) != 0 || path.find('/', 1) != std::string::npos)
      throw std::runtime_error("fixture requires an h09-prefixed isolated database and one /h09- root filename");
    IoTDBConfig config; config.host = env("IOTDB_HOST", "127.0.0.1");
    config.port = std::stoi(env("IOTDB_PORT", "6667"));
    config.user = env("IOTDB_USER", "root"); config.pass = env("IOTDB_PASS", "root"); config.db = database;
    std::string error;
    auto lock = NamespaceWriterLock::acquire(namespace_writer_lock_path(config.host, config.port, config.db), &error);
    if (!lock) throw std::runtime_error("namespace writer lock: " + error);
    IoTDBConnector::instance().initialize(config, 2);
    if (!IoTDBConnector::instance().executeNonQuery("CREATE DATABASE IF NOT EXISTS " + database))
      throw std::runtime_error("cannot create isolated fixture database");
    auto backend = std::make_shared<IoTDBStructuredBlobBackend>();
    if (!backend->ensure_schema()) throw std::runtime_error("cannot initialize fixture schema");
    StructuredBlobStorage storage(backend);
    if (command == "seed") {
      if (argc > 5) throw std::runtime_error("invalid seed arguments");
      const std::string mode = argc == 5 ? argv[4] : "pass";
      if (mode != "pass" && mode != "indexed" && mode != "layout1" && mode != "layout2" && mode != "other")
        throw std::runtime_error("unknown fixture mode");
      auto bytes = load_file(argv[3]);
      const auto latest = backend->load_inode(path);
      StructuredBlobVersionKey key{path, next_version(latest ? latest->row_time : 0)};
      FormatRegistry formats;
      if (mode == "indexed") formats.register_adapter(std::make_shared<Hdf5FormatAdapter>());
      else if (mode != "pass") formats.register_adapter(std::make_shared<MarkerAdapter>(mode));
      IngestionModule ingestion(storage, formats, {});
      FormatSource source{bytes.size(), [&bytes](std::uint64_t o, std::uint64_t n) -> std::optional<StructuredBlobBytes> {
        if (o > bytes.size() || n > bytes.size() - o) return std::nullopt;
        return StructuredBlobBytes(bytes.begin() + o, bytes.begin() + o + n);
      }};
      auto result = ingestion.ingest(key, source, [&](const IngestionOutcome& outcome) {
        Inode inode; inode.row_time = key.version; inode.path = path; inode.parent = "/"; inode.name = path.substr(1);
        inode.size = outcome.payload_size; inode.mode = 0644; inode.uid = inode.gid = 0;
        inode.ctime = inode.mtime = inode.atime = key.version; inode.format_id = outcome.format_id.value_or("");
        auto metadata = serialize_metadata(outcome.metadata, 65536);
        if (!metadata.ok()) return false;
        inode.metadata = std::move(metadata.value());
        return backend->upsert_inode(inode, false);
      });
      if (!result.ok()) throw std::runtime_error(result.message());
      auto opened = storage.open_published_version(key); auto layout = storage.load_layout(key);
      if (!opened.ok() || !layout.ok()) throw std::runtime_error("seed publication not readable");
      emit(opened.value(), bytes, layout.value());
    } else if (command == "verify") {
      if (argc != 5) throw std::runtime_error("invalid verify arguments");
      StructuredBlobVersionKey key{path, std::stoll(argv[3])};
      auto source = storage.open_published_version(key);
      if (!source.ok()) throw std::runtime_error(source.status().message());
      if (source.value().manifest().payload_size > 64ULL * 1024 * 1024)
        throw std::runtime_error("verification fixture exceeds 64 MiB");
      auto bytes = source.value().read_payload_range(0, source.value().manifest().payload_size);
      auto layout = storage.load_layout(key);
      if (!bytes.ok() || !layout.ok()) throw std::runtime_error("verification read failed");
      if (bytes.value() != load_file(argv[4])) throw std::runtime_error("stored bytes differ from exact original fixture");
      emit(source.value(), bytes.value(), layout.value());
    } else if (command == "pause-migrate") {
      if (argc != 5) throw std::runtime_error("invalid pause arguments");
      const std::filesystem::path report = std::filesystem::absolute(argv[4]);
      auto report_lock = NamespaceWriterLock::acquire(
        (report / ".migration.lock").string(), &error);
      if (!report_lock) throw std::runtime_error("report lock: " + error);
      auto policy = make_hdf5_reingestion_policy();
      auto frozen = load_migration_report(report,
        {config.host, config.port, config.db, policy->adapter_id(), policy->layout_protocol()}, &error);
      if (!frozen) throw std::runtime_error(error);
      const auto entry = std::find_if(frozen->inventory.begin(), frozen->inventory.end(),
        [&](const MigrationInventoryEntry& item) {
          return item.source.path == path && item.kind == MigrationKind::Candidate;
        });
      if (entry == frozen->inventory.end() || !frozen->journal.empty())
        throw std::runtime_error("pause fixture requires a fresh candidate report");
      FormatRegistry formats;
      formats.register_adapter(std::make_shared<Hdf5FormatAdapter>());
      ReingestionNamespace name_space;
      name_space.published_manifests = [backend] { return backend->load_published_manifests(); };
      name_space.latest_inode = [backend](const std::string& name) { return backend->load_inode(name); };
      name_space.exact_inode = [backend](const StructuredBlobVersionKey& key) {
        return backend->load_inode(key.path, key.version);
      };
      name_space.prepare_inode = [backend](const Inode& inode) { return backend->upsert_inode(inode, false); };
      name_space.next_version = next_version;
      ReingestionService service(storage, formats, std::move(name_space), policy);
      auto result = service.migrate(*entry, [&](const MigrationResult& started) {
        if (!append_migration_event(report, started)) return false;
        // The marker is published only after the real journal is durable and
        // before the service is allowed to create a destination version.
        const std::filesystem::path marker = std::filesystem::absolute(argv[3]);
        const int fd = ::open(marker.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
        if (fd < 0) throw std::runtime_error("cannot create pause marker");
        const std::string message = migration_result_json(started) + "\n";
        const bool written = ::write(fd, message.data(), message.size()) ==
          static_cast<ssize_t>(message.size()) && ::fsync(fd) == 0;
        ::close(fd);
        const int directory = ::open(marker.parent_path().c_str(), O_RDONLY);
        const bool synced = directory >= 0 && ::fsync(directory) == 0;
        if (directory >= 0) ::close(directory);
        if (!written || !synced) throw std::runtime_error("cannot durably write pause marker");
        for (;;) ::pause(); // Harness SIGKILLs only this child process.
        return false;
      });
      throw std::runtime_error("migration did not reach pause seam: " + result.error);
    } else throw std::runtime_error("unknown fixture command");
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n'; return 1;
  }
}
