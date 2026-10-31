#include "iotdbfs/migration/MigrationCli.h"
#include "iotdbfs/migration/MigrationJournal.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/NamespaceWriterLock.h"
#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <limits>
namespace iotdbfs {
int run_migration_cli(int argc, char* argv[],
    const IoTDBConfig& config, std::size_t pool_size,
    std::shared_ptr<const ReingestionPolicy> policy) {
  if (!policy) { std::cerr << "migration policy unavailable\n"; return 2; }
  if (argc < 2) {
    std::cerr << "usage: iotdb_fs {arrow,hdf5}-migrate inventory|run|resume "
                 "--report DIR [--path /exact/path]\n";
    return 2;
  }
  const std::string action = argv[1];
  if (action != "inventory" && action != "run" && action != "resume") {
    std::cerr << "unknown {arrow,hdf5}-migrate action\n";
    return 2;
  }
  std::optional<std::string> path;
  std::filesystem::path report;
  for (int i = 2; i < argc; ++i) {
    const std::string option = argv[i];
    if (i + 1 >= argc) {
      std::cerr << "missing {arrow,hdf5}-migrate option value\n";
      return 2;
    }
    if (option == "--report" && report.empty()) report = argv[++i];
    else if (option == "--path" && !path) path = argv[++i];
    else { std::cerr << "unknown or repeated {arrow,hdf5}-migrate option\n"; return 2; }
  }
  if (report.empty() || (path && (path->empty() || path->front() != '/'))) {
    std::cerr << "report directory and absolute exact path are required\n";
    return 2;
  }
  std::string lock_error;
  auto writer_lock = NamespaceWriterLock::acquire(
    namespace_writer_lock_path(config.host, config.port, config.db), &lock_error);
  if (!writer_lock) {
    std::cerr << "namespace writer lock unavailable: " << lock_error << '\n';
    return 3;
  }
  try {
    IoTDBConnector::instance().initialize(config, pool_size);
    auto backend = std::make_shared<IoTDBStructuredBlobBackend>();
    StructuredBlobStorage storage(backend);
    FormatRegistry formats;
    std::string registration_error;
    std::shared_ptr<const FormatAdapter> adapter;
    if (policy->adapter_id() == "arrow") adapter = std::make_shared<ArrowFormatAdapter>();
    else if (policy->adapter_id() == "hdf5") adapter = std::make_shared<Hdf5FormatAdapter>();
    if (!adapter || !formats.register_adapter(adapter, &registration_error)) {
      std::cerr << "cannot register migration adapter: " << registration_error << '\n';
      return 1;
    }
    std::int64_t last_version = 0;
    ReingestionNamespace name_space;
    name_space.published_manifests = [backend] {
      return backend->load_published_manifests();
    };
    name_space.latest_inode = [backend](const std::string& name) {
      return backend->load_inode(name);
    };
    name_space.exact_inode = [backend](const StructuredBlobVersionKey& key) {
      return backend->load_inode(key.path, key.version);
    };
    name_space.prepare_inode = [backend](const Inode& inode) {
      return backend->upsert_inode(inode, false);
    };
    name_space.next_version = [&last_version](std::int64_t previous) {
      const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
      if (previous == std::numeric_limits<std::int64_t>::max() ||
          last_version == std::numeric_limits<std::int64_t>::max())
        return previous;
      last_version = std::max({static_cast<std::int64_t>(now),
        previous + 1, last_version + 1});
      return last_version;
    };
    ReingestionService service(storage, formats, std::move(name_space), policy);
    const MigrationReportBinding binding{config.host, config.port, config.db,
      policy->adapter_id(), policy->layout_protocol()};
    std::error_code ec;
    if (action == "inventory") std::filesystem::create_directories(report, ec);
    if (ec || !std::filesystem::is_directory(report)) {
      std::cerr << "report directory unavailable\n"; return 1;
    }
    report = std::filesystem::absolute(report);
    auto report_lock = NamespaceWriterLock::acquire(
      (report / ".migration.lock").string(), &lock_error);
    if (!report_lock) {
      std::cerr << "report lock unavailable: " << lock_error << '\n'; return 3;
    }
    std::string report_error;
    if (action == "inventory") {
      auto entries = service.inventory(path);
      if (!entries.ok()) {
        std::cerr << "inventory failed: " << entries.status().message() << '\n';
        return 1;
      }
      if (!save_migration_inventory(report, binding, entries.value(), &report_error)) {
        std::cerr << "cannot durably write inventory: " << report_error << '\n';
        return 1;
      }
      for (const auto& entry : entries.value())
        std::cout << migration_inventory_json(entry) << '\n';
      return 0;
    }
    auto loaded = load_migration_report(report, binding, &report_error);
    if (!loaded) { std::cerr << report_error << '\n'; return 1; }
    auto* entries = &loaded->inventory;
    auto* journal = &loaded->journal;
    // All reserved destinations remain unavailable, including unpublished
    // attempts and earlier attempts followed by a preallocation conflict.
    last_version = loaded->max_reserved_version;
    if (path && std::none_of(entries->begin(), entries->end(),
          [&](const MigrationInventoryEntry& entry) {
            return entry.source.path == *path;
          })) {
      std::cerr << "exact path is not present in the frozen inventory\n";
      return 2;
    }
    bool all_verified = true;
    for (const auto& entry : *entries) {
      if (path && entry.source.path != *path) continue;
      if (entry.kind != MigrationKind::Candidate) continue;
      auto at = journal->find(migration_source_identity(entry.source));
      if (at != journal->end() && at->second.destination.version > 0) {
        const auto previous_state = at->second.state;
        auto verified = service.reconcile(at->second);
        if (verified.ok()) {
          at->second = verified;
          if (previous_state != "verified" && !append_migration_event(report, verified)) {
            std::cerr << "cannot durably record reconciled result\n";
            return 1;
          }
          std::cout << migration_result_json(verified) << '\n';
          continue;
        }
        auto destination = storage.load_manifest(at->second.destination);
        if (!destination.ok() && destination.status().code() != StructuredBlobError::NotFound) {
          std::cerr << "cannot determine destination state: " << destination.status().message() << '\n';
          return 1; // A backend failure never licenses allocation of another target.
        }
        if ((destination.ok() && destination.value().published) || at->second.state == "verified" ||
            at->second.state == "published_unverified") {
          at->second = verified;
          if (!append_migration_event(report, verified)) {
            std::cerr << "cannot durably record verification failure\n";
            return 1;
          }
          all_verified = false;
          std::cout << migration_result_json(verified) << '\n';
          continue; // Never overwrite a published but unverified result.
        }
        if (destination.ok()) {
          auto retained = at->second;
          retained.state = "failed";
          retained.error = "unpublished destination retained after interruption; retry uses a new version";
          if (!append_migration_event(report, retained)) {
            std::cerr << "cannot record retained unpublished destination\n"; return 1;
          }
          at->second = std::move(retained);
        }
      }
      bool start_journal_failed = false;
      auto migrated = service.migrate(entry, [&](const MigrationResult& start) {
        const bool okay = append_migration_event(report, start);
        start_journal_failed = !okay;
        return okay;
      });
      if (start_journal_failed) {
        std::cerr << "cannot durably record destination before ingestion\n";
        return 1;
      }
      if (!append_migration_event(report, migrated)) {
        std::cerr << "cannot durably record migration result\n";
        return 1;
      }
      (*journal)[migration_source_identity(entry.source)] = migrated;
      all_verified &= migrated.ok();
      std::cout << migration_result_json(migrated) << '\n';
    }
    if (!save_migration_results(report, *journal)) {
      std::cerr << "cannot save result report\n";
      return 1;
    }
    return all_verified ? 0 : 1;
  } catch (const std::exception& error) {
    std::cerr << "migration failed: " << error.what() << '\n';
    return 1;
  }
}

} // namespace iotdbfs
