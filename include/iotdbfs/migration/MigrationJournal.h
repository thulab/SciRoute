#pragma once
#include "iotdbfs/migration/ReingestionService.h"
#include <filesystem>
#include <map>
#include <optional>

namespace iotdbfs {
struct MigrationReportBinding {
  std::string host;
  int port{0};
  std::string database;
  std::string adapter_id;
  std::string layout_protocol;
};
struct MigrationReportState {
  std::vector<MigrationInventoryEntry> inventory;
  std::map<std::string, MigrationResult> journal;
  std::int64_t max_reserved_version{0};
};
std::string migration_source_identity(const StructuredBlobVersionKey& key);
std::string migration_inventory_json(const MigrationInventoryEntry& entry);
std::string migration_result_json(const MigrationResult& result);
// Caller holds both the namespace writer lock and the report directory lock.
// A fresh inventory writes report.manifest last as the commit marker.
bool save_migration_inventory(const std::filesystem::path& report,
  const MigrationReportBinding& binding,
  const std::vector<MigrationInventoryEntry>& entries, std::string* error);
// Verifies the binding and inventory digest before repairing a torn journal tail.
std::optional<MigrationReportState> load_migration_report(
  const std::filesystem::path& report, const MigrationReportBinding& binding,
  std::string* error);
bool append_migration_event(const std::filesystem::path& report,
  const MigrationResult& event);
bool save_migration_results(const std::filesystem::path& report,
  const std::map<std::string, MigrationResult>& results);
} // namespace iotdbfs
