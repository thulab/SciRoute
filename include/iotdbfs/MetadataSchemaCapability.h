#pragma once

#include "iotdbfs/MetadataConfig.h"

#include <set>
#include <string>
#include <string_view>
#include <vector>
#include <functional>

namespace iotdbfs {

// Availability of the optional dynamic metadata column on the inode table.
enum class MetadataSchemaCapability {
  Absent,
  Available,
  MigrationRequired,
  Unsupported,
};

using MetadataColumnSet = std::set<std::string>;

enum class MetadataSchemaStartupAction { None, Create, Migrate };

struct MetadataSchemaProbe {
  bool table_exists{false};
  MetadataColumnSet columns;
};

struct MetadataSchemaOperations {
  std::function<MetadataResult<MetadataSchemaProbe>()> inspect;
  std::function<MetadataResult<bool>()> create_table;
  std::function<MetadataResult<bool>(std::string)> execute_migration;
};

struct MetadataSchemaStartupResult {
  MetadataSchemaCapability capability{MetadataSchemaCapability::Unsupported};
  MetadataSchemaStartupAction action{MetadataSchemaStartupAction::None};
  std::string error;
  bool ok() const { return error.empty() && capability == MetadataSchemaCapability::Available; }
};

// Classify schema state from a table existence flag and the result of a
// metadata-column query. A failed query is Unsupported; a missing table is
// Absent; an existing table without the metadata column needs migration.
MetadataSchemaCapability inspect_metadata_schema(
    const MetadataColumnSet& columns,
    bool table_exists,
    bool query_succeeded);

MetadataSchemaCapability inspect_metadata_schema(
    const std::vector<std::string>& columns,
    bool table_exists,
    bool query_succeeded);

// Whether an existing table needs the metadata column migration.
bool metadata_migration_required(const MetadataColumnSet& columns,
                                 bool table_exists);

bool metadata_migration_required(const std::vector<std::string>& columns,
                                 bool table_exists);

// Build a deterministic ALTER statement for adding the metadata column. The
// table identifier is validated strictly and never interpolates arbitrary SQL.
MetadataResult<std::string> build_metadata_migration_sql(
    std::string_view table_name);

MetadataSchemaStartupResult ensure_metadata_schema(
    std::string_view table_name, const MetadataSchemaOperations& operations);

} // namespace iotdbfs
