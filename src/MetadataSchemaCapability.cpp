#include "iotdbfs/MetadataSchemaCapability.h"

#include <cctype>

namespace iotdbfs {
namespace {

bool valid_identifier(std::string_view value) {
  if (value.empty() || value.size() > 128)
    return false;
  const auto first = static_cast<unsigned char>(value.front());
  if (!(std::isalpha(first) || first == '_'))
    return false;
  for (const auto ch : value.substr(1)) {
    const auto c = static_cast<unsigned char>(ch);
    if (!(std::isalnum(c) || c == '_'))
      return false;
  }
  return true;
}

} // namespace

MetadataSchemaCapability inspect_metadata_schema(
    const MetadataColumnSet& columns,
    bool table_exists,
    bool query_succeeded) {
  if (!query_succeeded)
    return MetadataSchemaCapability::Unsupported;
  if (!table_exists)
    return MetadataSchemaCapability::Absent;
  if (columns.find("metadata") == columns.end())
    return MetadataSchemaCapability::MigrationRequired;
  return MetadataSchemaCapability::Available;
}

MetadataSchemaCapability inspect_metadata_schema(
    const std::vector<std::string>& columns,
    bool table_exists,
    bool query_succeeded) {
  return inspect_metadata_schema(MetadataColumnSet(columns.begin(), columns.end()),
                                 table_exists, query_succeeded);
}

bool metadata_migration_required(const MetadataColumnSet& columns,
                                 bool table_exists) {
  return table_exists && columns.find("metadata") == columns.end();
}

bool metadata_migration_required(const std::vector<std::string>& columns,
                                 bool table_exists) {
  return metadata_migration_required(
      MetadataColumnSet(columns.begin(), columns.end()), table_exists);
}

MetadataResult<std::string> build_metadata_migration_sql(
    std::string_view table_name) {
  if (!valid_identifier(table_name))
    return MetadataResult<std::string>::failure(
        "invalid IoTDB table identifier");
  std::string sql = "ALTER TABLE ";
  sql.append(table_name.data(), table_name.size());
  sql += " ADD COLUMN metadata STRING FIELD";
  return MetadataResult<std::string>::success(std::move(sql));
}

MetadataSchemaStartupResult ensure_metadata_schema(
    std::string_view table_name, const MetadataSchemaOperations& operations) {
  MetadataSchemaStartupResult result;
  if (!valid_identifier(table_name)) {
    result.error = "invalid IoTDB table identifier";
    return result;
  }
  if (!operations.inspect) {
    result.error = "schema inspection operation is unavailable";
    return result;
  }
  auto inspected = operations.inspect();
  if (!inspected.ok()) {
    result.error = inspected.error();
    return result;
  }
  auto state = inspect_metadata_schema(inspected.value().columns,
                                      inspected.value().table_exists, true);
  if (state == MetadataSchemaCapability::Available) {
    result.capability = state;
    return result;
  }
  if (state == MetadataSchemaCapability::Absent) {
    if (!operations.create_table) {
      result.error = "schema creation operation is unavailable";
      return result;
    }
    auto created = operations.create_table();
    if (!created.ok() || !created.value()) {
      result.error = created.ok() ? "schema creation failed" : created.error();
      return result;
    }
    result.action = MetadataSchemaStartupAction::Create;
  } else if (state == MetadataSchemaCapability::MigrationRequired) {
    if (!operations.execute_migration) {
      result.error = "schema migration operation is unavailable";
      return result;
    }
    auto sql = build_metadata_migration_sql(table_name);
    if (!sql.ok()) {
      result.error = sql.error();
      return result;
    }
    auto migrated = operations.execute_migration(sql.value());
    if (!migrated.ok() || !migrated.value()) {
      result.error = migrated.ok() ? "schema migration failed" : migrated.error();
      return result;
    }
    result.action = MetadataSchemaStartupAction::Migrate;
  } else {
    result.error = "schema is unsupported";
    return result;
  }

  auto verified = operations.inspect();
  if (!verified.ok()) {
    result.error = verified.error();
    result.capability = MetadataSchemaCapability::Unsupported;
    return result;
  }
  result.capability = inspect_metadata_schema(verified.value().columns,
                                              verified.value().table_exists, true);
  if (result.capability != MetadataSchemaCapability::Available) {
    result.error = "schema mutation could not be verified";
    result.capability = MetadataSchemaCapability::Unsupported;
  }
  return result;
}

} // namespace iotdbfs
