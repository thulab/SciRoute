#include <gtest/gtest.h>
#include "iotdbfs/MetadataSchemaCapability.h"

using namespace iotdbfs;

TEST(MetadataSchemaCapability, MissingTableIsAbsent) {
  EXPECT_EQ(inspect_metadata_schema(MetadataColumnSet{}, false, true), MetadataSchemaCapability::Absent);
}

TEST(MetadataSchemaCapability, ExistingTableWithoutMetadataNeedsMigration) {
  EXPECT_EQ(inspect_metadata_schema(MetadataColumnSet{"path", "name"}, true, true),
            MetadataSchemaCapability::MigrationRequired);
}

TEST(MetadataSchemaCapability, ExistingMetadataColumnIsAvailable) {
  EXPECT_EQ(inspect_metadata_schema(MetadataColumnSet{"path", "metadata"}, true, true),
            MetadataSchemaCapability::Available);
  EXPECT_EQ(inspect_metadata_schema(MetadataColumnSet{"metadata"}, true, true),
            MetadataSchemaCapability::Available);
}

TEST(MetadataSchemaCapability, FailedSchemaQueryIsUnsupported) {
  EXPECT_EQ(inspect_metadata_schema(MetadataColumnSet{}, true, false),
            MetadataSchemaCapability::Unsupported);
}

TEST(MetadataSchemaCapability, MigrationSqlUsesSafeIdentifierAndMetadataColumn) {
  auto sql = build_metadata_migration_sql("inode");
  ASSERT_TRUE(sql.ok()) << sql.error();
  EXPECT_EQ(sql.value(), "ALTER TABLE inode ADD COLUMN metadata STRING FIELD");
}

TEST(MetadataSchemaCapability, MigrationSqlRejectsUnsafeIdentifier) {
  EXPECT_FALSE(build_metadata_migration_sql("inode; DROP TABLE x").ok());
  EXPECT_FALSE(build_metadata_migration_sql("").ok());
  EXPECT_FALSE(build_metadata_migration_sql("dfs.inode").ok());
}

TEST(MetadataSchemaCapability, ExistingColumnDoesNotRequireMigrationSql) {
  EXPECT_FALSE(metadata_migration_required(MetadataColumnSet{"metadata"}, true));
  EXPECT_TRUE(metadata_migration_required(MetadataColumnSet{"name"}, true));
}

namespace {
MetadataSchemaProbe probe(bool exists, MetadataColumnSet columns) {
  return MetadataSchemaProbe{exists, std::move(columns)};
}
}

TEST(MetadataSchemaStartup, AvailableRequiresNoMutation) {
  int creates = 0, migrations = 0;
  MetadataSchemaOperations ops;
  ops.inspect = [] { return MetadataResult<MetadataSchemaProbe>::success(
      probe(true, {"metadata"})); };
  ops.create_table = [&] { ++creates; return MetadataResult<bool>::success(true); };
  ops.execute_migration = [&](std::string) { ++migrations; return MetadataResult<bool>::success(true); };
  auto result = ensure_metadata_schema("inode", ops);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.capability, MetadataSchemaCapability::Available);
  EXPECT_EQ(result.action, MetadataSchemaStartupAction::None);
  EXPECT_EQ(creates, 0); EXPECT_EQ(migrations, 0);
}

TEST(MetadataSchemaStartup, MissingTableCreatesAndVerifies) {
  bool exists = false; MetadataColumnSet columns; int creates = 0;
  MetadataSchemaOperations ops;
  ops.inspect = [&] { return MetadataResult<MetadataSchemaProbe>::success(probe(exists, columns)); };
  ops.create_table = [&] { ++creates; exists = true; columns.insert("metadata"); return MetadataResult<bool>::success(true); };
  auto result = ensure_metadata_schema("inode", ops);
  EXPECT_TRUE(result.ok()); EXPECT_EQ(result.action, MetadataSchemaStartupAction::Create);
  EXPECT_EQ(creates, 1);
}

TEST(MetadataSchemaStartup, MissingMetadataMigratesAndVerifies) {
  MetadataColumnSet columns{"name"}; int migrations = 0; std::string sql;
  MetadataSchemaOperations ops;
  ops.inspect = [&] { return MetadataResult<MetadataSchemaProbe>::success(probe(true, columns)); };
  ops.execute_migration = [&](std::string statement) { ++migrations; sql = std::move(statement); columns.insert("metadata"); return MetadataResult<bool>::success(true); };
  auto result = ensure_metadata_schema("inode", ops);
  EXPECT_TRUE(result.ok()); EXPECT_EQ(result.action, MetadataSchemaStartupAction::Migrate);
  EXPECT_EQ(sql, "ALTER TABLE inode ADD COLUMN metadata STRING FIELD"); EXPECT_EQ(migrations, 1);
}

TEST(MetadataSchemaStartup, MutationOrVerificationFailureIsUnsupported) {
  MetadataSchemaOperations ops;
  ops.inspect = [] { return MetadataResult<MetadataSchemaProbe>::success(probe(true, {"name"})); };
  ops.execute_migration = [](std::string) { return MetadataResult<bool>::failure("DDL denied"); };
  auto result = ensure_metadata_schema("inode", ops);
  EXPECT_FALSE(result.ok()); EXPECT_EQ(result.capability, MetadataSchemaCapability::Unsupported);
}

TEST(MetadataSchemaCapability, VectorAndSetOverloadsAgreeForAllProbeStates) {
  for (const std::vector<std::string> columns : {
      std::vector<std::string>{}, {"name"}, {"metadata", "metadata", "name"}}) {
    for (const bool exists : {false, true}) {
      for (const bool succeeded : {false, true}) {
        const auto expected = !succeeded ? MetadataSchemaCapability::Unsupported :
          !exists ? MetadataSchemaCapability::Absent :
          columns.empty() || columns.front() != "metadata" ?
            MetadataSchemaCapability::MigrationRequired : MetadataSchemaCapability::Available;
        EXPECT_EQ(inspect_metadata_schema(columns, exists, succeeded), expected);
        EXPECT_EQ(inspect_metadata_schema(MetadataColumnSet(columns.begin(), columns.end()),
          exists, succeeded), expected);
      }
      EXPECT_EQ(metadata_migration_required(columns, exists),
        exists && (columns.empty() || columns.front() != "metadata"));
    }
  }
}

TEST(MetadataSchemaCapability, ValidatesIdentifierCharacterAndLengthBoundaries) {
  for (const std::string name : std::vector<std::string>{"_", "_inode_2", "Inode2026", std::string(128, 'a')}) {
    const auto result = build_metadata_migration_sql(name);
    ASSERT_TRUE(result.ok()) << result.error();
    EXPECT_EQ(result.value(), "ALTER TABLE " + name + " ADD COLUMN metadata STRING FIELD");
  }
  for (const std::string name : std::vector<std::string>{"2inode", "inode-name", "inode name", "`inode`",
      "inode\n", std::string("inode\0suffix", 12), std::string(129, 'a')}) {
    EXPECT_FALSE(build_metadata_migration_sql(name).ok());
  }
}

TEST(MetadataSchemaStartup, InvalidIdentifierAndMissingInspectorNeverMutate) {
  int inspections = 0, mutations = 0;
  MetadataSchemaOperations ops;
  ops.inspect = [&] {
    ++inspections;
    return MetadataResult<MetadataSchemaProbe>::success(probe(false, {}));
  };
  ops.create_table = [&] { ++mutations; return MetadataResult<bool>::success(true); };
  ops.execute_migration = [&](std::string) { ++mutations; return MetadataResult<bool>::success(true); };
  const auto invalid = ensure_metadata_schema("inode; DROP TABLE inode", ops);
  EXPECT_FALSE(invalid.ok());
  EXPECT_FALSE(invalid.error.empty());
  EXPECT_EQ(inspections, 0);
  EXPECT_EQ(mutations, 0);
  ops.inspect = {};
  const auto missing = ensure_metadata_schema("inode", ops);
  EXPECT_FALSE(missing.ok());
  EXPECT_EQ(missing.capability, MetadataSchemaCapability::Unsupported);
  EXPECT_EQ(missing.action, MetadataSchemaStartupAction::None);
  EXPECT_EQ(mutations, 0);
}

TEST(MetadataSchemaStartup, InitialInspectionFailurePreservesErrorAndNeverMutates) {
  int mutations = 0;
  MetadataSchemaOperations ops;
  ops.inspect = [] { return MetadataResult<MetadataSchemaProbe>::failure("schema permission denied"); };
  ops.create_table = [&] { ++mutations; return MetadataResult<bool>::success(true); };
  ops.execute_migration = [&](std::string) { ++mutations; return MetadataResult<bool>::success(true); };
  const auto result = ensure_metadata_schema("inode", ops);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error, "schema permission denied");
  EXPECT_EQ(result.action, MetadataSchemaStartupAction::None);
  EXPECT_EQ(mutations, 0);
}

TEST(MetadataSchemaStartup, MissingCreationOrMigrationOperationFailsWithoutVerification) {
  for (const bool exists : {false, true}) {
    SCOPED_TRACE(exists);
    int inspections = 0;
    MetadataSchemaOperations ops;
    ops.inspect = [&] {
      ++inspections;
      return MetadataResult<MetadataSchemaProbe>::success(probe(exists, {"name"}));
    };
    const auto result = ensure_metadata_schema("inode", ops);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.capability, MetadataSchemaCapability::Unsupported);
    EXPECT_EQ(result.action, MetadataSchemaStartupAction::None);
    EXPECT_EQ(inspections, 1);
    EXPECT_NE(result.error.find(exists ? "migration" : "creation"), std::string::npos);
  }
}

TEST(MetadataSchemaStartup, CreationAndMigrationFailuresDoNotClaimCompletedAction) {
  for (const bool exists : {false, true}) {
    for (const bool reported_error : {false, true}) {
      SCOPED_TRACE(exists);
      SCOPED_TRACE(reported_error);
      int inspections = 0, creates = 0, migrations = 0;
      MetadataSchemaOperations ops;
      ops.inspect = [&] {
        ++inspections;
        return MetadataResult<MetadataSchemaProbe>::success(probe(exists, {"name"}));
      };
      auto failure = [&] {
        return reported_error ? MetadataResult<bool>::failure("DDL denied") :
          MetadataResult<bool>::success(false);
      };
      ops.create_table = [&] { ++creates; return failure(); };
      ops.execute_migration = [&](std::string) { ++migrations; return failure(); };
      const auto result = ensure_metadata_schema("inode", ops);
      EXPECT_FALSE(result.ok());
      EXPECT_EQ(result.capability, MetadataSchemaCapability::Unsupported);
      EXPECT_EQ(result.action, MetadataSchemaStartupAction::None);
      EXPECT_EQ(inspections, 1);
      EXPECT_EQ(creates, exists ? 0 : 1);
      EXPECT_EQ(migrations, exists ? 1 : 0);
      EXPECT_EQ(result.error, reported_error ? "DDL denied" :
        exists ? "schema migration failed" : "schema creation failed");
    }
  }
}

TEST(MetadataSchemaStartup, SuccessfulMutationRequiresFreshSuccessfulInspection) {
  for (const bool exists : {false, true}) {
    for (const int verification : {0, 1, 2}) {
      SCOPED_TRACE(exists);
      SCOPED_TRACE(verification);
      int inspections = 0;
      std::vector<std::string> calls;
      MetadataSchemaOperations ops;
      ops.inspect = [&] {
        calls.push_back("inspect");
        if (++inspections == 1)
          return MetadataResult<MetadataSchemaProbe>::success(probe(exists, {"name"}));
        if (verification == 0)
          return MetadataResult<MetadataSchemaProbe>::failure("verification disconnected");
        return MetadataResult<MetadataSchemaProbe>::success(probe(verification == 2, {"name"}));
      };
      ops.create_table = [&] { calls.push_back("create"); return MetadataResult<bool>::success(true); };
      ops.execute_migration = [&](std::string) { calls.push_back("migrate"); return MetadataResult<bool>::success(true); };
      const auto result = ensure_metadata_schema("inode", ops);
      EXPECT_FALSE(result.ok());
      EXPECT_EQ(result.capability, MetadataSchemaCapability::Unsupported);
      EXPECT_EQ(result.action, exists ? MetadataSchemaStartupAction::Migrate : MetadataSchemaStartupAction::Create);
      EXPECT_EQ(result.error, verification == 0 ? "verification disconnected" : "schema mutation could not be verified");
      EXPECT_EQ(calls, (std::vector<std::string>{"inspect", exists ? "migrate" : "create", "inspect"}));
    }
  }
}

TEST(MetadataSchemaStartup, VerifiedMigrationIsIdempotentOnSubsequentStartup) {
  MetadataColumnSet columns{"name"};
  int inspections = 0, migrations = 0;
  MetadataSchemaOperations ops;
  ops.inspect = [&] {
    ++inspections;
    return MetadataResult<MetadataSchemaProbe>::success(probe(true, columns));
  };
  ops.execute_migration = [&](std::string) {
    ++migrations;
    columns.insert("metadata");
    return MetadataResult<bool>::success(true);
  };
  const auto first = ensure_metadata_schema("inode", ops);
  ASSERT_TRUE(first.ok()) << first.error;
  EXPECT_EQ(first.action, MetadataSchemaStartupAction::Migrate);
  const auto second = ensure_metadata_schema("inode", ops);
  ASSERT_TRUE(second.ok()) << second.error;
  EXPECT_EQ(second.action, MetadataSchemaStartupAction::None);
  EXPECT_EQ(inspections, 3);
  EXPECT_EQ(migrations, 1);
}
