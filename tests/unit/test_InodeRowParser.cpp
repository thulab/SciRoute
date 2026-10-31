#include "iotdbfs/InodeRowParser.h"

#include <gtest/gtest.h>

#include <limits>

namespace {

std::vector<Field> valid_fields() {
  std::vector<Field> fields(15);
  fields[0].longV = 100;
  fields[1].stringV = std::string("/weather.grib2");
  fields[2].stringV = std::string("/");
  fields[3].stringV = std::string("weather.grib2");
  fields[4].boolV = false;
  fields[5].longV = 7;
  fields[6].intV = 0644;
  fields[7].intV = 1000;
  fields[8].intV = 1000;
  fields[9].intV = 1;
  fields[10].longV = 101;
  fields[11].longV = 102;
  fields[12].longV = 103;
  fields[13].stringV = std::string("grib");
  fields[14].stringV = std::string(
    R"({"shortName":{"type":"string","value":"t"}})");
  return fields;
}

} // namespace

TEST(InodeRowParser, ParsesFormatNeutralProjection) {
  Inode inode;
  ASSERT_TRUE(iotdbfs::parse_inode_fields(valid_fields(), inode));
  EXPECT_EQ(inode.path, "/weather.grib2");
  EXPECT_EQ(inode.parent, "/");
  EXPECT_EQ(inode.name, "weather.grib2");
  EXPECT_FALSE(inode.is_dir);
  EXPECT_EQ(inode.size, 7);
  EXPECT_EQ(inode.mode, 0644);
  EXPECT_EQ(inode.uid, 1000);
  EXPECT_EQ(inode.gid, 1000);
  EXPECT_EQ(inode.nlink, 1);
  EXPECT_EQ(inode.ctime, 101);
  EXPECT_EQ(inode.mtime, 102);
  EXPECT_EQ(inode.atime, 103);
  EXPECT_EQ(inode.row_time, 100);
  EXPECT_EQ(inode.format_id, "grib");
  EXPECT_EQ(inode.metadata, valid_fields()[14].stringV.value());
}

TEST(InodeRowParser, RequiresExactStableProjection) {
  for (std::size_t count = 0; count < 15; ++count) {
    auto fields = valid_fields();
    fields.resize(count);
    Inode inode;
    EXPECT_FALSE(iotdbfs::parse_inode_fields(fields, inode)) << count;
  }
  auto extra = valid_fields();
  extra.emplace_back();
  Inode inode;
  EXPECT_FALSE(iotdbfs::parse_inode_fields(extra, inode));
}

TEST(InodeRowParser, RejectsMissingOrWrongTypedRequiredColumns) {
  for (std::size_t index = 0; index < 13; ++index) {
    SCOPED_TRACE(index);
    auto fields = valid_fields();
    fields[index] = Field{};
    Inode inode;
    EXPECT_FALSE(iotdbfs::parse_inode_fields(fields, inode));
    fields[index].doubleV = 0.0;
    EXPECT_FALSE(iotdbfs::parse_inode_fields(fields, inode));
  }
}

TEST(InodeRowParser, AcceptsNullFormatIdentityAndMetadata) {
  auto fields = valid_fields();
  fields[13].stringV = {};
  fields[14].stringV = {};
  Inode inode;
  ASSERT_TRUE(iotdbfs::parse_inode_fields(fields, inode));
  EXPECT_TRUE(inode.format_id.empty());
  EXPECT_TRUE(inode.metadata.empty());
}

TEST(InodeRowParser, AcceptsEmptyTypedDocumentForBytePassThrough) {
  auto fields = valid_fields();
  fields[13].stringV = std::string();
  fields[14].stringV = std::string("{}");
  Inode inode;
  ASSERT_TRUE(iotdbfs::parse_inode_fields(fields, inode));
  EXPECT_TRUE(inode.format_id.empty());
  EXPECT_EQ(inode.metadata, "{}");
}

TEST(InodeRowParser, RejectsMetadataWithoutFormatIdentity) {
  auto fields = valid_fields();
  fields[13].stringV = {};
  Inode inode;
  EXPECT_FALSE(iotdbfs::parse_inode_fields(fields, inode));
}

TEST(InodeRowParser, RejectsWrongFormatColumnTypes) {
  for (const std::size_t index : {13u, 14u}) {
    for (int type = 0; type < 4; ++type) {
      auto fields = valid_fields();
      fields[index].stringV = {};
      if (type == 0) fields[index].longV = 0;
      if (type == 1) fields[index].intV = 0;
      if (type == 2) fields[index].doubleV = 0.0;
      if (type == 3) fields[index].boolV = false;
      Inode inode;
      EXPECT_FALSE(iotdbfs::parse_inode_fields(fields, inode))
        << index << ':' << type;
    }
  }
}

TEST(InodeRowParser, RejectsMalformedTypedMetadataDocuments) {
  for (const std::string json : {
      "not-json", "[]", "{}trailing",
      R"({"a":{"type":"bool","value":1}})",
      R"({"a":{"type":"int64","value":9223372036854775808}})"}) {
    auto fields = valid_fields();
    fields[14].stringV = json;
    Inode inode;
    EXPECT_FALSE(iotdbfs::parse_inode_fields(fields, inode)) << json;
  }
}

TEST(InodeRowParser, PreservesWideSizesAndTimestamps) {
  auto fields = valid_fields();
  fields[0].longV = std::numeric_limits<std::int64_t>::max();
  fields[5].longV = std::int64_t{1} << 40;
  fields[10].longV = std::int64_t{1} << 41;
  fields[11].longV = std::int64_t{1} << 42;
  fields[12].longV = std::int64_t{1} << 43;
  Inode inode;
  ASSERT_TRUE(iotdbfs::parse_inode_fields(fields, inode));
  EXPECT_EQ(inode.row_time, std::numeric_limits<std::int64_t>::max());
  EXPECT_EQ(inode.size, std::int64_t{1} << 40);
  EXPECT_EQ(inode.ctime, std::int64_t{1} << 41);
  EXPECT_EQ(inode.mtime, std::int64_t{1} << 42);
  EXPECT_EQ(inode.atime, std::int64_t{1} << 43);
}
