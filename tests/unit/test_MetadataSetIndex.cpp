#include "iotdbfs/MetadataSetIndex.h"

#include <gtest/gtest.h>

namespace {

TEST(MetadataSetIndex, ExtractsDeterministicTypedMembers) {
  const iotdbfs::MetadataMap metadata{
    {"shortName", iotdbfs::MetadataValue(
      iotdbfs::MetadataStringSet{"u", "t"})},
    {"level", iotdbfs::MetadataValue(
      iotdbfs::MetadataInt64Set{500, 850})},
    {"scalar", iotdbfs::MetadataValue("ignored")},
  };
  const auto members = iotdbfs::metadata_set_members(metadata);
  ASSERT_EQ(members.size(), 4u);
  EXPECT_EQ(members[0], (iotdbfs::MetadataSetMember{
    "level", "500", "int64"}));
  EXPECT_EQ(members[1], (iotdbfs::MetadataSetMember{
    "level", "850", "int64"}));
  EXPECT_EQ(members[2], (iotdbfs::MetadataSetMember{
    "shortName", "t", "string"}));
  EXPECT_EQ(members[3], (iotdbfs::MetadataSetMember{
    "shortName", "u", "string"}));
}

TEST(MetadataSetIndex, BuildsExactShortNameMembershipSql) {
  const auto sql = iotdbfs::build_grib_cross_file_inode_sql(true, "a'b");
  EXPECT_NE(sql.find("JOIN blob_metadata_set set1"), std::string::npos);
  EXPECT_NE(sql.find("set1.field_id='shortName'"), std::string::npos);
  EXPECT_NE(sql.find("set1.member_value='a''b'"), std::string::npos);
  EXPECT_NE(sql.find("table1.published=true"), std::string::npos);
  EXPECT_EQ(sql.find("LIKE"), std::string::npos);
}

TEST(MetadataSetIndex, OmitsMembershipJoinWithoutASetPredicate) {
  const auto sql = iotdbfs::build_grib_cross_file_inode_sql(true, "");
  EXPECT_EQ(sql.find("blob_metadata_set"), std::string::npos);
  EXPECT_NE(sql.find("SELECT time,path FROM inode"), std::string::npos);
}

} // namespace
