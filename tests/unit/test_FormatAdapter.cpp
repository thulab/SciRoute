#include "iotdbfs/FormatAdapter.h"

#include <gtest/gtest.h>

namespace {

using namespace iotdbfs;

class MinimalAdapter final : public FormatAdapter {
public:
  const std::string& id() const override {
    static const std::string value = "minimal";
    return value;
  }
  const std::vector<std::string>& extensions() const override {
    static const std::vector<std::string> value;
    return value;
  }
  std::unique_ptr<FormatIngestionSession> begin_ingestion(
    const FormatSource&) const override {
    return {};
  }
};

} // namespace

TEST(FormatAdapter, DefaultsExposeNoFormatSpecificBehavior) {
  MinimalAdapter adapter;
  FormatSource source;
  EXPECT_FALSE(adapter.matches(source));
  EXPECT_FALSE(adapter.parse_query("function=read"));
  EXPECT_FALSE(adapter.can_handle_virtual_query("file.bin"));
  EXPECT_FALSE(adapter.get_virtual_attr(1, "file.bin",
    FormatQuery(FormatViewType::None), "").ok);
  EXPECT_FALSE(adapter.open_virtual(1, "file.bin",
    FormatQuery(FormatViewType::None), "").ok);
  Inode inode;
  EXPECT_FALSE(adapter.get_xattr(1, "file.bin", inode, "user.key"));
  EXPECT_TRUE(adapter.list_xattrs(1, "file.bin", inode).empty());
}
