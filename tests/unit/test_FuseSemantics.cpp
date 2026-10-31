#include <gtest/gtest.h>

#include "iotdbfs/FuseSemantics.h"

#include <cerrno>
#include <limits>

using namespace fuse_semantics;

TEST(FuseSemantics, RootPath) {
  EXPECT_TRUE(is_root_path("/"));
  EXPECT_TRUE(is_root_path(""));
  EXPECT_FALSE(is_root_path("/a"));
}

TEST(FuseSemantics, StrictDescendantPath) {
  EXPECT_TRUE(is_strict_descendant_path("/a/b", "/a"));
  EXPECT_TRUE(is_strict_descendant_path("/a/b/c", "/a"));
  EXPECT_TRUE(is_strict_descendant_path("/a", "/"));
  EXPECT_FALSE(is_strict_descendant_path("/a", "/a"));
  EXPECT_FALSE(is_strict_descendant_path("/ab/c", "/a"));
  EXPECT_FALSE(is_strict_descendant_path("/", "/"));
}

TEST(FuseSemantics, RenameTargetError_NoTarget) {
  EXPECT_EQ(rename_target_error(false, false, false, true), 0);
  EXPECT_EQ(rename_target_error(true, false, false, true), 0);
}

TEST(FuseSemantics, RenameTargetError_FileToDirectory) {
  EXPECT_EQ(rename_target_error(false, true, true, true), -EISDIR);
}

TEST(FuseSemantics, RenameTargetError_DirectoryToFile) {
  EXPECT_EQ(rename_target_error(true, true, false, true), -ENOTDIR);
}

TEST(FuseSemantics, RenameTargetError_DirectoryToNonEmptyDirectory) {
  EXPECT_EQ(rename_target_error(true, true, true, false), -ENOTEMPTY);
}

TEST(FuseSemantics, RenameTargetError_AllowedReplacement) {
  EXPECT_EQ(rename_target_error(false, true, false, true), 0);
  EXPECT_EQ(rename_target_error(true, true, true, true), 0);
}

TEST(FuseSemantics, ReaddirRejectsNegativeOffset) {
  EXPECT_FALSE(valid_readdir_offset(static_cast<off_t>(-1)));
  EXPECT_TRUE(valid_readdir_offset(0));
}

TEST(FuseSemantics, ReaddirCookiesAdvanceFromEntryIndex) {
  off_t cookie = 0;
  ASSERT_TRUE(readdir_cookie(0, cookie));
  EXPECT_EQ(cookie, 1);
  ASSERT_TRUE(readdir_cookie(9, cookie));
  EXPECT_EQ(cookie, 10);
}

TEST(FuseSemantics, CheckedAddRejectsUnsignedWraparound) {
  std::uint64_t value = 0;
  EXPECT_TRUE(checked_add_u64(10, 20, value));
  EXPECT_EQ(value, 30);
  EXPECT_FALSE(checked_add_u64(std::numeric_limits<std::uint64_t>::max(), 1, value));
}

TEST(FuseSemantics, OffTConversionChecksRange) {
  EXPECT_TRUE(fits_off_t(0));
  EXPECT_TRUE(fits_off_t(static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())));
  EXPECT_FALSE(fits_off_t(static_cast<std::uint64_t>(std::numeric_limits<off_t>::max()) + 1));
}

TEST(FuseSemantics, DescendantChecksNormalizeRelativePathsAndRequireComponentBoundary) {
  EXPECT_TRUE(is_strict_descendant_path("a/b", "a"));
  EXPECT_TRUE(is_strict_descendant_path("a/b", "/a"));
  EXPECT_TRUE(is_strict_descendant_path("a", ""));
  EXPECT_FALSE(is_strict_descendant_path("a", "/a"));
  EXPECT_FALSE(is_strict_descendant_path("", "a"));
  EXPECT_FALSE(is_strict_descendant_path("/alphabet/b", "/alpha"));
  EXPECT_FALSE(is_strict_descendant_path("/a", "/a/b"));
}

TEST(FuseSemantics, MissingRenameTargetIgnoresUnusedTargetMetadata) {
  for (bool source_is_dir : {false, true}) {
    for (bool target_is_dir : {false, true}) {
      for (bool target_empty : {false, true}) {
        EXPECT_EQ(rename_target_error(source_is_dir, false, target_is_dir,
                    target_empty), 0);
      }
    }
  }
}

TEST(FuseSemantics, RenameTypeMismatchTakesPrecedenceOverDirectoryEmptiness) {
  EXPECT_EQ(rename_target_error(false, true, true, false), -EISDIR);
  EXPECT_EQ(rename_target_error(true, true, false, false), -ENOTDIR);
  EXPECT_EQ(rename_target_error(false, true, false, false), 0);
}

TEST(FuseSemantics, ReaddirOffsetAcceptsMaximumAndRejectsMinimum) {
  EXPECT_TRUE(valid_readdir_offset(std::numeric_limits<off_t>::max()));
  EXPECT_FALSE(valid_readdir_offset(std::numeric_limits<off_t>::min()));
}

TEST(FuseSemantics, ReaddirCookieBoundaryRejectsOverflowWithoutChangingOutput) {
  const auto max_cookie = std::numeric_limits<off_t>::max();
  off_t cookie = 17;
  ASSERT_TRUE(readdir_cookie(static_cast<std::size_t>(max_cookie) - 1, cookie));
  EXPECT_EQ(cookie, max_cookie);
  EXPECT_FALSE(readdir_cookie(static_cast<std::size_t>(max_cookie), cookie));
  EXPECT_EQ(cookie, max_cookie);
  EXPECT_FALSE(readdir_cookie(std::numeric_limits<std::size_t>::max(), cookie));
  EXPECT_EQ(cookie, max_cookie);
}

TEST(FuseSemantics, CheckedAddAcceptsExactLimitAndPreservesOutputOnFailure) {
  const auto limit = std::numeric_limits<std::uint64_t>::max();
  std::uint64_t result = 0;
  ASSERT_TRUE(checked_add_u64(limit - 7, 7, result));
  EXPECT_EQ(result, limit);
  ASSERT_TRUE(checked_add_u64(0, limit, result));
  EXPECT_EQ(result, limit);
  ASSERT_TRUE(checked_add_u64(limit, 0, result));
  EXPECT_EQ(result, limit);
  result = 42;
  EXPECT_FALSE(checked_add_u64(1, limit, result));
  EXPECT_EQ(result, 42u);
  EXPECT_FALSE(checked_add_u64(limit - 7, 8, result));
  EXPECT_EQ(result, 42u);
}
