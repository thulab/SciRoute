#include <gtest/gtest.h>
#include "iotdbfs/PathUtils.h"

using namespace path_utils;

TEST(PathUtils, Norm_Empty) {
  EXPECT_EQ(norm(""), "/");
}

TEST(PathUtils, Norm_Slash) {
  EXPECT_EQ(norm("/"), "/");
}

TEST(PathUtils, Norm_PathWithoutSlash) {
  EXPECT_EQ(norm("path"), "/path");
  EXPECT_EQ(norm("a/b"), "/a/b");
}

TEST(PathUtils, Norm_PathWithSlash) {
  EXPECT_EQ(norm("/path"), "/path");
  EXPECT_EQ(norm("/a/b/c"), "/a/b/c");
}

TEST(PathUtils, Norm_PreservesDoubleSlash) {
  EXPECT_EQ(norm("//path"), "//path");
}

TEST(PathUtils, Base_Root) {
  EXPECT_EQ(base("/"), "/");
}

TEST(PathUtils, Base_SimpleFile) {
  EXPECT_EQ(base("/file.txt"), "file.txt");
  EXPECT_EQ(base("file.txt"), "file.txt");
}

TEST(PathUtils, Base_NestedPath) {
  EXPECT_EQ(base("/path/to/file"), "file");
  EXPECT_EQ(base("/a/b/c/d/file.ext"), "file.ext");
}

TEST(PathUtils, Base_TrailingSlash) {
  EXPECT_EQ(base("/path/to/"), "");
  EXPECT_EQ(base("/a/b/c/"), "");
}

TEST(PathUtils, Base_MultipleTrailingSlashes) {
  EXPECT_EQ(base("/path/to///"), "");
}

TEST(PathUtils, ParentOf_Root) {
  EXPECT_EQ(parent_of("/"), "/");
}

TEST(PathUtils, ParentOf_SimpleFile) {
  EXPECT_EQ(parent_of("/file.txt"), "/");
  EXPECT_EQ(parent_of("/file"), "/");
}

TEST(PathUtils, ParentOf_NestedPath) {
  EXPECT_EQ(parent_of("/path/file"), "/path");
  EXPECT_EQ(parent_of("/a/b/c"), "/a/b");
}

TEST(PathUtils, ParentOf_NoSlash) {
  EXPECT_EQ(parent_of("file.txt"), "/");
}

TEST(PathUtils, ParentOf_TrailingSlash) {
  EXPECT_EQ(parent_of("/path/to/"), "/path/to");
}

TEST(PathUtils, ToLower_AllUpper) {
  EXPECT_EQ(to_lower("ABC"), "abc");
}

TEST(PathUtils, ToLower_MixedCase) {
  EXPECT_EQ(to_lower("AbC123"), "abc123");
}

TEST(PathUtils, ToLower_Empty) {
  EXPECT_EQ(to_lower(""), "");
}

TEST(PathUtils, ToLower_NonAlphabetic) {
  EXPECT_EQ(to_lower("!@#"), "!@#");
}

TEST(PathUtils, HasGribExt_Grib) {
  EXPECT_TRUE(has_grib_ext("file.grib"));
  EXPECT_TRUE(has_grib_ext("file.GRIB"));
  EXPECT_TRUE(has_grib_ext("/path/to/file.grib"));
}

TEST(PathUtils, HasGribExt_Grib2) {
  EXPECT_TRUE(has_grib_ext("file.grib2"));
  EXPECT_TRUE(has_grib_ext("file.GRIB2"));
}

TEST(PathUtils, HasGribExt_Grb) {
  EXPECT_TRUE(has_grib_ext("file.grb"));
  EXPECT_TRUE(has_grib_ext("file.GRB"));
}

TEST(PathUtils, HasGribExt_Grb2) {
  EXPECT_TRUE(has_grib_ext("file.grb2"));
  EXPECT_TRUE(has_grib_ext("file.GRB2"));
}

TEST(PathUtils, HasGribExt_NonGrib) {
  EXPECT_FALSE(has_grib_ext("file.txt"));
  EXPECT_FALSE(has_grib_ext("file.gribx"));
  EXPECT_FALSE(has_grib_ext("grib"));
}

TEST(PathUtils, HasGribExt_Root) {
  EXPECT_FALSE(has_grib_ext("/"));
}

TEST(PathUtils, EndsWith_Match) {
  EXPECT_TRUE(ends_with("hello", "llo"));
  EXPECT_TRUE(ends_with("hello", "hello"));
  EXPECT_TRUE(ends_with("hello.txt", ".txt"));
}

TEST(PathUtils, EndsWith_NoMatch) {
  EXPECT_FALSE(ends_with("hello", "world"));
  EXPECT_FALSE(ends_with("hi", "hello"));
}

TEST(PathUtils, EndsWith_SuffixLonger) {
  EXPECT_FALSE(ends_with("hi", "hello"));
}

TEST(PathUtils, EndsWith_EmptyBoth) {
  EXPECT_TRUE(ends_with("", ""));
}

TEST(PathUtils, EndsWith_EmptyString) {
  EXPECT_FALSE(ends_with("", "a"));
}

TEST(PathUtils, SplitPathAndQuery_NoQuery) {
  std::string bp, q;
  split_path_and_query("/path", bp, q);
  EXPECT_EQ(bp, "/path");
  EXPECT_EQ(q, "");
}

TEST(PathUtils, SplitPathAndQuery_WithQuery) {
  std::string bp, q;
  split_path_and_query("/path?key=value", bp, q);
  EXPECT_EQ(bp, "/path");
  EXPECT_EQ(q, "key=value");
}

TEST(PathUtils, SplitPathAndQuery_OnlyQuery) {
  std::string bp, q;
  split_path_and_query("?query=1", bp, q);
  EXPECT_EQ(bp, "/");
  EXPECT_EQ(q, "query=1");
}

TEST(PathUtils, SplitPathAndQuery_MultiplePairs) {
  std::string bp, q;
  split_path_and_query("/a?b=c&d=e", bp, q);
  EXPECT_EQ(bp, "/a");
  EXPECT_EQ(q, "b=c&d=e");
}

TEST(PathUtils, SplitPathAndQuery_EmptyInput) {
  std::string bp, q;
  split_path_and_query("", bp, q);
  EXPECT_EQ(bp, "/");
  EXPECT_EQ(q, "");
}
