#include "iotdbfs/query/core/QueryPlanning.h"
#include <gtest/gtest.h>
#include <type_traits>

using namespace iotdbfs;

namespace {
StructuredBlobManifest candidate(const char* path, std::int64_t version) {
  StructuredBlobManifest value;
  value.key = {path, version};
  value.format_id = "grib";
  value.published = true;
  value.phase = StructuredBlobPhase::Published;
  return value;
}
}

TEST(CatalogSelection, CopiesShareFrozenCandidatesWithoutCopyingManifests) {
  CatalogSelection original({candidate("/science/a", 7), candidate("/science/b", 9)});
  const auto* array = original.versions().data();
  auto copied = original;
  EXPECT_EQ(copied.versions().data(), array);
  EXPECT_EQ(copied.versions().size(), 2U);
  static_assert(std::is_same_v<decltype(copied.versions()),
      const std::vector<StructuredBlobManifest>&>);
}

TEST(CatalogSelection, ReplacingCandidatesDoesNotRetargetPinnedSelection) {
  CatalogSelection original({candidate("/science/a", 7)});
  auto pinned = original;
  original.set_versions({candidate("/science/a", 11)});
  EXPECT_EQ(pinned.versions().front().key.version, 7);
  EXPECT_EQ(original.versions().front().key.version, 11);
  EXPECT_NE(original.versions().data(), pinned.versions().data());
}

TEST(CatalogSelection, CopiedSelectionSurvivesOriginalAndCallerVector) {
  CatalogSelection pinned;
  const StructuredBlobManifest* array = nullptr;
  {
    std::vector<StructuredBlobManifest> built{candidate("/science/a", 7)};
    CatalogSelection original(std::move(built));
    array = original.versions().data();
    pinned = original;
  }
  ASSERT_EQ(pinned.versions().size(), 1U);
  EXPECT_EQ(pinned.versions().data(), array);
  EXPECT_EQ(pinned.versions().front().key.path, "/science/a");
  EXPECT_EQ(pinned.versions().front().key.version, 7);
}

TEST(CatalogSelection, ClearingOneSelectionPreservesOtherFrozenCandidates) {
  CatalogSelection original({candidate("/science/a", 7)});
  auto pinned = original;
  original.set_versions({});
  EXPECT_TRUE(original.versions().empty());
  ASSERT_EQ(pinned.versions().size(), 1U);
  EXPECT_EQ(pinned.versions().front().key.version, 7);
}
