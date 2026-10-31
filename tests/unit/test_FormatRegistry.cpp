#include "iotdbfs/FormatRegistry.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace iotdbfs;

class NullIngestionSession final : public FormatIngestionSession {
public:
  FormatStatus consume_chunk(std::uint64_t, const std::uint8_t*,
    std::size_t) override { return FormatStatus::success(); }
  FormatResult<FormatIngestionArtifacts> finish() override {
    return FormatResult<FormatIngestionArtifacts>::success({});
  }
};

class TestAdapter final : public FormatAdapter {
public:
  using Matcher = std::function<bool(const FormatSource&)>;

  TestAdapter(std::string id, std::vector<std::string> extensions = {},
    Matcher matcher = {})
    : id_(std::move(id)), extensions_(std::move(extensions)),
      matcher_(std::move(matcher)) {}

  const std::string& id() const override { return id_; }
  const std::vector<std::string>& extensions() const override {
    return extensions_;
  }
  bool matches(const FormatSource& source) const override {
    return matcher_ && matcher_(source);
  }
  std::unique_ptr<FormatIngestionSession> begin_ingestion(
    const FormatSource&) const override {
    return std::make_unique<NullIngestionSession>();
  }

private:
  std::string id_;
  std::vector<std::string> extensions_;
  Matcher matcher_;
};

FormatSource source_for(const std::vector<std::uint8_t>& bytes) {
  return { bytes.size(), [&bytes](std::uint64_t offset, std::uint64_t length)
      -> std::optional<std::vector<std::uint8_t>> {
    if (offset > bytes.size() || length > bytes.size() - offset)
      return std::nullopt;
    return std::vector<std::uint8_t>(bytes.begin() + offset,
      bytes.begin() + offset + length);
  } };
}

} // namespace

TEST(FormatRegistry, ResolvesNormalizedExtensionsAndRejectsUnknownPaths) {
  FormatRegistry registry;
  auto grib = std::make_shared<TestAdapter>("grib",
    std::vector<std::string>{ ".GRIB", "grib2" });
  ASSERT_TRUE(registry.register_adapter(grib));

  for (const auto* path : { "/a.b/file.GRIB", "a\\b\\file.GrIb2", ".grib" }) {
    SCOPED_TRACE(path);
    ASSERT_TRUE(registry.resolve_for_path(path));
    EXPECT_EQ(registry.resolve_for_path(path)->id(), "grib");
  }
  for (const auto* path : { "", "/dir.grib/file", "file.",
       "file.grib.backup", "/dir.grib/" }) {
    SCOPED_TRACE(path);
    EXPECT_FALSE(registry.resolve_for_path(path));
  }
}

TEST(FormatRegistry, RejectsInvalidAndDuplicateAdapters) {
  FormatRegistry registry;
  std::string error;
  EXPECT_FALSE(registry.register_adapter(nullptr, &error));
  EXPECT_FALSE(error.empty());
  EXPECT_FALSE(registry.register_adapter(
    std::make_shared<TestAdapter>(""), &error));
  EXPECT_TRUE(registry.ids().empty());

  ASSERT_TRUE(registry.register_adapter(std::make_shared<TestAdapter>("a")));
  EXPECT_FALSE(registry.register_adapter(
    std::make_shared<TestAdapter>("a"), &error));
}

TEST(FormatRegistry, ContentResolutionStopsAtFirstMatch) {
  FormatRegistry registry;
  std::vector<std::string> calls;
  ASSERT_TRUE(registry.register_adapter(std::make_shared<TestAdapter>("miss",
    std::vector<std::string>{}, [&](const FormatSource& source) {
      calls.push_back("miss");
      EXPECT_EQ(source.size, 4u);
      return false;
    })));
  ASSERT_TRUE(registry.register_adapter(std::make_shared<TestAdapter>("match",
    std::vector<std::string>{}, [&](const FormatSource& source) {
      calls.push_back("match");
      auto prefix = source.read(0, 4);
      return prefix && *prefix == std::vector<std::uint8_t>({ 1, 2, 3, 4 });
    })));
  ASSERT_TRUE(registry.register_adapter(std::make_shared<TestAdapter>("unused",
    std::vector<std::string>{}, [&](const FormatSource&) {
      calls.push_back("unused");
      return true;
    })));
  const std::vector<std::uint8_t> matching{ 1, 2, 3, 4 };
  auto found = registry.resolve_for_content(source_for(matching));
  ASSERT_TRUE(found);
  EXPECT_EQ(found->id(), "match");
  EXPECT_EQ(calls, (std::vector<std::string>{ "miss", "match" }));

  calls.clear();
  const std::vector<std::uint8_t> unknown{ 4, 3, 2, 1 };
  found = registry.resolve_for_content(source_for(unknown));
  ASSERT_TRUE(found);
  EXPECT_EQ(found->id(), "unused");
  EXPECT_EQ(calls, (std::vector<std::string>{ "miss", "match", "unused" }));
}

TEST(FormatRegistry, ResolvesPersistedIdentityIndependentlyOfPath) {
  FormatRegistry registry;
  auto grib = std::make_shared<TestAdapter>("grib",
    std::vector<std::string>{"grib2"});
  ASSERT_TRUE(registry.register_adapter(grib));
  EXPECT_EQ(registry.resolve_by_id("grib"), grib);
  EXPECT_FALSE(registry.resolve_by_id(""));
  EXPECT_FALSE(registry.resolve_by_id("arrow"));
  EXPECT_FALSE(registry.resolve_for_path("renamed.bin"));
}

TEST(FormatRegistry, UnknownContentHasNoAdapterAndRemovalPreservesOrder) {
  FormatRegistry registry;
  ASSERT_TRUE(registry.register_adapter(std::make_shared<TestAdapter>(
    "first", std::vector<std::string>{ "grib" })));
  ASSERT_TRUE(registry.register_adapter(std::make_shared<TestAdapter>(
    "second", std::vector<std::string>{ "grib" })));
  EXPECT_EQ(registry.ids(),
    (std::vector<std::string>{ "first", "second" }));
  ASSERT_EQ(registry.resolve_for_path("a.grib")->id(), "first");

  EXPECT_TRUE(registry.unregister_adapter("first"));
  EXPECT_FALSE(registry.unregister_adapter("first"));
  ASSERT_EQ(registry.resolve_for_path("a.grib")->id(), "second");

  const std::vector<std::uint8_t> bytes{ 0, 1 };
  EXPECT_FALSE(registry.resolve_for_content(source_for(bytes)));
}
