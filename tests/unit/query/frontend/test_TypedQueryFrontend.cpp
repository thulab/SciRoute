#include "iotdbfs/query/frontend/TypedQueryFrontend.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <variant>

namespace {
using namespace iotdbfs;

QueryRequest exact_request() {
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Exact, "/dataset/a.arrow"}};
  request.sources.versions = VersionSelection::exact_version(7);
  return request;
}

TEST(TypedQueryFrontend, RegistersImmutableTypedRequestWithExactVersion) {
  TypedQueryFrontend frontend;
  auto request = exact_request();
  auto route = frontend.register_typed(request);
  ASSERT_TRUE(route.ok()) << route.status().message();
  EXPECT_EQ(route.value().find("/.iotdbfs/query/v1/typed.ipc?id="), 0u);
  EXPECT_NE(route.value().find("&version=7"), std::string::npos);
  request.sources.versions = VersionSelection::all();
  auto resolved = frontend.resolve(route.value());
  ASSERT_TRUE(resolved.ok()) << resolved.status().message();
  ASSERT_TRUE(std::holds_alternative<TypedQueryExecutionRequest>(resolved.value()));
  EXPECT_EQ(std::get<TypedQueryExecutionRequest>(resolved.value()).request
    ->sources.versions.exact, 7);
  EXPECT_EQ(frontend.size(), 1u);
}

TEST(TypedQueryFrontend, RouteRejectsDuplicateUnknownAndMalformedParameters) {
  TypedQueryFrontend frontend;
  auto route = frontend.register_typed(exact_request());
  ASSERT_TRUE(route.ok());
  const auto& valid = route.value();
  const auto id = valid.substr(valid.find("id=") + 3, 48);
  for (const auto& candidate : {
        valid + "&id=" + id,
        valid + "&version=7",
        valid + "&unknown=x",
        valid + "&",
        valid + "#fragment",
        std::string("/.iotdbfs/query/v1/typed.ipc?id=%ff"),
        std::string("/.iotdbfs/query/v1/typed.ipc?id=%E4%ZZ"),
        std::string("/.iotdbfs/query/v1/typed.ipc?id=%C0%AF"),
        std::string("/.iotdbfs/query/v1/typed.ipc?id=" + id + "&snapshot=" +
          std::string(64, 'a')),
        std::string("/.iotdbfs/query/v1/windows.ipc?id=" + id +
          "&version=7")}) {
    auto rejected = frontend.resolve(candidate);
    EXPECT_FALSE(rejected.ok()) << candidate;
    if (!rejected.ok()) EXPECT_EQ(rejected.status().code(), QueryError::InvalidQuery);
  }
}

TEST(TypedQueryFrontend, EnforcesVersionPreconditionAndRouteKind) {
  TypedQueryFrontend frontend;
  auto route = frontend.register_typed(exact_request());
  ASSERT_TRUE(route.ok());
  auto changed = route.value();
  changed.replace(changed.find("&version=7"), 10, "&version=8");
  auto conflict = frontend.resolve(changed);
  ASSERT_FALSE(conflict.ok());
  EXPECT_EQ(conflict.status().code(), QueryError::InvalidQuery);

  auto wrong_kind = route.value();
  wrong_kind.replace(0, std::string("/.iotdbfs/query/v1/typed.ipc").size(),
    "/.iotdbfs/query/v1/windows.ipc");
  wrong_kind.erase(wrong_kind.find("&version=7"));
  auto rejected = frontend.resolve(wrong_kind);
  ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), QueryError::InvalidQuery);
}

TEST(TypedQueryFrontend, AcceptsEscapedIdAndRejectsOversizedOrOverflowingRoute) {
  TypedQueryFrontend frontend;
  auto route = frontend.register_typed(exact_request());
  ASSERT_TRUE(route.ok());
  const auto id = route.value().substr(route.value().find("id=") + 3, 48);
  constexpr char digits[] = "0123456789ABCDEF";
  const auto first = static_cast<unsigned char>(id.front());
  const std::string escaped_first = std::string("%") +
    digits[first >> 4] + digits[first & 15];
  auto escaped = std::string("/.iotdbfs/query/v1/typed.ipc?version=7&id=") +
    escaped_first + id.substr(1);
  EXPECT_TRUE(frontend.resolve(escaped).ok());
  auto overflow = std::string("/.iotdbfs/query/v1/typed.ipc?id=") + id +
    "&version=9223372036854775808";
  auto bad_version = frontend.resolve(overflow);
  ASSERT_FALSE(bad_version.ok());
  EXPECT_EQ(bad_version.status().code(), QueryError::InvalidQuery);
  auto oversized = frontend.resolve(route.value() + std::string(1024, 'x'));
  ASSERT_FALSE(oversized.ok());
  EXPECT_EQ(oversized.status().code(), QueryError::ResourceExhausted);
}

TEST(TypedQueryFrontend, BoundsRegistrationsAndRetainsResolvedRequestAfterErase) {
  TypedQueryFrontend frontend(1);
  auto route = frontend.register_typed(exact_request());
  ASSERT_TRUE(route.ok());
  auto second = frontend.register_typed(exact_request());
  ASSERT_FALSE(second.ok());
  EXPECT_EQ(second.status().code(), QueryError::ResourceExhausted);
  auto resolved = frontend.resolve(route.value());
  ASSERT_TRUE(resolved.ok());
  const auto id = route.value().substr(route.value().find("id=") + 3, 48);
  EXPECT_TRUE(frontend.erase(id));
  EXPECT_EQ(frontend.size(), 0u);
  EXPECT_FALSE(frontend.resolve(route.value()).ok());
  EXPECT_EQ(std::get<TypedQueryExecutionRequest>(resolved.value()).request
    ->sources.paths.front().path, "/dataset/a.arrow");
  EXPECT_TRUE(frontend.register_typed(exact_request()).ok());
}

TEST(TypedQueryFrontend, ExpiryReclaimsCapacityWithoutErasingInFlightRequest) {
  TypedQueryFrontend::Clock::time_point now{};
  TypedQueryFrontend frontend(1, std::chrono::milliseconds(10),
    [&] { return now; });
  auto first = frontend.register_typed(exact_request());
  ASSERT_TRUE(first.ok()) << first.status().message();
  auto held = frontend.resolve(first.value());
  ASSERT_TRUE(held.ok());
  now += std::chrono::milliseconds(9);
  EXPECT_TRUE(frontend.resolve(first.value()).ok());
  EXPECT_EQ(frontend.register_typed(exact_request()).status().code(),
    QueryError::ResourceExhausted);
  now += std::chrono::milliseconds(9);
  auto wrong_version = first.value();
  wrong_version.replace(wrong_version.find("&version=7"), 10, "&version=8");
  EXPECT_EQ(frontend.resolve(wrong_version).status().code(),
    QueryError::InvalidQuery);
  EXPECT_EQ(frontend.register_typed(exact_request()).status().code(),
    QueryError::ResourceExhausted);
  now += std::chrono::milliseconds(1);
  auto second = frontend.register_typed(exact_request());
  ASSERT_TRUE(second.ok()) << second.status().message();
  EXPECT_EQ(frontend.size(), 1u);
  EXPECT_EQ(frontend.resolve(first.value()).status().code(),
    QueryError::SourceNotFound);
  EXPECT_EQ(std::get<TypedQueryExecutionRequest>(held.value()).request
    ->sources.paths.front().path, "/dataset/a.arrow");
  EXPECT_TRUE(frontend.resolve(second.value()).ok());
}

TEST(TypedQueryFrontend, RejectsInvalidExpiryConfiguration) {
  EXPECT_THROW(TypedQueryFrontend(1, std::chrono::milliseconds(0)),
    std::invalid_argument);
  EXPECT_THROW(TypedQueryFrontend(1, std::chrono::milliseconds(-1)),
    std::invalid_argument);
  EXPECT_THROW((TypedQueryFrontend(1, std::chrono::milliseconds(10), {})),
    std::invalid_argument);
}

TEST(TypedQueryFrontend, RejectsNullWindowModuleAndInvalidTypedRequest) {
  TypedQueryFrontend frontend;
  WindowRequest windows;
  windows.max_windows = 1;
  auto missing = frontend.register_windows(nullptr, windows);
  ASSERT_FALSE(missing.ok());
  EXPECT_EQ(missing.status().code(), QueryError::InvalidQuery);
  auto request = exact_request();
  request.sources.versions.exact = -1;
  auto invalid = frontend.register_typed(std::move(request));
  ASSERT_FALSE(invalid.ok());
  EXPECT_EQ(invalid.status().code(), QueryError::InvalidQuery);
}

} // namespace
