#include "iotdbfs/formats/hdf5/Hdf5SourceDriver.h"
#include "iotdbfs/storage/NativeFileStore.h"
#include "iotdbfs/query/core/QueryProfile.h"

#include <gtest/gtest.h>
#include <hdf5.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <filesystem>
#include <unistd.h>

using namespace iotdbfs;

namespace {

void h5check(herr_t status, const char* operation) {
  if (status < 0) throw std::runtime_error(operation);
}

struct H5Object {
  H5Object(hid_t value, herr_t (*close)(hid_t)) : value(value), close(close) {
    if (value < 0) throw std::runtime_error("cannot create HDF5 test fixture");
  }
  ~H5Object() { close(value); }
  operator hid_t() const { return value; }
  H5Object(const H5Object&) = delete;
  hid_t value;
  herr_t (*close)(hid_t);
};

struct SavedErrorState {
  SavedErrorState() {
    h5check(H5Eget_auto2(H5E_DEFAULT, &handler, &data), "save error handler");
    stack = H5Eget_current_stack();
    if (stack < 0) throw std::runtime_error("save error stack");
  }
  ~SavedErrorState() {
    H5Eclear2(H5E_DEFAULT);
    H5Eset_auto2(H5E_DEFAULT, handler, data);
    H5Eset_current_stack(stack);
  }
  H5E_auto2_t handler{nullptr};
  void* data{nullptr};
  hid_t stack{-1};
};

herr_t count_hdf_error(hid_t, void* data) {
  ++*static_cast<std::size_t*>(data);
  return 0;
}

struct ScopedEnvironment {
  ScopedEnvironment(const char* name, const char* value) : name(name) {
    if (const auto* previous = std::getenv(name)) saved = previous;
    if (::setenv(name, value, 1) != 0) throw std::runtime_error("cannot configure test environment");
  }
  ~ScopedEnvironment() {
    if (saved) ::setenv(name.c_str(), saved->c_str(), 1);
    else ::unsetenv(name.c_str());
  }
  std::string name;
  std::optional<std::string> saved;
};

enum class FixtureKind { Contiguous, Chunked, LargeChunk, Deflate, ExternalLink,
  ExternalStorage, VirtualDataset, UnknownFilter };

// A whole-file image is used only to CREATE tiny deterministic test fixtures.
// The source driver under test receives size + bounded range-read capability.
BinaryValue fixture(FixtureKind kind = FixtureKind::Chunked, float bias = 0,
    std::pair<std::uint64_t, std::uint64_t>* sibling_range = nullptr,
    std::uint64_t* object_header = nullptr) {
  H5Object fapl(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
  h5check(H5Pset_fapl_core(fapl, 4096, false), "configure memory fixture");
  H5Object file(H5Fcreate("iotdbfs-h02-fixture", H5F_ACC_TRUNC,
    H5P_DEFAULT, fapl), H5Fclose);
  const hsize_t dims[]{kind == FixtureKind::LargeChunk ? 64u : 5u,
    kind == FixtureKind::LargeChunk ? 64u : 7u};
  H5Object space(H5Screate_simple(2, dims, nullptr), H5Sclose);
  H5Object dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
  if (kind == FixtureKind::Chunked || kind == FixtureKind::LargeChunk || kind == FixtureKind::Deflate ||
      kind == FixtureKind::UnknownFilter) {
    const hsize_t chunks[]{kind == FixtureKind::LargeChunk ? 64u : 2u,
      kind == FixtureKind::LargeChunk ? 64u : 3u};
    h5check(H5Pset_chunk(dcpl, 2, chunks), "configure fixture chunks");
  }
  if (kind == FixtureKind::Deflate) {
    h5check(H5Pset_shuffle(dcpl), "configure shuffle");
    h5check(H5Pset_deflate(dcpl, 1), "configure deflate");
  }
  if (kind == FixtureKind::UnknownFilter)
    h5check(H5Pset_filter(dcpl, static_cast<H5Z_filter_t>(32029),
      H5Z_FLAG_OPTIONAL, 0, nullptr), "configure unsupported filter");
  if (kind == FixtureKind::ExternalStorage)
    h5check(H5Pset_external(dcpl, "/iotdbfs-h02-unauthorized-raw.bin", 0,
      H5F_UNLIMITED), "configure external raw storage");
  if (kind == FixtureKind::VirtualDataset)
    h5check(H5Pset_virtual(dcpl, space, "/iotdbfs-h02-unauthorized.h5",
      "/values", space), "configure virtual dataset");
  {
    H5Object dataset(H5Dcreate2(file, "/values", H5T_IEEE_F32LE, space,
      H5P_DEFAULT, dcpl, H5P_DEFAULT), H5Dclose);
    if (kind != FixtureKind::ExternalStorage && kind != FixtureKind::VirtualDataset &&
        kind != FixtureKind::UnknownFilter) {
      std::vector<float> values;
      for (std::size_t row = 0; row < dims[0]; ++row)
        for (std::size_t column = 0; column < dims[1]; ++column)
          values.push_back(bias + static_cast<float>(100*row + column));
      h5check(H5Dwrite(dataset, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL,
        H5P_DEFAULT, values.data()), "write fixture values");
    }
    if (object_header) {
      // Test-only legacy address API locates bytes to corrupt; production
      // persists neither opaque object tokens nor guessed object addresses.
      H5O_info1_t info{};
      h5check(H5Oget_info1(dataset, &info), "locate fixture object header");
      *object_header = info.addr;
    }
  }
  if (kind == FixtureKind::ExternalLink)
    h5check(H5Lcreate_external("/iotdbfs-h02-unauthorized.h5", "/values",
      file, "/outside", H5P_DEFAULT, H5P_DEFAULT), "configure external link");
  if (sibling_range) {
    const hsize_t sibling_dims[]{4096};
    H5Object sibling_space(H5Screate_simple(1, sibling_dims, nullptr), H5Sclose);
    H5Object sibling(H5Dcreate2(file, "/sibling", H5T_IEEE_F32LE, sibling_space,
      H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    std::vector<float> values(4096, -12345.0f);
    h5check(H5Dwrite(sibling, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL,
      H5P_DEFAULT, values.data()), "write sibling values");
    const auto offset = H5Dget_offset(sibling);
    if (offset == HADDR_UNDEF) throw std::runtime_error("sibling has no contiguous storage");
    *sibling_range = {offset, values.size() * sizeof(float)};
  }
  h5check(H5Fflush(file, H5F_SCOPE_LOCAL), "flush fixture");
  const auto size = H5Fget_file_image(file, nullptr, 0);
  if (size <= 0) throw std::runtime_error("cannot size fixture image");
  BinaryValue bytes(static_cast<std::size_t>(size));
  if (H5Fget_file_image(file, bytes.data(), bytes.size()) != size)
    throw std::runtime_error("cannot copy fixture image");
  return bytes;
}

enum class ReadFault { None, Failure, Short, Exception, NonstandardException };

class RecordingSource : public QuerySource {
public:
  explicit RecordingSource(BinaryValue bytes, std::int64_t version = 7)
    : bytes(std::move(bytes)) {
    manifest_.key = {"/same/logical-name.h5", version};
    manifest_.format_id = "hdf5";
    manifest_.payload_size = this->bytes.size();
    manifest_.phase = StructuredBlobPhase::Published;
    manifest_.published = true;
    manifest_.revision = 3;
  }
  const StructuredBlobManifest& manifest() const override { return manifest_; }
  QueryResult<StructuredBlobBytes> read_payload(std::uint64_t offset,
      std::uint64_t length) const override {
    calls.push_back({offset, length});
    if (on_read) on_read();
    if (fault == ReadFault::Exception) throw std::runtime_error("callback failure");
    if (fault == ReadFault::NonstandardException) throw 42;
    if (offset > bytes.size() || length > bytes.size() - offset) {
      ++out_of_bounds;
      return failed("out of bounds reached underlying source");
    }
    if (fault == ReadFault::Failure) return failed("injected source read error");
    const auto actual = fault == ReadFault::Short && length ? length - 1 : length;
    return QueryResult<StructuredBlobBytes>::success(BinaryValue(
      bytes.begin() + offset, bytes.begin() + offset + actual));
  }
  QueryResult<StructuredBlobBytes> read_layout(std::uint64_t, std::uint64_t) const override {
    return failed("H02 has no layout artifact");
  }
  StructuredBlobManifest manifest_;
  BinaryValue bytes;
  mutable std::vector<std::pair<std::uint64_t, std::uint64_t>> calls;
  mutable std::size_t out_of_bounds{0};
  mutable ReadFault fault{ReadFault::None};
  std::function<void()> on_read;
private:
  static QueryResult<StructuredBlobBytes> failed(const char* text) {
    return QueryResult<StructuredBlobBytes>::failure(QueryStatus::failure(
      QueryError::ReadFailure, QueryPhase::Storage, text));
  }
};

class LocalSource final : public RecordingSource {
public:
  explicit LocalSource(BinaryValue bytes) : RecordingSource(std::move(bytes)) {
    char directory[] = "/tmp/iotdbfs-native-driver-XXXXXX";
    if (!mkdtemp(directory)) throw std::runtime_error("managed fixture directory failed");
    root = directory;
    NativeFileStore store(root);
    NativeFileLocation location;
    location.namespace_identity = {"11111111-1111-4111-8111-111111111111",
      "22222222-2222-4222-8222-222222222222"};
    location.key = manifest_.key;
    location.descriptor_id = std::string(64, 'a');
    location.seal_id = std::string(64, 'b');
    location.payload_root = std::string(64, 'c');
    location.size = this->bytes.size();
    auto installed = store.install(location, [this](auto offset, auto length) {
      return StructuredBlobResult<StructuredBlobBytes>::success(StructuredBlobBytes(
        this->bytes.begin() + offset, this->bytes.begin() + offset + length));
    });
    if (!installed.ok()) throw std::runtime_error(installed.status().message());
    location_ = installed.value();
    auto identity = store.cache_identity(location_);
    if (!identity.ok()) throw std::runtime_error(identity.status().message());
    identity_ = identity.value();
    auto opened = store.open(installed.value());
    if (!opened.ok()) throw std::runtime_error(opened.status().message());
    handle = std::move(opened.value());
  }
  ~LocalSource() override {
    handle.reset();
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
  }
  QueryResult<std::shared_ptr<const NativeFileHandle>> open_native_file() const override {
    return QueryResult<std::shared_ptr<const NativeFileHandle>>::success(handle);
  }
  QueryStatus reserve_logical_payload(std::uint64_t length) const override {
    if (length > byte_limit - std::min(charged, byte_limit))
      return QueryStatus::failure(QueryError::ResourceExhausted, QueryPhase::Storage,
        "local fixture request budget exhausted");
    charged += length;
    return QueryStatus::success();
  }
  QueryResult<std::optional<std::string>> native_file_cache_identity() const override {
    return QueryResult<std::optional<std::string>>::success(identity_);
  }
  QueryResult<std::shared_ptr<const NativeFileHandle>> reuse_native_file(
      const std::shared_ptr<const NativeFileDescriptor> &cached) const override {
    NativeFileStore store(root);
    auto reused = store.reuse(location_, cached);
    if (!reused.ok()) return QueryResult<std::shared_ptr<const NativeFileHandle>>::failure(
      QueryStatus::failure(QueryError::CorruptPayload, QueryPhase::Storage, reused.status().message()));
    return QueryResult<std::shared_ptr<const NativeFileHandle>>::success(std::move(reused.value()));
  }
  NativeFileLocation location_;
  std::string identity_;
  std::shared_ptr<const NativeFileHandle> handle;
  std::string root;
  mutable std::uint64_t charged{0};
  std::uint64_t byte_limit{std::numeric_limits<std::uint64_t>::max()};
};

// A fresh request over a separately retained immutable fixture. The request's
// guard deliberately owns a proof so tests detect stale proof retention.
class RequestLocalSource final : public QuerySource {
public:
  struct Proof { bool admitted{true}; std::size_t checks{0}; };
  explicit RequestLocalSource(std::shared_ptr<LocalSource> data)
    : data(std::move(data)), proof(std::make_shared<Proof>()) {}
  const StructuredBlobManifest &manifest() const override { return data->manifest(); }
  QueryResult<StructuredBlobBytes> read_payload(std::uint64_t, std::uint64_t) const override {
    throw std::runtime_error("database reads forbidden");
  }
  QueryResult<StructuredBlobBytes> read_layout(std::uint64_t, std::uint64_t) const override {
    throw std::runtime_error("layout reads forbidden");
  }
  QueryResult<std::optional<std::string>> native_file_cache_identity() const override {
    return QueryResult<std::optional<std::string>>::success(head + data->identity_);
  }
  QueryResult<std::shared_ptr<const NativeFileHandle>> open_native_file() const override {
    ++opens;
    return guarded(data->handle);
  }
  QueryResult<std::shared_ptr<const NativeFileHandle>> reuse_native_file(
      const std::shared_ptr<const NativeFileDescriptor> &cached) const override {
    ++reuses;
    if (throw_reuse) throw std::runtime_error("injected rebind exception");
    auto fresh = data->reuse_native_file(cached);
    if (!fresh.ok()) return fresh;
    return guarded(fresh.value());
  }
  QueryStatus reserve_logical_payload(std::uint64_t n) const override {
    if (n > limit - std::min(limit, charged)) return QueryStatus::failure(
      QueryError::ResourceExhausted, QueryPhase::Storage, "request byte budget");
    if (shared_budget) {
      if (n > shared_limit - std::min(shared_limit, *shared_budget)) return QueryStatus::failure(
        QueryError::ResourceExhausted, QueryPhase::Storage, "aggregate request byte budget");
      *shared_budget += n;
    }
    ++charge_calls; charged += n; return QueryStatus::success();
  }
  QueryResult<std::shared_ptr<const NativeFileHandle>> guarded(
      std::shared_ptr<const NativeFileHandle> handle) const {
    return QueryResult<std::shared_ptr<const NativeFileHandle>>::success(
      handle->with_read_guard([proof = proof] {
        ++proof->checks;
        return proof->admitted ? StructuredBlobStatus::success() :
          StructuredBlobStatus::failure(StructuredBlobError::InvalidState, "revoked request");
      }));
  }
  std::shared_ptr<LocalSource> data;
  std::shared_ptr<Proof> proof;
  std::string head{"head-1:"};
  mutable std::size_t opens{0}, reuses{0}, charge_calls{0};
  mutable std::uint64_t charged{0};
  std::uint64_t limit{std::numeric_limits<std::uint64_t>::max()};
  bool throw_reuse{false};
  std::shared_ptr<std::uint64_t> shared_budget;
  std::uint64_t shared_limit{std::numeric_limits<std::uint64_t>::max()};
};

std::vector<float> expected(float bias = 0) {
  std::vector<float> values;
  for (std::size_t row = 1; row < 4; ++row)
    for (std::size_t column = 2; column < 6; ++column)
      values.push_back(bias + static_cast<float>(100*row + column));
  return values;
}

class Hdf5SourceDriverTest : public ::testing::Test {};

TEST_F(Hdf5SourceDriverTest, ManagedFilesystemUsesNativeIndexWithoutDatabasePayload) {
  auto source = std::make_shared<LocalSource>(fixture(FixtureKind::Deflate));
  source->fault = ReadFault::Exception; // Any database payload read fails the test.
  QueryProfileRecord profile;
  ScopedQueryProfile profiling(&profile);
  auto opened = Hdf5SourceDriver::open_local(source);
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  auto selected = opened.value()->read("/values", {1, 2}, {3, 4});
  ASSERT_TRUE(selected.ok()) << selected.status().message();
  EXPECT_EQ(std::get<std::vector<float>>(selected.value().values.values), expected());
  EXPECT_TRUE(source->calls.empty());
  const auto trace = opened.value()->trace();
  EXPECT_EQ(trace.metadata_bytes + trace.raw_data_bytes, source->charged);
  EXPECT_EQ(profile.counters["hdf5.native.filesystem.opens"], 1u);
  EXPECT_EQ(profile.counters["hdf5.native.filesystem.calls"], trace.reads.size());
  EXPECT_EQ(profile.counters["hdf5.native.filesystem.bytes"], source->charged);
}

TEST_F(Hdf5SourceDriverTest, ManagedFilesystemRequiresCapabilityAndMatchingSealedSize) {
  auto old_source = std::make_shared<RecordingSource>(fixture());
  auto missing = Hdf5SourceDriver::open_local(old_source);
  ASSERT_FALSE(missing.ok());
  EXPECT_EQ(missing.status().code(), QueryError::UnsupportedCapability);
  EXPECT_TRUE(old_source->calls.empty());
  auto source = std::make_shared<LocalSource>(fixture());
  ++source->manifest_.payload_size;
  auto changed = Hdf5SourceDriver::open_local(source);
  ASSERT_FALSE(changed.ok());
  EXPECT_EQ(changed.status().code(), QueryError::CorruptPayload);
  EXPECT_TRUE(source->calls.empty());
}

TEST_F(Hdf5SourceDriverTest, ManagedFilesystemPreservesRequestAndVfdReadBudgets) {
  auto source = std::make_shared<LocalSource>(fixture());
  source->byte_limit = 1;
  auto request = Hdf5SourceDriver::open_local(source);
  ASSERT_FALSE(request.ok());
  EXPECT_EQ(request.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(source->charged, 0u);
  source->byte_limit = std::numeric_limits<std::uint64_t>::max();
  Hdf5SourceOptions options;
  options.max_read_bytes = 1;
  auto bounded = Hdf5SourceDriver::open_local(source, options);
  ASSERT_FALSE(bounded.ok());
  EXPECT_EQ(bounded.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(source->charged, 0u);
  EXPECT_TRUE(source->calls.empty());
}

Hdf5SourceOptions cached_options() {
  Hdf5SourceOptions options;
  options.native_selection_identity = "/values|origin=1,2|shape=3,4";
  return options;
}

TEST_F(Hdf5SourceDriverTest, NativeSessionWarmHitRetainsHandlesAndChunkWithoutSourceReads) {
  auto pool = std::make_shared<Hdf5NativeSessionPool>();
  auto data = std::make_shared<LocalSource>(fixture(FixtureKind::Deflate));
  std::uint64_t logical_bytes = 0, logical_calls = 0;
  for (unsigned pass = 0; pass < 2; ++pass) {
    auto request = std::make_shared<RequestLocalSource>(data);
    QueryProfileRecord profile;
    ScopedQueryProfile scope(&profile);
    auto opened = Hdf5SourceDriver::open_local(request, cached_options(), pool);
    ASSERT_TRUE(opened.ok()) << opened.status().message();
    auto descriptor = opened.value()->describe("/values");
    ASSERT_TRUE(descriptor.ok()) << descriptor.status().message();
    auto result = opened.value()->read("/values", {1, 2}, {3, 4});
    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(std::get<std::vector<float>>(result.value().values.values), expected());
    EXPECT_GE(request->proof->checks, 5u);
    if (pass) {
      EXPECT_EQ(profile.counters["hdf5.native.session.hits"], 1u);
      EXPECT_EQ(profile.counters["hdf5.native.session.rebinds"], 1u);
      EXPECT_EQ(profile.counters["hdf5.native.file.opens"], 0u);
      EXPECT_EQ(profile.counters["hdf5.native.dataset.opens"], 0u);
      EXPECT_EQ(profile.counters["hdf5.native.filesystem.calls"], 0u);
      EXPECT_EQ(request->opens, 0u);
      EXPECT_EQ(request->reuses, 1u);
      EXPECT_EQ(request->charged, logical_bytes);
      EXPECT_EQ(request->charge_calls, logical_calls);
      EXPECT_GT(profile.counters["hdf5.native.logical.replays"], 0u);
      EXPECT_TRUE(opened.value()->trace().reads.empty());
    } else {
      EXPECT_EQ(profile.counters["hdf5.native.file.opens"], 1u);
      EXPECT_EQ(profile.counters["hdf5.native.dataset.opens"], 1u);
      EXPECT_GT(request->charged, 0u);
      logical_bytes = request->charged; logical_calls = request->charge_calls;
    }
  }
  EXPECT_EQ(pool->stats().files, 1u);
  EXPECT_EQ(pool->stats().datasets, 1u);
  EXPECT_EQ(pool->stats().busy, 0u);
  EXPECT_LE(pool->stats().accounted_bytes, 64ULL * 1024 * 1024);
}

TEST_F(Hdf5SourceDriverTest, NativeSessionIdleDropsRequestProofAndCancellationClosures) {
  auto pool = std::make_shared<Hdf5NativeSessionPool>();
  auto data = std::make_shared<LocalSource>(fixture(FixtureKind::Deflate));
  std::weak_ptr<const QuerySource> weak_request;
  std::weak_ptr<RequestLocalSource::Proof> weak_proof;
  std::weak_ptr<int> weak_cancel;
  {
    auto request = std::make_shared<RequestLocalSource>(data);
    weak_request = request; weak_proof = request->proof;
    auto marker = std::make_shared<int>(0); weak_cancel = marker;
    Hdf5SourceOptions options = cached_options();
    options.cancelled = [marker] { return *marker != 0; };
    auto opened = Hdf5SourceDriver::open_local(request, std::move(options), pool);
    ASSERT_TRUE(opened.ok()) << opened.status().message();
    ASSERT_TRUE(opened.value()->read("/values", {1, 2}, {3, 4}).ok());
    EXPECT_FALSE(weak_request.expired());
  }
  EXPECT_TRUE(weak_request.expired());
  EXPECT_TRUE(weak_proof.expired());
  EXPECT_TRUE(weak_cancel.expired());
  EXPECT_EQ(pool->stats().files, 1u);
}

TEST_F(Hdf5SourceDriverTest, NativeSessionWarmHitChecksNewAuthorityAndRetiresFailedRebind) {
  auto pool = std::make_shared<Hdf5NativeSessionPool>();
  auto data = std::make_shared<LocalSource>(fixture(FixtureKind::Deflate));
  {
    auto first = Hdf5SourceDriver::open_local(std::make_shared<RequestLocalSource>(data), cached_options(), pool);
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(first.value()->read("/values", {1, 2}, {3, 4}).ok());
  }
  auto revoked = std::make_shared<RequestLocalSource>(data);
  revoked->proof->admitted = false;
  auto denied = Hdf5SourceDriver::open_local(revoked, cached_options(), pool);
  EXPECT_FALSE(denied.ok());
  EXPECT_EQ(pool->stats().files, 0u);
  EXPECT_EQ(revoked->opens, 0u); // Invalid capability is never treated as a miss.
  {
    auto first = Hdf5SourceDriver::open_local(std::make_shared<RequestLocalSource>(data), cached_options(), pool);
    ASSERT_TRUE(first.ok());
  }
  auto throwing = std::make_shared<RequestLocalSource>(data);
  throwing->throw_reuse = true;
  auto failed = Hdf5SourceDriver::open_local(throwing, cached_options(), pool);
  EXPECT_FALSE(failed.ok());
  EXPECT_EQ(pool->stats().files, 0u);
  EXPECT_EQ(pool->stats().busy, 0u);
}

TEST_F(Hdf5SourceDriverTest, NativeSessionChecksIdentityAfterChunkHitAndHonoursCancellation) {
  auto pool = std::make_shared<Hdf5NativeSessionPool>();
  auto data = std::make_shared<LocalSource>(fixture(FixtureKind::Deflate));
  {
    auto first = Hdf5SourceDriver::open_local(std::make_shared<RequestLocalSource>(data), cached_options(), pool);
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(first.value()->read("/values", {1, 2}, {3, 4}).ok());
  }
  auto request = std::make_shared<RequestLocalSource>(data);
  bool cancelled = false;
  Hdf5SourceOptions options = cached_options(); options.cancelled = [&] { return cancelled; };
  auto opened = Hdf5SourceDriver::open_local(request, options, pool);
  ASSERT_TRUE(opened.ok());
  cancelled = true;
  auto stopped = opened.value()->read("/values", {1, 2}, {3, 4});
  ASSERT_FALSE(stopped.ok()); EXPECT_EQ(stopped.status().code(), QueryError::Cancelled);
  opened.value().reset();
  EXPECT_EQ(pool->stats().files, 0u);
  {
    auto first = Hdf5SourceDriver::open_local(std::make_shared<RequestLocalSource>(data), cached_options(), pool);
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(first.value()->read("/values", {1, 2}, {3, 4}).ok());
  }
  auto live = Hdf5SourceDriver::open_local(request, cached_options(), pool);
  ASSERT_TRUE(live.ok());
  // Replacing the pathname with the exact same bytes must fail even though
  // HDF5 can satisfy the following request entirely from retained raw chunks.
  const auto path = std::filesystem::path(data->root) / data->location_.relative_path;
  const auto replacement = path.string() + ".replacement";
  std::filesystem::copy_file(path, replacement);
  std::filesystem::rename(replacement, path);
  auto changed = live.value()->read("/values", {1, 2}, {3, 4});
  ASSERT_FALSE(changed.ok());
  EXPECT_EQ(changed.status().code(), QueryError::CorruptPayload);
}

TEST_F(Hdf5SourceDriverTest, NativeSessionTraceAndBudgetAreFreshAndActualReadsStillCharged) {
  auto pool = std::make_shared<Hdf5NativeSessionPool>();
  auto data = std::make_shared<LocalSource>(fixture(FixtureKind::Deflate));
  std::uint64_t cold_bytes = 0;
  {
    auto request = std::make_shared<RequestLocalSource>(data);
    auto first = Hdf5SourceDriver::open_local(request, cached_options(), pool);
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(first.value()->read("/values", {1, 2}, {3, 4}).ok());
    cold_bytes = request->charged;
  }
  auto request = std::make_shared<RequestLocalSource>(data); request->limit = cold_bytes;
  auto warm = Hdf5SourceDriver::open_local(request, cached_options(), pool);
  ASSERT_TRUE(warm.ok());
  EXPECT_TRUE(warm.value()->read("/values", {1, 2}, {3, 4}).ok());
  EXPECT_EQ(request->charged, cold_bytes);
  EXPECT_TRUE(warm.value()->trace().reads.empty());
  auto bytes = warm.value()->diagnostic_read_range(0, 1, false);
  ASSERT_FALSE(bytes.ok()); EXPECT_EQ(bytes.status().code(), QueryError::ResourceExhausted);
  warm.value().reset();
  EXPECT_EQ(pool->stats().files, 0u);
  {
    auto first = Hdf5SourceDriver::open_local(std::make_shared<RequestLocalSource>(data), cached_options(), pool);
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(first.value()->read("/values", {1, 2}, {3, 4}).ok());
  }
  auto limited = std::make_shared<RequestLocalSource>(data); limited->limit = cold_bytes - 1;
  auto failed = Hdf5SourceDriver::open_local(limited, cached_options(), pool);
  ASSERT_FALSE(failed.ok()); EXPECT_EQ(failed.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(limited->opens, 0u); EXPECT_LE(limited->charged, limited->limit);
  EXPECT_EQ(pool->stats().files, 0u);
}

TEST_F(Hdf5SourceDriverTest, NativeSessionWarmLogicalDemandHonoursAggregateBudgetAndRoiScope) {
  auto pool = std::make_shared<Hdf5NativeSessionPool>();
  auto data = std::make_shared<LocalSource>(fixture(FixtureKind::Deflate));
  std::uint64_t demand = 0;
  {
    auto request = std::make_shared<RequestLocalSource>(data);
    auto driver = Hdf5SourceDriver::open_local(request, cached_options(), pool);
    ASSERT_TRUE(driver.ok());
    ASSERT_TRUE(driver.value()->read("/values", {1, 2}, {3, 4}).ok());
    demand = request->charged;
  }
  auto aggregate = std::make_shared<std::uint64_t>(0);
  for (unsigned item = 0; item < 2; ++item) {
    auto request = std::make_shared<RequestLocalSource>(data);
    request->shared_budget = aggregate; request->shared_limit = 2 * demand - 1;
    auto driver = Hdf5SourceDriver::open_local(request, cached_options(), pool);
    if (item == 0) {
      ASSERT_TRUE(driver.ok());
      ASSERT_TRUE(driver.value()->read("/values", {1, 2}, {3, 4}).ok());
      EXPECT_EQ(*aggregate, demand);
    } else {
      ASSERT_FALSE(driver.ok());
      EXPECT_EQ(driver.status().code(), QueryError::ResourceExhausted);
      EXPECT_EQ(request->opens, 0u);
      EXPECT_LE(*aggregate, 2 * demand - 1);
    }
  }
  {
    auto driver = Hdf5SourceDriver::open_local(std::make_shared<RequestLocalSource>(data), cached_options(), pool);
    ASSERT_TRUE(driver.ok());
    ASSERT_TRUE(driver.value()->read("/values", {1, 2}, {3, 4}).ok());
  }
  auto request = std::make_shared<RequestLocalSource>(data);
  auto other_roi = cached_options(); other_roi.native_selection_identity = "/values|origin=0,0|shape=1,1";
  auto changed = Hdf5SourceDriver::open_local(request, other_roi, pool);
  ASSERT_TRUE(changed.ok());
  EXPECT_EQ(request->opens, 1u); EXPECT_EQ(request->reuses, 0u);
  EXPECT_TRUE(changed.value()->read("/values", {0, 0}, {1, 1}).ok());
  EXPECT_EQ(pool->stats().files, 2u);
}

TEST_F(Hdf5SourceDriverTest, NativeSessionFullHeadSeparatesEntriesAndBusyLeaseUsesBoundedFallback) {
  auto pool = std::make_shared<Hdf5NativeSessionPool>();
  auto data = std::make_shared<LocalSource>(fixture());
  std::vector<std::unique_ptr<Hdf5SourceDriver>> held;
  for (unsigned i = 0; i < 8; ++i) {
    auto request = std::make_shared<RequestLocalSource>(data);
    request->head = "full-head-" + std::to_string(i) + ":";
    auto opened = Hdf5SourceDriver::open_local(request, cached_options(), pool);
    ASSERT_TRUE(opened.ok()) << opened.status().message();
    ASSERT_TRUE(opened.value()->read("/values", {1, 2}, {3, 4}).ok());
    held.push_back(std::move(opened.value()));
  }
  EXPECT_EQ(pool->stats().busy, 8u); EXPECT_EQ(pool->stats().datasets, 8u);
  QueryProfileRecord profile;
  ScopedQueryProfile scope(&profile);
  auto extra = Hdf5SourceDriver::open_local(std::make_shared<RequestLocalSource>(data), cached_options(), pool);
  ASSERT_TRUE(extra.ok()) << extra.status().message();
  EXPECT_EQ(profile.counters["hdf5.native.session.fallbacks"], 1u);
  EXPECT_EQ(pool->stats().files, 8u);
  auto task = std::async(std::launch::async, [pool, data] {
    auto simultaneous = Hdf5SourceDriver::open_local(std::make_shared<RequestLocalSource>(data), cached_options(), pool);
    if (!simultaneous.ok()) return false;
    return simultaneous.value()->read("/values", {1, 2}, {3, 4}).ok();
  });
  EXPECT_TRUE(task.get());
  held.clear(); extra.value().reset();
  EXPECT_EQ(pool->stats().busy, 0u);
  auto replacement = Hdf5SourceDriver::open_local(std::make_shared<RequestLocalSource>(data), cached_options(), pool);
  ASSERT_TRUE(replacement.ok());
  EXPECT_EQ(pool->stats().files, 8u); EXPECT_LE(pool->stats().datasets, 8u);
  EXPECT_LE(pool->stats().accounted_bytes, 64ULL * 1024 * 1024);
}

TEST_F(Hdf5SourceDriverTest, NativeSubsetMatchesFormulaAcrossLayoutsAndDoesNotReadWholeSource) {
  for (const auto kind : {FixtureKind::Contiguous, FixtureKind::Chunked, FixtureKind::Deflate}) {
    auto bytes = fixture(kind);
    bytes.resize(bytes.size() + 2 * 1024 * 1024, 0); // Valid trailing bytes are not an image-read excuse.
    auto source = std::make_shared<RecordingSource>(std::move(bytes));
    auto opened = Hdf5SourceDriver::open(source);
    ASSERT_TRUE(opened.ok()) << opened.status().message();
    auto descriptor = opened.value()->describe("/values");
    ASSERT_TRUE(descriptor.ok()) << descriptor.status().message();
    EXPECT_EQ(descriptor.value().shape, (std::vector<std::uint64_t>{5, 7}));
    EXPECT_EQ(descriptor.value().value_type, LogicalTypeId::Float32);
    auto selected = opened.value()->read("/values", {1, 2}, {3, 4});
    ASSERT_TRUE(selected.ok()) << selected.status().message();
    EXPECT_EQ(selected.value().origin, (std::vector<std::uint64_t>{1, 2}));
    EXPECT_EQ(selected.value().shape, (std::vector<std::uint64_t>{3, 4}));
    EXPECT_EQ(std::get<std::vector<float>>(selected.value().values.values), expected());
    EXPECT_FALSE(selected.value().values.type.nullable);
    const auto trace = opened.value()->trace();
    EXPECT_GT(trace.metadata_bytes, 0u);
    EXPECT_GT(trace.raw_data_bytes, 0u);
    EXPECT_LT(trace.metadata_bytes + trace.raw_data_bytes, source->bytes.size());
    ASSERT_EQ(trace.reads.size(), source->calls.size());
    std::uint64_t metadata_bytes = 0, raw_bytes = 0;
    for (std::size_t i = 0; i < trace.reads.size(); ++i) {
      const auto& read = trace.reads[i];
      EXPECT_TRUE(read.success);
      EXPECT_EQ(read.offset, source->calls[i].first);
      EXPECT_EQ(read.length, source->calls[i].second);
      EXPECT_LE(read.offset, source->bytes.size());
      EXPECT_LE(read.length, source->bytes.size() - read.offset);
      (read.raw_data ? raw_bytes : metadata_bytes) += read.length;
    }
    EXPECT_EQ(trace.metadata_bytes, metadata_bytes);
    EXPECT_EQ(trace.raw_data_bytes, raw_bytes);
    EXPECT_EQ(source->out_of_bounds, 0u);
  }
}

TEST_F(Hdf5SourceDriverTest, QuerySourceLifetimeAndSameNameVersionIsolation) {
  auto first = std::make_shared<RecordingSource>(fixture(FixtureKind::Chunked, 0), 7);
  auto second = std::make_shared<RecordingSource>(fixture(FixtureKind::Chunked, 1000), 7);
  ASSERT_NE(first->storage_namespace(), second->storage_namespace());
  const std::weak_ptr<RecordingSource> first_lifetime = first;
  auto a = Hdf5SourceDriver::open(first);
  auto b = Hdf5SourceDriver::open(second);
  ASSERT_TRUE(a.ok()) << a.status().message();
  ASSERT_TRUE(b.ok()) << b.status().message();
  first.reset();
  EXPECT_FALSE(first_lifetime.expired());
  auto ar = a.value()->read("/values", {1, 2}, {3, 4});
  auto br = b.value()->read("/values", {1, 2}, {3, 4});
  ASSERT_TRUE(ar.ok()) << ar.status().message();
  ASSERT_TRUE(br.ok()) << br.status().message();
  EXPECT_EQ(std::get<std::vector<float>>(ar.value().values.values), expected());
  EXPECT_EQ(std::get<std::vector<float>>(br.value().values.values), expected(1000));
  a.value().reset();
  EXPECT_TRUE(first_lifetime.expired());
  auto older = std::make_shared<RecordingSource>(fixture(FixtureKind::Chunked, 2000), 6);
  auto c = Hdf5SourceDriver::open(older);
  ASSERT_TRUE(c.ok()) << c.status().message();
  auto cr = c.value()->read("/values", {1, 2}, {3, 4});
  ASSERT_TRUE(cr.ok()) << cr.status().message();
  EXPECT_EQ(std::get<std::vector<float>>(cr.value().values.values), expected(2000));
}

TEST_F(Hdf5SourceDriverTest, OwnedQuerySourceRequiresBothPublishedFlagAndPublicationPhase) {
  const auto bytes = fixture();
  for (const auto phase : {StructuredBlobPhase::PayloadOpen, StructuredBlobPhase::PayloadSealed,
      StructuredBlobPhase::LayoutStored, StructuredBlobPhase::Described}) {
    auto source = std::make_shared<RecordingSource>(bytes);
    source->manifest_.phase = phase;
    source->manifest_.published = true;
    auto opened = Hdf5SourceDriver::open(source);
    ASSERT_FALSE(opened.ok());
    EXPECT_EQ(opened.status().code(), QueryError::SourceNotPublished);
    EXPECT_TRUE(source->calls.empty());
  }
  auto source = std::make_shared<RecordingSource>(bytes);
  source->manifest_.published = false;
  auto opened = Hdf5SourceDriver::open(source);
  ASSERT_FALSE(opened.ok());
  EXPECT_EQ(opened.status().code(), QueryError::SourceNotPublished);
  EXPECT_TRUE(source->calls.empty());
}

TEST_F(Hdf5SourceDriverTest, MissingDatasetIsDistinctFromExistingLinkWithCorruptObjectHeader) {
  std::uint64_t address = 0;
  auto bytes = fixture(FixtureKind::Chunked, 0, nullptr, &address);
  auto source = std::make_shared<RecordingSource>(bytes);
  auto opened = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  auto missing = opened.value()->describe("/missing");
  ASSERT_FALSE(missing.ok());
  EXPECT_EQ(missing.status().code(), QueryError::UnknownField);
  ASSERT_LT(address, bytes.size());
  bytes[address] = 0xff; // Invalid V1 version or V2 signature; link remains intact.
  auto corrupt_source = std::make_shared<RecordingSource>(std::move(bytes));
  auto corrupt = Hdf5SourceDriver::open(corrupt_source);
  ASSERT_TRUE(corrupt.ok()) << corrupt.status().message();
  auto damaged = corrupt.value()->describe("/values");
  ASSERT_FALSE(damaged.ok());
  EXPECT_EQ(damaged.status().code(), QueryError::CorruptPayload);
}

TEST_F(Hdf5SourceDriverTest, ConcurrentSamePathSourcesKeepVersionsAndNamespacesIsolated) {
  std::vector<std::shared_ptr<RecordingSource>> sources;
  for (int i = 0; i < 4; ++i)
    sources.push_back(std::make_shared<RecordingSource>(
      fixture(FixtureKind::Chunked, 1000.0f * i), 7 + i % 2));
  std::vector<std::future<QueryResult<Hdf5NativeSubset>>> results;
  for (const auto& source : sources)
    results.push_back(std::async(std::launch::async, [source] {
      auto opened = Hdf5SourceDriver::open(source);
      if (!opened.ok()) return QueryResult<Hdf5NativeSubset>::failure(opened.status());
      return opened.value()->read("/values", {1, 2}, {3, 4});
    }));
  for (std::size_t i = 0; i < results.size(); ++i) {
    auto value = results[i].get();
    ASSERT_TRUE(value.ok()) << value.status().message();
    EXPECT_EQ(std::get<std::vector<float>>(value.value().values.values), expected(1000.0f*i));
    EXPECT_EQ(sources[i]->out_of_bounds, 0u);
  }
}

TEST_F(Hdf5SourceDriverTest, SubsetDoesNotReadSiblingDatasetPayload) {
  std::pair<std::uint64_t, std::uint64_t> sibling;
  auto source = std::make_shared<RecordingSource>(fixture(FixtureKind::Chunked, 0, &sibling));
  auto opened = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  auto selected = opened.value()->read("/values", {1, 2}, {3, 4});
  ASSERT_TRUE(selected.ok()) << selected.status().message();
  EXPECT_EQ(std::get<std::vector<float>>(selected.value().values.values), expected());
  bool saw_raw = false;
  for (const auto& read : opened.value()->trace().reads) {
    if (!read.raw_data) continue;
    saw_raw = true;
    EXPECT_TRUE(read.offset + read.length <= sibling.first ||
      read.offset >= sibling.first + sibling.second);
  }
  EXPECT_TRUE(saw_raw);
}

TEST_F(Hdf5SourceDriverTest, SynchronousIngestionSourceMatchesOwnedSourceAndReleasesBorrowedCallbacks) {
  auto bytes = std::make_shared<BinaryValue>(fixture());
  std::size_t reads = 0;
  bool active = true;
  FormatSource source{bytes->size(), [bytes, &reads, &active](std::uint64_t offset,
      std::uint64_t length) -> std::optional<BinaryValue> {
    EXPECT_TRUE(active);
    ++reads;
    if (offset > bytes->size() || length > bytes->size() - offset) return std::nullopt;
    return BinaryValue(bytes->begin() + offset, bytes->begin() + offset + length);
  }};
  const auto owners = bytes.use_count();
  std::vector<float> values;
  auto status = with_hdf5_ingestion_source(source, {}, [&](const Hdf5SourceDriver& driver) {
    auto result = driver.read("/values", {1, 2}, {3, 4});
    if (!result.ok()) return result.status();
    values = std::get<std::vector<float>>(result.value().values.values);
    return QueryStatus::success();
  });
  active = false;
  ASSERT_TRUE(status.ok()) << status.message();
  EXPECT_EQ(values, expected());
  EXPECT_GT(reads, 0u);
  EXPECT_EQ(bytes.use_count(), owners);
}

TEST_F(Hdf5SourceDriverTest, DiagnosticRangesHonorZeroExactEofOverflowAndUnderlyingBounds) {
  auto source = std::make_shared<RecordingSource>(fixture());
  auto opened = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  const auto calls = source->calls.size();
  const auto size = source->bytes.size();
  auto zero = opened.value()->diagnostic_read_range(size, 0);
  ASSERT_TRUE(zero.ok()) << zero.status().message();
  EXPECT_TRUE(zero.value().empty());
  EXPECT_EQ(source->calls.size(), calls);
  auto tail = opened.value()->diagnostic_read_range(size - 1, 1);
  ASSERT_TRUE(tail.ok()) << tail.status().message();
  EXPECT_EQ(tail.value(), BinaryValue{source->bytes.back()});
  const auto after_valid = source->calls.size();
  for (const auto& range : std::vector<std::pair<std::uint64_t, std::uint64_t>>{
      {size, 1}, {size + 1, 0}, {std::numeric_limits<std::uint64_t>::max(), 1},
      {1, std::numeric_limits<std::uint64_t>::max()}}) {
    auto invalid = opened.value()->diagnostic_read_range(range.first, range.second);
    EXPECT_FALSE(invalid.ok());
  }
  EXPECT_EQ(source->calls.size(), after_valid);
  EXPECT_EQ(source->out_of_bounds, 0u);
}

TEST_F(Hdf5SourceDriverTest, TruncatedSourcesFailWithoutOutOfBoundsUnderlyingReads) {
  const auto original = fixture();
  for (const auto size : std::vector<std::size_t>{0, 3, 8, original.size() - 1}) {
    auto bytes = original;
    bytes.resize(size);
    auto source = std::make_shared<RecordingSource>(std::move(bytes));
    auto opened = Hdf5SourceDriver::open(source);
    ASSERT_FALSE(opened.ok()) << "truncated source size " << size;
    EXPECT_EQ(source->out_of_bounds, 0u);
    for (const auto& call : source->calls) {
      ASSERT_LE(call.first, size);
      EXPECT_LE(call.second, size - call.first);
    }
  }
}

TEST_F(Hdf5SourceDriverTest, FailedShortAndThrowingCallbacksNeverCrossTheCAbiAndRetryCleansUp) {
  const auto bytes = fixture();
  for (const auto fault : {ReadFault::Failure, ReadFault::Short,
      ReadFault::Exception, ReadFault::NonstandardException}) {
    auto source = std::make_shared<RecordingSource>(bytes);
    const auto before = H5Fget_obj_count(H5F_OBJ_ALL, H5F_OBJ_ALL);
    source->fault = fault;
    auto failed_open = Hdf5SourceDriver::open(source);
    ASSERT_FALSE(failed_open.ok());
    EXPECT_EQ(failed_open.status().code(), QueryError::ReadFailure);
    EXPECT_EQ(H5Fget_obj_count(H5F_OBJ_ALL, H5F_OBJ_ALL), before);
    source->fault = ReadFault::None;
    auto opened = Hdf5SourceDriver::open(source);
    ASSERT_TRUE(opened.ok()) << opened.status().message();
    source->fault = fault;
    auto failed_read = opened.value()->diagnostic_read_range(0, 8);
    ASSERT_FALSE(failed_read.ok());
    EXPECT_EQ(failed_read.status().code(), QueryError::ReadFailure);
    auto trace = opened.value()->trace();
    ASSERT_FALSE(trace.reads.empty());
    EXPECT_FALSE(trace.reads.back().success);
    opened.value().reset();
    EXPECT_EQ(H5Fget_obj_count(H5F_OBJ_ALL, H5F_OBJ_ALL), before);
    source->fault = ReadFault::None;
    auto retry = Hdf5SourceDriver::open(source);
    ASSERT_TRUE(retry.ok()) << retry.status().message();
    auto selected = retry.value()->read("/values", {1, 2}, {3, 4});
    ASSERT_TRUE(selected.ok()) << selected.status().message();
    EXPECT_EQ(std::get<std::vector<float>>(selected.value().values.values), expected());
  }
}

TEST_F(Hdf5SourceDriverTest, FailedThenSuccessfulCallsRestoreCallerErrorStackAndHandler) {
  auto source = std::make_shared<RecordingSource>(fixture());
  SavedErrorState restore;
  std::size_t errors = 0;
  h5check(H5Eset_auto2(H5E_DEFAULT, count_hdf_error, &errors), "set caller error handler");
  h5check(H5Epush2(H5E_DEFAULT, __FILE__, __func__, __LINE__, H5E_ERR_CLS,
    H5E_FILE, H5E_CANTOPENFILE, "caller marker"), "set caller error stack");
  ASSERT_EQ(H5Eget_num(H5E_DEFAULT), 1);
  source->fault = ReadFault::Failure;
  auto failed = Hdf5SourceDriver::open(source);
  ASSERT_FALSE(failed.ok());
  EXPECT_EQ(failed.status().code(), QueryError::ReadFailure);
  EXPECT_EQ(H5Eget_num(H5E_DEFAULT), 1);
  H5E_auto2_t handler = nullptr;
  void* data = nullptr;
  h5check(H5Eget_auto2(H5E_DEFAULT, &handler, &data), "read restored handler");
  EXPECT_EQ(handler, count_hdf_error);
  EXPECT_EQ(data, &errors);
  EXPECT_EQ(errors, 0u);
  h5check(H5Eclear2(H5E_DEFAULT), "clear caller marker");
  source->fault = ReadFault::None;
  auto opened = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  auto value = opened.value()->read("/values", {1, 2}, {3, 4});
  ASSERT_TRUE(value.ok()) << value.status().message();
  EXPECT_EQ(H5Eget_num(H5E_DEFAULT), 0);
  EXPECT_EQ(errors, 0u);
}

TEST_F(Hdf5SourceDriverTest, CancellationBeforeOpenAndInsideSourceCallbackIsObserved) {
  auto source = std::make_shared<RecordingSource>(fixture());
  std::atomic<bool> cancelled{true};
  Hdf5SourceOptions options;
  options.cancelled = [&] { return cancelled.load(); };
  auto cancelled_open = Hdf5SourceDriver::open(source, options);
  ASSERT_FALSE(cancelled_open.ok());
  EXPECT_EQ(cancelled_open.status().code(), QueryError::Cancelled);
  EXPECT_TRUE(source->calls.empty());
  cancelled.store(false);
  auto opened = Hdf5SourceDriver::open(source, options);
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  source->on_read = [&] { cancelled.store(true); };
  auto interrupted = opened.value()->diagnostic_read_range(0, 8);
  ASSERT_FALSE(interrupted.ok());
  EXPECT_EQ(interrupted.status().code(), QueryError::Cancelled);
  const auto count = source->calls.size();
  auto subsequent = opened.value()->read("/values", {1, 2}, {3, 4});
  ASSERT_FALSE(subsequent.ok());
  EXPECT_EQ(subsequent.status().code(), QueryError::Cancelled);
  EXPECT_EQ(source->calls.size(), count);
}

TEST_F(Hdf5SourceDriverTest, CumulativeRangeAndByteBudgetsFailBeforeFurtherSourceReads) {
  auto bytes = fixture();
  auto baseline_source = std::make_shared<RecordingSource>(bytes);
  auto baseline = Hdf5SourceDriver::open(baseline_source);
  ASSERT_TRUE(baseline.ok()) << baseline.status().message();
  const auto trace = baseline.value()->trace();
  ASSERT_FALSE(trace.reads.empty());
  Hdf5SourceOptions range_limit;
  range_limit.max_read_calls = trace.reads.size();
  auto range_source = std::make_shared<RecordingSource>(bytes);
  auto limited_ranges = Hdf5SourceDriver::open(range_source, range_limit);
  ASSERT_TRUE(limited_ranges.ok()) << limited_ranges.status().message();
  const auto range_calls = range_source->calls.size();
  auto exhausted = limited_ranges.value()->diagnostic_read_range(0, 1);
  ASSERT_FALSE(exhausted.ok());
  EXPECT_EQ(exhausted.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(range_source->calls.size(), range_calls);

  Hdf5SourceOptions byte_limit;
  byte_limit.max_read_bytes = trace.metadata_bytes + trace.raw_data_bytes;
  auto byte_source = std::make_shared<RecordingSource>(bytes);
  auto limited_bytes = Hdf5SourceDriver::open(byte_source, byte_limit);
  ASSERT_TRUE(limited_bytes.ok()) << limited_bytes.status().message();
  const auto byte_calls = byte_source->calls.size();
  exhausted = limited_bytes.value()->diagnostic_read_range(0, 1);
  ASSERT_FALSE(exhausted.ok());
  EXPECT_EQ(exhausted.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(byte_source->calls.size(), byte_calls);
}

TEST_F(Hdf5SourceDriverTest, OutputAndDecodedChunkBudgetsRejectBeforeRawValueReads) {
  for (const bool chunk_limit : {false, true}) {
    auto source = std::make_shared<RecordingSource>(fixture());
    Hdf5SourceOptions options;
    if (chunk_limit) options.max_decoded_chunk_bytes = 23; // Native chunk is 2*3*4 bytes.
    else options.max_result_bytes = 47; // The requested 3*4 Float32 values need 48 bytes.
    auto opened = Hdf5SourceDriver::open(source, options);
    ASSERT_TRUE(opened.ok()) << opened.status().message();
    const auto before = opened.value()->trace().raw_data_bytes;
    auto result = opened.value()->read("/values", {1, 2}, {3, 4});
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
    EXPECT_EQ(opened.value()->trace().raw_data_bytes, before);
  }
}

TEST_F(Hdf5SourceDriverTest, StoredChunkSizeIsRejectedBeforeNativeAllocationAndRawRead) {
  auto source = std::make_shared<RecordingSource>(fixture(FixtureKind::LargeChunk));
  Hdf5SourceOptions options;
  options.max_single_read_bytes = 8192; // Metadata fits; stored native chunk is 16384 bytes.
  options.max_decoded_chunk_bytes = 16384;
  options.max_result_bytes = 4; // A one-value output does not excuse a large encoded chunk.
  auto opened = Hdf5SourceDriver::open(source, options);
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  const auto before = opened.value()->trace().raw_data_bytes;
  auto selected = opened.value()->read("/values", {0, 0}, {1, 1});
  ASSERT_FALSE(selected.ok());
  EXPECT_EQ(selected.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(opened.value()->trace().raw_data_bytes, before);
}

TEST_F(Hdf5SourceDriverTest, AppliedCachesAndSingleReadLimitAreObservableAndEnforced) {
  auto bytes = fixture();
  bytes.resize(bytes.size() + 8192, 0);
  auto source = std::make_shared<RecordingSource>(std::move(bytes));
  Hdf5SourceOptions options;
  options.metadata_cache_bytes = 1024 * 1024;
  options.raw_chunk_cache_bytes = 64 * 1024;
  options.max_single_read_bytes = 4096;
  options.max_result_bytes = 512;
  auto opened = Hdf5SourceDriver::open(source, options);
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  auto selected = opened.value()->read("/values", {1, 2}, {3, 4});
  ASSERT_TRUE(selected.ok()) << selected.status().message();
  const auto trace = opened.value()->trace();
  EXPECT_EQ(trace.metadata_cache_bytes, options.metadata_cache_bytes);
  EXPECT_EQ(trace.raw_chunk_cache_bytes, options.raw_chunk_cache_bytes);
  EXPECT_EQ(trace.sieve_buffer_bytes, 0u);
  EXPECT_EQ(trace.conversion_buffer_bytes, 512u);
  const auto count = source->calls.size();
  auto too_large = opened.value()->diagnostic_read_range(0, 4097);
  ASSERT_FALSE(too_large.ok());
  EXPECT_EQ(too_large.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(source->calls.size(), count);
}

TEST_F(Hdf5SourceDriverTest, EmptySelectionsHaveTypeAndNoRawReadsAndOutOfExtentIsRejected) {
  auto source = std::make_shared<RecordingSource>(fixture());
  auto opened = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  const auto before = opened.value()->trace().raw_data_bytes;
  auto empty = opened.value()->read("/values", {5, 0}, {0, 7});
  ASSERT_TRUE(empty.ok()) << empty.status().message();
  EXPECT_EQ(empty.value().shape, (std::vector<std::uint64_t>{0, 7}));
  EXPECT_EQ(empty.value().values.type.id, LogicalTypeId::Float32);
  EXPECT_TRUE(std::get<std::vector<float>>(empty.value().values.values).empty());
  EXPECT_EQ(opened.value()->trace().raw_data_bytes, before);
  auto invalid = opened.value()->read("/values", {4, 0}, {2, 1});
  ASSERT_FALSE(invalid.ok());
  EXPECT_EQ(invalid.status().code(), QueryError::InvalidDomainSlice);
  EXPECT_EQ(opened.value()->trace().raw_data_bytes, before);
}

TEST_F(Hdf5SourceDriverTest, ExternalLinksExternalRawStorageVdsAndUnknownFiltersCannotReadValues) {
  for (const auto kind : {FixtureKind::ExternalLink, FixtureKind::ExternalStorage,
      FixtureKind::VirtualDataset, FixtureKind::UnknownFilter}) {
    SCOPED_TRACE("fixture kind " + std::to_string(static_cast<int>(kind)));
    auto source = std::make_shared<RecordingSource>(fixture(kind));
    auto opened = Hdf5SourceDriver::open(source);
    ASSERT_TRUE(opened.ok()) << opened.status().message();
    const auto before = opened.value()->trace();
    const auto path = kind == FixtureKind::ExternalLink ? "/outside" : "/values";
    auto result = opened.value()->read(path, {1, 2}, {3, 4});
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), QueryError::UnsupportedCapability);
    const auto after = opened.value()->trace();
    if (kind == FixtureKind::VirtualDataset) {
      // HDF5 1.14.6 maps H5FD_MEM_GHEAP to H5FD_MEM_DRAW before VFD
      // dispatch. VDS mapping metadata can therefore be labelled raw_data;
      // it is not the external dataset's value payload. Only known global
      // heap collection reads are allowed in this fixture before rejection.
      for (std::size_t i = before.reads.size(); i < after.reads.size(); ++i) {
        const auto& read = after.reads[i];
        if (!read.raw_data) continue;
        ASSERT_GE(read.length, 4u);
        ASSERT_LE(read.offset, source->bytes.size());
        ASSERT_LE(read.length, source->bytes.size() - read.offset);
        EXPECT_EQ(BinaryValue(source->bytes.begin() + read.offset,
          source->bytes.begin() + read.offset + 4), (BinaryValue{'G', 'C', 'O', 'L'}));
      }
    } else EXPECT_EQ(after.raw_data_bytes, before.raw_data_bytes);
    EXPECT_EQ(source->out_of_bounds, 0u);
  }
}

TEST_F(Hdf5SourceDriverTest, DriverAndVolEnvironmentPollutionAfterInitializationFailsBeforeSourceReads) {
  auto source = std::make_shared<RecordingSource>(fixture());
  auto initialized = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(initialized.ok()) << initialized.status().message();
  initialized.value().reset();
  for (const auto* variable : {"HDF5_DRIVER", "HDF5_VOL_CONNECTOR"}) {
    const auto calls = source->calls.size();
    {
      ScopedEnvironment environment(variable, "iotdbfs-unauthorized-plugin");
      auto rejected = Hdf5SourceDriver::open(source);
      ASSERT_FALSE(rejected.ok());
      EXPECT_EQ(rejected.status().code(), QueryError::UnsupportedCapability);
      EXPECT_EQ(source->calls.size(), calls);
    }
    auto clean = Hdf5SourceDriver::open(source);
    ASSERT_TRUE(clean.ok()) << clean.status().message();
    auto selected = clean.value()->read("/values", {1, 2}, {3, 4});
    ASSERT_TRUE(selected.ok()) << selected.status().message();
    EXPECT_EQ(std::get<std::vector<float>>(selected.value().values.values), expected());
  }
}

TEST_F(Hdf5SourceDriverTest, PluginSearchPathCannotEnableLoadingAfterInitialization) {
  auto source = std::make_shared<RecordingSource>(fixture(FixtureKind::Deflate));
  auto initialized = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(initialized.ok()) << initialized.status().message();
  initialized.value().reset();
  ScopedEnvironment environment("HDF5_PLUGIN_PATH", "/iotdbfs-untrusted-plugin-directory");
  auto opened = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  auto selected = opened.value()->read("/values", {1, 2}, {3, 4});
  ASSERT_TRUE(selected.ok()) << selected.status().message();
  EXPECT_EQ(std::get<std::vector<float>>(selected.value().values.values), expected());
  unsigned int loading = ~0u;
  h5check(H5PLget_loading_state(&loading), "inspect plugin loading policy");
  EXPECT_EQ(loading, 0u);
  opened.value().reset();
  loading = ~0u;
  h5check(H5PLget_loading_state(&loading), "inspect policy after last driver closes");
  EXPECT_EQ(loading, 0u);
  FormatSource borrowed{source->bytes.size(), [source](std::uint64_t offset,
      std::uint64_t length) -> std::optional<BinaryValue> {
    auto bytes = source->read_payload(offset, length);
    if (!bytes.ok()) return std::nullopt;
    return std::move(bytes.value());
  }};
  auto status = with_hdf5_ingestion_source(borrowed, {}, [](const Hdf5SourceDriver&) {
    return QueryStatus::success();
  });
  ASSERT_TRUE(status.ok()) << status.message();
  loading = ~0u;
  h5check(H5PLget_loading_state(&loading), "inspect policy after synchronous ingestion closes");
  EXPECT_EQ(loading, 0u);
}

} // namespace
