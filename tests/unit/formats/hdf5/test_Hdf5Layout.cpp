#include "iotdbfs/formats/hdf5/Hdf5Layout.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include <algorithm>
#include <cstring>
#include <gtest/gtest.h>
#include <openssl/sha.h>
#include <type_traits>

using namespace iotdbfs;
namespace {
Hdf5LayoutDocument document() {
  Hdf5LayoutDocument d;
  d.source_size = 10000;
  d.root_object_id = "root";
  d.objects.push_back({"root", Hdf5ObjectKind::Group, {}});
  Hdf5DatasetLayout a;
  a.descriptor = {"/a", LogicalTypeId::Float32, {10, 8}, true, {}};
  a.storage = Hdf5StorageLayout::Chunked;
  a.byte_order = Hdf5ByteOrder::Little;
  a.element_bytes = 4;
  a.native_type_class = 1;
  a.fill_status = Hdf5FillStatus::Explicit;
  a.fill_value = {0, 0, 128, 63};
  a.chunk_shape = {2, 4};
  a.filters = {{1, 1, {6}}, {2, 0, {4}}};
  a.index_complete = true;
  a.chunks = {{{0, 0}, 100, 32, 0},
              {{0, 4}, 200, 32, 1},
              {{4, 0}, 300, 32, 2},
              {{8, 4}, 400, 32, 3}};
  a.allocated_chunk_count = a.chunks.size();
  d.objects.push_back({"dataset-a", Hdf5ObjectKind::Dataset, a});
  auto b = a;
  b.descriptor.path = "/b";
  b.descriptor.shape = {4, 4};
  b.chunks = {{{0, 0}, 500, 32, 0}};
  b.allocated_chunk_count = 1;
  d.objects.push_back({"dataset-b", Hdf5ObjectKind::Dataset, b});
  d.links = {
      {"root", "a", Hdf5LinkKind::Hard, "dataset-a", {}},
      {"root", "b", Hdf5LinkKind::Hard, "dataset-b", {}},
      {"root", "alias", Hdf5LinkKind::Hard, "dataset-a", {}},
      {"root", "cycle", Hdf5LinkKind::Hard, "root", {}},
      {"root", "soft", Hdf5LinkKind::Soft, {}, "soft links are unsupported"}};
  return d;
}
Hdf5LayoutLimits limits() {
  Hdf5LayoutLimits l;
  l.chunks_per_page = 1;
  return l;
}
struct Bytes {
  BinaryValue data;
  std::vector<std::pair<std::uint64_t, std::uint64_t>> reads;
  Hdf5LayoutRead reader() {
    return [this](std::uint64_t o, std::uint64_t n) {
      reads.emplace_back(o, n);
      if (o > data.size() || n > data.size() - o)
        return QueryResult<BinaryValue>::failure(QueryStatus::failure(
            QueryError::ReadFailure, QueryPhase::Storage, "outside bytes"));
      return QueryResult<BinaryValue>::success(
          BinaryValue(data.begin() + o, data.begin() + o + n));
    };
  }
};
Bytes encoded(const Hdf5LayoutDocument &d = document(),
              Hdf5LayoutLimits l = limits()) {
  auto e = serialize_hdf5_layout(d, l);
  EXPECT_TRUE(e.ok()) << e.status().message();
  return {e.ok() ? e.value() : BinaryValue{}, {}};
}
void put64(BinaryValue &b, std::size_t p, std::uint64_t n) {
  for (unsigned i = 0; i < 8; ++i)
    b[p + i] = n >> (8 * i);
}
void rehash_directory(BinaryValue &b) {
  const auto m = sizeof(kHdf5LayoutMagic) - 1;
  std::uint64_t n = 0;
  for (unsigned i = 0; i < 8; ++i)
    n |= std::uint64_t(b[m + i]) << (8 * i);
  SHA256(b.data() + m + 48, n, b.data() + m + 16);
}
} // namespace

TEST(Hdf5Layout, RoundTripRetainsNativeMetadataGraphAndPagedRecords) {
  auto d = document();
  d.objects[1].dataset->chunk_options = 2;
  auto bytes = encoded(d);
  auto result = read_hdf5_layout_directory(bytes.reader(), bytes.data.size(),
                                           10000, limits());
  ASSERT_TRUE(result.ok()) << result.status().message();
  const auto &dir = result.value();
  EXPECT_EQ(bytes.reads.size(), 2U);
  EXPECT_EQ(dir.document().objects.size(), 3U);
  EXPECT_EQ(dir.pages().size(), 5U);
  const auto &a = *dir.document().objects[1].dataset;
  EXPECT_TRUE(a.chunks.empty());
  EXPECT_EQ(a.allocated_chunk_count, 4U);
  EXPECT_EQ(a.chunk_options, 2U);
  EXPECT_EQ(a.fill_value, (BinaryValue{0, 0, 128, 63}));
  EXPECT_EQ(a.filters[0].parameters, (std::vector<std::uint32_t>{6}));
  Hdf5LayoutReadBudget used;
  auto chunks = read_hdf5_layout_chunks(bytes.reader(), dir, "dataset-a",
                                        {0, 0}, {10, 8}, used, limits());
  ASSERT_TRUE(chunks.ok()) << chunks.status().message();
  ASSERT_EQ(chunks.value().size(), 4U);
  EXPECT_EQ(chunks.value()[3].filter_mask, 3U);
  EXPECT_EQ(bytes.reads.size(), 6U);
  EXPECT_LT(used.bytes, bytes.data.size()); // /b page was never read.
  static_assert(
      std::is_const_v<std::remove_reference_t<decltype(dir.document())>>);
  static_assert(
      std::is_const_v<std::remove_reference_t<decltype(dir.pages())>>);
}
TEST(Hdf5Layout, PreviousFourLayoutRemainsReadableForPhysicalQueries) {
  auto bytes = encoded();
  bytes.data[sizeof(kHdf5LayoutMagic) - 3] = '4';
  auto result = read_hdf5_layout_directory(bytes.reader(), bytes.data.size(),
                                           10000, limits());
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result.value().protocol_version(), 4U);
  Hdf5LayoutReadBudget budget;
  auto chunks = read_hdf5_layout_chunks(bytes.reader(), result.value(),
                                        "dataset-a", {0,0}, {2,4}, budget,
                                        limits());
  ASSERT_TRUE(chunks.ok()) << chunks.status().message();
  EXPECT_EQ(chunks.value().size(), 1U);
}
TEST(Hdf5Layout, SelectionReadsOnlyIntersectingDatasetPagesAndHandlesEdges) {
  auto bytes = encoded();
  auto result = read_hdf5_layout_directory(bytes.reader(), bytes.data.size(),
                                           10000, limits());
  ASSERT_TRUE(result.ok());
  Hdf5LayoutReadBudget used;
  auto chunks =
      read_hdf5_layout_chunks(bytes.reader(), result.value(), "dataset-a",
                              {9, 7}, {1, 1}, used, limits());
  ASSERT_TRUE(chunks.ok());
  ASSERT_EQ(chunks.value().size(), 1U);
  EXPECT_EQ(chunks.value()[0].origin, (std::vector<std::uint64_t>{8, 4}));
  EXPECT_EQ(bytes.reads.size(), 3U);
  EXPECT_EQ(used.bytes, result.value().directory_read_bytes() +
                            result.value().pages()[3].length);
}
TEST(Hdf5Layout, SparseAndEmptySelectionsDoNotReadNoncandidatePages) {
  auto bytes = encoded();
  auto result = read_hdf5_layout_directory(bytes.reader(), bytes.data.size(),
                                           10000, limits());
  ASSERT_TRUE(result.ok());
  Hdf5LayoutReadBudget used;
  auto sparse =
      read_hdf5_layout_chunks(bytes.reader(), result.value(), "dataset-a",
                              {6, 0}, {2, 8}, used, limits());
  ASSERT_TRUE(sparse.ok());
  EXPECT_TRUE(sparse.value().empty());
  auto empty =
      read_hdf5_layout_chunks(bytes.reader(), result.value(), "dataset-a",
                              {10, 0}, {0, 8}, used, limits());
  ASSERT_TRUE(empty.ok());
  EXPECT_TRUE(empty.value().empty());
  EXPECT_EQ(bytes.reads.size(), 2U);
}
TEST(Hdf5Layout,
     HardLinkAliasesCyclesAndUnsupportedLinksResolveWithoutExpansion) {
  auto bytes = encoded();
  auto result = read_hdf5_layout_directory(bytes.reader(), bytes.data.size(),
                                           10000, limits());
  ASSERT_TRUE(result.ok());
  for (const char *p : {"/a", "/alias", "/cycle/alias", "/cycle/cycle/a"}) {
    auto r = resolve_hdf5_layout_path(result.value(), p);
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(r.value(), "dataset-a");
  }
  EXPECT_EQ(
      resolve_hdf5_layout_path(result.value(), "/missing").status().code(),
      QueryError::UnknownField);
  EXPECT_EQ(resolve_hdf5_layout_path(result.value(), "/soft").status().code(),
            QueryError::UnsupportedCapability);
  EXPECT_EQ(resolve_hdf5_layout_path(result.value(), "/a/../b").status().code(),
            QueryError::InvalidQuery);
}
TEST(Hdf5Layout, DroppedAllocatedRecordCannotBecomeSparseFill) {
  auto d = document();
  d.objects[1].dataset->chunks.pop_back();
  auto r = serialize_hdf5_layout(d, limits());
  EXPECT_EQ(r.status().code(), QueryError::CorruptLayout);
  d.objects[1].dataset->index_complete = false;
  d.objects[1].dataset->index_unavailable_reason = "resource_limit";
  d.objects[1].dataset->chunks.clear();
  auto bytes = encoded(d);
  auto dir = read_hdf5_layout_directory(bytes.reader(), bytes.data.size(),
                                        10000, limits());
  ASSERT_TRUE(dir.ok());
  Hdf5LayoutReadBudget used;
  auto chunks =
      read_hdf5_layout_chunks(bytes.reader(), dir.value(), "dataset-a", {0, 0},
                              {10, 8}, used, limits());
  EXPECT_EQ(chunks.status().code(), QueryError::LayoutUnavailable);
}
TEST(Hdf5Layout,
     BuildOverflowHasExplicitUnavailableRepresentationWithinSameCap) {
  Hdf5LayoutDocument d;
  d.source_size = 10000;
  d.directory_complete = false;
  d.index_unavailable_reason = "resource_limit";
  auto l = limits();
  l.max_objects = 0;
  l.max_links = 0;
  l.max_chunks = 0;
  l.max_pages = 0;
  l.max_layout_bytes = 512;
  l.max_directory_bytes = 400;
  auto bytes = encoded(d, l);
  auto dir =
      read_hdf5_layout_directory(bytes.reader(), bytes.data.size(), 10000, l);
  ASSERT_TRUE(dir.ok());
  EXPECT_FALSE(dir.value().document().directory_complete);
  EXPECT_EQ(resolve_hdf5_layout_path(dir.value(), "/a").status().code(),
            QueryError::LayoutUnavailable);
  auto full = serialize_hdf5_layout(document(), l);
  EXPECT_EQ(full.status().code(), QueryError::ResourceExhausted);
}
TEST(Hdf5Layout, HugeNativeCountIsRetainedOnlyForUnavailableIndex) {
  auto d = document();
  auto &a = *d.objects[1].dataset;
  a.allocated_chunk_count = UINT64_MAX;
  a.chunks.clear();
  a.index_complete = false;
  a.index_unavailable_reason = "resource_limit";
  auto bytes = encoded(d);
  auto dir = read_hdf5_layout_directory(bytes.reader(), bytes.data.size(),
                                        10000, limits());
  ASSERT_TRUE(dir.ok());
  EXPECT_EQ(dir.value().document().objects[1].dataset->allocated_chunk_count,
            UINT64_MAX);
}
TEST(Hdf5Layout, DirectoryAndChunkChecksumsRejectCorruption) {
  auto bytes = encoded();
  bytes.data[sizeof(kHdf5LayoutMagic) + 60] ^= 1;
  EXPECT_EQ(read_hdf5_layout_directory(bytes.reader(), bytes.data.size(), 10000,
                                       limits())
                .status()
                .code(),
            QueryError::CorruptLayout);
  bytes = encoded();
  auto dir = read_hdf5_layout_directory(bytes.reader(), bytes.data.size(),
                                        10000, limits());
  ASSERT_TRUE(dir.ok());
  bytes.data[dir.value().pages()[0].offset + 10] ^= 1;
  Hdf5LayoutReadBudget used;
  auto chunks = read_hdf5_layout_chunks(
      bytes.reader(), dir.value(), "dataset-a", {0, 0}, {2, 4}, used, limits());
  EXPECT_EQ(chunks.status().code(), QueryError::CorruptLayout);
}
TEST(Hdf5Layout, PageRecordCountAndBoundsAreCheckedBeyondDigest) {
  auto bytes = encoded();
  auto initial = read_hdf5_layout_directory(bytes.reader(), bytes.data.size(),
                                            10000, limits());
  ASSERT_TRUE(initial.ok());
  const auto page = initial.value().pages()[0];
  put64(bytes.data, page.offset, 0);
  Hdf5LayoutDigest sha{};
  SHA256(bytes.data.data() + page.offset, page.length, sha.data());
  auto begin = bytes.data.begin() + sizeof(kHdf5LayoutMagic) - 1 + 48;
  auto found = std::search(
      begin, bytes.data.begin() + initial.value().directory_read_bytes(),
      page.checksum.begin(), page.checksum.end());
  ASSERT_NE(found, bytes.data.begin() + initial.value().directory_read_bytes());
  std::copy(sha.begin(), sha.end(), found);
  rehash_directory(bytes.data);
  auto dir = read_hdf5_layout_directory(bytes.reader(), bytes.data.size(),
                                        10000, limits());
  ASSERT_TRUE(dir.ok());
  Hdf5LayoutReadBudget used;
  EXPECT_EQ(read_hdf5_layout_chunks(bytes.reader(), dir.value(), "dataset-a",
                                    {0, 0}, {2, 4}, used, limits())
                .status()
                .code(),
            QueryError::CorruptLayout);
}
TEST(Hdf5Layout, MissingPageAndMismatchedSourceOrVersionNeverParse) {
  auto bytes = encoded();
  EXPECT_EQ(read_hdf5_layout_directory(bytes.reader(), bytes.data.size(), 9999,
                                       limits())
                .status()
                .code(),
            QueryError::CorruptLayout);
  for (char obsolete : {'1', '2'}) {
  bytes.data[sizeof(kHdf5LayoutMagic) - 3] = obsolete;
  EXPECT_EQ(read_hdf5_layout_directory(bytes.reader(), bytes.data.size(), 10000,
                                       limits())
                .status()
                .code(),
            QueryError::UnsupportedLayoutVersion);
  }
  bytes = encoded();
  bytes.data.pop_back();
  put64(bytes.data, sizeof(kHdf5LayoutMagic) - 1 + 8, bytes.data.size());
  EXPECT_EQ(read_hdf5_layout_directory(bytes.reader(), bytes.data.size(), 10000,
                                       limits())
                .status()
                .code(),
            QueryError::CorruptLayout);
}
TEST(Hdf5Layout, QueryReadBudgetIncludesDirectoryAndCandidatePages) {
  auto bytes = encoded();
  auto l = limits();
  l.max_read_bytes = 20;
  EXPECT_EQ(
      read_hdf5_layout_directory(bytes.reader(), bytes.data.size(), 10000, l)
          .status()
          .code(),
      QueryError::ResourceExhausted);
  EXPECT_TRUE(bytes.reads.empty());
  l = limits();
  auto dir =
      read_hdf5_layout_directory(bytes.reader(), bytes.data.size(), 10000, l);
  ASSERT_TRUE(dir.ok());
  l.max_read_bytes =
      dir.value().directory_read_bytes() + dir.value().pages()[0].length - 1;
  Hdf5LayoutReadBudget used;
  auto chunks = read_hdf5_layout_chunks(bytes.reader(), dir.value(),
                                        "dataset-a", {0, 0}, {2, 4}, used, l);
  EXPECT_EQ(chunks.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(bytes.reads.size(), 2U);
}
TEST(Hdf5Layout, RejectsOutOfBoundsDuplicateAndUnorderedPhysicalRecords) {
  for (int mode = 0; mode < 5; ++mode) {
    auto d = document();
    auto &a = *d.objects[1].dataset;
    if (mode == 0)
      a.chunks[0].offset = UINT64_MAX;
    if (mode == 1)
      a.chunks[0].encoded_size = 0;
    if (mode == 2)
      a.chunks[1].origin = a.chunks[0].origin;
    if (mode == 3)
      std::swap(a.chunks[0], a.chunks[1]);
    if (mode == 4)
      a.chunks[0].filter_mask = 4;
    EXPECT_EQ(validate_hdf5_layout(d, limits()).code(),
              QueryError::CorruptLayout)
        << mode;
  }
}
TEST(Hdf5Layout, ContiguousUnallocatedAndEmptyDatasetsRoundtrip) {
  auto d = document();
  auto &a = *d.objects[1].dataset;
  a.storage = Hdf5StorageLayout::Contiguous;
  a.chunk_shape.clear();
  a.chunks.clear();
  a.filters.clear();
  a.allocated_chunk_count = 0;
  a.descriptor.shape = {0, 8};
  auto bytes = encoded(d);
  auto dir = read_hdf5_layout_directory(bytes.reader(), bytes.data.size(),
                                        10000, limits());
  ASSERT_TRUE(dir.ok());
  EXPECT_FALSE(
      dir.value().document().objects[1].dataset->contiguous_offset.has_value());
  a.contiguous_offset = 9990;
  a.contiguous_size = 20;
  EXPECT_EQ(validate_hdf5_layout(d, limits()).code(),
            QueryError::CorruptLayout);
}
TEST(Hdf5Layout, InvalidGraphAndMalformedSelectionsFailExplicitly) {
  auto d = document();
  d.links[0].target_id = "missing";
  EXPECT_EQ(validate_hdf5_layout(d, limits()).code(),
            QueryError::CorruptLayout);
  d = document();
  d.links.push_back(d.links[0]);
  EXPECT_EQ(validate_hdf5_layout(d, limits()).code(),
            QueryError::CorruptLayout);
  auto bytes = encoded();
  auto dir = read_hdf5_layout_directory(bytes.reader(), bytes.data.size(),
                                        10000, limits());
  ASSERT_TRUE(dir.ok());
  Hdf5LayoutReadBudget used;
  EXPECT_EQ(read_hdf5_layout_chunks(bytes.reader(), dir.value(), "dataset-a",
                                    {UINT64_MAX, 0}, {2, 1}, used, limits())
                .status()
                .code(),
            QueryError::InvalidDomainSlice);
}

TEST(Hdf5Layout, RejectsLogicalAndChunkByteOverflowButAllowsEmptyExtent) {
  auto d = document();
  auto &a = *d.objects[1].dataset;
  a.storage = Hdf5StorageLayout::Contiguous;
  a.chunks.clear();
  a.chunk_shape.clear();
  a.filters.clear();
  a.allocated_chunk_count = 0;
  a.descriptor.shape = {UINT64_MAX, UINT64_MAX};
  EXPECT_EQ(validate_hdf5_layout(d, limits()).code(),
            QueryError::CorruptLayout);
  a.descriptor.shape = {UINT64_MAX, UINT64_MAX, 0};
  EXPECT_TRUE(validate_hdf5_layout(d, limits()).ok());
  d = document();
  d.objects[1].dataset->chunk_shape = {UINT64_MAX, 4};
  EXPECT_EQ(validate_hdf5_layout(d, limits()).code(),
            QueryError::CorruptLayout);
  d = document();
  auto &b = *d.objects[1].dataset;
  b.storage = Hdf5StorageLayout::Contiguous;
  b.chunks.clear();
  b.chunk_shape.clear();
  b.filters.clear();
  b.allocated_chunk_count = 0;
  b.contiguous_offset = 100;
  b.contiguous_size = 316;
  EXPECT_EQ(validate_hdf5_layout(d, limits()).code(),
            QueryError::CorruptLayout);
  b.contiguous_size = 320;
  EXPECT_TRUE(validate_hdf5_layout(d, limits()).ok());
}
TEST(Hdf5Layout, RejectsForgedNativeEnumsFiltersAndPartialIndexes) {
  for (int mode = 0; mode < 7; ++mode) {
    auto d = document();
    auto &a = *d.objects[1].dataset;
    if (mode == 0)
      a.allocation_time = 4;
    if (mode == 1)
      a.fill_time = 3;
    if (mode == 2)
      a.allocation_status = 3;
    if (mode == 3)
      a.native_type_class = UINT32_MAX;
    if (mode == 4)
      a.descriptor.value_type = static_cast<LogicalTypeId>(255);
    if (mode == 5)
      a.filters[0].id = 32000;
    if (mode == 6) {
      a.index_complete = false;
      a.index_unavailable_reason = "resource_limit";
    }
    EXPECT_EQ(validate_hdf5_layout(d, limits()).code(),
              QueryError::CorruptLayout)
        << mode;
  }
}
TEST(Hdf5Layout, RejectsUnreachableObjectsAndWrongDatasetPathIdentity) {
  auto d = document();
  d.objects.push_back({"orphan", Hdf5ObjectKind::Group, {}});
  EXPECT_EQ(validate_hdf5_layout(d, limits()).code(),
            QueryError::CorruptLayout);
  d = document();
  d.objects[1].dataset->descriptor.path = "/b";
  EXPECT_EQ(validate_hdf5_layout(d, limits()).code(),
            QueryError::CorruptLayout);
  d = document();
  auto &a = *d.objects[1].dataset;
  a.descriptor.path = "/\xFF";
  a.descriptor.supported = false;
  a.descriptor.unsupported_reason = "unsupported path";
  a.index_complete = false;
  a.index_unavailable_reason = "unsupported path";
  a.chunks.clear();
  d.links[0].name = "\xFF";
  EXPECT_TRUE(validate_hdf5_layout(d, limits()).ok());
}
TEST(Hdf5Layout, ReadCallAndPageScanBudgetsAreIndependentAndCumulative) {
  auto bytes = encoded();
  auto l = limits();
  l.max_read_calls = 1;
  EXPECT_EQ(
      read_hdf5_layout_directory(bytes.reader(), bytes.data.size(), 10000, l)
          .status()
          .code(),
      QueryError::ResourceExhausted);
  ASSERT_EQ(bytes.reads.size(), 1U);
  bytes.reads.clear();
  l = limits();
  auto dir =
      read_hdf5_layout_directory(bytes.reader(), bytes.data.size(), 10000, l);
  ASSERT_TRUE(dir.ok());
  Hdf5LayoutReadBudget used;
  l.max_read_calls = 2;
  EXPECT_EQ(read_hdf5_layout_chunks(bytes.reader(), dir.value(), "dataset-a",
                                    {0, 0}, {2, 4}, used, l)
                .status()
                .code(),
            QueryError::ResourceExhausted);
  EXPECT_EQ(bytes.reads.size(), 2U);
  used = {};
  l = limits();
  l.max_page_scans = dir.value().directory_page_scans() + 3;
  EXPECT_EQ(read_hdf5_layout_chunks(bytes.reader(), dir.value(), "dataset-b",
                                    {0, 0}, {2, 4}, used, l)
                .status()
                .code(),
            QueryError::ResourceExhausted);
  EXPECT_EQ(bytes.reads.size(), 2U);
  used = {};
  l = limits();
  l.max_page_scans =
      dir.value().directory_page_scans() + dir.value().pages().size();
  auto sparse = read_hdf5_layout_chunks(bytes.reader(), dir.value(),
                                        "dataset-a", {6, 0}, {2, 8}, used, l);
  ASSERT_TRUE(sparse.ok());
  EXPECT_EQ(used.page_scans, l.max_page_scans);
  EXPECT_EQ(read_hdf5_layout_chunks(bytes.reader(), dir.value(), "dataset-a",
                                    {6, 0}, {2, 8}, used, l)
                .status()
                .code(),
            QueryError::ResourceExhausted);
}

TEST(Hdf5Layout, RejectsUnknownOptionsForSupportedChunks) {
  auto d = document();
  d.objects[1].dataset->chunk_options = 4;
  EXPECT_EQ(serialize_hdf5_layout(d, limits()).status().code(), QueryError::CorruptLayout);
}

TEST(Hdf5Layout, ProfileSeparatesDirectoryAndSelectedPageReadAttempts) {
  auto bytes = encoded();
  QueryProfileRecord profile;
  ScopedQueryProfile scope(&profile);
  auto directory = read_hdf5_layout_directory(bytes.reader(), bytes.data.size(), 10000, limits());
  ASSERT_TRUE(directory.ok());
  EXPECT_EQ(profile.counters["hdf5.layout.directory.calls"], 2u);
  EXPECT_EQ(profile.counters["hdf5.layout.directory.bytes"], directory.value().directory_read_bytes());
  Hdf5LayoutReadBudget budget;
  auto chunks = read_hdf5_layout_chunks(bytes.reader(), directory.value(), "dataset-a", {0, 0}, {2, 4}, budget, limits());
  ASSERT_TRUE(chunks.ok());
  EXPECT_EQ(profile.counters["hdf5.layout.pages.calls"], 1u);
  EXPECT_EQ(profile.counters["hdf5.layout.pages.bytes"], directory.value().pages()[0].length);
  EXPECT_GT(profile.nanoseconds["hdf5.layout.directory.read"], 0u);
  EXPECT_GT(profile.nanoseconds["hdf5.layout.directory.parse"], 0u);
  EXPECT_GT(profile.nanoseconds["hdf5.layout.pages.read"], 0u);
  EXPECT_GT(profile.nanoseconds["hdf5.layout.pages.parse"], 0u);
  auto failed_read = [](std::uint64_t, std::uint64_t) {
    return QueryResult<BinaryValue>::failure(QueryStatus::failure(QueryError::ReadFailure, QueryPhase::Storage, "injected"));
  };
  auto failed = read_hdf5_layout_directory(failed_read, bytes.data.size(), 10000, limits());
  EXPECT_EQ(failed.status().code(), QueryError::ReadFailure);
  EXPECT_EQ(profile.counters["hdf5.layout.directory.calls"], 3u);
  EXPECT_EQ(profile.counters["hdf5.layout.pages.calls"], 1u);
}
