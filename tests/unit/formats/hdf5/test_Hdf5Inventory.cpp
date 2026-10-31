#include "iotdbfs/formats/hdf5/Hdf5SourceDriver.h"
#include <gtest/gtest.h>
#include <hdf5.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <fstream>
#include <iterator>
#include <stdexcept>

using namespace iotdbfs;
namespace {
struct Native {
  hid_t id;
  herr_t (*close)(hid_t);
  Native(hid_t id, herr_t (*close)(hid_t)) : id(id), close(close) {
    if (id < 0)
      throw std::runtime_error("native fixture failed");
  }
  ~Native() { close(id); }
  operator hid_t() const { return id; }
  Native(const Native &) = delete;
};
void checked(herr_t status) {
  if (status < 0)
    throw std::runtime_error("native fixture operation failed");
}
struct Fixture {
  BinaryValue bytes;
  std::vector<std::pair<std::uint64_t, std::uint64_t>> payloads;
};
Fixture fixture(bool sparse = false, std::uint64_t userblock = 0,
                bool corrupt = false, bool opaque_name = false,
                bool undefined_fill = false) {
  char path[] = "/tmp/iotdbfs-h03-inventory-XXXXXX";
  const int fd = mkstemp(path);
  if (fd < 0)
    throw std::runtime_error("mkstemp");
  close(fd);
  struct Unlink {
    const char *path;
    ~Unlink() { unlink(path); }
  } unlinker{path};
  Fixture result;
  {
    Native fcpl(H5Pcreate(H5P_FILE_CREATE), H5Pclose);
    if (userblock)
      checked(H5Pset_userblock(fcpl, userblock));
    Native file(H5Fcreate(path, H5F_ACC_TRUNC, fcpl, H5P_DEFAULT), H5Fclose);
    Native group(
        H5Gcreate2(file, "/group", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT),
        H5Gclose);
    const hsize_t dims[]{5, 7}, chunks[]{2, 3};
    Native space(H5Screate_simple(2, dims, nullptr), H5Sclose);
    Native dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
    checked(H5Pset_chunk(dcpl, 2, chunks));
    checked(H5Pset_shuffle(dcpl));
    checked(H5Pset_deflate(dcpl, 1));
    checked(H5Pset_fletcher32(dcpl));
    const float fill = -7.5f;
    checked(H5Pset_fill_value(dcpl, H5T_NATIVE_FLOAT, &fill));
    Native dataset(H5Dcreate2(file, "/group/data", H5T_IEEE_F32BE, space,
                              H5P_DEFAULT, dcpl, H5P_DEFAULT),
                   H5Dclose);
    std::vector<float> values(35, 11.0f);
    if (sparse) {
      const hsize_t origin[]{0, 0}, count[]{2, 3};
      checked(H5Sselect_hyperslab(space, H5S_SELECT_SET, origin, nullptr, count,
                                  nullptr));
      Native memory(H5Screate_simple(2, count, nullptr), H5Sclose);
      checked(H5Dwrite(dataset, H5T_NATIVE_FLOAT, memory, space, H5P_DEFAULT,
                       values.data()));
    } else
      checked(H5Dwrite(dataset, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                       values.data()));
    checked(H5Lcreate_hard(file, "/group/data", file, "/alias", H5P_DEFAULT,
                           H5P_DEFAULT));
    checked(
        H5Lcreate_hard(file, "/", group, "cycle", H5P_DEFAULT, H5P_DEFAULT));
    checked(H5Lcreate_soft("/does-not-exist", file, "/soft", H5P_DEFAULT,
                           H5P_DEFAULT));
    checked(H5Lcreate_external("/no-such-file.h5", "/data", file, "/external",
                               H5P_DEFAULT, H5P_DEFAULT));
    Native integer(H5Dcreate2(file, "/integer", H5T_STD_I32LE, space,
                              H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT),
                   H5Dclose);
    Native external_dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
    checked(
        H5Pset_external(external_dcpl, "/no-such-raw.bin", 0, H5F_UNLIMITED));
    Native external(H5Dcreate2(file, "/external_raw", H5T_IEEE_F32LE, space,
                               H5P_DEFAULT, external_dcpl, H5P_DEFAULT),
                    H5Dclose);
    Native fill_dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
    if (undefined_fill)
      checked(H5Pset_fill_value(fill_dcpl, H5T_NATIVE_DOUBLE, nullptr));
    Native unallocated(H5Dcreate2(file, "/unallocated", H5T_IEEE_F64LE, space,
                                  H5P_DEFAULT, fill_dcpl, H5P_DEFAULT),
                       H5Dclose);
    if (opaque_name) {
      const std::string name = std::string("/opaque-") + char(0xff);
      Native opaque(H5Dcreate2(file, name.c_str(), H5T_IEEE_F32LE, space,
                               H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT),
                    H5Dclose);
    }
    checked(H5Fflush(file, H5F_SCOPE_LOCAL));
    hsize_t allocated = 0;
    checked(H5Dget_num_chunks(dataset, H5S_ALL, &allocated));
    for (hsize_t i = 0; i < allocated; ++i) {
      hsize_t origin[2], size;
      haddr_t address;
      unsigned mask;
      checked(H5Dget_chunk_info(dataset, H5S_ALL, i, origin, &mask, &address,
                                &size));
      result.payloads.emplace_back(address, size);
    }
  }
  std::ifstream input(path, std::ios::binary);
  result.bytes.assign(std::istreambuf_iterator<char>(input), {});
  if (corrupt)
    result.bytes.at(result.payloads.front().first +
                    result.payloads.front().second - 1) ^= 1;
  return result;
}
class Source final : public QuerySource {
public:
  explicit Source(BinaryValue bytes) : bytes(std::move(bytes)) {
    manifest_.key = {"/inventory.h5", 9};
    manifest_.payload_size = this->bytes.size();
    manifest_.published = true;
    manifest_.phase = StructuredBlobPhase::Published;
  }
  const StructuredBlobManifest &manifest() const override { return manifest_; }
  QueryResult<StructuredBlobBytes>
  read_payload(std::uint64_t offset, std::uint64_t size) const override {
    calls.emplace_back(offset, size);
    if (fail || offset > bytes.size() || size > bytes.size() - offset)
      return QueryResult<StructuredBlobBytes>::failure(
          QueryStatus::failure(QueryError::ReadFailure, QueryPhase::Storage,
                               "injected read failure"));
    return QueryResult<StructuredBlobBytes>::success(
        BinaryValue(bytes.begin() + offset, bytes.begin() + offset + size));
  }
  QueryResult<StructuredBlobBytes> read_layout(std::uint64_t,
                                               std::uint64_t) const override {
    return QueryResult<StructuredBlobBytes>::failure(QueryStatus::failure(
        QueryError::ReadFailure, QueryPhase::Storage, "no layout"));
  }
  BinaryValue bytes;
  StructuredBlobManifest manifest_;
  bool fail = false;
  mutable std::vector<std::pair<std::uint64_t, std::uint64_t>> calls;
};
const Hdf5DatasetLayout &dataset(const Hdf5LayoutDocument &doc,
                                 const std::string &path) {
  for (const auto &object : doc.objects)
    if (object.dataset && object.dataset->descriptor.path == path)
      return *object.dataset;
  throw std::runtime_error("missing dataset " + path);
}
} // namespace

TEST(Hdf5Inventory, PreservesAliasesCyclesUnsupportedLinksAndDatasetProfiles) {
  auto source = std::make_shared<Source>(fixture().bytes);
  auto opened = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  auto inventory = opened.value()->inventory();
  ASSERT_TRUE(inventory.ok()) << inventory.status().message();
  const auto &doc = inventory.value();
  ASSERT_TRUE(doc.directory_complete);
  EXPECT_EQ(doc.objects.size(), 6u);
  EXPECT_EQ(doc.links.size(), 9u);
  EXPECT_EQ(std::count_if(doc.links.begin(), doc.links.end(),
                          [](const auto &link) {
                            return link.kind == Hdf5LinkKind::Soft ||
                                   link.kind == Hdf5LinkKind::External;
                          }),
            2);
  EXPECT_TRUE(dataset(doc, "/alias").descriptor.supported);
  EXPECT_FALSE(dataset(doc, "/integer").descriptor.supported);
  EXPECT_FALSE(dataset(doc, "/external_raw").descriptor.supported);
  EXPECT_TRUE(validate_hdf5_layout(doc).ok())
      << validate_hdf5_layout(doc).message();
  auto again = opened.value()->inventory();
  ASSERT_TRUE(again.ok());
  EXPECT_EQ(again.value().root_object_id, doc.root_object_id);
}
TEST(Hdf5Inventory,
     RecordsAbsoluteChunkRangesFiltersAndCanonicalFillWithoutReadingValues) {
  auto input = fixture(false, 4096, true);
  auto ranges = input.payloads;
  auto source = std::make_shared<Source>(std::move(input.bytes));
  auto opened = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  auto inventory = opened.value()->inventory();
  ASSERT_TRUE(inventory.ok()) << inventory.status().message();
  const auto &data = dataset(inventory.value(), "/alias");
  ASSERT_TRUE(data.index_complete);
  ASSERT_EQ(data.chunks.size(), 9u);
  EXPECT_EQ(data.allocated_chunk_count, 9u);
  EXPECT_EQ(data.byte_order, Hdf5ByteOrder::Big);
  EXPECT_EQ(data.fill_status, Hdf5FillStatus::Explicit);
  EXPECT_EQ(data.fill_value, (BinaryValue{0, 0, 0xf0, 0xc0}));
  ASSERT_EQ(data.filters.size(), 3u);
  EXPECT_EQ(data.filters[0].id, H5Z_FILTER_SHUFFLE);
  EXPECT_EQ(data.filters[1].parameters, (std::vector<std::uint32_t>{1}));
  for (const auto &chunk : data.chunks) {
    EXPECT_GE(chunk.offset, 4096u);
    EXPECT_NE(std::find(ranges.begin(), ranges.end(),
                        std::make_pair(chunk.offset, chunk.encoded_size)),
              ranges.end());
  }
  // Native object-header speculative reads can overlap adjacent small raw
  // allocations. This fixture has no global heaps: no DRAW request and a
  // successful inventory despite a bad Fletcher32 value prove no value read.
  EXPECT_EQ(opened.value()->trace().raw_data_bytes, 0u);
  EXPECT_TRUE(validate_hdf5_layout(inventory.value()).ok());
}
TEST(Hdf5Inventory, SparseAllocationIsCompleteWithOnlyAllocatedChunks) {
  auto source = std::make_shared<Source>(fixture(true).bytes);
  auto opened = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(opened.ok());
  auto result = opened.value()->inventory();
  ASSERT_TRUE(result.ok()) << result.status().message();
  const auto &data = dataset(result.value(), "/alias");
  EXPECT_TRUE(data.index_complete);
  EXPECT_EQ(data.allocated_chunk_count, 1u);
  ASSERT_EQ(data.chunks.size(), 1u);
  EXPECT_EQ(data.chunks[0].origin, (std::vector<std::uint64_t>{0, 0}));
  const auto &empty = dataset(result.value(), "/unallocated");
  EXPECT_TRUE(empty.index_complete);
  EXPECT_FALSE(empty.contiguous_offset);
  EXPECT_EQ(empty.contiguous_size, 0u);
  EXPECT_EQ(empty.fill_value, BinaryValue(8, 0));
}
TEST(Hdf5Inventory,
     ChunkLimitDisablesOnlyAffectedDatasetWithoutPartialCompleteness) {
  auto source = std::make_shared<Source>(fixture().bytes);
  auto opened = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(opened.ok());
  Hdf5LayoutLimits limits;
  limits.max_chunks = 2;
  auto result = opened.value()->inventory(limits);
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result.value().directory_complete);
  const auto &data = dataset(result.value(), "/alias");
  EXPECT_FALSE(data.index_complete);
  EXPECT_TRUE(data.chunks.empty());
  EXPECT_EQ(data.allocated_chunk_count, 9u);
  EXPECT_FALSE(data.index_unavailable_reason.empty());
  EXPECT_TRUE(dataset(result.value(), "/unallocated").index_complete);
  EXPECT_TRUE(validate_hdf5_layout(result.value(), limits).ok());
}
TEST(Hdf5Inventory, DirectoryObjectLinkAndDepthLimitsReturnExplicitFileMarker) {
  for (int mode = 0; mode < 4; ++mode) {
    auto source = std::make_shared<Source>(fixture().bytes);
    auto opened = Hdf5SourceDriver::open(source);
    ASSERT_TRUE(opened.ok());
    Hdf5LayoutLimits limits;
    if (mode == 0)
      limits.max_objects = 1;
    if (mode == 1)
      limits.max_links = 1;
    if (mode == 2)
      limits.max_depth = 0;
    if (mode == 3)
      limits.max_directory_bytes = 128;
    auto result = opened.value()->inventory(limits);
    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_FALSE(result.value().directory_complete);
    EXPECT_TRUE(result.value().objects.empty());
    EXPECT_TRUE(result.value().links.empty());
    EXPECT_FALSE(result.value().index_unavailable_reason.empty());
  }
}
TEST(Hdf5Inventory,
     InventoryIoBudgetProducesUnavailableButRealReadFailureStaysFatal) {
  auto source = std::make_shared<Source>(fixture().bytes);
  auto opened = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(opened.ok());
  Hdf5LayoutLimits limits;
  limits.max_read_bytes = 1;
  auto limited = opened.value()->inventory(limits);
  ASSERT_TRUE(limited.ok());
  EXPECT_FALSE(limited.value().directory_complete);
  auto source2 = std::make_shared<Source>(fixture().bytes);
  auto opened2 = Hdf5SourceDriver::open(source2);
  ASSERT_TRUE(opened2.ok());
  source2->fail = true;
  auto failed = opened2.value()->inventory();
  ASSERT_FALSE(failed.ok());
  EXPECT_EQ(failed.status().code(), QueryError::ReadFailure);
}
TEST(Hdf5Inventory, CancellationIsNotConvertedToUnavailableMarker) {
  bool cancelled = false;
  Hdf5SourceOptions options;
  options.cancelled = [&] { return cancelled; };
  auto source = std::make_shared<Source>(fixture().bytes);
  auto opened = Hdf5SourceDriver::open(source, options);
  ASSERT_TRUE(opened.ok());
  cancelled = true;
  auto result = opened.value()->inventory();
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::Cancelled);
}

TEST(Hdf5Inventory,
     UnallocatedUndefinedFillWithUserBlockIsNotAnAllocatedAddress) {
  auto source =
      std::make_shared<Source>(fixture(false, 4096, false, false, true).bytes);
  auto opened = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(opened.ok());
  auto result = opened.value()->inventory();
  ASSERT_TRUE(result.ok()) << result.status().message();
  const auto &empty = dataset(result.value(), "/unallocated");
  EXPECT_FALSE(empty.contiguous_offset);
  EXPECT_EQ(empty.fill_status, Hdf5FillStatus::Undefined);
  EXPECT_TRUE(empty.fill_value.empty());
  EXPECT_EQ(empty.allocation_status, H5D_SPACE_STATUS_NOT_ALLOCATED);
  auto values = opened.value()->read("/unallocated", {0, 0}, {1, 1});
  ASSERT_FALSE(values.ok());
  EXPECT_EQ(values.status().code(), QueryError::UnsupportedCapability);
}
TEST(Hdf5Inventory,
     OpaqueNonUtf8NameIsUnavailableWithoutHidingSupportedSibling) {
  auto source = std::make_shared<Source>(fixture(false, 0, false, true).bytes);
  auto opened = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(opened.ok());
  auto result = opened.value()->inventory();
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_TRUE(result.value().directory_complete);
  EXPECT_TRUE(dataset(result.value(), "/alias").descriptor.supported);
  const auto &opaque =
      dataset(result.value(), std::string("/opaque-") + char(0xff));
  EXPECT_FALSE(opaque.descriptor.supported);
  EXPECT_FALSE(opaque.index_complete);
  EXPECT_FALSE(opaque.index_unavailable_reason.empty());
  EXPECT_TRUE(validate_hdf5_layout(result.value()).ok())
      << validate_hdf5_layout(result.value()).message();
}
