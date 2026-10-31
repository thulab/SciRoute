#include "iotdbfs/formats/hdf5/Hdf5SourceDriver.h"

#include <gtest/gtest.h>
#include <hdf5.h>
#include <unistd.h>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <fstream>
#include <functional>
#include <iterator>
#include <limits>
#include <stdexcept>

namespace {
using namespace iotdbfs;
class Native {
public:
  Native(hid_t id, herr_t (*close)(hid_t)) : id_(id), close_(close) {
    if (id < 0) throw std::runtime_error("H07 native fixture handle failed");
  }
  ~Native() { close_(id_); }
  Native(const Native&) = delete;
  operator hid_t() const { return id_; }
private:
  hid_t id_; herr_t (*close_)(hid_t);
};
void checked(herr_t code) {
  if (code < 0) throw std::runtime_error("H07 native fixture operation failed");
}
void number(hid_t owner, const char* name, hid_t disk_type, hid_t memory_type,
    const std::vector<hsize_t>& shape, const void* values, bool null_space = false) {
  Native space(null_space ? H5Screate(H5S_NULL) : shape.empty() ? H5Screate(H5S_SCALAR) :
    H5Screate_simple(static_cast<int>(shape.size()), shape.data(), nullptr), H5Sclose);
  Native attribute(H5Acreate2(owner, name, disk_type, space, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
  if (values) checked(H5Awrite(attribute, memory_type, values));
}
void fixed(hid_t owner, const char* name, const std::vector<std::string>& values,
    std::size_t width, H5T_str_t padding = H5T_STR_NULLPAD,
    H5T_cset_t charset = H5T_CSET_ASCII, bool scalar = true) {
  Native type(H5Tcopy(H5T_C_S1), H5Tclose);
  checked(H5Tset_size(type, width)); checked(H5Tset_strpad(type, padding)); checked(H5Tset_cset(type, charset));
  BinaryValue bytes(width * values.size(), padding == H5T_STR_SPACEPAD ? ' ' : 0);
  for (std::size_t i = 0; i < values.size(); ++i)
    std::memcpy(bytes.data() + i * width, values[i].data(), std::min(width, values[i].size()));
  number(owner, name, type, type, scalar ? std::vector<hsize_t>{} :
    std::vector<hsize_t>{values.size()}, bytes.data());
}
BinaryValue fixture(bool giant_vlen = false, bool scales = false, bool dense = false) {
  char name[] = "/tmp/iotdbfs-h07-metadata-XXXXXX";
  const auto fd = mkstemp(name); if (fd < 0) throw std::runtime_error("H07 mkstemp failed");
  close(fd);
  struct Remove { const char* path; ~Remove() { unlink(path); } } remove{name};
  {
    Native access(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    if (dense) checked(H5Pset_libver_bounds(access, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST));
    Native file(H5Fcreate(name, H5F_ACC_TRUNC, H5P_DEFAULT, access), H5Fclose);
    Native group(H5Gcreate2(file, "/group", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
    const hsize_t shape[]{4}, chunks[]{2};
    Native space(H5Screate_simple(1, shape, nullptr), H5Sclose);
    Native dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose); checked(H5Pset_chunk(dcpl, 1, chunks));
    Native data(H5Dcreate2(file, "/group/data", H5T_IEEE_F32LE, space,
      H5P_DEFAULT, dcpl, H5P_DEFAULT), H5Dclose);
    const float data_values[]{.25f, 1.25f, 2.25f, 3.25f};
    checked(H5Dwrite(data, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data_values));
    fixed(file, "owner", {"file"}, 8); fixed(group, "owner", {"group"}, 8); fixed(data, "owner", {"dataset"}, 8);
    const long long signed_scalar = -7;
    number(data, "signed", H5T_STD_I64BE, H5T_NATIVE_LLONG, {}, &signed_scalar);
    const unsigned long long unsigned_values[]{0, std::numeric_limits<unsigned long long>::max()};
    number(data, "unsigned", H5T_STD_U64BE, H5T_NATIVE_ULLONG, {2}, unsigned_values);
    const float float_values[]{.5f, -0.f, std::numeric_limits<float>::infinity(), std::numeric_limits<float>::quiet_NaN()};
    number(data, "float_matrix", H5T_IEEE_F32BE, H5T_NATIVE_FLOAT, {2, 2}, float_values);
    number(data, "empty", H5T_STD_I32LE, H5T_NATIVE_INT, {0}, nullptr);
    number(data, "null", H5T_STD_I32LE, H5T_NATIVE_INT, {}, nullptr, true);
    fixed(data, "nullterm", {std::string("a\0junk", 6)}, 8, H5T_STR_NULLTERM);
    fixed(data, "nullpad", {"ab"}, 8, H5T_STR_NULLPAD);
    fixed(data, "embedded_nullpad", {std::string("a\0b\0", 4)}, 4, H5T_STR_NULLPAD);
    fixed(data, "spacepad", {"ab"}, 8, H5T_STR_SPACEPAD);
    fixed(data, "utf8", {"科学"}, 8, H5T_STR_NULLPAD, H5T_CSET_UTF8);
    fixed(data, "strings", {"east", "west"}, 8, H5T_STR_NULLPAD, H5T_CSET_ASCII, false);
    fixed(data, "oversized", {std::string(96, 'x')}, 96);
    if (dense) {
      for (unsigned i = 0; i < 64; ++i) {
        const auto attribute_name = "dense_" + std::to_string(i);
        fixed(data, attribute_name.c_str(), {std::string(1024, char('a' + i % 26))}, 1024);
      }
    }
    if (giant_vlen) {
      Native type(H5Tcopy(H5T_C_S1), H5Tclose); checked(H5Tset_size(type, H5T_VARIABLE));
      const std::string huge(1024 * 1024, 'x'); const char* ptr = huge.c_str();
      number(data, "vlen", type, type, {}, &ptr);
    }
    if (scales) {
      Native scale(H5Dcreate2(file, "/time", H5T_IEEE_F64LE, space,
        H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
      const double coordinates[]{100, 101, 102, 103};
      checked(H5Dwrite(scale, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, coordinates));
      fixed(scale, "CLASS", {"DIMENSION_SCALE"}, 16);
      fixed(scale, "NAME", {"time"}, 8); fixed(scale, "units", {"seconds"}, 8);
      struct BackReference { hobj_ref_t dataset; unsigned dimension; } reference{};
      checked(H5Rcreate(&reference.dataset, file, "/group/data", H5R_OBJECT, H5I_INVALID_HID));
      Native compound(H5Tcreate(H5T_COMPOUND, sizeof(reference)), H5Tclose);
      checked(H5Tinsert(compound, "dataset", HOFFSET(BackReference, dataset), H5T_STD_REF_OBJ));
      checked(H5Tinsert(compound, "dimension", HOFFSET(BackReference, dimension), H5T_NATIVE_UINT));
      number(scale, "REFERENCE_LIST", compound, compound, {1}, &reference);
      hobj_ref_t forward{}; checked(H5Rcreate(&forward, file, "/time", H5R_OBJECT, H5I_INVALID_HID));
      Native variable_refs(H5Tvlen_create(H5T_STD_REF_OBJ), H5Tclose);
      hvl_t list{1, &forward}; number(data, "DIMENSION_LIST", variable_refs, variable_refs, {1}, &list);
    }
  }
  std::ifstream input(name, std::ios::binary);
  return {std::istreambuf_iterator<char>(input), {}};
}
class Source final : public QuerySource {
public:
  explicit Source(BinaryValue bytes) : bytes(std::move(bytes)) {
    manifest_.key = {"/metadata.h5", 4}; manifest_.payload_size = this->bytes.size();
    manifest_.published = true; manifest_.phase = StructuredBlobPhase::Published;
  }
  const StructuredBlobManifest& manifest() const override { return manifest_; }
  QueryResult<BinaryValue> read_payload(std::uint64_t offset, std::uint64_t length) const override {
    if (offset > bytes.size() || length > bytes.size() - offset)
      return QueryResult<BinaryValue>::failure(QueryStatus::failure(QueryError::ReadFailure,
        QueryPhase::Storage, "H07 fixture source bounds"));
    return QueryResult<BinaryValue>::success({bytes.begin() + offset, bytes.begin() + offset + length});
  }
  QueryResult<BinaryValue> read_layout(std::uint64_t, std::uint64_t) const override {
    return QueryResult<BinaryValue>::failure(QueryStatus::failure(QueryError::ReadFailure,
      QueryPhase::Storage, "H07 source has no persisted layout"));
  }
  BinaryValue bytes; StructuredBlobManifest manifest_;
};
const Hdf5LayoutObject& data_object(const Hdf5LayoutDocument& doc, const std::string& path = "/group/data") {
  for (const auto& object : doc.objects)
    if (object.dataset && object.dataset->descriptor.path == path) return object;
  throw std::runtime_error("H07 fixture dataset missing: " + path);
}
const Hdf5LayoutObject& object_id(const Hdf5LayoutDocument& doc, const std::string& id) {
  for (const auto& object : doc.objects) if (object.id == id) return object;
  throw std::runtime_error("H07 fixture object missing");
}
const Hdf5Attribute& attribute(const Hdf5LayoutObject& object, const std::string& name) {
  for (const auto& value : object.attributes) if (value.name == name) return value;
  throw std::runtime_error("H07 attribute missing: " + name);
}
Hdf5LayoutDocument inventory(Hdf5LayoutLimits limits = {}, bool giant_vlen = false, bool scales = false) {
  auto source = std::make_shared<Source>(fixture(giant_vlen, scales));
  auto opened = Hdf5SourceDriver::open(source);
  if (!opened.ok()) throw std::runtime_error(opened.status().message());
  auto result = opened.value()->inventory(limits);
  if (!result.ok()) throw std::runtime_error(result.status().message());
  return std::move(result.value());
}
std::uint64_t bits(const BinaryValue& bytes, std::size_t index = 0) {
  if (bytes.size() < (index + 1) * 8) throw std::runtime_error("H07 missing canonical numeric bytes");
  std::uint64_t result = 0;
  for (unsigned byte = 0; byte < 8; ++byte) result |= std::uint64_t(bytes[index * 8 + byte]) << (byte * 8);
  return result;
}
double floating(const BinaryValue& bytes, std::size_t index) {
  const auto value = bits(bytes, index); double result; std::memcpy(&result, &value, sizeof(result)); return result;
}
Hdf5LayoutDirectory roundtrip(const Hdf5LayoutDocument& document) {
  auto encoded = serialize_hdf5_layout(document);
  if (!encoded.ok()) throw std::runtime_error(encoded.status().message());
  auto result = read_hdf5_layout_directory([&](std::uint64_t offset, std::uint64_t length) {
    return QueryResult<BinaryValue>::success({encoded.value().begin() + offset, encoded.value().begin() + offset + length});
  }, encoded.value().size(), document.source_size);
  if (!result.ok()) throw std::runtime_error(result.status().message());
  return std::move(result.value());
}
} // namespace

TEST(Hdf5Metadata, NumericValuesRemainTypedWithShapeAndCanonicalByteOrder) {
  const auto doc = inventory(); const auto& object = data_object(doc);
  ASSERT_TRUE(object.attributes_complete); EXPECT_TRUE(object.attribute_count_known);
  EXPECT_EQ(object.attribute_count, object.attributes.size());
  const auto& signed_value = attribute(object, "signed");
  EXPECT_EQ(signed_value.state, Hdf5AttributeState::Complete); EXPECT_EQ(signed_value.type, LogicalTypeId::Int64);
  EXPECT_TRUE(signed_value.shape.empty()); EXPECT_EQ(bits(signed_value.values), static_cast<std::uint64_t>(-7));
  const auto& unsigned_value = attribute(object, "unsigned");
  EXPECT_EQ(unsigned_value.type, LogicalTypeId::UInt64); EXPECT_EQ(unsigned_value.shape, (std::vector<std::uint64_t>{2}));
  EXPECT_EQ(bits(unsigned_value.values), 0U); EXPECT_EQ(bits(unsigned_value.values, 1), UINT64_MAX);
  const auto& floats = attribute(object, "float_matrix");
  EXPECT_EQ(floats.type, LogicalTypeId::Float64); EXPECT_EQ(floats.shape, (std::vector<std::uint64_t>{2, 2}));
  EXPECT_DOUBLE_EQ(floating(floats.values, 0), .5); EXPECT_TRUE(std::signbit(floating(floats.values, 1)));
  EXPECT_TRUE(std::isinf(floating(floats.values, 2))); EXPECT_TRUE(std::isnan(floating(floats.values, 3)));
  const auto& empty = attribute(object, "empty");
  EXPECT_EQ(empty.shape, (std::vector<std::uint64_t>{0})); EXPECT_FALSE(empty.null_dataspace); EXPECT_TRUE(empty.values.empty());
  const auto& null_value = attribute(object, "null");
  EXPECT_TRUE(null_value.null_dataspace); EXPECT_TRUE(null_value.values.empty());
}

TEST(Hdf5Metadata, FixedStringsHonorDeclaredPaddingCharsetAndArrayShape) {
  const auto doc = inventory(); const auto& object = data_object(doc);
  for (const auto& item : std::vector<std::pair<std::string, std::string>>{
      {"nullterm", "a"}, {"nullpad", "ab"}, {"spacepad", "ab"}, {"utf8", "科学"}}) {
    const auto& value = attribute(object, item.first);
    EXPECT_EQ(value.state, Hdf5AttributeState::Complete); EXPECT_EQ(value.type, LogicalTypeId::Utf8);
    EXPECT_EQ(value.strings, (std::vector<std::string>{item.second})); EXPECT_TRUE(value.values.empty());
  }
  const auto& values = attribute(object, "strings");
  EXPECT_EQ(values.shape, (std::vector<std::uint64_t>{2}));
  EXPECT_EQ(values.strings, (std::vector<std::string>{"east", "west"}));
}

TEST(Hdf5Metadata, FileGroupAndDatasetAttributesHaveSeparateOwners) {
  const auto doc = inventory();
  EXPECT_EQ(attribute(object_id(doc, doc.root_object_id), "owner").strings, (std::vector<std::string>{"file"}));
  EXPECT_EQ(attribute(data_object(doc), "owner").strings, (std::vector<std::string>{"dataset"}));
  auto group = std::find_if(doc.links.begin(), doc.links.end(), [&](const auto& link) {
    return link.parent_id == doc.root_object_id && link.name == "group";
  });
  ASSERT_NE(group, doc.links.end());
  EXPECT_EQ(attribute(object_id(doc, group->target_id), "owner").strings, (std::vector<std::string>{"group"}));
}

TEST(Hdf5Metadata, VariableLengthMegabyteStringIsUnsupportedWithoutReadingItsHeap) {
  auto source = std::make_shared<Source>(fixture(true)); auto driver = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(driver.ok()); auto result = driver.value()->inventory(); ASSERT_TRUE(result.ok()) << result.status().message();
  const auto& object = data_object(result.value()); const auto& value = attribute(object, "vlen");
  EXPECT_EQ(value.state, Hdf5AttributeState::UnsupportedVariableLength); EXPECT_FALSE(value.reason.empty());
  EXPECT_TRUE(value.values.empty()); EXPECT_TRUE(value.strings.empty());
  EXPECT_TRUE(object.attributes_complete); EXPECT_TRUE(object.dataset->index_complete);
  EXPECT_EQ(object.dataset->chunks.size(), 2U); EXPECT_EQ(driver.value()->trace().raw_data_bytes, 0U);
}

TEST(Hdf5Metadata, OptionalAttributeLimitsNeverDisableThePhysicalChunkIndex) {
  Hdf5LayoutLimits bytes; bytes.max_attribute_bytes = 8;
  const auto limited = inventory(bytes); const auto& object = data_object(limited);
  EXPECT_EQ(attribute(object, "oversized").state, Hdf5AttributeState::TooLarge);
  EXPECT_TRUE(object.attributes_complete); EXPECT_TRUE(object.dataset->index_complete);
  EXPECT_EQ(object.dataset->chunks.size(), 2U); EXPECT_TRUE(validate_hdf5_layout(limited).ok());
  Hdf5LayoutLimits names; names.max_attributes_per_object = 2;
  const auto partial = inventory(names); const auto& partial_object = data_object(partial);
  EXPECT_FALSE(partial_object.attributes_complete); EXPECT_FALSE(partial_object.attributes_unavailable_reason.empty());
  EXPECT_GT(partial_object.attribute_count, partial_object.attributes.size());
  EXPECT_LE(partial_object.attributes.size(), 2U); EXPECT_TRUE(partial_object.dataset->index_complete);
  EXPECT_EQ(partial_object.dataset->chunks.size(), 2U); EXPECT_TRUE(validate_hdf5_layout(partial).ok());
  const auto decoded = roundtrip(partial);
  EXPECT_FALSE(data_object(decoded.document()).attributes_complete);
  EXPECT_EQ(data_object(decoded.document()).attributes_unavailable_reason, partial_object.attributes_unavailable_reason);
}

TEST(Hdf5Metadata, MissingNameAndIncompleteAttributeDirectoryAreDistinguishable) {
  const auto complete = inventory(); const auto& object = data_object(complete);
  EXPECT_TRUE(object.attributes_complete);
  EXPECT_EQ(std::count_if(object.attributes.begin(), object.attributes.end(), [](const auto& a) { return a.name == "absent"; }), 0);
  Hdf5LayoutLimits limits; limits.max_attributes_per_object = 1;
  const auto partial = inventory(limits); const auto& incomplete = data_object(partial);
  EXPECT_FALSE(incomplete.attributes_complete); EXPECT_GT(incomplete.attribute_count, incomplete.attributes.size());
}

TEST(Hdf5Metadata, LayoutThreeRoundtripPreservesValuesCompletenessAndShapes) {
  const auto doc = inventory({}, true, true); const auto decoded = roundtrip(doc);
  EXPECT_STREQ(kHdf5LayoutMagic, "IOTDBFS-HDF5-LAYOUT/5\n");
  const auto& before = data_object(doc); const auto& after = data_object(decoded.document());
  EXPECT_EQ(after.attribute_count, before.attribute_count); EXPECT_EQ(after.attribute_count_known, before.attribute_count_known);
  EXPECT_EQ(after.attributes_complete, before.attributes_complete);
  ASSERT_EQ(after.attributes.size(), before.attributes.size());
  for (const auto& value : before.attributes) {
    const auto& actual = attribute(after, value.name);
    EXPECT_EQ(actual.type, value.type); EXPECT_EQ(actual.state, value.state); EXPECT_EQ(actual.shape, value.shape);
    EXPECT_EQ(actual.null_dataspace, value.null_dataspace); EXPECT_EQ(actual.values, value.values); EXPECT_EQ(actual.strings, value.strings);
    EXPECT_EQ(actual.reason, value.reason);
  }
}

TEST(Hdf5Metadata, LayoutRejectsDuplicateAttributesAndInconsistentValueCardinality) {
  const auto original = inventory();
  auto duplicated = original; auto& object = duplicated.objects.at(&data_object(duplicated) - duplicated.objects.data());
  object.attributes.push_back(object.attributes.front()); ++object.attribute_count;
  EXPECT_FALSE(validate_hdf5_layout(duplicated).ok()); EXPECT_FALSE(serialize_hdf5_layout(duplicated).ok());
  auto invalid = original;
  for (auto& entry : invalid.objects) for (auto& value : entry.attributes)
    if (value.name == "signed") value.values.pop_back();
  EXPECT_FALSE(validate_hdf5_layout(invalid).ok()); EXPECT_FALSE(serialize_hdf5_layout(invalid).ok());
  invalid = original;
  for (auto& entry : invalid.objects) for (auto& value : entry.attributes)
    if (value.name == "strings") value.strings.pop_back();
  EXPECT_FALSE(validate_hdf5_layout(invalid).ok());
}

TEST(Hdf5Metadata, ReverseDimensionScaleReferencesAreCandidatesWithoutImplicitAxisBinding) {
  auto source = std::make_shared<Source>(fixture(false, true)); auto driver = Hdf5SourceDriver::open(source);
  ASSERT_TRUE(driver.ok()); auto inventory = driver.value()->inventory(); ASSERT_TRUE(inventory.ok()) << inventory.status().message();
  const auto& doc = inventory.value(); const auto& data = data_object(doc); const auto& scale = data_object(doc, "/time");
  EXPECT_TRUE(scale.dimension_scales_complete); ASSERT_EQ(scale.dimension_scales.size(), 1U);
  EXPECT_EQ(scale.dimension_scales[0].dataset_object_id, data.id); EXPECT_EQ(scale.dimension_scales[0].axis, 0U);
  EXPECT_EQ(attribute(data, "DIMENSION_LIST").state, Hdf5AttributeState::UnsupportedVariableLength);
  const auto decoded = roundtrip(doc); const auto& after = data_object(decoded.document(), "/time");
  ASSERT_EQ(after.dimension_scales.size(), 1U); EXPECT_EQ(after.dimension_scales[0].dataset_object_id, data.id);
  QueryFragment physical; physical.projection = {make_hdf5_value_field("/group/data").value(), hdf5_axis_field(0)};
  physical.slices = {{hdf5_axis_field(0), DomainBound{LogicalValue::uint64(1), BoundKind::Inclusive},
    DomainBound{LogicalValue::uint64(3), BoundKind::Exclusive}}};
  auto binding = bind_hdf5_query(*driver.value(), physical); ASSERT_TRUE(binding.ok()) << binding.status().message();
  EXPECT_EQ(binding.value().origin(), (std::vector<std::uint64_t>{1}));
  EXPECT_EQ(binding.value().shape(), (std::vector<std::uint64_t>{2}));
  physical.slices[0].dimension = FieldRef{"time"};
  EXPECT_FALSE(bind_hdf5_query(*driver.value(), physical).ok());
}

TEST(Hdf5Metadata, FileAttributeCountAndElementBudgetsPreserveIndexCompleteness) {
  Hdf5LayoutLimits counts; counts.max_attributes = 2;
  const auto limited = inventory(counts);
  std::size_t retained = 0; bool incomplete = false;
  for (const auto& object : limited.objects) {
    retained += object.attributes.size(); incomplete |= !object.attributes_complete;
  }
  EXPECT_LE(retained, 2U); EXPECT_TRUE(incomplete);
  EXPECT_TRUE(data_object(limited).dataset->index_complete);
  EXPECT_EQ(data_object(limited).dataset->chunks.size(), 2U);
  Hdf5LayoutLimits elements; elements.max_attribute_elements = 1;
  const auto element_limited = inventory(elements); const auto& object = data_object(element_limited);
  EXPECT_EQ(attribute(object, "unsigned").state, Hdf5AttributeState::TooLarge);
  EXPECT_EQ(attribute(object, "signed").state, Hdf5AttributeState::Complete);
  EXPECT_TRUE(object.dataset->index_complete); EXPECT_EQ(object.dataset->chunks.size(), 2U);
}

TEST(Hdf5Metadata, ExhaustedOptionalByteBudgetReportsUnknownMetadataAndKeepsPhysicalIndex) {
  Hdf5LayoutLimits limits; limits.max_metadata_bytes = 0;
  const auto doc = inventory(limits);
  const auto& object = data_object(doc);
  EXPECT_FALSE(object.attributes_complete); EXPECT_TRUE(object.attributes.empty());
  EXPECT_GT(object.attribute_count, 0U); EXPECT_FALSE(object.attributes_unavailable_reason.empty());
  EXPECT_TRUE(object.dataset->index_complete); EXPECT_EQ(object.dataset->chunks.size(), 2U);
  EXPECT_TRUE(validate_hdf5_layout(doc).ok());
}

TEST(Hdf5Metadata, OptionalMetadataReadBudgetExhaustionPreservesCompletedPhysicalIndex) {
  auto source = std::make_shared<Source>(fixture(false, false, true));
  std::size_t physical_reads = 0, full_reads = 0;
  {
    auto baseline = Hdf5SourceDriver::open(source); ASSERT_TRUE(baseline.ok());
    Hdf5LayoutLimits physical_only; physical_only.max_attributes = 0;
    auto index = baseline.value()->inventory(physical_only); ASSERT_TRUE(index.ok()) << index.status().message();
    ASSERT_TRUE(index.value().directory_complete); ASSERT_TRUE(data_object(index.value()).dataset->index_complete);
    ASSERT_EQ(data_object(index.value()).dataset->chunks.size(), 2U);
    physical_reads = baseline.value()->trace().reads.size();
  }
  ASSERT_GT(physical_reads, 0U);
  {
    auto unrestricted = Hdf5SourceDriver::open(source); ASSERT_TRUE(unrestricted.ok());
    auto index = unrestricted.value()->inventory(); ASSERT_TRUE(index.ok()) << index.status().message();
    ASSERT_TRUE(data_object(index.value()).attributes_complete);
    // Dense native attributes require additional heap/B-tree reads after the
    // physical graph, proving the following limit trips in optional metadata.
    full_reads = unrestricted.value()->trace().reads.size();
    ASSERT_GT(full_reads, physical_reads + 1);
  }
  // Exercise every cut point through optional native enumeration, including
  // failures inside an HDF5 operation. Each driver owns a fresh native cache.
  for (std::size_t limit = physical_reads; limit < full_reads; ++limit) {
    SCOPED_TRACE(limit);
    const auto native_objects_before = H5Fget_obj_count(H5F_OBJ_ALL, H5F_OBJ_ALL);
    ASSERT_GE(native_objects_before, 0);
    Hdf5SourceOptions options; options.max_read_calls = limit;
    auto bounded = Hdf5SourceDriver::open(source, options); ASSERT_TRUE(bounded.ok());
    auto result = bounded.value()->inventory(); ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_TRUE(result.value().directory_complete);
    const auto& object = data_object(result.value());
    ASSERT_TRUE(object.dataset); EXPECT_TRUE(object.dataset->index_complete);
    EXPECT_EQ(object.dataset->chunks.size(), 2U);
    EXPECT_FALSE(object.attributes_complete); EXPECT_FALSE(object.attributes_unavailable_reason.empty());
    EXPECT_TRUE(validate_hdf5_layout(result.value()).ok());
    bounded.value().reset();
    EXPECT_EQ(H5Fget_obj_count(H5F_OBJ_ALL, H5F_OBJ_ALL), native_objects_before);
  }
}

TEST(Hdf5Metadata, NullPaddingDoesNotSilentlyTruncateEmbeddedNullContent) {
  const auto doc = inventory(); const auto& object = data_object(doc);
  const auto& padded = attribute(object, "embedded_nullpad");
  EXPECT_EQ(padded.state, Hdf5AttributeState::InvalidValue);
  EXPECT_FALSE(padded.reason.empty()); EXPECT_TRUE(padded.strings.empty());
  const auto& terminated = attribute(object, "nullterm");
  EXPECT_EQ(terminated.state, Hdf5AttributeState::Complete);
  EXPECT_EQ(terminated.strings, (std::vector<std::string>{"a"}));
  EXPECT_TRUE(object.dataset->index_complete); EXPECT_TRUE(validate_hdf5_layout(doc).ok());
}
