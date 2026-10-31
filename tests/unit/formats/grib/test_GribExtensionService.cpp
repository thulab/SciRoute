#include <gtest/gtest.h>

#include "iotdbfs/formats/grib/GribExtensionService.h"
#include "iotdbfs/formats/grib/gribUtils.h"

#include <cstring>
#include <map>
#include <optional>
#include <vector>

using iotdbfs::GribExtensionService;
using iotdbfs::GribVirtualType;

static GribExtensionService make_service(bool enabled = true,
  bool writing = false) {
  GribExtensionService::Dependencies deps;
  deps.read_object_part = [](long long, const std::string&, uint64_t, uint64_t) {
    return std::optional<std::vector<uint8_t>>(std::vector<uint8_t>{});
  };
  deps.read_layout_text = [](long long, const std::string&) {
    return std::optional<std::string>(std::string());
  };
  deps.is_path_in_writing = [writing](const std::string&) {
    return writing;
  };
  return GribExtensionService(enabled, deps);
}

static GribExtensionService make_service_with_index(
  const std::string& index_text) {
  GribExtensionService::Dependencies deps;
  deps.read_object_part = [](long long, const std::string&, uint64_t, uint64_t) {
    return std::optional<std::vector<uint8_t>>(std::vector<uint8_t>{});
  };
  deps.read_layout_text = [index_text](long long, const std::string&) {
    return std::optional<std::string>(index_text);
  };
  deps.is_path_in_writing = [](const std::string&) {
    return false;
  };
  return GribExtensionService(true, deps);
}

TEST(GribExtensionService, PersistedAdapterDispatchDoesNotDependOnSuffix) {
  auto service = make_service(true);
  EXPECT_TRUE(service.enabled_for_path("/data/a.grib2"));
  EXPECT_TRUE(service.enabled_for_path("/data/a.grb"));
  EXPECT_TRUE(service.enabled_for_path("/data/a.txt"));
}

TEST(GribExtensionService, DisabledServiceRejectsGribPath) {
  auto service = make_service(false);
  EXPECT_FALSE(service.enabled_for_path("/data/a.grib2"));
}

TEST(GribExtensionService, ParseFunctionReadBinaryQuery) {
  auto service = make_service();
  auto spec = service.parse_query_spec(
    "function=read&type=binary&level=500&shortName=t");
  EXPECT_TRUE(spec.ok);
  EXPECT_TRUE(spec.is_function_chain);
  EXPECT_EQ(spec.type, GribVirtualType::Binary);
  EXPECT_EQ(spec.kv_idx.at("level"), "500");
  EXPECT_EQ(spec.kv_idx.at("shortName"), "t");
  EXPECT_EQ(spec.kv_idx.count("type"), 0);
}

TEST(GribExtensionService, ParseFunctionReadDataWithCropAndInterp) {
  auto service = make_service();
  auto spec = service.parse_query_spec(
    "function=read&level=500?function=crop&lat0=30&lon0=100"
    "?function=interp&outNi=200&outNj=160");
  EXPECT_TRUE(spec.ok);
  EXPECT_EQ(spec.type, GribVirtualType::Data);
  EXPECT_EQ(spec.kv_idx.at("level"), "500");
  EXPECT_EQ(spec.kv_idx.count("lat0"), 0);
  EXPECT_EQ(spec.kv_idx.count("outNi"), 0);
  EXPECT_EQ(spec.kv_all.at("lat0"), "30");
  EXPECT_EQ(spec.kv_all.at("outNi"), "200");
  EXPECT_EQ(spec.kv_all.at("interp"), "bilinear");
}

TEST(GribExtensionService, RejectsQueryWithoutFunctionChain) {
  auto service = make_service();
  auto spec = service.parse_query_spec("level=500&shortName=t");
  EXPECT_FALSE(spec.ok);
}

TEST(GribExtensionService, RejectsReadWithoutMessageSelection) {
  auto service = make_service();
  auto spec = service.parse_query_spec("function=read&type=binary&dtype=f64");
  EXPECT_FALSE(spec.ok);
}

TEST(GribExtensionService, RejectsUnsupportedFunctionAndType) {
  auto service = make_service();
  EXPECT_FALSE(service.parse_query_spec("function=sum&level=500").ok);
  EXPECT_FALSE(service.parse_query_spec(
    "function=read&type=jpeg&level=500").ok);
}

TEST(GribExtensionService, HasNonTypeParamsMatchesMainBehavior) {
  auto service = make_service();
  EXPECT_FALSE(service.has_non_type_params({}));
  EXPECT_FALSE(service.has_non_type_params({{"type", "binary"}}));
  EXPECT_TRUE(service.has_non_type_params({{"type", "binary"},
    {"level", "500"}}));
}

TEST(GribExtensionService, BuildsInodeXattrs) {
  auto service = make_service();
  Inode inode;
  inode.format_id = "grib";
  inode.metadata = R"({"Ni":{"type":"int64_set","value":[1440]},"Nj":{"type":"int64_set","value":[721]},"centre":{"type":"int64_set","value":[38]},"shortName":{"type":"string_set","value":["t","u"]},"typeOfLevel":{"type":"string_set","value":["isobaricInhPa"]},"typeOfProcessedData":{"type":"string_set","value":["forecast"]}})";
  auto attrs = service.build_inode_xattrs(inode);
  std::map<std::string, std::string> by_name(attrs.begin(), attrs.end());
  EXPECT_EQ(by_name["user.grib.shortName"], "[\"t\",\"u\"]");
  EXPECT_EQ(by_name["user.grib.typeOfProcessedData"], "[\"forecast\"]");
  EXPECT_EQ(by_name["user.grib.typeOfLevel"], "[\"isobaricInhPa\"]");
  EXPECT_EQ(by_name["user.grib.centre"], "[38]");
  EXPECT_EQ(by_name["user.grib.Ni"], "[1440]");
  EXPECT_EQ(by_name["user.grib.Nj"], "[721]");
}

TEST(GribExtensionService, BuildsIndexXattrs) {
  auto service = make_service();
  iotdbfs::GribXattrList attrs;
  service.append_index_xattrs(
    "{\"sn\":\"t\",\"lv\":500,\"of\":100,\"ln\":200,\"ed\":2}\n",
    attrs);
  std::map<std::string, std::string> by_name(attrs.begin(), attrs.end());
  EXPECT_NE(by_name["user.grib.index.keys"].find("shortName"),
    std::string::npos);
  EXPECT_NE(by_name["user.grib.index.keys"].find("level"),
    std::string::npos);
  EXPECT_EQ(by_name["user.grib.index.keys"].find("offset"),
    std::string::npos);
  EXPECT_NE(by_name["user.grib.index.values"].find("\"shortName\":[\"t\"]"),
    std::string::npos);
}

TEST(GribExtensionService, AppendsIndexXattrNames) {
  auto service = make_service();
  iotdbfs::GribXattrList attrs;
  service.append_index_xattr_names(attrs);
  std::map<std::string, std::string> by_name(attrs.begin(), attrs.end());
  EXPECT_EQ(by_name.count("user.grib.index.keys"), 1);
  EXPECT_EQ(by_name.count("user.grib.index.values"), 1);
}

TEST(GribExtensionService, CanHandleVirtualQueryRequiresEnabledAndNotWriting) {
  EXPECT_TRUE(make_service(true, false).can_handle_virtual_query("/a.grib2"));
  EXPECT_FALSE(make_service(false, false).can_handle_virtual_query("/a.grib2"));
  EXPECT_FALSE(make_service(true, true).can_handle_virtual_query("/a.grib2"));
  EXPECT_TRUE(make_service(true, false).can_handle_virtual_query("/a.txt"));
}

TEST(GribExtensionService, BinaryVirtualAttrUsesIndexLength) {
  auto service = make_service_with_index(
    "{\"sn\":\"t\",\"lv\":500,\"of\":100,\"ln\":200,\"ed\":2}\n");
  auto spec = service.parse_query_spec(
    "function=read&type=binary&shortName=t&level=500");
  ASSERT_TRUE(spec.ok);

  auto attr = service.get_virtual_attr(1, "/a.grib2", spec,
    "function=read&type=binary&shortName=t&level=500");

  EXPECT_TRUE(attr.ok);
  EXPECT_EQ(attr.size, 200);
}

TEST(GribExtensionService, BinaryVirtualOpenReturnsSlice) {
  auto service = make_service_with_index(
    "{\"sn\":\"t\",\"lv\":500,\"of\":100,\"ln\":200,\"ed\":2}\n");
  auto spec = service.parse_query_spec(
    "function=read&type=binary&shortName=t&level=500");
  ASSERT_TRUE(spec.ok);

  auto opened = service.open_virtual(1, "/a.grib2", spec,
    "function=read&type=binary&shortName=t&level=500");

  EXPECT_TRUE(opened.ok);
  EXPECT_EQ(opened.type, GribVirtualType::Binary);
  EXPECT_EQ(opened.binary.offset, 100);
  EXPECT_EQ(opened.binary.length, 200);
}

TEST(GribExtensionService, ConfiguredPersistedFieldsOverrideWithoutDuplicateNames) {
  auto config = iotdbfs::MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.name.source=shortName\nmetadata.field.name.type=string\n"
    "metadata.field.name.xattr=user.grib.shortName\n"
    "metadata.field.experiment.source=edition\nmetadata.field.experiment.type=int64\n"
    "metadata.field.experiment.xattr=user.weather.experiment\n");
  ASSERT_TRUE(config.ok());
  GribExtensionService::Dependencies deps;
  deps.metadata_config = std::make_shared<iotdbfs::MetadataConfig>(config.value());
  GribExtensionService service(true, deps);
  Inode inode;
  iotdbfs::MetadataMap values;
  values.emplace("name", iotdbfs::MetadataValue("custom"));
  values.emplace("experiment", iotdbfs::MetadataValue(std::int64_t(2)));
  values.emplace("hidden", iotdbfs::MetadataValue("private"));
  auto serialized = iotdbfs::serialize_metadata(values, 65536);
  ASSERT_TRUE(serialized.ok());
  inode.metadata = serialized.value();
  auto attrs = service.build_inode_xattrs(inode);
  std::map<std::string, std::string> by_name(attrs.begin(), attrs.end());
  EXPECT_EQ(by_name.size(), attrs.size());
  EXPECT_EQ(by_name.at("user.grib.shortName"), "custom");
  EXPECT_EQ(by_name.at("user.weather.experiment"), "2");
  EXPECT_EQ(by_name.count("hidden"), 0u);

  attrs = service.build_inode_xattrs(inode);
  EXPECT_EQ(attrs.size(), 2u); // no invented legacy zero values
  inode.metadata = "malformed";
  EXPECT_TRUE(service.build_inode_xattrs(inode).empty());
  inode.metadata = R"({"experiment":{"type":"string","value":"old-type"}})";
  EXPECT_TRUE(service.build_inode_xattrs(inode).empty());
}

TEST(GribExtensionService, EmptyQueryRepresentsOrdinaryFile) {
  auto service = make_service();
  const auto spec = service.parse_query_spec("");
  ASSERT_TRUE(spec.ok);
  EXPECT_EQ(spec.type, GribVirtualType::None);
  EXPECT_FALSE(spec.is_function_chain);
  EXPECT_FALSE(service.get_virtual_attr(1, "/a.grib2", spec, "").ok);
  EXPECT_FALSE(service.open_virtual(1, "/a.grib2", spec, "").ok);
}

TEST(GribExtensionService, RejectsMalformedFunctionChains) {
  auto service = make_service();
  const std::vector<std::string> invalid{
    "function=crop&lat0=1&lon0=2",
    "function=interp&outNi=2&outNj=2",
    "function=read&level=500?lat0=1&lon0=2",
    "function=read&level=500?function=crop&type=data",
    "function=read&level=500?function=interp&type=data",
    "function=read&dtype=f64?function=crop&lat0=1&lon0=2",
    "function=read&level=500?function=unknown"};
  for (const auto& query : invalid) {
    SCOPED_TRACE(query);
    const auto spec = service.parse_query_spec(query);
    EXPECT_FALSE(spec.ok);
    EXPECT_FALSE(service.get_virtual_attr(1, "/a.grib2", spec, query).ok);
    EXPECT_FALSE(service.open_virtual(1, "/a.grib2", spec, query).ok);
  }
}

TEST(GribExtensionService, NormalizesFunctionAndTypeValues) {
  auto service = make_service();
  auto binary = service.parse_query_spec("function=READ&type=BINARY&level=500");
  ASSERT_TRUE(binary.ok);
  EXPECT_EQ(binary.type, GribVirtualType::Binary);
  auto data = service.parse_query_spec("function=Read&type=DATA&level=500?function=INTERP&outNi=3&outNj=3");
  ASSERT_TRUE(data.ok);
  EXPECT_EQ(data.type, GribVirtualType::Data);
  EXPECT_EQ(data.kv_all.at("interp"), "bilinear");
}

TEST(GribExtensionService, IndexXattrsDeduplicateSortAndEscapeValues) {
  auto service = make_service();
  iotdbfs::GribXattrList attrs;
  service.append_index_xattrs(
    "not an index\n\n"
    "{\"sn\":\"z\",\"ed\":2,\"of\":1,\"ln\":10}\n"
    "{\"sn\":\"a\\\"b\\\\c\\n\",\"ed\":2,\"of\":11,\"ln\":10}\n"
    "{\"sn\":\"z\",\"ed\":2,\"of\":21,\"ln\":10}\n", attrs);
  const std::map<std::string, std::string> by_name(attrs.begin(), attrs.end());
  ASSERT_EQ(by_name.size(), 2u);
  EXPECT_EQ(by_name.at("user.grib.index.keys"), "edition,shortName");
  EXPECT_EQ(by_name.at("user.grib.index.values"),
    "{\"edition\":[\"2\"],\"shortName\":[\"a\\\"b\\\\c\\n\",\"z\"]}");
}

TEST(GribExtensionService, IndexXattrsNormalizeEveryPublishedAlias) {
  auto service = make_service();
  const std::map<std::string, std::string> aliases{
    {"sn", "shortName"}, {"sr", "stepRange"}, {"tl", "typeOfLevel"},
    {"lv", "level"}, {"st", "step"}, {"u", "unit"},
    {"gd", "globalDomain"}, {"np", "numberOfDataPoints"},
    {"ed", "edition"}, {"customKey", "customKey"}};
  for (const auto& alias : aliases) {
    SCOPED_TRACE(alias.first);
    iotdbfs::GribXattrList attrs;
    service.append_index_xattrs("{\"" + alias.first + "\":\"value\"}", attrs);
    const std::map<std::string, std::string> by_name(attrs.begin(), attrs.end());
    ASSERT_EQ(by_name.size(), 2u);
    EXPECT_EQ(by_name.at("user.grib.index.keys"), alias.second);
    EXPECT_EQ(by_name.at("user.grib.index.values"),
      "{\"" + alias.second + "\":[\"value\"]}");
  }
}

TEST(GribExtensionService, IndexXattrsEscapeControlCharactersInNamesAndValues) {
  auto service = make_service();
  iotdbfs::GribXattrList attrs;
  // Even externally supplied index text containing a literal control byte
  // must not leak an unescaped control byte into the JSON xattr.
  const auto control = std::string(1, '\x01');
  service.append_index_xattrs(
    "{\"custom\\tkey\":\"\\b\\f\\r\\t\\/" + control + "\"}", attrs);
  const std::map<std::string, std::string> by_name(attrs.begin(), attrs.end());
  ASSERT_EQ(by_name.size(), 2u);
  EXPECT_EQ(by_name.at("user.grib.index.keys"), "custom\tkey");
  EXPECT_EQ(by_name.at("user.grib.index.values"),
    "{\"custom\\tkey\":[\"\\b\\f\\r\\t/\\u0001\"]}");
}

TEST(GribExtensionService, IndexXattrsIgnoreMalformedRecordsAndTrimNumericValues) {
  auto service = make_service();
  iotdbfs::GribXattrList attrs{{"user.existing", "preserved"}};
  for (const auto& malformed : {"", " \t", "{}", "{no_quotes:1}",
       "{\"missingColon\" 1}", "{\"key\""}) {
    SCOPED_TRACE(malformed);
    service.append_index_xattrs(malformed, attrs);
    ASSERT_EQ(attrs.size(), 1u);
    EXPECT_EQ(attrs[0].second, "preserved");
  }
  service.append_index_xattrs("  { \"lv\" : 500 \t, \"ed\": 2 }  \n", attrs);
  const std::map<std::string, std::string> by_name(attrs.begin(), attrs.end());
  EXPECT_EQ(by_name.at("user.grib.index.keys"), "edition,level");
  EXPECT_EQ(by_name.at("user.grib.index.values"),
    "{\"edition\":[\"2\"],\"level\":[\"500\"]}");
}

TEST(GribExtensionService, ReadAndTransformStagesDiscardParametersOwnedByOtherStages) {
  auto service = make_service();
  const auto spec = service.parse_query_spec(
    "?&&?function=read&type=&level=500&lat0=99&lon0=99&lat1=99&lon1=99"
    "&interp=invalid&outNi=99&outNj=99&outni=99&outnj=99&dlon=99&dlat=99&border=invalid"
    "?function=crop&lat0=1&lon0=0&lat1=0&lon1=1&interp=invalid&outNi=99"
    "?function=interp&outNi=3&outNj=4&lat0=99&lon0=99&lat1=99&lon1=99?");
  ASSERT_TRUE(spec.ok);
  EXPECT_EQ(spec.type, GribVirtualType::Binary);
  EXPECT_EQ(spec.kv_idx, (std::map<std::string, std::string>{{"level", "500"}}));
  EXPECT_EQ(spec.kv_all, (std::map<std::string, std::string>{
    {"level", "500"}, {"lat0", "1"}, {"lon0", "0"},
    {"lat1", "0"}, {"lon1", "1"}, {"interp", "bilinear"},
    {"outNi", "3"}, {"outNj", "4"}}));
}

TEST(GribExtensionService, EmptyInodeHasNoInventedMetadataAttributes) {
  auto service = make_service();
  EXPECT_TRUE(service.build_inode_xattrs(Inode{}).empty());
}

TEST(GribExtensionService, MissingReaderDependenciesRejectSelectedVirtualFiles) {
  for (const bool have_index : {false, true}) {
    GribExtensionService::Dependencies deps;
    if (have_index) {
      deps.read_layout_text = [](long long, const std::string&) {
        return std::optional<std::string>("{\"ed\":2,\"of\":4,\"ln\":12}");
      };
    }
    GribExtensionService service(true, std::move(deps));
    EXPECT_TRUE(service.can_handle_virtual_query("/reader.grib2"));
    const std::string data_query = "function=read&edition=2";
    const auto data_spec = service.parse_query_spec(data_query);
    EXPECT_FALSE(service.get_virtual_attr(1, "/reader.grib2", data_spec, data_query).ok);
    EXPECT_FALSE(service.open_virtual(1, "/reader.grib2", data_spec, data_query).ok);
    const std::string binary_query = "function=read&edition=2&type=binary";
    const auto binary_spec = service.parse_query_spec(binary_query);
    EXPECT_EQ(service.get_virtual_attr(1, "/reader.grib2", binary_spec, binary_query).ok,
      have_index);
    EXPECT_EQ(service.open_virtual(1, "/reader.grib2", binary_spec, binary_query).ok,
      have_index);
  }
}

TEST(GribExtensionService, DirectEmptySelectionSpecsHaveDefinedResultsWithoutReaders) {
  GribExtensionService service(true, {});
  // The public result API also accepts specs constructed by a caller. An
  // empty binary selection denotes the whole-file slice; data needs a match.
  iotdbfs::GribQuerySpec spec;
  spec.ok = true;
  spec.kv_all = {{"type", "binary"}};
  spec.type = GribVirtualType::Binary;
  const auto binary_attr = service.get_virtual_attr(1, "/empty.grib2", spec, "");
  EXPECT_TRUE(binary_attr.ok);
  EXPECT_EQ(binary_attr.size, 0u);
  const auto binary = service.open_virtual(1, "/empty.grib2", spec, "");
  EXPECT_TRUE(binary.ok);
  EXPECT_EQ(binary.type, GribVirtualType::Binary);
  EXPECT_EQ(binary.binary.offset, 0u);
  EXPECT_EQ(binary.binary.length, 0u);

  spec.type = GribVirtualType::Data;
  spec.kv_all = {{"type", "data"}};
  const auto data_attr = service.get_virtual_attr(1, "/empty.grib2", spec, "");
  EXPECT_TRUE(data_attr.ok);
  EXPECT_EQ(data_attr.size, 0u);
  EXPECT_FALSE(service.open_virtual(1, "/empty.grib2", spec, "").ok);
}

TEST(GribExtensionService, DirectSpecWithOnlyTransformParametersCannotSelectAnIndexRecord) {
  auto service = make_service_with_index("{\"ed\":2,\"of\":4,\"ln\":12}");
  iotdbfs::GribQuerySpec spec;
  spec.ok = true;
  spec.kv_all = {{"dtype", "f64"}, {"lat0", "10"}};
  for (const auto type : {GribVirtualType::Data, GribVirtualType::Binary}) {
    spec.type = type;
    EXPECT_FALSE(service.get_virtual_attr(1, "/empty.grib2", spec, "").ok);
    EXPECT_FALSE(service.open_virtual(1, "/empty.grib2", spec, "").ok);
  }
}

TEST(GribExtensionService, MissingIndexAndUnmatchedSelectionFailWithoutObjectReads) {
  for (const auto& index : {std::optional<std::string>{},
       std::optional<std::string>{""},
       std::optional<std::string>{"{\"sn\":\"u\",\"of\":0,\"ln\":100}"}}) {
    GribExtensionService::Dependencies deps;
    deps.read_layout_text = [index](long long, const std::string&) { return index; };
    deps.read_object_part = [](long long, const std::string&, uint64_t, uint64_t)
      -> std::optional<std::vector<uint8_t>> {
      ADD_FAILURE() << "unmatched query must not read object data";
      return std::nullopt;
    };
    GribExtensionService service(true, deps);
    for (const auto* type : {"data", "binary"}) {
      const auto query = std::string("function=read&shortName=t&type=") + type;
      auto spec = service.parse_query_spec(query);
      ASSERT_TRUE(spec.ok);
      EXPECT_FALSE(service.get_virtual_attr(1, "/a.grib2", spec, query).ok);
      EXPECT_FALSE(service.open_virtual(1, "/a.grib2", spec, query).ok);
    }
  }
}

namespace {

class GribVirtualData : public ::testing::Test {
protected:
  std::vector<uint8_t> message;
  std::string index;
  std::string path;
  std::unique_ptr<GribExtensionService> service;
  size_t object_reads{0};
  bool read_fails{false};
  bool read_empty{false};

  void SetUp() override {
    std::unique_ptr<codes_handle, decltype(&codes_handle_delete)> handle(
      codes_grib_handle_new_from_samples(nullptr, "regular_ll_sfc_grib2"), codes_handle_delete);
    ASSERT_NE(handle, nullptr);
    ASSERT_EQ(codes_set_long(handle.get(), "Ni", 2), CODES_SUCCESS);
    ASSERT_EQ(codes_set_long(handle.get(), "Nj", 2), CODES_SUCCESS);
    ASSERT_EQ(codes_set_double(handle.get(), "latitudeOfFirstGridPointInDegrees", 1), CODES_SUCCESS);
    ASSERT_EQ(codes_set_double(handle.get(), "latitudeOfLastGridPointInDegrees", 0), CODES_SUCCESS);
    ASSERT_EQ(codes_set_double(handle.get(), "longitudeOfFirstGridPointInDegrees", 0), CODES_SUCCESS);
    ASSERT_EQ(codes_set_double(handle.get(), "longitudeOfLastGridPointInDegrees", 1), CODES_SUCCESS);
    ASSERT_EQ(codes_set_double(handle.get(), "iDirectionIncrementInDegrees", 1), CODES_SUCCESS);
    ASSERT_EQ(codes_set_double(handle.get(), "jDirectionIncrementInDegrees", 1), CODES_SUCCESS);
    const double values[]{1, 3, 5, 7};
    ASSERT_EQ(codes_set_double_array(handle.get(), "values", values, 4), CODES_SUCCESS);
    const void* data = nullptr;
    size_t size = 0;
    ASSERT_EQ(codes_get_message(handle.get(), &data, &size), CODES_SUCCESS);
    const auto* first = static_cast<const uint8_t*>(data);
    message.assign(first, first + size);
    index = grib_utils::build_index_line(handle.get(), 51, size, 2);
    // Every fixture instance needs a new cache identity, including repeated
    // runs of the same test in one process.
    static std::uint64_t sequence = 0;
    path = "/virtual-ut/" + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name()) +
      "-" + std::to_string(++sequence) + ".grib2";
    GribExtensionService::Dependencies deps;
    deps.read_layout_text = [this](long long row, const std::string& requested_path) {
      EXPECT_EQ(row, 123);
      EXPECT_EQ(requested_path, path);
      return std::optional<std::string>(index);
    };
    deps.read_object_part = [this](long long row, const std::string& requested_path,
      uint64_t offset, uint64_t length) -> std::optional<std::vector<uint8_t>> {
      EXPECT_EQ(row, 123);
      EXPECT_EQ(requested_path, path);
      EXPECT_EQ(offset, 51u);
      EXPECT_EQ(length, message.size());
      ++object_reads;
      if (read_fails) return std::nullopt;
      if (read_empty) return std::vector<uint8_t>{};
      return message;
    };
    service = std::make_unique<GribExtensionService>(true, std::move(deps));
  }

  template <typename T>
  void expect_data(const std::string& query, const std::vector<T>& expected) {
    const auto spec = service->parse_query_spec(query);
    ASSERT_TRUE(spec.ok);
    const auto attr = service->get_virtual_attr(123, path, spec, query);
    ASSERT_TRUE(attr.ok);
    EXPECT_EQ(attr.size, expected.size() * sizeof(T));
    const auto opened = service->open_virtual(123, path, spec, query);
    ASSERT_TRUE(opened.ok);
    EXPECT_EQ(opened.type, GribVirtualType::Data);
    ASSERT_EQ(opened.data.bytes.size(), expected.size() * sizeof(T));
    std::vector<T> actual(expected.size());
    std::memcpy(actual.data(), opened.data.bytes.data(), opened.data.bytes.size());
    EXPECT_EQ(actual, expected);
  }
};

TEST_F(GribVirtualData, BinarySelectionUsesOnlyTheIndex) {
  const std::string query = "function=read&edition=2&type=binary";
  const auto spec = service->parse_query_spec(query);
  ASSERT_TRUE(spec.ok);
  const auto attr = service->get_virtual_attr(123, path, spec, query);
  ASSERT_TRUE(attr.ok);
  EXPECT_EQ(attr.size, message.size());
  const auto opened = service->open_virtual(123, path, spec, query);
  ASSERT_TRUE(opened.ok);
  EXPECT_EQ(opened.type, GribVirtualType::Binary);
  EXPECT_EQ(opened.binary.offset, 51u);
  EXPECT_EQ(opened.binary.length, message.size());
  EXPECT_EQ(object_reads, 0u);
}

TEST_F(GribVirtualData, AmbiguousSelectionFailsBeforeReadingPayload) {
  index += index;
  for (const auto& query : {
      "function=read&edition=2&type=binary",
      "function=read&edition=2"}) {
    SCOPED_TRACE(query);
    const auto spec = service->parse_query_spec(query);
    ASSERT_TRUE(spec.ok);
    EXPECT_FALSE(service->get_virtual_attr(123, path, spec, query).ok);
    EXPECT_FALSE(service->open_virtual(123, path, spec, query).ok);
  }
  EXPECT_EQ(object_reads, 0u);
}

TEST_F(GribVirtualData, DefaultDtypeProducesFloat32Bytes) {
  expect_data<float>("function=read&edition=2", {1, 3, 5, 7});
  EXPECT_EQ(object_reads, 1u);
}

TEST_F(GribVirtualData, RequestedDtypeProducesFloat64Bytes) {
  expect_data<double>("function=read&edition=2&dtype=f64", {1, 3, 5, 7});
  EXPECT_EQ(object_reads, 1u);
}

TEST_F(GribVirtualData, ConfiguredCacheCapacityEvictsLeastRecentlyUsedMessage) {
  struct RestoreCacheCapacity {
    ~RestoreCacheCapacity() { iotdbfs::configure_grib_message_cache(32); }
  } restore_cache;
  iotdbfs::configure_grib_message_cache(1);
  const auto first_path = path;
  expect_data<float>("function=read&edition=2", {1, 3, 5, 7});
  ASSERT_EQ(object_reads, 1u);
  path += ".second.grib2";
  expect_data<float>("function=read&edition=2", {1, 3, 5, 7});
  ASSERT_EQ(object_reads, 2u);
  path = first_path;
  expect_data<float>("function=read&edition=2", {1, 3, 5, 7});
  EXPECT_EQ(object_reads, 3u);
}

TEST_F(GribVirtualData, DirectSpecWithoutSeparateIndexMapUsesSelectionFromAllParameters) {
  const std::string query = "function=read&edition=2&dtype=f64";
  auto spec = service->parse_query_spec(query);
  ASSERT_TRUE(spec.ok);
  spec.kv_idx.clear();
  const auto attr = service->get_virtual_attr(123, path, spec, query);
  ASSERT_TRUE(attr.ok);
  EXPECT_EQ(attr.size, 4 * sizeof(double));
  const auto opened = service->open_virtual(123, path, spec, query);
  ASSERT_TRUE(opened.ok);
  ASSERT_EQ(opened.data.bytes.size(), 4 * sizeof(double));
  std::vector<double> values(4);
  std::memcpy(values.data(), opened.data.bytes.data(), opened.data.bytes.size());
  EXPECT_EQ(values, (std::vector<double>{1, 3, 5, 7}));
  EXPECT_EQ(object_reads, 1u);
}

TEST_F(GribVirtualData, CropAttributesAndOpenAgreeOnSelectedValues) {
  expect_data<double>("function=read&edition=2?function=crop&lat0=1&lon0=1&lat1=0&lon1=1&dtype=f64", {3, 7});
  EXPECT_EQ(object_reads, 1u);
}

TEST_F(GribVirtualData, InterpolationAttributesAndOpenAgreeOnSelectedValues) {
  expect_data<float>("function=read&edition=2?function=interp&outNi=3&outNj=3", {1, 2, 3, 3, 4, 5, 5, 6, 7});
  EXPECT_EQ(object_reads, 1u);
}

TEST_F(GribVirtualData, CropThenInterpolationUsesOnlySelectedColumn) {
  expect_data<float>(
    "function=read&edition=2?function=crop&lat0=1&lon0=1&lat1=0&lon1=1"
    "?function=interp&outNi=3&outNj=3", {3, 3, 3, 5, 5, 5, 7, 7, 7});
  EXPECT_EQ(object_reads, 1u);
}

TEST_F(GribVirtualData, CropThenInterpolationUsesOnlySelectedRow) {
  expect_data<double>(
    "function=read&edition=2?function=crop&lat0=1&lon0=0&lat1=1&lon1=1"
    "?function=interp&outNi=3&outNj=2&dtype=f64", {1, 2, 3, 1, 2, 3});
  EXPECT_EQ(object_reads, 1u);
}

TEST_F(GribVirtualData, RegionalCropClampsAtGridBoundary) {
  expect_data<float>(
    "function=read&edition=2?function=crop&lat0=1&lon0=0&lat1=0&lon1=10",
    {1, 3, 5, 7});
  EXPECT_EQ(object_reads, 1u);
}

TEST_F(GribVirtualData, EmptyReadsFailAndCanRecoverWithTheSameCacheKey) {
  const std::string plain = "function=read&edition=2";
  const std::string cropped = plain + "?function=crop&lat0=1&lon0=1&lat1=0&lon1=1";
  const std::string interpolated = plain + "?function=interp&outNi=3&outNj=3";
  const std::string transformed = cropped + "?function=interp&outNi=3&outNj=3";
  read_empty = true;
  for (const auto& query : {cropped, interpolated, transformed}) {
    SCOPED_TRACE(query);
    const auto spec = service->parse_query_spec(query);
    ASSERT_TRUE(spec.ok);
    EXPECT_FALSE(service->get_virtual_attr(123, path, spec, query).ok);
    EXPECT_FALSE(service->open_virtual(123, path, spec, query).ok);
  }
  const auto spec = service->parse_query_spec(plain);
  ASSERT_TRUE(spec.ok);
  EXPECT_FALSE(service->open_virtual(123, path, spec, plain).ok);
  const auto failed_reads = object_reads;

  read_empty = false;
  expect_data<float>(plain, {1, 3, 5, 7});
  expect_data<float>(cropped, {3, 7});
  expect_data<float>(interpolated, {1, 2, 3, 3, 4, 5, 5, 6, 7});
  expect_data<float>(transformed, {3, 3, 3, 5, 5, 5, 7, 7, 7});
  EXPECT_EQ(object_reads, failed_reads + 1);
}

TEST_F(GribVirtualData, ReadFailurePreventsDataOpenAndTransformedAttributes) {
  read_fails = true;
  for (const auto& query : {"function=read&edition=2?function=crop&lat0=1&lon0=1&lat1=0&lon1=1",
       "function=read&edition=2?function=interp&outNi=3&outNj=3"}) {
    SCOPED_TRACE(query);
    const auto spec = service->parse_query_spec(query);
    ASSERT_TRUE(spec.ok);
    EXPECT_FALSE(service->get_virtual_attr(123, path, spec, query).ok);
    EXPECT_FALSE(service->open_virtual(123, path, spec, query).ok);
  }
  const std::string query = "function=read&edition=2";
  const auto spec = service->parse_query_spec(query);
  EXPECT_FALSE(service->open_virtual(123, path, spec, query).ok);
}

} // namespace
