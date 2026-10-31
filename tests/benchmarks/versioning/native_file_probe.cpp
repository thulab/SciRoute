// Functional integration only: a fresh private namespace in the existing DB.
// No scientific experiment case, service control, cleanup, or timing claim.
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/IoTDBProfile.h"
#include "iotdbfs/Ingestion.h"
#include "iotdbfs/MetadataSerialization.h"
#include "iotdbfs/PathUtils.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatQueryAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5QueryModel.h"
#include "iotdbfs/query/core/QueryModule.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/storage/versioning/IoTDBVersionStore.h"
#include "iotdbfs/storage/versioning/VersionNamespaceRecord.h"
#include <hdf5.h>
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>

using namespace iotdbfs;
using namespace iotdbfs::versioning;
namespace {
struct ProbeFailure : std::runtime_error { using std::runtime_error::runtime_error; };
void check(bool okay, const char* message) { if (!okay) throw ProbeFailure(message); }
std::string env(const char* name) {
  const auto* value = std::getenv(name); check(value && *value, "required environment missing"); return value;
}
std::string quoted(const std::string& value) {
  std::ostringstream out; out << '"'; const char* hex = "0123456789abcdef";
  for (unsigned char c : value) {
    if (c == '"' || c == '\\') out << '\\' << c;
    else if (c < 32) out << "\\u00" << hex[c >> 4] << hex[c & 15];
    else out << c;
  }
  out << '"'; return out.str();
}
struct H5 {
  hid_t id; herr_t (*close)(hid_t);
  H5(hid_t value, herr_t (*fn)(hid_t)) : id(value), close(fn) { check(id >= 0, "HDF5 fixture allocation failed"); }
  ~H5() { close(id); } operator hid_t() const { return id; }
};
void h5check(herr_t result) { check(result >= 0, "HDF5 fixture operation failed"); }
Bytes fixture(float base) {
  H5 access(H5Pcreate(H5P_FILE_ACCESS), H5Pclose); h5check(H5Pset_fapl_core(access, 4096, 0));
  H5 file(H5Fcreate("native-file-probe-memory", H5F_ACC_TRUNC, H5P_DEFAULT, access), H5Fclose);
  const hsize_t dimensions[]{4, 8}, chunks[]{2, 4};
  H5 shape(H5Screate_simple(2, dimensions, nullptr), H5Sclose);
  H5 properties(H5Pcreate(H5P_DATASET_CREATE), H5Pclose); h5check(H5Pset_chunk(properties, 2, chunks));
  h5check(H5Pset_fletcher32(properties));
  H5 dataset(H5Dcreate2(file, "/values", H5T_IEEE_F32LE, shape, H5P_DEFAULT, properties, H5P_DEFAULT), H5Dclose);
  float values[32]; for (int i = 0; i < 32; ++i) values[i] = base + i + .25f;
  h5check(H5Dwrite(dataset, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, values));
  H5 scalar(H5Screate(H5S_SCALAR), H5Sclose);
  for (const auto& item : std::vector<std::pair<std::string, std::string>>{{"shortName", "t"}, {"typeOfLevel", "isobaricInhPa"}}) {
    H5 type(H5Tcopy(H5T_C_S1), H5Tclose); h5check(H5Tset_size(type, item.second.size()));
    H5 attribute(H5Acreate2(dataset, item.first.c_str(), type, scalar, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    h5check(H5Awrite(attribute, type, item.second.data()));
  }
  for (const auto& item : std::vector<std::pair<std::string, std::int64_t>>{{"level", 850}, {"step", 0}, {"validityDate", 20260101}, {"validityTime", 0}}) {
    H5 attribute(H5Acreate2(dataset, item.first.c_str(), H5T_STD_I64LE, scalar, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    h5check(H5Awrite(attribute, H5T_NATIVE_INT64, &item.second));
  }
  h5check(H5Fflush(file, H5F_SCOPE_GLOBAL)); const auto size = H5Fget_file_image(file, nullptr, 0);
  check(size > 0, "HDF5 fixture image empty"); Bytes bytes(size);
  check(H5Fget_file_image(file, bytes.data(), bytes.size()) == size, "HDF5 fixture image differs"); return bytes;
}
Inode inode(const std::string& path, std::int64_t version, bool directory) {
  Inode value; value.path = path; value.parent = path_utils::parent_of(path); value.name = path_utils::base(path);
  value.row_time = version; value.is_dir = directory; value.mode = directory ? 0755 : 0640;
  value.uid = 1001; value.gid = 40077; value.nlink = 1; value.metadata = "{}";
  value.ctime = value.mtime = value.atime = 1700000000; return value;
}
// Independent read-only inspection of the committed directory tree. It never
// takes a second owner journal lock and never writes the head or any object.
Bytes object(IoTDBVersionStore& store, const Head& head, const std::string& type, const ContentId& id) {
  const auto protocol = "iotdbfs-" + type + "/1";
  auto value = store.get_object(head.identity, {type, protocol, id});
  check(content_id(type, protocol, value) == id, "inspection object hash differs"); return value;
}
std::optional<Bytes> record(IoTDBVersionStore& store, Table table, const std::string& key) {
  const auto bytes = store.read_head(); check(bool(bytes), "inspection head absent"); const auto head = decode_head(*bytes);
  auto page = decode_roots(object(store, head, "catalog-root", head.root)).at(static_cast<std::size_t>(table));
  for (unsigned depth = 0; !page.empty() && depth <= 8; ++depth) {
    const auto decoded = decode_page(object(store, head, "catalog-page", page));
    const auto found = std::lower_bound(decoded.entries.begin(), decoded.entries.end(), key,
      [](const PageEntry& entry, const std::string& value) { return entry.key < value; });
    if (found == decoded.entries.end()) return {};
    if (!decoded.level) return found->key == key ? std::optional<Bytes>(object(store, head, "catalog-record", found->value)) : std::nullopt;
    page = found->value;
  }
  throw ProbeFailure("inspection tree exceeds bound");
}
std::uint64_t count(const QueryProfileRecord& profile, const char* name) {
  const auto found = profile.counters.find(name); return found == profile.counters.end() ? 0 : found->second;
}
QueryRequest request(const StructuredBlobVersionKey& key) {
  QueryRequest value; value.sources.paths = {{PathScopeKind::Exact, key.path}};
  value.sources.versions = VersionSelection::exact_version(key.version);
  value.sources.formats = {AdapterSelectionKind::Listed, {"hdf5"}, false};
  QueryFragment fragment; const auto field = make_hdf5_value_field("/values"); check(field.ok(), "value field invalid");
  fragment.projection = {field.value()};
  fragment.slices = {{hdf5_axis_field(0), DomainBound{LogicalValue::uint64(1), BoundKind::Inclusive}, DomainBound{LogicalValue::uint64(3), BoundKind::Exclusive}},
    {hdf5_axis_field(1), DomainBound{LogicalValue::uint64(2), BoundKind::Inclusive}, DomainBound{LogicalValue::uint64(6), BoundKind::Exclusive}}};
  value.subset = std::move(fragment); value.limits.target_batch_rows = 3; return value;
}
struct Execution {
  bool okay{false}; std::uint64_t rows{0}, sql{0}, store_queries{0}, payload_reads{0}, payload_bytes{0}; QueryProfileRecord profile;
};
Execution query(const std::shared_ptr<IoTDBStructuredBlobBackend>& backend, const StructuredBlobVersionKey& key,
    float base, bool expect_success) {
  ScopedQueryMetadataBudget budget;
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto module = QueryModule::create(make_iotdb_published_version_catalog(backend), storage,
    {std::make_shared<Hdf5FormatQueryAdapter>(Hdf5QueryKernelConfig{}, nullptr, Hdf5SemanticConfig{},
      Hdf5LayoutCacheConfig{}, std::vector<std::shared_ptr<const Hdf5SemanticProfile>>{}, nullptr, Hdf5QueryMode::NativeHdf5)});
  check(module.ok(), "native query module creation failed"); const auto req = request(key);
  auto selected = module.value()->select(req); check(selected.ok() && bool(selected.value().read_context), "owner source selection failed");
  std::shared_ptr<QueryExecutionLease> lease;
  { auto admitted = selected.value().read_context->admit({1001, 40077, {}}); check(admitted.ok(), "owner planning admission failed"); lease = admitted.value()->execution_lease(); }
  ScopedQueryExecutionLease execution_lease(lease);
  auto prepared = module.value()->prepare(req, &selected.value());
  check(prepared.ok(), "native prepare failed before filesystem execution");
  { auto admitted = prepared.value().read_context()->admit({1001, 40077, {}}); check(admitted.ok(), "owner execution admission failed"); lease = admitted.value()->execution_lease(); }
  ScopedQueryExecutionLease actual_execution_lease(lease);
  Execution out; const auto before = iotdb_profile_snapshot();
  { ScopedQueryProfile profile(&out.profile);
    auto cursor = module.value()->execute(prepared.value());
    if (cursor.ok()) {
      out.okay = true;
      for (;;) {
        auto next = cursor.value()->next();
        if (!next.ok()) { out.okay = false; break; }
        if (!next.value()) break;
        const auto& batch = *next.value(); check(batch.columns().size() == 1, "native projection differs");
        check(batch.columns()[0].type.id == LogicalTypeId::Float32, "native dtype differs");
        const auto& values = std::get<std::vector<float>>(batch.columns()[0].values);
        check(values.size() == batch.row_count(), "native row storage differs");
        for (std::size_t i = 0; i < values.size(); ++i) {
          const auto ordinal = out.rows++; const auto y = 1 + ordinal / 4, x = 2 + ordinal % 4;
          check(ordinal < 8 && values[i] == base + y * 8 + x + .25f, "native ROI value oracle differs");
          const auto& mask = batch.columns()[0].validity;
          check(mask.empty() || ((mask.at(i / 8) >> (i % 8)) & 1), "native validity oracle differs");
        }
      }
    }
  }
  const auto after = iotdb_profile_snapshot(); out.sql = after.query_calls - before.query_calls;
  out.store_queries = after.version_store_query_calls - before.version_store_query_calls;
  out.payload_reads = after.payload_calls - before.payload_calls;
  out.payload_bytes = after.payload_returned_bytes - before.payload_returned_bytes;
  check(out.okay == expect_success, "native execution outcome differs");
  check(!out.payload_reads && !out.payload_bytes && !count(out.profile, "payload_map.block_reads"), "native query fell back to database payload");
  if (expect_success) check(out.rows == 8 && count(out.profile, "hdf5.native.filesystem.calls") > 0, "native filesystem oracle not executed");
  return out;
}
} // namespace

int main(int argc, char** argv) {
  set_logging_level(LogLevel::OFF); std::string stage = "arguments", prefix, journal, native_root;
  std::vector<std::string> cases; std::map<std::string, std::uint64_t> observations;
  try {
    for (int i = 1; i < argc; ++i) {
      const std::string arg = argv[i]; check(i + 1 < argc, "missing argument value");
      if (arg == "--prefix") prefix = argv[++i]; else if (arg == "--journal") journal = argv[++i];
      else if (arg == "--native-root") native_root = argv[++i]; else throw ProbeFailure("unknown argument");
    }
    check(prefix.rfind("vd_native_fs_", 0) == 0 && prefix.size() > 13 &&
      std::all_of(prefix.begin(), prefix.end(), [](unsigned char c) { return std::isalnum(c) || c == '_'; }), "fresh native fixture prefix required");
    check(!journal.empty() && journal.front() == '/' && !native_root.empty() && native_root.front() == '/' && journal != native_root,
      "distinct absolute fixture paths required");
    check(::access(journal.c_str(), F_OK) != 0 && ::access(native_root.c_str(), F_OK) != 0, "fixture paths exist; no resume");
    stage = "configuration"; IoTDBClientConfig config;
    config.host = env("IOTDB_HOST"); config.user = env("IOTDB_USER"); config.pass = env("IOTDB_PASS"); config.db = env("IOTDB_DB");
    const auto port = env("IOTDB_PORT"); std::size_t used = 0; config.port = std::stoi(port, &used);
    check(used == port.size() && config.port == 33667 && (config.host == "localhost" || config.host == "127.0.0.1"), "original loopback DB required");
    check(config.db == "h09probe1790939889dbfc1f", "existing experiment DB required"); config.host = "127.0.0.1"; config.auto_create_db = false;
    IoTDBClient existing_database(config, 1);
    IoTDBConnector::instance().initialize({config.host, config.port, config.user, config.pass, config.db}, 2);
    auto adapter = std::make_shared<Hdf5FormatAdapter>(); FormatRegistry formats; check(formats.register_adapter(adapter), "HDF5 registration failed");
    VersionDirectoryConfig settings; settings.table_prefix = prefix; settings.owner_directory = journal; settings.initialize = true;
    settings.artifact_validator = [adapter](const std::string& format, const Bytes& layout, std::uint64_t size) {
      if (format != "hdf5") return StructuredBlobStatus::failure(StructuredBlobError::InvalidArgument, "unexpected fixture format");
      const auto valid = adapter->validate_layout_artifact(layout, size);
      return valid.ok() ? StructuredBlobStatus::success() : StructuredBlobStatus::failure(StructuredBlobError::InvalidState, valid.error());
    };
    IoTDBVersionStore store(config, prefix);
    const StructuredBlobVersionKey a{"/data/a.h5", 7}, b{"/data/b.h5", 8};
    const auto bytes_a = fixture(0), bytes_b = fixture(100);
    auto publish = [&](const std::shared_ptr<IoTDBStructuredBlobBackend>& backend, const StructuredBlobVersionKey& key, const Bytes& bytes) {
      StructuredBlobStorage storage(backend); IngestionModule ingestion(storage, formats, {});
      FormatSource source{bytes.size(), [&bytes](std::uint64_t offset, std::uint64_t length) -> std::optional<Bytes> {
        if (offset > bytes.size() || length > bytes.size() - offset) return {};
        return Bytes(bytes.begin() + offset, bytes.begin() + offset + length);
      }};
      const auto result = ingestion.ingest(key, source, [&](const IngestionOutcome& outcome) {
        auto value = inode(key.path, key.version, false); value.size = outcome.payload_size; value.format_id = outcome.format_id.value_or("");
        const auto metadata = serialize_metadata(outcome.metadata, 65536); check(metadata.ok(), "fixture metadata encoding failed"); value.metadata = metadata.value();
        return backend->upsert_inode(value, false);
      }); check(result.ok(), "real HDF5 fixture publication failed");
    };
    stage = "publish_unbound";
    { auto backend = std::make_shared<IoTDBStructuredBlobBackend>(); check(backend->configure_version_directory(settings), "initial owner setup failed");
      check(backend->ensure_schema(), "fixture staging schema failed");
      for (const auto& path : {"/", "/data"}) check(backend->upsert_inode(inode(path, 1, true), true), "fixture parent publication failed");
      publish(backend, a, bytes_a);
      StructuredBlobStorage storage(backend); const auto handle = storage.open_published_version(a); check(handle.ok(), "unbound source open failed");
      check(!handle.value().open_native_file().ok(), "unbound source unexpectedly grants native file");
    }
    cases.push_back("ordinary_publication_without_native_root_has_no_filesystem_capability");
    const auto exact_before = record(store, Table::Exact, exact_key(a.path, a.version)); check(bool(exact_before), "published exact record absent");
    stage = "recover_and_supplement"; settings.initialize = false; settings.native_file_root = native_root;
    auto backend = std::make_shared<IoTDBStructuredBlobBackend>(); check(backend->configure_version_directory(settings), "native owner recovery failed");
    { StructuredBlobStorage storage(backend); auto old = storage.open_published_version(a); check(old.ok(), "old capability absent");
      check(!old.value().open_native_file().ok(), "old unbound capability unexpectedly native");
      const auto unbound = query(backend, a, 0, false);
      observations["unbound_execute_payload_calls"] = unbound.payload_reads;
      check(backend->bind_native_file(a).ok(), "exact supplement failed");
      check(!old.value().open_native_file().ok(), "old capability retroactively gained locator");
    }
    check(record(store, Table::Exact, exact_key(a.path, a.version)) == exact_before, "supplement changed scientific exact record");
    const auto bound_head = store.read_head(); check(bool(bound_head), "bound head absent");
    check(backend->bind_native_file(a).ok() && store.read_head() == bound_head, "supplement idempotence changed head");
    auto locator_bytes_a = record(store, Table::Current, native_file_location_key(a)); check(bool(locator_bytes_a), "supplement locator absent");
    const auto locator_a = decode_native_file_location(*locator_bytes_a);
    const auto exact_a = decode_namespace_record(*exact_before);
    check(locator_a.key == a && locator_a.descriptor_id == exact_a.descriptor_id && locator_a.seal_id == exact_a.seal_id, "locator exact identity differs");
    cases.push_back("supplement_preserves_exact_descriptor_seal_and_old_capability_with_idempotent_head");
    stage = "native_roi"; const auto cold = query(backend, a, 0, true); const auto warm = query(backend, a, 0, true);
    observations["cold_execute_sql"] = cold.sql; observations["cold_execute_store_queries"] = cold.store_queries;
    observations["warm_execute_sql"] = warm.sql; observations["warm_execute_store_queries"] = warm.store_queries;
    observations["cold_execute_payload_calls"] = cold.payload_reads; observations["warm_execute_payload_calls"] = warm.payload_reads;
    observations["cold_filesystem_calls"] = count(cold.profile, "hdf5.native.filesystem.calls");
    observations["warm_filesystem_calls"] = count(warm.profile, "hdf5.native.filesystem.calls");
    observations["roi_rows_per_execution"] = warm.rows;
    cases.push_back("official_native_hdf5_roi_float32_and_validity_oracle_zero_database_payload");
    stage = "auto_publication"; publish(backend, b, bytes_b);
    const auto locator_bytes_b = record(store, Table::Current, native_file_location_key(b)); check(bool(locator_bytes_b), "new publication omitted locator");
    const auto locator_b = decode_native_file_location(*locator_bytes_b); check(locator_a.relative_path != locator_b.relative_path, "fixture file contents not distinct");
    (void)query(backend, b, 100, true);
    { StructuredBlobStorage storage(backend); auto handle = storage.open_published_version(b); check(handle.ok(), "auto-bound version absent");
      auto bytes = handle.value().read_payload_range(0, bytes_b.size()); check(bytes.ok() && bytes.value() == bytes_b, "database payload changed after native publication");
    }
    cases.push_back("new_publication_atomically_auto_binds_complete_file_and_retains_database_payload");
    stage = "directory_predicate";
    {
      ScopedQueryMetadataBudget budget;
      DatasetCatalogRequest discovery; discovery.sources.paths = {{PathScopeKind::Subtree, "/data"}};
      discovery.sources.versions = VersionSelection::latest();
      discovery.sources.formats = {AdapterSelectionKind::Listed, {"hdf5"}, false};
      std::vector<QueryExpressionPtr> fields;
      for (const auto& value : std::vector<std::pair<std::string, std::string>>{{"shortName", "t"}, {"typeOfLevel", "isobaricInhPa"}})
        fields.push_back(make_comparison({"hdf5.attr:" + value.first}, ComparisonOperator::Equal, LogicalValue::utf8(value.second)));
      for (const auto& value : std::vector<std::pair<std::string, std::int64_t>>{{"level", 850}, {"step", 0}, {"validityDate", 20260101}, {"validityTime", 0}})
        fields.push_back(make_comparison({"hdf5.attr:" + value.first}, ComparisonOperator::Equal, LogicalValue::int64(value.second)));
      discovery.predicate = make_logical(LogicalOperator::And, std::move(fields));
      auto selected = backend->discover_datasets(discovery, [](const auto&, const CatalogSelection& candidates) {
        if (!candidates.read_context) return QueryStatus::failure(QueryError::Internal, QueryPhase::Catalog, "fixture missing owner context");
        auto admitted = candidates.read_context->admit({1001, 40077, {}});
        return admitted.ok() ? QueryStatus::success() : admitted.status();
      });
      check(selected.ok() && selected.value().datasets.size() == 2 && selected.value().selected_sources == 2,
        "database cross-file six-attribute selection differs");
      for (const auto& dataset : selected.value().datasets)
        check((dataset.source.path == a.path || dataset.source.path == b.path) && dataset.dataset_path == "/values", "directory result locator differs");
    }
    cases.push_back("database_cross_file_six_attribute_and_version_selection_remains_active");
    stage = "recovery_after_binding"; backend.reset(); backend = std::make_shared<IoTDBStructuredBlobBackend>();
    check(backend->configure_version_directory(settings), "bound owner recovery failed");
    check(record(store, Table::Current, native_file_location_key(a)) == locator_bytes_a && record(store, Table::Current, native_file_location_key(b)) == locator_bytes_b,
      "recovery changed immutable locator records");
    (void)query(backend, a, 0, true); (void)query(backend, b, 100, true);
    cases.push_back("bound_namespace_recovery_preserves_locators_and_native_values");
    stage = "missing_file_no_fallback";
    const auto path_b = native_root + "/" + locator_b.relative_path;
    std::filesystem::rename(path_b, path_b + ".missing-preserved");
    const auto missing = query(backend, b, 100, false); check(!backend->bind_native_file(b).ok(), "missing file was silently repaired");
    observations["missing_execute_payload_calls"] = missing.payload_reads;
    cases.push_back("missing_managed_file_fails_without_payload_fallback_or_implicit_repair");
    stage = "replaced_file_no_fallback";
    const auto path_a = native_root + "/" + locator_a.relative_path;
    std::filesystem::rename(path_a, path_a + ".original-preserved");
    { std::ofstream output(path_a, std::ios::binary); output.write(reinterpret_cast<const char*>(bytes_a.data()), bytes_a.size()); check(bool(output), "replacement fixture write failed"); }
    check(::chmod(path_a.c_str(), 0444) == 0, "replacement fixture permission failed");
    const auto replaced = query(backend, a, 0, false); check(!backend->bind_native_file(a).ok(), "replaced file was silently rebound");
    observations["replaced_execute_payload_calls"] = replaced.payload_reads;
    check(record(store, Table::Current, native_file_location_key(a)) == locator_bytes_a && record(store, Table::Exact, exact_key(a.path, a.version)) == exact_before,
      "failed filesystem validation retargeted locator or scientific identity");
    cases.push_back("same_bytes_replacement_inode_fails_without_retarget_or_payload_fallback");
    stage = "complete";
    std::cout << "{\"completed\":true,\"command\":\"native_file_functional\",\"prefix\":" << quoted(prefix)
      << ",\"journal\":" << quoted(journal) << ",\"native_root\":" << quoted(native_root)
      << ",\"actual_backend\":true,\"existing_database\":true,\"real_hdf5_fixture\":true,\"mode\":\"native-hdf5\",\"data_backend\":\"managed-filesystem\""
      << ",\"case_count\":" << cases.size() << ",\"cases\":[";
    for (std::size_t i = 0; i < cases.size(); ++i) std::cout << (i ? "," : "") << quoted(cases[i]);
    std::cout << "],\"observations\":{"; bool first = true;
    for (const auto& value : observations) { std::cout << (first ? "" : ",") << quoted(value.first) << ':' << value.second; first = false; }
    std::cout << "},\"oracle\":{\"source_shape\":[4,8],\"origin\":[1,2],\"shape\":[2,4],\"dtype\":\"float32\",\"all_valid\":true,\"value_formula\":\"base+row*8+column+0.25\"}"
      << ",\"recovery\":{\"unbound_namespace\":true,\"bound_namespace\":true,\"exact_record_preserved\":true,\"locators_preserved\":true}"
      << ",\"scientific_case\":false,\"performance_claim\":false,\"fixtures_preserved\":true,\"intentional_terminal_file_faults_preserved\":true,\"service_restarted\":false}\n";
    return 0;
  } catch (const ProbeFailure& error) {
    std::cout << "{\"completed\":false,\"stage\":" << quoted(stage) << ",\"completed_cases\":" << cases.size() << ",\"error\":" << quoted(error.what()) << "}\n"; return 1;
  } catch (const std::exception&) {
    std::cout << "{\"completed\":false,\"stage\":" << quoted(stage) << ",\"completed_cases\":" << cases.size()
      << ",\"error\":\"functional probe failed; private fixture evidence preserved; no automatic retry\"}\n"; return 1;
  }
}
