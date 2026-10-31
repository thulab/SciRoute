// Read-only live verification of SQL catalog pushdown against the original
// whole-namespace, in-memory selection. No schema creation or fixture writes.
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/IoTDBPublishedCatalogSql.h"
#include "iotdbfs/MetadataConfig.h"
#include "iotdbfs/PathUtils.h"
#include "iotdbfs/query/core/QueryPlanning.h"
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdexcept>

using namespace iotdbfs;
namespace {
std::string env(const char* key, const char* fallback = "") {
  const auto* value = std::getenv(key);
  return value ? value : fallback;
}
void need(bool valid, const std::string& why) {
  if (!valid) throw std::runtime_error(why);
}
void equivalent(const CatalogSelection& a, const CatalogSelection& b) {
  need(a.versions().size() == b.versions().size(), "selection size differs");
  for (std::size_t i = 0; i < a.versions().size(); ++i) {
    const auto& x = a.versions()[i];
    const auto& y = b.versions()[i];
    auto xm = serialize_metadata(x.metadata, 65536);
    auto ym = serialize_metadata(y.metadata, 65536);
    need(xm.ok() && ym.ok(), "cannot compare metadata");
    need(x.key == y.key && x.format_id == y.format_id &&
      x.payload_size == y.payload_size && x.layout_size == y.layout_size &&
      x.revision == y.revision && x.phase == y.phase &&
      x.published == y.published && xm.value() == ym.value(),
      "selected manifest differs");
  }
}
} // namespace

int main(int argc, char** argv) {
  try {
    IoTDBConfig config{env("IOTDB_HOST", "127.0.0.1"),
      std::stoi(env("IOTDB_PORT", "6667")), env("IOTDB_USER"),
      env("IOTDB_PASS"), env("IOTDB_DB"), 0};
    need(config.db == "h09probe1790939889dbfc1f", "unexpected experiment database");
    IoTDBConnector::instance().initialize(config, 2);
    if (argc == 2 && std::string(argv[1]) == "--explain") {
      const std::vector<std::string> parents{"/", "/geos-pilot-9856a092a812"};
      for (const auto* format : {"grib", "hdf5"}) {
        SourceSelector exact;
        exact.paths = {{PathScopeKind::Exact, std::string("/geos-pilot-9856a092a812/") +
          (std::string(format) == "grib" ? "grib-control.grib2" : "file1.nc4")}};
        exact.versions = std::string(format) == "hdf5"
          ? VersionSelection::exact_version(1791271058856LL) : VersionSelection::latest();
        auto source = compile_published_catalog_sql(exact);
        auto combined = compile_published_catalog_sql(exact, parents);
        auto bulk = compile_inode_permission_sql_batches(parents);
        need(source.ok() && combined.ok() && bulk.ok(), "cannot compile explain statements");
        const std::string marker = " WHERE published=true AND ";
        const auto predicate = source.value().find(marker) + marker.size();
        const auto end = source.value().rfind(" ORDER BY time DESC");
        const auto unpruned = source.value().substr(0, predicate) + "((" +
          source.value().substr(predicate, end - predicate) + ") OR path IN (" +
          inode_permission_path_list(parents) + ")) ORDER BY time DESC";
        for (const auto& query : std::vector<std::pair<std::string, std::string>>{
            {"source", source.value()}, {"unpruned_or", unpruned},
            {"pruned_or", combined.value()}, {"parents_bulk", bulk.value().at(0)}}) {
          std::cout << "\nEXPLAIN " << format << '/' << query.first << '\n' << query.second << '\n';
          auto plan = IoTDBConnector::instance().executeQuery("EXPLAIN " + query.second);
          need(static_cast<bool>(plan), "EXPLAIN failed");
          while (plan->hasNext()) {
            const auto record = plan->next();
            for (const auto& field : record->fields)
              if (field.stringV) std::cout << field.stringV.value() << '\n';
          }
        }
      }
      return 0;
    }
    need(argc == 1, "unknown probe option");
    auto backend = std::make_shared<IoTDBStructuredBlobBackend>();
    auto inventory = backend->load_published_manifests();
    need(inventory.ok(), inventory.status().message());
    const auto limit = inventory.value().size() + 1;
    auto reference = make_in_memory_published_version_catalog(inventory.value());
    auto filtered = make_iotdb_published_version_catalog(backend);
    std::cout << "{\"namespace_published_versions\":" << inventory.value().size()
      << ",\"cases\":[";
    bool first = true;
    auto check = [&](const char* name, const SourceSelector& selector) {
      auto expected = reference->scan(selector, limit);
      const auto start = std::chrono::steady_clock::now();
      auto narrowed = backend->load_published_manifests(selector);
      auto actual = filtered->scan(selector, limit);
      const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - start).count();
      need(expected.ok(), expected.status().message());
      need(narrowed.ok(), narrowed.status().message());
      need(actual.ok(), actual.status().message());
      equivalent(expected.value(), actual.value());
      const auto& facts = actual.value().source_permissions;
      need(facts && facts->storage_namespace == backend->storage_namespace() &&
        facts->metadata_generation == backend->metadata_generation(),
        "missing or unbound catalog permission snapshot");
      for (const auto& manifest : actual.value().versions()) {
        const auto fact = facts->sources.find(manifest.key);
        need(fact != facts->sources.end(), "missing experiment source permissions");
        need(fact->second.manifest_revision == manifest.revision &&
          fact->second.payload_size == manifest.payload_size &&
          fact->second.layout_size == manifest.layout_size &&
          fact->second.format_id == manifest.format_id.value_or(""),
          "permission evidence does not match selected content");
        if (std::string(name) == "grib_exact" || std::string(name) == "hdf5_exact") {
          auto inode = backend->load_inode(manifest.key.path, manifest.key.version);
          need(inode && inode->mode == fact->second.mode &&
            inode->uid == fact->second.uid &&
            inode->gid == fact->second.gid &&
            inode->is_dir == fact->second.is_dir,
            "catalog permission fields differ from direct source read");
          for (auto parent = path_utils::parent_of(manifest.key.path);;
               parent = path_utils::parent_of(parent)) {
            const auto observed = facts->parents.find(parent);
            auto latest = backend->load_inode(parent);
            need(observed != facts->parents.end() && observed->second && latest &&
              observed->second->version == latest->row_time &&
              observed->second->mode == latest->mode &&
              observed->second->uid == latest->uid &&
              observed->second->gid == latest->gid &&
              observed->second->is_dir == latest->is_dir,
              "combined catalog parent permissions differ from latest inode");
            if (parent == "/") break;
          }
        }
      }
      if (!first) std::cout << ',';
      first = false;
      std::cout << "{\"name\":\"" << name << "\",\"sql_rows\":"
        << narrowed.value().size() << ",\"selected\":"
        << actual.value().versions().size() << ",\"permission_rows\":"
        << facts->sources.size() << ",\"parent_rows\":" << facts->parents.size()
        << ",\"two_scans_ns\":" << ns << '}';
    };
    SourceSelector selector;
    selector.versions = VersionSelection::all();
    check("all", selector);
    selector.versions = VersionSelection::latest();
    check("latest", selector);
    selector.paths = {{PathScopeKind::Subtree, "/geos-pilot-9856a092a812"}};
    check("subtree_latest", selector);
    selector.versions = VersionSelection::all();
    check("subtree_all", selector);
    selector.paths = {{PathScopeKind::Subtree, "/"}};
    check("root_subtree", selector);
    selector.paths = {{PathScopeKind::Subtree, "/nonexistent_%'\\catalog"}};
    check("escaped_subtree", selector);
    selector.paths = {{PathScopeKind::Exact, "/nonexistent_%'\\catalog"}};
    check("escaped_exact", selector);
    selector.paths = {};
    selector.formats = {AdapterSelectionKind::Listed, {"grib", "hdf5"}, false};
    check("listed_formats", selector);
    selector.formats = {AdapterSelectionKind::Listed, {}, true};
    check("passthrough_only", selector);
    selector.formats.include_byte_passthrough = false;
    check("no_formats", selector);
    selector.formats = {AdapterSelectionKind::All, {}, false};
    check("all_without_passthrough", selector);
    for (const auto* format : {"grib", "hdf5"}) {
      const auto found = std::find_if(inventory.value().begin(), inventory.value().end(),
        [format](const auto& m) { return m.format_id == format; });
      need(found != inventory.value().end(), "missing format source");
      selector.formats = {AdapterSelectionKind::Listed, {format}, false};
      selector.paths = {{PathScopeKind::Exact, found->key.path}};
      selector.versions = VersionSelection::exact_version(found->key.version);
      check(format == std::string("grib") ? "grib_exact" : "hdf5_exact", selector);
      selector.versions = VersionSelection::between(found->key.version, found->key.version + 1);
      check(format == std::string("grib") ? "grib_between" : "hdf5_between", selector);
      selector.versions = VersionSelection::latest();
      check(format == std::string("grib") ? "grib_latest" : "hdf5_latest", selector);
    }
    selector.paths = {{PathScopeKind::Exact, "/geos-pilot-9856a092a812/file1.nc4"},
      {PathScopeKind::Exact, "/geos-pilot-9856a092a812/grib-control.grib2"}};
    selector.formats = {};
    check("multiple_paths", selector);
    std::cout << "],\"ok\":true}\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
