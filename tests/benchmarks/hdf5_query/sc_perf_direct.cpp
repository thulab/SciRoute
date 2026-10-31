// Matched OISST scientific-value benchmark for the production HDF5 reader.
// B uses local pread and D uses the exact published IoTDB source. The
// HDF5/CF query, owned output, hashing, and counters are identical.
// Reuse the H11 bounded benchmark backend and its transport accounting.
#define main h11_query_probe_unused_main
#include "h11_query_probe.cpp"
#undef main

#include <cmath>
#include <iomanip>

namespace {
struct ScCase {
  std::string id, source, dataset;
  std::optional<std::pair<double, double>> latitude, longitude;
};

std::optional<std::pair<double, double>> bounds(const Json &n,
                                                const char *key) {
  auto child = n.get_child_optional(key);
  if (!child)
    return {};
  std::vector<double> v;
  for (const auto &part : *child) {
    need(part.first.empty(), std::string(key) + " must be an array");
    v.push_back(part.second.get_value<double>());
  }
  need(v.size() == 2 && std::isfinite(v[0]) && std::isfinite(v[1]) &&
           v[0] < v[1],
       std::string("invalid ") + key + " bounds");
  return std::pair{v[0], v[1]};
}

void fnv_byte(std::uint64_t &state, std::uint8_t byte) {
  state ^= byte;
  state *= 1099511628211ULL;
}
void fnv_value(std::uint64_t &state, bool valid, double value) {
  fnv_byte(state, valid ? 1 : 0);
  std::uint64_t bits = 0;
  if (valid)
    std::memcpy(&bits, &value, sizeof(bits));
  for (unsigned i = 0; i < 8; ++i)
    fnv_byte(state, static_cast<std::uint8_t>(bits >> (i * 8)));
}
std::string fnv_hex(std::uint64_t value) {
  std::ostringstream s;
  s << std::hex << std::setfill('0') << std::setw(16) << value;
  return s.str();
}

QueryRequest scientific_request(const StructuredBlobManifest &manifest,
                                const ScCase &c, std::uint64_t rows) {
  QueryRequest r;
  r.sources.paths = {{PathScopeKind::Exact, manifest.key.path}};
  r.sources.versions = VersionSelection::exact_version(manifest.key.version);
  r.sources.formats = {AdapterSelectionKind::Listed, {"hdf5"}, false};
  r.subset = QueryFragment{};
  auto field = make_hdf5_science_value_field(c.dataset);
  need(field.ok(), field.status().message());
  r.subset->projection = {field.value()};
  auto add = [&](const char *name, const std::pair<double, double> &v) {
    r.subset->slices.push_back(
        {{name},
         DomainBound{LogicalValue::float64(v.first), BoundKind::Inclusive},
         DomainBound{LogicalValue::float64(v.second), BoundKind::Exclusive}});
  };
  if (c.latitude)
    add(kHdf5LatitudeField, *c.latitude);
  if (c.longitude)
    add(kHdf5LongitudeField, *c.longitude);
  r.limits.target_batch_rows = rows;
  return r;
}

struct ScObservation {
  std::uint64_t rows{0}, masked{0}, value_bytes{0};
  std::uint64_t hash{14695981039346656037ULL};
  std::map<std::string, std::uint64_t> timings;
  QueryProfileRecord profile;
};

ScObservation scientific_query(const QueryModule &module,
                               const StructuredBlobManifest &manifest,
                               const ScCase &c, std::uint64_t rows) {
  ScObservation o;
  ScopedQueryProfile scope(&o.profile);
  const auto total = Clock::now();
  auto t = Clock::now();
  auto prepared = module.prepare(scientific_request(manifest, c, rows));
  o.timings["prepare"] = ns(t);
  need(prepared.ok(), prepared.status().message());
  t = Clock::now();
  auto cursor = module.execute(prepared.value());
  o.timings["execute_setup"] = ns(t);
  need(cursor.ok(), cursor.status().message());
  o.timings["cursor_next"] = 0;
  o.timings["owned_copy"] = 0;
  std::vector<double> owned;
  std::vector<std::uint8_t> owned_valid;
  for (;;) {
    t = Clock::now();
    auto next = cursor.value()->next();
    o.timings["cursor_next"] += ns(t);
    need(next.ok(), next.status().message());
    if (!next.value())
      break;
    need(next.value()->columns().size() == 1,
         "scientific query must have one value column");
    const auto &column = next.value()->columns()[0];
    need(column.type.id == LogicalTypeId::Float64,
         "scientific output is not Float64");
    const auto &values = std::get<std::vector<double>>(column.values);
    need(values.size() == next.value()->row_count(),
         "value length differs from batch rows");
    t = Clock::now();
    for (std::size_t i = 0; i < values.size(); ++i) {
      const bool valid = column.validity.empty() ||
                         bool(column.validity[i / 8] & (1U << (i % 8)));
      owned.push_back(valid ? values[i] : 0.0);
      owned_valid.push_back(valid);
    }
    o.timings["owned_copy"] += ns(t);
  }
  o.rows = owned.size();
  o.value_bytes = owned.size() * sizeof(double) + owned_valid.size();
  o.masked = std::count(owned_valid.begin(), owned_valid.end(), 0);
  // Hashing is deliberately outside the timed query, as in the H11 oracle.
  o.timings["total"] = ns(total);
  for (std::size_t i = 0; i < owned.size(); ++i)
    fnv_value(o.hash, owned_valid[i], owned[i]);
  return o;
}

std::map<std::string, std::uint64_t> backend_delta(const Counts &a,
                                                   const Counts &b) {
  return {{"manifest_calls", b.manifests - a.manifests},
          {"payload_calls", b.payload_calls - a.payload_calls},
          {"payload_ranges", b.payload_ranges - a.payload_ranges},
          {"payload_bytes", b.payload_bytes - a.payload_bytes},
          {"layout_calls", b.layout_calls - a.layout_calls},
          {"layout_bytes", b.layout_bytes - a.layout_bytes}};
}
} // namespace

int main(int argc, char **argv) {
  try {
    need(argc == 3, "usage: sc_perf_direct RUN.json OUTPUT.jsonl");
    Json input;
    boost::property_tree::read_json(argv[1], input);
    need(input.get<std::string>("protocol") ==
             "iotdbfs-hdf5-sc-perf-run/1",
         "wrong run protocol");
    const auto path = input.get<std::string>("path");
    need(path == "B" || path == "D", "path must be B or D");
    const auto passes = get_uint(input, "passes", 4);
    const auto budget = get_uint(input, "cache_bytes", max_bytes);
    const auto parallel = get_uint(input, "parallelism", 1);
    const auto rows = get_uint(input, "target_batch_rows", 65536);
    need(passes && passes <= 32 && budget >= 65536 &&
             budget <= 1024ULL * 1024 * 1024 &&
             (parallel == 1 || parallel == 2 || parallel == 4) && rows &&
             rows <= 8 * 1024 * 1024,
         "invalid benchmark bounds");
    std::vector<Source> sources;
    std::set<std::string> ids;
    for (const auto &p : input.get_child("sources")) {
      need(p.first.empty(), "sources must be array");
      Source s;
      s.id = p.second.get<std::string>("id");
      s.local = p.second.get<std::string>("local_path", "");
      s.key = {p.second.get<std::string>("logical_path"),
               p.second.get<std::int64_t>("version")};
      need(ids.insert(s.id).second, "duplicate source id");
      sources.push_back(std::move(s));
    }
    need(!sources.empty() && sources.size() <= 64, "source count exceeds cap");
    std::vector<ScCase> cases;
    for (const auto &p : input.get_child("cases")) {
      need(p.first.empty(), "cases must be array");
      ScCase c;
      c.id = p.second.get<std::string>("id");
      c.source = p.second.get<std::string>("source");
      c.dataset = p.second.get<std::string>("dataset");
      c.latitude = bounds(p.second, "latitude");
      c.longitude = bounds(p.second, "longitude");
      need(ids.count(c.source) && !c.id.empty() && !c.dataset.empty(),
           "invalid case");
      cases.push_back(std::move(c));
    }
    need(!cases.empty() && cases.size() <= 256, "case count exceeds cap");
    need(!std::getenv("HDF5_DRIVER") && !std::getenv("HDF5_VOL_CONNECTOR"),
         "HDF5 driver/VOL overrides are forbidden");
    need(H5PLset_loading_state(0) >= 0, "cannot disable HDF5 plugins");

    const auto setup_start = Clock::now();
    std::shared_ptr<IoTDBStructuredBlobBackend> remote;
    if (path == "D") {
      initialize_remote(input.get<std::string>("config"),
                        std::max<std::uint64_t>(4, parallel));
      remote = std::make_shared<IoTDBStructuredBlobBackend>();
    }
    auto backend = std::make_shared<Backend>(remote, path == "D", 0);
    auto storage = std::make_shared<StructuredBlobStorage>(backend);
    std::uint64_t index_build_ns = 0, index_read_calls = 0,
                  index_read_bytes = 0;
    std::vector<StructuredBlobManifest> catalog;
    std::map<std::string, StructuredBlobManifest> manifests;
    for (std::size_t i = 0; i < sources.size(); ++i) {
      auto &s = sources[i];
      if (path == "B") {
        s.layout = std::string(argv[2]) + ".layout-" + std::to_string(i);
        backend->add(s);
        backend->sidecars[s.key] = s.layout;
        Fd fd(s.local);
        FormatSource raw{size(fd.value), {}};
        raw.read = [&](std::uint64_t o,
                       std::uint64_t n) -> std::optional<BinaryValue> {
          if (o > raw.size || n > raw.size - o)
            return std::nullopt;
          ++index_read_calls;
          index_read_bytes += n;
          return pread_bytes(fd.value, o, n);
        };
        FormatRegistry formats;
        need(formats.register_adapter(std::make_shared<Hdf5FormatAdapter>()),
             "format adapter registration failed");
        IngestionModule ingestion(*storage, formats, {65536});
        auto t = Clock::now();
        auto outcome = ingestion.ingest(
            s.key, raw, [](const IngestionOutcome &) { return true; });
        index_build_ns += ns(t);
        need(outcome.ok(), outcome.message());
        backend->seal_layout(s);
      }
      auto result = backend->load_manifest(s.key);
      need(result.ok(), result.status().message());
      need(result.value().published &&
               result.value().phase == StructuredBlobPhase::Published &&
               result.value().format_id == std::optional<std::string>{"hdf5"},
           "source is not published HDF5");
      manifests[s.id] = result.value();
      catalog.push_back(result.value());
    }
    QueryRuntimeConfig rc;
    rc.memory_budget_bytes = budget;
    rc.max_entries = 65536;
    rc.max_inflight = 256;
    rc.worker_threads = 4;
    rc.wait_timeout_ms = 30000;
    rc.enable_detailed_profile = true;
    auto runtime = std::make_shared<QueryRuntime>(rc);
    Hdf5QueryKernelConfig hc;
    hc.read_parallelism = parallel;
    hc.max_batch_fragments = 32;
    auto created = QueryModule::create(
        make_in_memory_published_version_catalog(catalog), storage,
        {std::make_shared<Hdf5FormatQueryAdapter>(hc, runtime)});
    need(created.ok(), created.status().message());
    const auto &module = *created.value();
    const auto setup_ns = ns(setup_start);

    std::ofstream output(argv[2]);
    need(bool(output), "cannot create output JSONL");
    output << "{\"kind\":\"setup\",\"protocol\":\"iotdbfs-hdf5-sc-perf-result/1\",\"path\":"
           << q(path) << ",\"wall_ns\":" << setup_ns
           << ",\"index_build_ns\":" << index_build_ns
           << ",\"index_read_calls\":" << index_read_calls
           << ",\"index_read_bytes\":" << index_read_bytes
           << ",\"cache_bytes\":" << budget
           << ",\"parallelism\":" << parallel << "}\n";
    for (std::uint64_t pass = 0; pass < passes; ++pass) {
      for (const auto &c : cases) {
        const auto before = backend->counts();
        const auto runtime_before = runtime->stats();
        const auto result = scientific_query(module, manifests.at(c.source),
                                             c, rows);
        const auto runtime_after = runtime->stats();
        const auto after = backend->counts();
        output << "{\"kind\":\"query\",\"protocol\":\"iotdbfs-hdf5-sc-perf-result/1\",\"path\":"
               << q(path) << ",\"pass\":" << pass << ",\"case_id\":"
               << q(c.id) << ",\"source_id\":" << q(c.source)
               << ",\"dataset\":" << q(c.dataset)
               << ",\"rows\":" << result.rows << ",\"masked\":"
               << result.masked << ",\"value_bytes\":" << result.value_bytes
               << ",\"fnv1a64\":" << q(fnv_hex(result.hash))
               << ",\"latency_ns\":" << result.timings.at("total")
               << ",\"timings_ns\":";
        object(output, result.timings);
        output << ",\"stages_ns\":";
        object(output, result.profile.nanoseconds);
        output << ",\"counters\":";
        object(output, result.profile.counters);
        output << ",\"runtime_delta\":";
        object(output, delta(runtime_before, runtime_after));
        output << ",\"backend_delta\":";
        object(output, backend_delta(before, after));
        output << "}\n";
      }
    }
    need(bool(output), "failed to save benchmark output");
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
