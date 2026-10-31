// H07 independent h5py oracle entry: production ingestion + QueryModule.
// Reuse the recording backend, canonical byte writer and source helper from H04.
#define main h04_probe_entry_point
#include "h04_query_probe.cpp"
#undef main
#include "iotdbfs/formats/hdf5/Hdf5SemanticBinding.h"
#include <iomanip>

int main(int argc, char** argv) { try {
  need(argc == 9, "usage: h07_query_probe FILE OUT DATASET LOWER UPPER ENABLED ROWS EXPECTED_ORIGIN");
  const std::filesystem::path output = argv[2]; std::filesystem::create_directories(output);
  const double lower = std::stod(argv[4]), upper = std::stod(argv[5]);
  const bool enabled = std::string(argv[6]) == "1";
  Fd file; file.value = ::open(argv[1], O_RDONLY | O_CLOEXEC); struct stat info{};
  need(file.value >= 0 && ::fstat(file.value, &info) == 0 && S_ISREG(info.st_mode) &&
    info.st_size >= 0 && info.st_size <= 64*1024*1024, "invalid fixture");
  auto backend = std::make_shared<Backend>();
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  FormatSource source{static_cast<std::uint64_t>(info.st_size), {}};
  source.read = [&](std::uint64_t offset, std::uint64_t count) -> std::optional<BinaryValue> {
    if (offset > source.size || count > source.size-offset) return std::nullopt;
    BinaryValue bytes(count); std::size_t done = 0;
    while (done < bytes.size()) {
      auto got = ::pread(file.value, bytes.data()+done, bytes.size()-done, offset+done);
      if (got < 0 && errno == EINTR) continue;
      if (got <= 0) return std::nullopt;
      done += got;
    }
    return bytes;
  };
  FormatRegistry formats;
  need(formats.register_adapter(std::make_shared<Hdf5FormatAdapter>()), "format registration");
  IngestionModule ingestion(*storage, formats, {65536});
  StructuredBlobVersionKey key{"/baseline/gwosc.h5", 17};
  const auto ingested = ingestion.ingest(key, source, [](const IngestionOutcome&) { return true; });
  need(ingested.ok(), ingested.message());
  Hdf5SemanticConfig semantics;
  if (enabled) semantics.gwosc_bindings.push_back({key, argv[3], storage->storage_namespace()});
  auto runtime = std::make_shared<QueryRuntime>();
  auto adapter = std::make_shared<Hdf5FormatQueryAdapter>(Hdf5QueryKernelConfig{}, runtime, semantics);
  auto module = QueryModule::create(make_in_memory_published_version_catalog({*backend->manifest}), storage, {adapter});
  need(module.ok(), module.status().message());
  auto value = make_hdf5_value_field(argv[3]); need(value.ok(), value.status().message());
  QueryRequest request; request.sources.paths = {{PathScopeKind::Exact, key.path}};
  request.sources.versions = VersionSelection::exact_version(key.version);
  request.sources.formats = {AdapterSelectionKind::Listed, {"hdf5"}, false};
  request.subset = QueryFragment{};
  request.subset->projection = {{kHdf5GpsSecondsField}, value.value()};
  request.subset->slices = {{{kHdf5GpsSecondsField},
    DomainBound{LogicalValue::float64(lower), BoundKind::Inclusive},
    DomainBound{LogicalValue::float64(upper), BoundKind::Exclusive}}};
  request.limits.target_batch_rows = std::stoull(argv[7]);
  backend->phase = "prepare";
  auto prepared = module.value()->prepare(request); auto status = prepared.status();
  BinaryValue values, coordinates; std::uint64_t count = 0;
  std::vector<std::pair<std::uint64_t, std::uint64_t>> slabs;
  if (prepared.ok()) {
    backend->phase = "execute";
    auto cursor = module.value()->execute(prepared.value()); status = cursor.status();
    if (cursor.ok()) for (;;) {
      auto batch = cursor.value()->next(); if (!batch.ok()) { status = batch.status(); break; }
      if (!batch.value()) break;
      const auto& b = *batch.value(); need(b.domain().has_value(), "missing ordinal domain");
      need(b.domain()->dimensions[0].field.field_id == "hdf5.axis.0", "GPS replaced ordinal domain");
      slabs.emplace_back(b.domain()->origin[0], b.row_count()); count += b.row_count();
      append_canonical(coordinates, std::get<std::vector<double>>(b.columns()[0].values));
      if (b.columns()[1].type.id == LogicalTypeId::Float32)
        append_canonical(values, std::get<std::vector<float>>(b.columns()[1].values));
      else append_canonical(values, std::get<std::vector<double>>(b.columns()[1].values));
    }
  }
  const auto calls = backend->calls.load();
  BinaryValue physical; QueryStatus physical_status = QueryStatus::success();
  if (status.ok()) {
    auto physical_request = request;
    physical_request.subset->projection = {value.value()};
    const auto origin = std::stoull(argv[8]);
    physical_request.subset->slices = {{hdf5_axis_field(0),
      DomainBound{LogicalValue::uint64(origin), BoundKind::Inclusive},
      DomainBound{LogicalValue::uint64(origin+count), BoundKind::Exclusive}}};
    backend->phase = "physical_prepare";
    auto pp = module.value()->prepare(physical_request); physical_status = pp.status();
    if (pp.ok()) {
      backend->phase = "physical_execute";
      auto cursor = module.value()->execute(pp.value()); physical_status = cursor.status();
      if (cursor.ok()) for (;;) {
        auto b = cursor.value()->next(); if (!b.ok()) { physical_status = b.status(); break; }
        if (!b.value()) break;
        const auto& c = b.value()->columns()[0];
        if (c.type.id == LogicalTypeId::Float32) append_canonical(physical, std::get<std::vector<float>>(c.values));
        else append_canonical(physical, std::get<std::vector<double>>(c.values));
      }
    }
  }
  save(output/"values.bin", values); save(output/"gps.bin", coordinates);
  save(output/"layout.bin", backend->layout);
  std::ofstream json(output/"query.json");
  json << "{\"schema_version\":\"iotdbfs-hdf5-h07-probe/1\",\"ok\":" << (status.ok()?"true":"false")
    << ",\"error\":" << q(error_name(status.code())) << ",\"message\":" << q(status.message())
    << ",\"rows\":" << count << ",\"physical_equal\":" << (physical_status.ok() && physical==values ? "true":"false")
    << ",\"physical_extra_payload_calls\":" << backend->calls-calls << ",\"slabs\":[";
  for (std::size_t i=0; i<slabs.size(); ++i) { if(i)json<<','; json<<'['<<slabs[i].first<<','<<slabs[i].second<<']'; }
  json << "],\"reads\":[";
  for (std::size_t i=0; i<backend->reads.size(); ++i) {
    if(i)json<<','; const auto& r=backend->reads[i];
    json<<"{\"phase\":"<<q(r.phase)<<",\"kind\":"<<q(r.kind)<<",\"offset\":"<<r.offset<<",\"length\":"<<r.length<<'}';
  }
  json << "]}\n"; json.close(); need(bool(json), "report write"); return 0;
} catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; } }
