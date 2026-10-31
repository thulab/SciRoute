// Scalar assembly microprobe through the real published ReadHandle.
// The reader is memory-backed: this measures allocation/copy/assembly, not SQL.
#include "iotdbfs/StructuredBlobStorage.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

using namespace iotdbfs;
using Clock = std::chrono::steady_clock;
using Ns = std::chrono::nanoseconds;

struct Reader final : StructuredBlobPayloadReader {
  StructuredBlobBytes source;
  mutable const std::uint8_t* last_buffer{};
  mutable std::uint64_t calls{}, supplied{};
  explicit Reader(std::size_t size) : source(size) {
    for (std::size_t i = 0; i < size; ++i) source[i] = (i * 17 + i / 1048576) % 251;
  }
  StructuredBlobResult<StructuredBlobPayloadBatch> read_payload_ranges(
      const StructuredBlobPayloadRanges& ranges) const override {
    if (ranges.size() != 1) throw std::runtime_error("expected one scalar slice");
    const auto r = ranges.front();
    if (r.offset > source.size() || r.length > source.size() - r.offset)
      throw std::runtime_error("source bounds");
    StructuredBlobPayloadBatch out;
    out.emplace_back(source.begin() + r.offset, source.begin() + r.offset + r.length);
    last_buffer = out[0].data(); ++calls; supplied += r.length;
    return StructuredBlobResult<StructuredBlobPayloadBatch>::success(std::move(out));
  }
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(
      std::uint64_t, std::uint64_t) const override {
    return StructuredBlobResult<StructuredBlobBytes>::failure(StructuredBlobError::NotFound, "no layout");
  }
  StructuredBlobResult<std::optional<std::string>> verified_layout_identity() const override {
    return StructuredBlobResult<std::optional<std::string>>::success(std::nullopt);
  }
};

struct Backend final : StructuredBlobStorageBackend {
  std::shared_ptr<Reader> reader;
  explicit Backend(std::shared_ptr<Reader> r) : reader(std::move(r)) {}
  StructuredBlobResult<std::shared_ptr<const StructuredBlobPayloadReader>>
  bind_published_payload(const StructuredBlobManifest&) const override {
    return StructuredBlobResult<std::shared_ptr<const StructuredBlobPayloadReader>>::success(reader);
  }
  StructuredBlobStatus create_manifest(const StructuredBlobManifest&) override { throw std::runtime_error("unexpected write"); }
  StructuredBlobResult<StructuredBlobManifest> load_manifest(const StructuredBlobVersionKey&) const override { throw std::runtime_error("unexpected lookup"); }
  StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey&,std::uint64_t,const StructuredBlobManifest&) override { throw std::runtime_error("unexpected write"); }
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey&,std::uint64_t,const StructuredBlobBytes&) override { throw std::runtime_error("unexpected write"); }
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey&,const StructuredBlobBytes&) override { throw std::runtime_error("unexpected write"); }
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey&,const MetadataMap&) override { throw std::runtime_error("unexpected write"); }
  StructuredBlobResult<StructuredBlobBytes> read_payload_range(const StructuredBlobVersionKey&,std::uint64_t,std::uint64_t) const override { throw std::runtime_error("unexpected unbound read"); }
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(const StructuredBlobVersionKey&,std::uint64_t,std::uint64_t) const override { throw std::runtime_error("unexpected layout read"); }
  StructuredBlobStatus erase(const StructuredBlobVersionKey&,std::uint64_t) override { throw std::runtime_error("unexpected write"); }
};

int main(int argc, char** argv) {
  try {
    if (argc != 2) throw std::runtime_error("supply OLD or NEW label");
    const std::string label = argv[1];
    if (label != "OLD" && label != "NEW") throw std::runtime_error("unknown label");
    auto reader = std::make_shared<Reader>(16ULL * 1048576 + 2);
    auto backend = std::make_shared<Backend>(reader);
    StructuredBlobStorage storage(backend);
    StructuredBlobManifest manifest;
    manifest.key = {"/scalar-microprobe", 1};
    manifest.phase = StructuredBlobPhase::Published; manifest.published = true;
    manifest.payload_size = reader->source.size();
    auto opened = storage.open_catalog_version(manifest.key, manifest, storage.storage_namespace());
    if (!opened.ok()) throw std::runtime_error(opened.status().message());
    struct Case { std::uint64_t bytes; int repeats; };
    const std::vector<Case> cases{{8371,2048},{33484,1024},{1048576,256},
      {8ULL*1048576,64},{16ULL*1048576,32}};
    for (auto c : cases) {
      std::vector<std::uint64_t> samples;
      std::uint64_t forwarded = 0, calls = 0, supplied = 0;
      for (int i = -16; i < c.repeats; ++i) {
        const auto before_calls = reader->calls, before_bytes = reader->supplied;
        const auto t0 = Clock::now();
        auto result = opened.value().read_payload_range(1, c.bytes);
        const auto t1 = Clock::now();
        if (!result.ok() || result.value().size() != c.bytes ||
            std::memcmp(result.value().data(), reader->source.data()+1, c.bytes) != 0)
          throw std::runtime_error("incorrect output");
        const bool same = result.value().data() == reader->last_buffer;
        if (c.bytes <= kMaxPayloadBytesPerBatch && same != (label == "NEW"))
          throw std::runtime_error("single-slice ownership transfer differs");
        if (i >= 0) {
          samples.push_back(std::chrono::duration_cast<Ns>(t1-t0).count());
          forwarded += same; calls += reader->calls-before_calls;
          supplied += reader->supplied-before_bytes;
        }
      }
      std::sort(samples.begin(), samples.end());
      std::uint64_t sum = 0; for (auto t : samples) sum += t;
      std::cout << "{\"arm\":\"" << label << "\",\"length\":" << c.bytes
        << ",\"offset\":1,\"iterations\":" << c.repeats
        << ",\"elapsed_sum_ns\":" << sum << ",\"median_ns\":" << samples[samples.size()/2]
        << ",\"p95_ns\":" << samples[samples.size()*95/100]
        << ",\"forwarded_buffers\":" << forwarded << ",\"reader_calls\":" << calls
        << ",\"reader_supplied_bytes\":" << supplied << ",\"all_outputs_verified\":true}\n";
    }
  } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
