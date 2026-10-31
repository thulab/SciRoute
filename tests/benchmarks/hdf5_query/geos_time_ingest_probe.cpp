#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace iotdbfs;

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cerr << "usage: geos_time_ingest_probe FILE...\n";
    return 2;
  }
  try {
    for (int arg = 1; arg < argc; ++arg) {
      const int fd = ::open(argv[arg], O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
      if (fd < 0) throw std::runtime_error("cannot open source");
      struct Close { int fd; ~Close() { ::close(fd); } } closer{fd};
      struct stat st {};
      if (::fstat(fd, &st) || !S_ISREG(st.st_mode) || st.st_size <= 0)
        throw std::runtime_error("source must be a nonempty regular file");
      const auto size = static_cast<std::uint64_t>(st.st_size);
      FormatSource source{size, [fd, size](std::uint64_t offset,
          std::uint64_t length) -> std::optional<BinaryValue> {
        if (offset > size || length > size - offset ||
            length > static_cast<std::uint64_t>(std::numeric_limits<ssize_t>::max()))
          return std::nullopt;
        BinaryValue bytes(static_cast<std::size_t>(length));
        const auto n = ::pread(fd, bytes.data(), bytes.size(),
                               static_cast<off_t>(offset));
        if (n < 0 || static_cast<std::uint64_t>(n) != length)
          return std::nullopt;
        return bytes;
      }};
      Hdf5FormatAdapter adapter;
      if (!adapter.matches(source))
        throw std::runtime_error("source is not HDF5");
      auto session = adapter.begin_ingestion(source);
      std::vector<std::uint8_t> chunk(1 << 20);
      for (std::uint64_t offset = 0; offset < size;) {
        const auto count = static_cast<std::size_t>(
            std::min<std::uint64_t>(chunk.size(), size - offset));
        if (::pread(fd, chunk.data(), count, static_cast<off_t>(offset)) !=
            static_cast<ssize_t>(count))
          throw std::runtime_error("source streaming read failed");
        auto consumed = session->consume_chunk(offset, chunk.data(), count);
        if (!consumed.ok()) throw std::runtime_error(consumed.error());
        offset += count;
      }
      auto artifacts = session->finish();
      if (!artifacts.ok()) throw std::runtime_error(artifacts.error());
      const auto &meta = artifacts.value().metadata;
      const auto second = meta.find("__iotdbfs.time_coverage.utc_second");
      const auto digest = meta.find("__iotdbfs.time_coverage.layout_digest");
      const auto path = meta.find("__iotdbfs.time_coverage.dataset_path");
      if (second == meta.end() || digest == meta.end() || path == meta.end() ||
          second->second.type() != MetadataValueType::Int64 ||
          digest->second.type() != MetadataValueType::String ||
          path->second.type() != MetadataValueType::String ||
          !artifacts.value().dataset_catalog ||
          std::get<std::string>(digest->second.value()) !=
              artifacts.value().dataset_catalog->layout_digest)
        throw std::runtime_error("GEOS time coverage is absent or unbound");
      std::cout << "{\"source_index\":" << arg << ",\"utc_second\":"
                << std::get<std::int64_t>(second->second.value())
                << ",\"dataset_path\":\""
                << std::get<std::string>(path->second.value())
                << "\",\"layout_digest\":\""
                << std::get<std::string>(digest->second.value()) << "\"}\n";
    }
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
