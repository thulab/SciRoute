// Native HDF5 side of the matched OISST scientific-value benchmark.
//
// The file and dataset remain open between passes, as in a reusable reader.
// Opening and reading the coordinate vectors are separately reported setup.
// Each timed pass resolves its coordinate predicate to a physical slab and
// materializes an owned Float64 value vector and byte mask.
#include <hdf5.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/resource.h>
#include <sys/stat.h>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
using Dims = std::array<hsize_t, 4>;

void require(bool condition, const std::string &message) {
  if (!condition) throw std::runtime_error(message);
}
struct Id {
  hid_t value{-1};
  herr_t (*close)(hid_t){nullptr};
  Id(hid_t id, herr_t (*closer)(hid_t)) : value(id), close(closer) {
    require(id >= 0, "HDF5 open failed");
  }
  Id(const Id &) = delete;
  Id &operator=(const Id &) = delete;
  ~Id() { if (value >= 0) close(value); }
  operator hid_t() const { return value; }
};
std::uint64_t elapsed(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count();
}
std::uint64_t clock_ns(Clock::time_point instant) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(instant.time_since_epoch()).count();
}
std::string json_string(const std::string &value) {
  std::string result{"\""};
  const char *hex = "0123456789abcdef";
  for (unsigned char c : value) {
    if (c == '\"' || c == '\\') { result += '\\'; result += c; }
    else if (c < 0x20) { result += "\\u00"; result += hex[c >> 4]; result += hex[c & 15]; }
    else result += c;
  }
  return result + '"';
}
std::string dimensions(const Dims &dims) {
  std::ostringstream out;
  out << '[';
  for (std::size_t i = 0; i < dims.size(); ++i) {
    if (i) out << ',';
    out << dims[i];
  }
  return out.str() + ']';
}
std::uint64_t elements(const Dims &dims) {
  std::uint64_t count = 1;
  for (auto dimension : dims) {
    require(dimension <= 10000000 && count <= 10000000 / std::max<hsize_t>(1, dimension),
            "selection exceeds 10 million elements");
    count *= dimension;
  }
  return count;
}
struct Packing {
  double scale{1}, offset{0};
  std::vector<double> missing;
  std::optional<double> minimum, maximum;
};
std::vector<double> attribute(hid_t dataset, const char *name) {
  const auto present = H5Aexists(dataset, name);
  require(present >= 0, std::string("cannot probe packing attribute: ") + name);
  if (!present) return {};
  Id a(H5Aopen(dataset, name, H5P_DEFAULT), H5Aclose);
  Id space(H5Aget_space(a), H5Sclose);
  const auto count = H5Sget_simple_extent_npoints(space);
  require(count >= 1 && count <= 2, std::string("unexpected packing attribute size: ") + name);
  std::vector<double> result(static_cast<std::size_t>(count));
  require(H5Aread(a, H5T_NATIVE_DOUBLE, result.data()) >= 0,
          std::string("cannot read packing attribute: ") + name);
  return result;
}
Packing packing(hid_t dataset) {
  Packing result;
  if (auto a = attribute(dataset, "scale_factor"); !a.empty()) {
    require(a.size() == 1, "scale_factor must be scalar"); result.scale = a[0];
  }
  if (auto a = attribute(dataset, "add_offset"); !a.empty()) {
    require(a.size() == 1, "add_offset must be scalar"); result.offset = a[0];
  }
  for (const auto *name : {"_FillValue", "missing_value"}) {
    auto a = attribute(dataset, name);
    result.missing.insert(result.missing.end(), a.begin(), a.end());
  }
  const auto range = attribute(dataset, "valid_range");
  const auto min = attribute(dataset, "valid_min"), max = attribute(dataset, "valid_max");
  require(range.empty() || (range.size() == 2 && min.empty() && max.empty()),
          "conflicting valid_range and valid_min/max");
  if (!range.empty()) { result.minimum = range[0]; result.maximum = range[1]; }
  else {
    if (!min.empty()) { require(min.size() == 1, "invalid valid_min"); result.minimum = min[0]; }
    if (!max.empty()) { require(max.size() == 1, "invalid valid_max"); result.maximum = max[0]; }
  }
  return result;
}
std::vector<double> coordinate(hid_t file, const char *name) {
  Id dataset(H5Dopen2(file, name, H5P_DEFAULT), H5Dclose);
  Id space(H5Dget_space(dataset), H5Sclose);
  require(H5Sget_simple_extent_ndims(space) == 1, "coordinate must be one-dimensional");
  hsize_t length = 0;
  H5Sget_simple_extent_dims(space, &length, nullptr);
  require(length > 0 && length <= 10000000, "coordinate exceeds bound");
  std::vector<double> result(length);
  require(H5Dread(dataset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL,
                  H5P_DEFAULT, result.data()) >= 0, "coordinate H5Dread failed");
  return result;
}
std::pair<hsize_t, hsize_t> coordinate_span(const std::vector<double> &axis,
                                            double lower, double upper,
                                            bool allow_empty = false) {
  hsize_t first = axis.size(), last = 0;
  for (hsize_t i = 0; i < axis.size(); ++i) {
    if (axis[i] >= lower && axis[i] < upper) {
      first = std::min(first, i); last = std::max(last, i + 1);
    }
  }
  if (first == axis.size()) {
    require(allow_empty, "coordinate range does not intersect the axis");
    return {0, 0};
  }
  for (hsize_t i = first; i < last; ++i)
    require(axis[i] >= lower && axis[i] < upper, "coordinate selection is discontiguous");
  return {first, last - first};
}
void mix(std::uint64_t &digest, std::uint8_t byte) {
  digest ^= byte;
  digest *= 1099511628211ULL;
}
std::string digest_hex(std::uint64_t digest) {
  std::ostringstream out;
  out << std::hex << std::setfill('0') << std::setw(16) << digest;
  return out.str();
}
struct Cpu {
  std::uint64_t user_ns{0}, system_ns{0}, minor_faults{0}, major_faults{0};
};
Cpu cpu() {
  rusage r{};
  require(getrusage(RUSAGE_SELF, &r) == 0, "getrusage failed");
  return {std::uint64_t(r.ru_utime.tv_sec) * 1000000000ULL + std::uint64_t(r.ru_utime.tv_usec) * 1000ULL,
          std::uint64_t(r.ru_stime.tv_sec) * 1000000000ULL + std::uint64_t(r.ru_stime.tv_usec) * 1000ULL,
          std::uint64_t(r.ru_minflt), std::uint64_t(r.ru_majflt)};
}
} // namespace

int main(int argc, char **argv) {
  try {
    require(argc == 5, "usage: sc_perf_native FILE DATASET {point|roi|full|negative|all} REPEAT");
    const std::string path = argv[1], dataset_path = argv[2], requested_scenario = argv[3];
    require(requested_scenario == "point" || requested_scenario == "roi" ||
            requested_scenario == "full" || requested_scenario == "negative" ||
            requested_scenario == "all", "unknown scenario");
    const std::string repeat_text = argv[4];
    require(!repeat_text.empty() && repeat_text.find_first_not_of("0123456789") == std::string::npos,
            "invalid repeat count");
    const auto repeats = std::stoull(repeat_text);
    require(repeats > 0 && repeats <= 1000, "repeat count out of range");
    require(!std::getenv("HDF5_DRIVER") && !std::getenv("HDF5_VOL_CONNECTOR"),
            "HDF5 driver/VOL override forbidden");
    require(H5PLset_loading_state(0) >= 0, "cannot disable dynamic HDF5 plugins");
    struct stat st{};
    require(::stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode) &&
            st.st_size > 0 && st.st_size <= 64 * 1024 * 1024,
            "file outside 64 MiB bound");
    const auto setup_start = Clock::now();
    Id file(H5Fopen(path.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose);
    Id dapl(H5Pcreate(H5P_DATASET_ACCESS), H5Pclose);
    require(H5Pset_chunk_cache(dapl, 10007, 64ULL * 1024 * 1024, 0.75) >= 0,
            "cannot configure HDF5 chunk cache");
    Id dataset(H5Dopen2(file, dataset_path.c_str(), dapl), H5Dclose);
    Id space(H5Dget_space(dataset), H5Sclose);
    require(H5Sget_simple_extent_ndims(space) == 4, "OISST dataset must be rank 4");
    Dims source_shape{};
    require(H5Sget_simple_extent_dims(space, source_shape.data(), nullptr) >= 0,
            "cannot get OISST shape");
    require(source_shape[1] == 1, "OISST depth dimension must be singleton");
    Id datatype(H5Dget_type(dataset), H5Tclose);
    require(H5Tget_class(datatype) == H5T_INTEGER && H5Tget_size(datatype) == 2,
            "OISST packed values must be Int16");
    const auto stored_bytes = H5Dget_storage_size(dataset);
    Id layout(H5Dget_create_plist(dataset), H5Pclose);
    Dims chunk_shape{};
    if (H5Pget_layout(layout) == H5D_CHUNKED)
      require(H5Pget_chunk(layout, 4, chunk_shape.data()) == 4,
              "cannot read chunk shape");
    const auto cf = packing(dataset);
    std::vector<double> latitude, longitude;
    if (requested_scenario != "full") {
      latitude = coordinate(file, "/lat");
      longitude = coordinate(file, "/lon");
    }
    const auto setup_ns = elapsed(setup_start);
    const std::vector<std::string> scenarios = requested_scenario == "all"
      ? std::vector<std::string>{"point", "roi", "full", "negative"}
      : std::vector<std::string>{requested_scenario};
    for (std::uint64_t pass = 0; pass < repeats; ++pass) {
      for (const auto &scenario : scenarios) {
        const auto started = Clock::now();
        const auto usage_start = cpu();
        Dims origin{0, 0, 0, 0}, shape = source_shape;
        std::uint64_t coordinate_resolve_ns = 0;
        if (scenario == "point" || scenario == "roi" || scenario == "negative") {
          const auto coordinate_start = Clock::now();
          if (scenario == "negative") {
            const auto empty = coordinate_span(latitude, 91.0, 92.0, true);
            require(empty.second == 0, "negative latitude unexpectedly matched");
            shape = {0, 0, 0, 0};
          } else {
            auto y = coordinate_span(latitude, scenario == "point" ? 0.0 : -1.0,
                                     scenario == "point" ? 0.25 : 1.0);
            auto x = coordinate_span(longitude, 180.0,
                                     scenario == "point" ? 180.25 : 182.0);
            if (scenario == "point")
              require(y.second == 1 && x.second == 1, "point selection is not one cell");
            origin[2] = y.first; origin[3] = x.first;
            shape[2] = y.second; shape[3] = x.second;
          }
          coordinate_resolve_ns = elapsed(coordinate_start);
        }
        const auto row_count = scenario == "negative" ? 0 : elements(shape);
        std::uint64_t hyperslab_ns = 0, read_ns = 0, transform_ns = 0;
        std::uint64_t masked = 0, digest = 14695981039346656037ULL;
        std::vector<double> values;
        std::vector<std::uint8_t> validity;
        if (row_count) {
          const auto stage = Clock::now();
          Id file_space(H5Dget_space(dataset), H5Sclose);
          require(H5Sselect_hyperslab(file_space, H5S_SELECT_SET, origin.data(),
                                      nullptr, shape.data(), nullptr) >= 0,
                  "native hyperslab selection failed");
          Id memory_space(H5Screate_simple(4, shape.data(), nullptr), H5Sclose);
          std::vector<std::int16_t> raw(row_count);
          hyperslab_ns = elapsed(stage);
          const auto read_start = Clock::now();
          require(H5Dread(dataset, H5T_NATIVE_SHORT, memory_space, file_space,
                          H5P_DEFAULT, raw.data()) >= 0, "native H5Dread failed");
          read_ns = elapsed(read_start);
          const auto transform_start = Clock::now();
          values.assign(row_count, 0);
          validity.assign(row_count, 1);
          for (std::uint64_t i = 0; i < row_count; ++i) {
            const double value = raw[i];
            bool valid = std::isfinite(value) &&
              (!cf.minimum || value >= *cf.minimum) &&
              (!cf.maximum || value <= *cf.maximum);
            for (const auto missing : cf.missing)
              if (value == missing || (std::isnan(value) && std::isnan(missing))) valid = false;
            if (valid) {
              volatile double scaled = value * cf.scale;
              values[i] = scaled + cf.offset;
              require(std::isfinite(values[i]), "scientific transform overflow");
            } else { validity[i] = 0; ++masked; }
          }
          transform_ns = elapsed(transform_start);
        }
        const auto ended = Clock::now();
        const auto usage_end = cpu();
        // Hashing is outside the timing, as for the H11 baseline. Invalid
        // values have canonical zero bits, independent of a reader's fill.
        for (std::uint64_t i = 0; i < row_count; ++i) {
          mix(digest, validity[i]);
          std::uint64_t bits = 0;
          if (validity[i]) std::memcpy(&bits, &values[i], sizeof(bits));
          for (unsigned byte = 0; byte < 8; ++byte)
            mix(digest, std::uint8_t((bits >> (8 * byte)) & 0xff));
        }
        std::cout << "{\"protocol\":\"iotdbfs-hdf5-sc-native/1\",\"source\":"
                  << json_string(path) << ",\"dataset\":" << json_string(dataset_path)
                  << ",\"scenario\":" << json_string(scenario)
                  << ",\"pass\":" << pass << ",\"setup_ns\":" << setup_ns
                  << ",\"coordinate_resolve_ns\":" << coordinate_resolve_ns
                  << ",\"steady_start_ns\":" << clock_ns(started)
                  << ",\"steady_end_ns\":" << clock_ns(ended)
                  << ",\"total_ns\":" << std::chrono::duration_cast<std::chrono::nanoseconds>(ended-started).count()
                  << ",\"hyperslab_ns\":" << hyperslab_ns
                  << ",\"h5dread_ns\":" << read_ns
                  << ",\"transform_ns\":" << transform_ns
                  << ",\"cpu_user_ns\":" << usage_end.user_ns - usage_start.user_ns
                  << ",\"cpu_system_ns\":" << usage_end.system_ns - usage_start.system_ns
                  << ",\"minor_faults\":" << usage_end.minor_faults - usage_start.minor_faults
                  << ",\"major_faults\":" << usage_end.major_faults - usage_start.major_faults
                  << ",\"source_bytes\":" << st.st_size
                  << ",\"dataset_storage_bytes\":" << stored_bytes
                  << ",\"chunk_shape\":" << dimensions(chunk_shape)
                  << ",\"logical_raw_bytes\":" << row_count * sizeof(std::int16_t)
                  << ",\"owned_value_bytes\":" << row_count * (sizeof(double)+1)
                  << ",\"origin\":" << dimensions(origin)
                  << ",\"shape\":" << dimensions(shape)
                  << ",\"rows\":" << row_count << ",\"masked\":" << masked
                  << ",\"digest_fnv64\":" << json_string(digest_hex(digest)) << "}\n";
      }
    }
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
  return 0;
}
