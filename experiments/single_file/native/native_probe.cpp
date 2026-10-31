// Native filesystem baseline: reopen each query, or bind a dataset once and
// keep its native handles for repeated reads; select physical ordinals,
// H5Dread and copy into an owned value buffer. No sidecar, coordinate pre-read,
// dataset catalog, file inventory or scientific-value conversion is performed.
#include <hdf5.h>
#include <openssl/evp.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
constexpr std::uint64_t kMaxResultBytes = 64ULL * 1024 * 1024;
void require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}
std::uint64_t ns(Clock::time_point start, Clock::time_point end) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}
struct Id {
  hid_t id{-1};
  herr_t (*close_fn)(hid_t);
  Id(hid_t value, herr_t (*close)(hid_t)) : id(value), close_fn(close) {
    require(id >= 0, "native HDF5 operation failed");
  }
  ~Id() { if (id >= 0) close_fn(id); }
  Id(const Id&) = delete;
  operator hid_t() const { return id; }
  void close() { require(close_fn(id) >= 0, "native HDF5 close failed"); id = -1; }
};
std::uint64_t integer(const std::string& text) {
  require(!text.empty() && text.find_first_not_of("0123456789") == std::string::npos,
          "expected unsigned decimal integer");
  std::size_t used = 0;
  const auto result = std::stoull(text, &used);
  require(used == text.size(), "invalid integer");
  return result;
}
std::vector<hsize_t> dimensions(const std::string& text) {
  std::vector<hsize_t> result;
  std::size_t start = 0;
  do {
    const auto end = text.find(',', start);
    result.push_back(integer(text.substr(start, end == std::string::npos ? end : end - start)));
    require(result.size() <= 4, "rank exceeds four");
    if (end == std::string::npos) break;
    start = end + 1;
  } while (true);
  return result;
}
std::string json_dims(const std::vector<hsize_t>& values) {
  std::ostringstream out;
  out << '[';
  for (std::size_t i = 0; i < values.size(); ++i) { if (i) out << ','; out << values[i]; }
  return out.str() + ']';
}
std::string digest(const std::vector<std::uint8_t>& bytes) {
  unsigned char result[EVP_MAX_MD_SIZE];
  unsigned size = 0;
  require(EVP_Digest(bytes.data(), bytes.size(), result, &size, EVP_sha256(), nullptr) == 1,
          "SHA-256 failed");
  std::ostringstream out;
  out << std::hex << std::setfill('0');
  for (unsigned i = 0; i < size; ++i) out << std::setw(2) << unsigned(result[i]);
  return out.str();
}
void runtime() {
  for (const auto* key : {"HDF5_DRIVER", "HDF5_VOL_CONNECTOR"}) {
    const auto* value = std::getenv(key);
    require(!value || !*value, "HDF5 driver/VOL override forbidden");
  }
  require(H5PLset_loading_state(0) >= 0, "cannot disable HDF5 dynamic plugins");
  unsigned major = 0, minor = 0, release = 0;
  hbool_t threadsafe = false;
  require(H5get_libversion(&major, &minor, &release) >= 0 && major == 1 && minor == 14 && release == 4,
          "native baseline requires HDF5 1.14.4");
  require(H5is_library_threadsafe(&threadsafe) >= 0 && threadsafe,
          "native baseline requires thread-safe HDF5");
  H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
  const std::uint16_t endian = 1;
  require(*reinterpret_cast<const std::uint8_t*>(&endian) == 1,
          "canonical value digest requires a little-endian host");
}

struct Selection {
  std::vector<hsize_t> source_shape;
  hid_t memory_type{-1};
  std::size_t width{0};
  const char* dtype{nullptr};
  std::uint64_t count{1};
};
Selection describe(hid_t space, hid_t type, const std::vector<hsize_t>& origin,
                   const std::vector<hsize_t>& shape) {
  const auto rank = H5Sget_simple_extent_ndims(space);
  require(rank == 4, "baseline dataset must have rank four");
  Selection selection;
  selection.source_shape.resize(rank);
  require(H5Sget_simple_extent_dims(space, selection.source_shape.data(), nullptr) >= 0,
          "cannot get shape");
  if (H5Tequal(type, H5T_IEEE_F32LE) > 0 || H5Tequal(type, H5T_IEEE_F32BE) > 0) {
    selection.memory_type = H5T_NATIVE_FLOAT; selection.width = 4; selection.dtype = "<f4";
  } else if (H5Tequal(type, H5T_IEEE_F64LE) > 0 || H5Tequal(type, H5T_IEEE_F64BE) > 0) {
    selection.memory_type = H5T_NATIVE_DOUBLE; selection.width = 8; selection.dtype = "<f8";
  } else if (H5Tequal(type, H5T_STD_I16LE) > 0 || H5Tequal(type, H5T_STD_I16BE) > 0) {
    selection.memory_type = H5T_NATIVE_INT32; selection.width = 4; selection.dtype = "<i4";
  } else require(false, "unsupported physical dataset dtype");
  for (std::size_t i = 0; i < shape.size(); ++i) {
    require(shape[i] > 0 && origin[i] <= selection.source_shape[i] &&
            shape[i] <= selection.source_shape[i] - origin[i],
            "selection outside dataset shape or empty");
    require(selection.count <= kMaxResultBytes / selection.width / shape[i],
            "result exceeds 64 MiB");
    selection.count *= shape[i];
  }
  return selection;
}

std::string observation(std::uint64_t iteration, const char* mode,
                        const Selection& selection, const std::string& policy,
                        const std::vector<hsize_t>& origin, const std::vector<hsize_t>& shape,
                        const std::vector<std::uint8_t>& owned,
                        std::uint64_t open_file_ns, std::uint64_t open_dataset_ns,
                        std::uint64_t setup_ns, std::uint64_t read_ns,
                        std::uint64_t copy_ns, std::uint64_t close_ns,
                        std::uint64_t total_ns, std::uint64_t bind_ns,
                        std::uint64_t session_close_ns) {
  // Hashing, mask allocation and JSON formatting are outside all timers.
  const std::vector<std::uint8_t> validity(selection.count, 1);
  std::ostringstream out;
  out << "{\"protocol\":\"iotdbfs-single-file-native/1\",\"iteration\":" << iteration
      << ",\"mode\":\"" << mode << "\",\"runtime_hdf5\":\"1.14.4\",\"library_threadsafe\":true,\"dtype\":\""
      << selection.dtype << "\",\"cache_config\":" << policy
      << ",\"origin\":" << json_dims(origin) << ",\"shape\":" << json_dims(shape)
      << ",\"source_shape\":" << json_dims(selection.source_shape)
      << ",\"rows\":" << selection.count << ",\"order\":\"C\",\"null_count\":0,\"value_bytes\":" << owned.size()
      << ",\"open_file_ns\":" << open_file_ns << ",\"open_dataset_ns\":" << open_dataset_ns
      << ",\"selection_setup_ns\":" << setup_ns << ",\"read_ns\":" << read_ns
      << ",\"owned_copy_ns\":" << copy_ns << ",\"close_ns\":" << close_ns
      << ",\"total_ns\":" << total_ns << ",\"session_bind_ns\":" << bind_ns
      << ",\"session_close_ns\":" << session_close_ns
      << ",\"value_sha256\":\"" << digest(owned)
      << "\",\"mask_dtype\":\"uint8\",\"mask_sha256\":\"" << digest(validity) << "\"}";
  return out.str();
}
}

int main(int argc, char** argv) {
  try {
    runtime();
    // Record and explicitly use native default cache policies. Cache policy
    // introspection is setup outside all query timers, not a dataset pre-read.
    Id fapl(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    Id dapl(H5Pcreate(H5P_DATASET_ACCESS), H5Pclose);
    std::size_t slots = 0, raw_bytes = 0;
    double w0 = 0;
    require(H5Pget_chunk_cache(dapl, &slots, &raw_bytes, &w0) >= 0, "cannot get chunk cache policy");
    int ignored_mdc_elements = 0;
    std::size_t file_slots = 0, file_raw_bytes = 0;
    double file_w0 = 0;
    require(H5Pget_cache(fapl, &ignored_mdc_elements, &file_slots, &file_raw_bytes, &file_w0) >= 0,
            "cannot resolve inherited chunk cache policy");
    if (slots == H5D_CHUNK_CACHE_NSLOTS_DEFAULT) slots = file_slots;
    if (raw_bytes == H5D_CHUNK_CACHE_NBYTES_DEFAULT) raw_bytes = file_raw_bytes;
    if (w0 == H5D_CHUNK_CACHE_W0_DEFAULT) w0 = file_w0;
    require(H5Pset_chunk_cache(dapl, slots, raw_bytes, w0) >= 0, "cannot apply effective default cache policy");
    H5AC_cache_config_t mdc{};
    mdc.version = H5AC__CURR_CACHE_CONFIG_VERSION;
    require(H5Pget_mdc_config(fapl, &mdc) >= 0, "cannot get metadata cache policy");
    std::ostringstream policy;
    policy << "{\"raw_chunk_slots\":" << slots << ",\"raw_chunk_bytes\":" << raw_bytes
           << ",\"raw_chunk_w0\":" << std::setprecision(17) << w0
           << ",\"mdc_initial_bytes\":" << mdc.initial_size
           << ",\"mdc_min_bytes\":" << mdc.min_size << ",\"mdc_max_bytes\":" << mdc.max_size
           << ",\"mdc_policy\":\"native-default-adaptive\",\"library_policy\":\"native-default-fapl\"}";
    if (argc == 2 && std::string(argv[1]) == "--versions") {
      std::cout << "{\"runtime_hdf5\":\"1.14.4\",\"library_threadsafe\":true,"
                   "\"dynamic_plugins\":false,\"cache_config\":" << policy.str()
                << ",\"max_result_bytes\":" << kMaxResultBytes << "}\n";
      return 0;
    }
    require(argc == 6 || (argc == 7 && std::string(argv[6]) == "--bound"),
            "usage: native_probe FILE DATASET ORIGIN_CSV SHAPE_CSV REPEAT [--bound]");
    const auto origin = dimensions(argv[3]), shape = dimensions(argv[4]);
    require(origin.size() == 4 && shape.size() == 4, "baseline origin and shape must have rank four");
    const auto repeat = integer(argv[5]);
    require(repeat > 0 && repeat <= 10000, "repeat outside [1,10000]");
    if (argc == 7) {
      const auto bind_begin = Clock::now();
      Id file(H5Fopen(argv[1], H5F_ACC_RDONLY, fapl), H5Fclose);
      Id dataset(H5Dopen2(file, argv[2], dapl), H5Dclose);
      Id space(H5Dget_space(dataset), H5Sclose);
      Id type(H5Dget_type(dataset), H5Tclose);
      const auto selection = describe(space, type, origin, shape);
      const auto bind_ns = ns(bind_begin, Clock::now());
      for (std::uint64_t iteration = 0; iteration < repeat; ++iteration) {
        const auto begin = Clock::now();
        require(H5Sselect_hyperslab(space, H5S_SELECT_SET, origin.data(), nullptr, shape.data(), nullptr) >= 0,
                "cannot select physical hyperslab");
        Id memory_space(H5Screate_simple(4, shape.data(), nullptr), H5Sclose);
        std::vector<std::uint8_t> read_buffer(selection.count * selection.width);
        const auto ready = Clock::now();
        require(H5Dread(dataset, selection.memory_type, memory_space, space, H5P_DEFAULT, read_buffer.data()) >= 0,
                "native H5Dread failed");
        const auto read_done = Clock::now();
        std::vector<std::uint8_t> owned(read_buffer);
        const auto copied = Clock::now();
        memory_space.close();
        const auto end = Clock::now();
        std::uint64_t session_close_ns = 0;
        if (iteration + 1 == repeat) {
          // Close only native session handles here; no digest, mask allocation,
          // JSON output or per-query close work is part of this timer.
          const auto close_begin = Clock::now();
          type.close(); space.close(); dataset.close(); file.close();
          session_close_ns = ns(close_begin, Clock::now());
        }
        std::cout << observation(iteration, "bound", selection, policy.str(), origin, shape,
                                 owned, 0, 0, ns(begin, ready), ns(ready, read_done),
                                 ns(read_done, copied), ns(copied, end), ns(begin, end),
                                 bind_ns, session_close_ns) << '\n';
      }
      return 0;
    }
    for (std::uint64_t iteration = 0; iteration < repeat; ++iteration) {
      const auto begin = Clock::now();
      Id file(H5Fopen(argv[1], H5F_ACC_RDONLY, fapl), H5Fclose);
      const auto file_opened = Clock::now();
      Id dataset(H5Dopen2(file, argv[2], dapl), H5Dclose);
      const auto dataset_opened = Clock::now();
      Id space(H5Dget_space(dataset), H5Sclose);
      Id type(H5Dget_type(dataset), H5Tclose);
      const auto selection = describe(space, type, origin, shape);
      require(H5Sselect_hyperslab(space, H5S_SELECT_SET, origin.data(), nullptr, shape.data(), nullptr) >= 0,
              "cannot select physical hyperslab");
      Id memory_space(H5Screate_simple(4, shape.data(), nullptr), H5Sclose);
      std::vector<std::uint8_t> read_buffer(selection.count * selection.width);
      const auto ready = Clock::now();
      require(H5Dread(dataset, selection.memory_type, memory_space, space, H5P_DEFAULT, read_buffer.data()) >= 0,
              "native H5Dread failed");
      const auto read_done = Clock::now();
      std::vector<std::uint8_t> owned(read_buffer);
      const auto copied = Clock::now();
      memory_space.close(); type.close(); space.close(); dataset.close(); file.close();
      const auto end = Clock::now();
      std::cout << observation(iteration, "reopen", selection, policy.str(), origin, shape,
                               owned, ns(begin, file_opened), ns(file_opened, dataset_opened),
                               ns(dataset_opened, ready), ns(ready, read_done),
                               ns(read_done, copied), ns(copied, end), ns(begin, end), 0, 0) << '\n';
    }
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "native_probe: " << error.what() << '\n';
    return 2;
  }
}
