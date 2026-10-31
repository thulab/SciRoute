#include "iotdbfs/formats/hdf5/Hdf5SourceDriver.h"
#include "iotdbfs/formats/hdf5/Hdf5ScaleWorker.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/core/QueryTaskScheduler.h"
#include "iotdbfs/storage/NativeFileStore.h"
#include <cmath>
#include <hdf5.h>

#include <H5FDdevelop.h>
#include <H5VLnative.h>
#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <limits>
#include <map>
#include <mutex>
#include <new>
#include <set>
#include <stdexcept>
#include <utility>

namespace iotdbfs {
namespace {
std::recursive_mutex library_mutex;
std::unique_lock<std::recursive_mutex> native_library_lock() {
  QueryProfileTimer timer("hdf5.native.lock_wait");
  return std::unique_lock<std::recursive_mutex>(library_mutex);
}
QueryStatus failure(QueryError code, const std::string &message) {
  return QueryStatus::failure(code, QueryPhase::Storage, message);
}
QueryStatus native_file_failure(const StructuredBlobStatus &status) {
  const auto code = status.code() == StructuredBlobError::InvalidState
      ? QueryError::CorruptPayload
      : status.code() == StructuredBlobError::NotFound
          ? QueryError::SourceNotFound : QueryError::ReadFailure;
  return failure(code, status.message());
}
struct Failure {
  QueryStatus status;
};
struct ErrorScope {
  H5E_auto2_t handler{};
  void *client{};
  hid_t saved{-1};
  ErrorScope() {
    H5Eget_auto2(H5E_DEFAULT, &handler, &client);
    saved = H5Eget_current_stack();
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
  }
  ~ErrorScope() {
    H5Eclear2(H5E_DEFAULT);
    if (saved >= 0)
      H5Eset_current_stack(saved);
    H5Eset_auto2(H5E_DEFAULT, handler, client);
  }
};
void environment_policy() {
  // Inspect these BEFORE the first library API: H5open consults both variables.
  for (const char *key : {"HDF5_DRIVER", "HDF5_VOL_CONNECTOR"}) {
    const char *value = std::getenv(key);
    if (value && *value)
      throw Failure{
          failure(QueryError::UnsupportedCapability,
                  "HDF5 driver/VOL environment overrides are forbidden")};
  }
}
void policy() {
  // Process-owned safety policy: filtered heap metadata can request a plugin
  // before any dataset-level filter whitelist is available.
  if (H5PLset_loading_state(0) < 0)
    throw Failure{
        failure(QueryError::Internal, "cannot disable HDF5 dynamic plugins")};
  unsigned major = 0, minor = 0, release = 0;
  hbool_t threadsafe = false;
  if (H5get_libversion(&major, &minor, &release) < 0 || major != 1 ||
      minor != 14 || release != 4 || H5is_library_threadsafe(&threadsafe) < 0 ||
      !threadsafe)
    throw Failure{
        failure(QueryError::UnsupportedCapability,
                "source driver requires thread-safe serial HDF5 1.14.4")};
}
struct Id {
  hid_t value{-1};
  herr_t (*closer)(hid_t){};
  Id(hid_t v, herr_t (*c)(hid_t)) : value(v), closer(c) {}
  ~Id() {
    if (value >= 0 && closer)
      closer(value);
  }
  operator hid_t() const { return value; }
  Id(const Id &) = delete;
};
struct File;
struct State {
  std::uint64_t size{0};
  Hdf5SourceOptions options;
  std::function<QueryResult<StructuredBlobBytes>(std::uint64_t, std::uint64_t)>
      reader;
  Hdf5SourceTrace trace;
  struct LogicalFee {
    std::uint64_t offset{0}, length{0};
    std::uint32_t count{0}, remaining{0};
  };
  // Sorted, bounded logical cold demand for one exact adapter selection.
  std::vector<LogicalFee> fees;
  std::uint64_t logical_bytes{0}, logical_calls{0};
  std::function<QueryStatus(std::uint64_t)> logical_reserver;
  QueryStatus error;
  QueryError fallback{QueryError::None};
  File *active{nullptr};
  std::shared_ptr<const NativeFileHandle> local_handle;
  void set_error(QueryError code, const char *message) noexcept {
    if (fallback == QueryError::None)
      fallback = code;
    try {
      if (error.ok())
        error = failure(code, message);
    } catch (...) {
    }
  }
  bool cancelled() noexcept {
    try {
      if (options.cancelled && options.cancelled()) {
        set_error(QueryError::Cancelled, "HDF5 source access cancelled");
        return true;
      }
    } catch (...) {
      set_error(QueryError::ReadFailure, "HDF5 cancellation callback threw");
      return true;
    }
    return false;
  }
};
struct Fapl {
  std::shared_ptr<State> state;
};
struct File {
  H5FD_t base{};
  std::shared_ptr<State> state;
  haddr_t eoa{0};
};
File *as_file(H5FD_t *p) { return reinterpret_cast<File *>(p); }
const File *as_file(const H5FD_t *p) {
  return reinterpret_cast<const File *>(p);
}
void *copy_fapl(const void *p) noexcept {
  try {
    return new Fapl(*static_cast<const Fapl *>(p));
  } catch (...) {
    return nullptr;
  }
}
herr_t free_fapl(void *p) noexcept {
  try {
    delete static_cast<Fapl *>(p);
    return 0;
  } catch (...) {
    return -1;
  }
}
void *get_fapl(H5FD_t *p) noexcept {
  try {
    return new Fapl{as_file(p)->state};
  } catch (...) {
    return nullptr;
  }
}
H5FD_t *open_file(const char *name, unsigned flags, hid_t fapl,
                  haddr_t) noexcept {
  try {
    auto info = static_cast<const Fapl *>(H5Pget_driver_info(fapl));
    if (!info || !info->state || flags != H5F_ACC_RDONLY)
      return nullptr;
    if (!name || std::strcmp(name, "iotdbfs-source") != 0 ||
        info->state->active) {
      info->state->set_error(QueryError::UnsupportedCapability,
                             "HDF5 secondary file opens are forbidden");
      return nullptr;
    }
    if (info->state->cancelled())
      return nullptr;
    auto file = new File;
    file->state = info->state;
    file->state->active = file;
    return &file->base;
  } catch (...) {
    return nullptr;
  }
}
herr_t close_file(H5FD_t *p) noexcept {
  try {
    auto file = as_file(p);
    if (file->state->active == file)
      file->state->active = nullptr;
    delete file;
    return 0;
  } catch (...) {
    return -1;
  }
}
int compare_files(const H5FD_t *a, const H5FD_t *b) noexcept {
  auto x = as_file(a)->state.get(), y = as_file(b)->state.get();
  if (x == y)
    return 0;
  return std::less<const State *>{}(x, y) ? -1 : 1;
}
herr_t query_file(const H5FD_t *, unsigned long *flags) noexcept {
  if (flags)
    *flags = 0;
  return 0;
}
haddr_t get_eoa(const H5FD_t *p, H5FD_mem_t) noexcept {
  return as_file(p)->eoa;
}
haddr_t get_eof(const H5FD_t *p, H5FD_mem_t) noexcept {
  return as_file(p)->state->size;
}
herr_t set_eoa(H5FD_t *p, H5FD_mem_t, haddr_t address) noexcept {
  auto file = as_file(p);
  if (address == HADDR_UNDEF || address > file->state->size) {
    file->state->set_error(QueryError::ReadFailure,
                           "HDF5 EOA exceeds source EOF");
    return -1;
  }
  file->eoa = address;
  return 0;
}
herr_t read_file(H5FD_t *p, H5FD_mem_t kind, hid_t, haddr_t address,
                 size_t size, void *destination) noexcept {
  auto file = as_file(p);
  auto &s = *file->state;
  try {
    if (s.cancelled())
      return -1;
    if (address == HADDR_UNDEF || address > s.size || size > s.size - address ||
        (size && (address > file->eoa || size > file->eoa - address))) {
      s.set_error(QueryError::ReadFailure, "HDF5 source read exceeds EOF/EOA");
      return -1;
    }
    if (!size)
      return 0;
    if (!destination) {
      s.set_error(QueryError::ReadFailure, "HDF5 read destination is null");
      return -1;
    }
    const auto charged = s.trace.metadata_bytes + s.trace.raw_data_bytes;
    if (size > s.options.max_single_read_bytes ||
        s.trace.reads.size() >= s.options.max_read_calls ||
        charged > s.options.max_read_bytes ||
        size > s.options.max_read_bytes - charged) {
      s.set_error(QueryError::ResourceExhausted,
                  "HDF5 source read budget exhausted");
      return -1;
    }
    const auto fee = std::lower_bound(s.fees.begin(), s.fees.end(), std::make_pair(address, size),
      [](const State::LogicalFee &entry, const auto &range) {
        return std::make_pair(entry.offset, entry.length) < range;
      });
    const bool prepaid = fee != s.fees.end() && fee->offset == address &&
      fee->length == size && fee->remaining;
    if (prepaid) --fee->remaining;
    else {
      if (s.logical_calls >= s.options.max_read_calls ||
          s.logical_bytes > s.options.max_read_bytes || size > s.options.max_read_bytes - s.logical_bytes) {
        s.set_error(QueryError::ResourceExhausted, "HDF5 logical source budget exhausted");
        return -1;
      }
      if (s.logical_reserver) {
        auto charge = s.logical_reserver(size);
        if (!charge.ok()) { s.error = std::move(charge); return -1; }
      }
      ++s.logical_calls; s.logical_bytes += size;
      query_profile_count("hdf5.native.logical.calls");
      query_profile_count("hdf5.native.logical.bytes", size);
    }
    const bool raw = kind == H5FD_MEM_DRAW;
    const auto index = s.trace.reads.size();
    s.trace.reads.push_back({address, size, raw, false, !prepaid});
    if (raw)
      s.trace.raw_data_bytes += size;
    else
      s.trace.metadata_bytes += size;
    query_profile_count("hdf5.native.source.calls");
    query_profile_count("hdf5.native.source.bytes", size);
    auto bytes = [&] {
      QueryProfileTimer timer("hdf5.native.source.read");
      return s.reader(address, size);
    }();
    if (!bytes.ok()) {
      if (s.error.ok())
        s.error = bytes.status();
      return -1;
    }
    if (bytes.value().size() != size) {
      s.set_error(QueryError::ReadFailure, "HDF5 source returned a short read");
      return -1;
    }
    if (s.cancelled())
      return -1;
    std::memcpy(destination, bytes.value().data(), size);
    s.trace.reads[index].success = true;
    return 0;
  } catch (const std::bad_alloc &) {
    s.set_error(QueryError::ResourceExhausted, "HDF5 read allocation failed");
    return -1;
  } catch (...) {
    s.set_error(QueryError::ReadFailure, "HDF5 source read callback threw");
    return -1;
  }
}
herr_t write_file(H5FD_t *p, H5FD_mem_t, hid_t, haddr_t, size_t,
                  const void *) noexcept {
  as_file(p)->state->set_error(QueryError::PermissionDenied,
                               "HDF5 source driver is read-only");
  return -1;
}
herr_t flush_file(H5FD_t *, hid_t, hbool_t) noexcept { return 0; }
herr_t truncate_file(H5FD_t *, hid_t, hbool_t) noexcept { return -1; }
herr_t lock_file(H5FD_t *, hbool_t writing) noexcept {
  return writing ? -1 : 0;
}
herr_t unlock_file(H5FD_t *) noexcept { return 0; }
hid_t driver_id() {
  static hid_t id = -1;
  if (id >= 0 && H5Iis_valid(id) > 0)
    return id;
  H5FD_class_t cls{};
  cls.version = H5FD_CLASS_VERSION;
  cls.value = H5_VFD_RESERVED;
  cls.name = "iotdbfs-readonly-source";
  cls.maxaddr = HADDR_MAX;
  cls.fc_degree = H5F_CLOSE_STRONG;
  cls.fapl_size = sizeof(Fapl);
  cls.fapl_get = get_fapl;
  cls.fapl_copy = copy_fapl;
  cls.fapl_free = free_fapl;
  cls.open = open_file;
  cls.close = close_file;
  cls.cmp = compare_files;
  cls.query = query_file;
  cls.get_eoa = get_eoa;
  cls.set_eoa = set_eoa;
  cls.get_eof = get_eof;
  cls.read = read_file;
  cls.write = write_file;
  cls.flush = flush_file;
  cls.truncate = truncate_file;
  cls.lock = lock_file;
  cls.unlock = unlock_file;
  for (auto &value : cls.fl_map)
    value = H5FD_MEM_DEFAULT;
  id = H5FDregister(&cls);
  if (id < 0)
    throw Failure{
        failure(QueryError::Internal, "cannot register HDF5 source VFD")};
  return id;
}
void checked(std::int64_t result, State &state, const char *message,
             QueryError code = QueryError::CorruptPayload) {
  if (result < 0 || !state.error.ok() || state.fallback != QueryError::None)
    throw Failure{state.error.ok() ? failure(state.fallback == QueryError::None
                                                 ? code
                                                 : state.fallback,
                                             message)
                                   : state.error};
}
void check_cancel(State &state) {
  if (state.cancelled())
    throw Failure{
        state.error.ok()
            ? failure(state.fallback, "HDF5 cancellation callback failed")
            : state.error};
}
void validate_local(State &state) {
  check_cancel(state);
  if (state.local_handle) {
    auto valid = state.local_handle->validate();
    query_profile_count("hdf5.native.session.validations");
    if (!valid.ok()) {
      state.error = native_file_failure(valid);
      throw Failure{state.error};
    }
  }
}
void options_valid(const Hdf5SourceOptions &o) {
  constexpr std::uint64_t gib = 1024ULL * 1024 * 1024;
  if (!o.max_read_calls || o.max_read_calls > 1048576 || !o.max_read_bytes ||
      !o.max_single_read_bytes || o.max_single_read_bytes > gib ||
      !o.max_result_bytes || o.max_result_bytes > gib ||
      !o.max_decoded_chunk_bytes || o.max_decoded_chunk_bytes > gib ||
      o.metadata_cache_bytes < 1024 ||
      o.metadata_cache_bytes > 64ULL * 1024 * 1024 ||
      o.raw_chunk_cache_bytes > 64ULL * 1024 * 1024)
    throw Failure{failure(QueryError::InvalidQuery,
                          "invalid HDF5 source budgets/cache configuration")};
}
std::uint64_t product(const std::vector<std::uint64_t> &dims,
                      std::uint64_t width, std::uint64_t cap) {
  if (std::find(dims.begin(), dims.end(), 0) != dims.end())
    return 0;
  auto n = width;
  for (auto d : dims) {
    if (d > cap / n)
      throw Failure{failure(QueryError::ResourceExhausted,
                            "HDF5 array/chunk exceeds byte budget")};
    n *= d;
  }
  return n;
}
herr_t deny_external(const char *, const char *, const char *, const char *,
                     unsigned *, hid_t, void *data) noexcept {
  static_cast<State *>(data)->set_error(
      QueryError::UnsupportedCapability,
      "HDF5 external link traversal is forbidden");
  return -1;
}
void path_links(hid_t file, State &state, const std::string &path) {
  auto parsed = make_hdf5_value_field(path);
  if (!parsed.ok())
    throw Failure{parsed.status()};
  Id access(H5Pcreate(H5P_LINK_ACCESS), H5Pclose);
  checked(access.value, state, "cannot create link access settings",
          QueryError::Internal);
  checked(H5Pset_elink_cb(access, deny_external, &state), state,
          "cannot deny external traversal", QueryError::Internal);
  std::size_t start = 1;
  std::string prefix;
  while (start < path.size()) {
    auto end = path.find('/', start);
    if (end == std::string::npos)
      end = path.size();
    prefix += "/" + path.substr(start, end - start);
    const auto exists = H5Lexists(file, prefix.c_str(), access);
    checked(exists, state, "cannot inspect HDF5 path existence");
    if (!exists)
      throw Failure{
          failure(QueryError::UnknownField, "HDF5 dataset path is absent")};
    H5L_info2_t info{};
    checked(H5Lget_info2(file, prefix.c_str(), &info, access), state,
            "cannot read HDF5 link metadata");
    if (info.type != H5L_TYPE_HARD)
      throw Failure{failure(QueryError::UnsupportedCapability,
                            "HDF5 soft/external/user links are not followed")};
    H5O_info2_t object{};
    checked(H5Oget_info_by_name3(file, prefix.c_str(), &object, H5O_INFO_BASIC,
                                 access),
            state, "cannot read HDF5 object metadata");
    if ((end == path.size() && object.type != H5O_TYPE_DATASET) ||
        (end != path.size() && object.type != H5O_TYPE_GROUP))
      throw Failure{failure(QueryError::UnknownField,
                            "HDF5 path does not name a dataset")};
    start = end + 1;
  }
}
struct Profile {
  Hdf5DatasetDescriptor descriptor;
  std::vector<std::uint64_t> chunks;
  H5D_layout_t layout;
  H5D_fill_value_t fill;
  H5D_fill_time_t fill_time;
};
Profile inspect(hid_t dataset, hid_t dcpl, State &s, const std::string &path,
                bool enforce_chunk_budget = true) {
  Id space(H5Dget_space(dataset), H5Sclose),
      type(H5Dget_type(dataset), H5Tclose);
  checked(space.value, s, "cannot read HDF5 dataspace");
  checked(type.value, s, "cannot read HDF5 datatype");
  Profile p{};
  p.descriptor.path = path;
  auto &d = p.descriptor;
  int rank = H5Sget_simple_extent_ndims(space);
  checked(rank, s, "invalid HDF5 rank");
  if (rank > 42)
    throw Failure{failure(QueryError::UnsupportedCapability,
                          "HDF5 rank exceeds native bound")};
  std::vector<hsize_t> dims(rank);
  checked(H5Sget_simple_extent_dims(space, dims.data(), nullptr), s,
          "invalid HDF5 dimensions");
  d.shape.assign(dims.begin(), dims.end());
  auto unavailable = [&](const char *why) {
    d.supported = false;
    d.unsupported_reason = why;
  };
  if (H5Tequal(type, H5T_IEEE_F32LE) > 0 || H5Tequal(type, H5T_IEEE_F32BE) > 0)
    d.value_type = LogicalTypeId::Float32;
  else if (H5Tequal(type, H5T_IEEE_F64LE) > 0 ||
           H5Tequal(type, H5T_IEEE_F64BE) > 0)
    d.value_type = LogicalTypeId::Float64;
  else if (H5Tequal(type, H5T_STD_I16LE) > 0 ||
           H5Tequal(type, H5T_STD_I16BE) > 0)
    d.value_type = LogicalTypeId::Int32;
  else {
    d.value_type = LogicalTypeId::Binary;
    unavailable("HDF5 requires standard IEEE Float32/Float64 or signed Int16");
  }
  if (rank < 1 || rank > 4 || H5Sget_simple_extent_type(space) != H5S_SIMPLE)
    unavailable("HDF5 requires rank 1..4 simple dataspace");
  p.layout = H5Pget_layout(dcpl);
  if (p.layout != H5D_CONTIGUOUS && p.layout != H5D_CHUNKED)
    unavailable("HDF5 compact/virtual layout is unsupported");
  auto external = H5Pget_external_count(dcpl);
  checked(external, s, "invalid external storage metadata");
  if (external)
    unavailable("HDF5 external raw storage is forbidden");
  if (p.layout == H5D_CHUNKED) {
    std::vector<hsize_t> dims2(rank);
    checked(H5Pget_chunk(dcpl, rank, dims2.data()), s, "invalid chunk shape");
    p.chunks.assign(dims2.begin(), dims2.end());
    if (d.supported && enforce_chunk_budget)
      product(p.chunks, H5Tget_size(type), s.options.max_decoded_chunk_bytes);
  }
  int nf = H5Pget_nfilters(dcpl);
  checked(nf, s, "invalid HDF5 filter count");
  if (nf > 32)
    throw Failure{
        failure(QueryError::UnsupportedCapability, "too many HDF5 filters")};
  for (int i = 0; i < nf; ++i) {
    unsigned flags = 0, parameters[32]{};
    size_t count = 32;
    auto id = H5Pget_filter2(dcpl, i, &flags, &count, parameters, 0, nullptr,
                             nullptr);
    checked(id, s, "invalid filter pipeline");
    if (count > 32)
      unavailable("filter parameter limit exceeded");
    if (id != H5Z_FILTER_DEFLATE && id != H5Z_FILTER_SHUFFLE &&
        id != H5Z_FILTER_FLETCHER32)
      unavailable("HDF5 filter is outside the built-in whitelist");
  }
  checked(H5Pfill_value_defined(dcpl, &p.fill), s, "invalid fill declaration");
  checked(H5Pget_fill_time(dcpl, &p.fill_time), s,
          "invalid fill-time declaration");
  return p;
}
} // namespace

std::shared_ptr<QueryTaskResource> hdf5_query_task_resource() {
  // This reservation is a thread-independent logical gate, not ownership of
  // library_mutex. The availability probe acquires and releases the real
  // mutex on the dispatching thread; no native lock crosses to a worker.
  static auto resource = std::make_shared<QueryTaskResource>(1, [] {
    if (!library_mutex.try_lock()) return false;
    library_mutex.unlock();
    return true;
  });
  // Ingestion and other native callers still use library_mutex. They can win
  // the race after this probe, so at most one admitted HDF5 query may then
  // wait in the execution pool. The gate does not replace native locking.
  return resource;
}

struct Hdf5SourceDriver::Impl {
  std::shared_ptr<State> state;
  hid_t file{-1};
  struct Dataset { hid_t id{-1}; std::uint64_t used{0}, charge{0}; };
  std::map<std::string, Dataset> datasets;
  std::shared_ptr<const NativeFileDescriptor> cached_handle;
  std::weak_ptr<Hdf5NativeSessionPool::Impl> pool;
  bool reusable{true};
  Id dataset(const std::string &path, bool metadata_only = false);
  explicit Impl(std::shared_ptr<State> s) : state(std::move(s)) {
    QueryProfileTimer timer("hdf5.native.setup");
    options_valid(state->options);
    environment_policy();
    ErrorScope errors;
    policy();
    check_cancel(*state);
    Id access(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    checked(access.value, *state, "cannot create HDF5 access properties",
            QueryError::Internal);
    Fapl info{state};
    checked(H5Pset_driver(access, driver_id(), &info), *state,
            "cannot install source VFD", QueryError::Internal);
    checked(H5Pset_vol(access, H5VL_NATIVE, nullptr), *state,
            "cannot select native VOL", QueryError::Internal);
    checked(H5Pset_fclose_degree(access, H5F_CLOSE_STRONG), *state,
            "cannot set close degree", QueryError::Internal);
    checked(H5Pset_sieve_buf_size(access, 0), *state,
            "cannot disable data sieve", QueryError::Internal);
    checked(H5Pset_cache(access, 0, state->options.raw_chunk_cache_bytes ? 521 : 1,
            state->options.raw_chunk_cache_bytes, state->options.raw_chunk_cache_bytes ? 0.75 : 0),
            *state, "cannot bound raw chunk cache", QueryError::Internal);
    checked(H5Pset_page_buffer_size(access, 0, 0, 0), *state,
            "cannot disable page buffer", QueryError::Internal);
    H5AC_cache_config_t cache{};
    cache.version = H5AC__CURR_CACHE_CONFIG_VERSION;
    checked(H5Pget_mdc_config(access, &cache), *state,
            "cannot inspect metadata cache", QueryError::Internal);
    cache.set_initial_size = true;
    cache.initial_size = state->options.metadata_cache_bytes;
    cache.min_size = cache.initial_size;
    cache.max_size = cache.initial_size;
    cache.incr_mode = H5C_incr__off;
    cache.flash_incr_mode = H5C_flash_incr__off;
    cache.decr_mode = H5C_decr__off;
    checked(H5Pset_mdc_config(access, &cache), *state,
            "cannot bound metadata cache", QueryError::Internal);
    state->trace.metadata_cache_bytes = cache.max_size;
    state->trace.raw_chunk_cache_bytes = state->options.raw_chunk_cache_bytes;
    state->trace.conversion_buffer_bytes =
        std::min<std::uint64_t>(1024 * 1024, state->options.max_result_bytes);
    Id opened(H5Fopen("iotdbfs-source", H5F_ACC_RDONLY, access), H5Fclose);
    checked(opened.value, *state, "cannot open source as HDF5");
    file = opened.value;
    opened.value = -1;
    query_profile_count("hdf5.native.file.opens");
  }
  ~Impl() {
    if (file >= 0) {
      auto lock = native_library_lock();
      QueryProfileTimer timer("hdf5.native.close");
      ErrorScope errors;
      for (const auto &item : datasets) H5Dclose(item.second.id);
      H5Fclose(file);
    }
  }
};

// All pool mutations run under library_mutex, including destruction. Thus an
// HDF5 handle is never touched concurrently and no pool/library lock inversion
// is possible. Busy entries remain charged and cannot be evicted.
struct Hdf5NativeSessionPool::Impl {
  static constexpr std::uint64_t max_bytes = 64ULL * 1024 * 1024;
  static constexpr std::size_t max_files = 8, max_datasets = 8;
  struct Entry {
    std::string key;
    std::shared_ptr<Hdf5SourceDriver::Impl> session;
    bool busy{true};
    std::uint64_t used{0}, charge{0};
  };
  std::vector<Entry> entries;
  std::uint64_t clock{0};
  ~Impl() { auto lock = native_library_lock(); entries.clear(); }
  std::uint64_t bytes() const {
    std::uint64_t value = sizeof(*this) + entries.capacity() * sizeof(Entry);
    for (const auto &entry : entries) {
      value += entry.charge;
      for (const auto &dataset : entry.session->datasets) value += dataset.second.charge;
    }
    return value;
  }
  std::size_t dataset_count() const {
    std::size_t n = 0;
    for (const auto &entry : entries) n += entry.session->datasets.size();
    return n;
  }
  bool evict_file() {
    auto found = entries.end();
    for (auto it = entries.begin(); it != entries.end(); ++it)
      if (!it->busy && (found == entries.end() || it->used < found->used)) found = it;
    if (found == entries.end()) return false;
    entries.erase(found);
    query_profile_count("hdf5.native.session.evictions");
    return true;
  }
  bool dataset_room(Hdf5SourceDriver::Impl *current, std::uint64_t charge) {
    while (dataset_count() >= max_datasets || bytes() > max_bytes - charge) {
      Hdf5SourceDriver::Impl *victim = nullptr;
      std::string path;
      std::uint64_t oldest = std::numeric_limits<std::uint64_t>::max();
      for (auto &entry : entries) {
        if (entry.busy && entry.session.get() != current) continue;
        for (const auto &item : entry.session->datasets)
          if (item.second.used < oldest) {
            oldest = item.second.used; victim = entry.session.get(); path = item.first;
          }
      }
      if (!victim) return false;
      H5Dclose(victim->datasets.at(path).id);
      victim->datasets.erase(path);
      query_profile_count("hdf5.native.dataset.evictions");
    }
    return true;
  }
  void release(const std::shared_ptr<Hdf5SourceDriver::Impl> &session) {
    for (auto it = entries.begin(); it != entries.end(); ++it) if (it->session == session) {
      if (!session->reusable || !session->state->error.ok() ||
          session->state->fallback != QueryError::None) { entries.erase(it); return; }
      auto &state = *session->state;
      // The library retains State through its VFD. Remove every request-owned
      // closure/capability before making the entry visible to another lease.
      // Add only previously uncharged range occurrences. Repeated warm
      // executions preserve the cold demand, rather than accumulating history.
      for (const auto &read : state.trace.reads)
        if (read.logical_charged) state.fees.push_back({read.offset, read.length, 1, 0});
      std::sort(state.fees.begin(), state.fees.end(), [](const auto &a, const auto &b) {
        return std::make_pair(a.offset, a.length) < std::make_pair(b.offset, b.length);
      });
      std::size_t output = 0;
      for (const auto &fee : state.fees) {
        if (output && state.fees[output - 1].offset == fee.offset && state.fees[output - 1].length == fee.length)
          state.fees[output - 1].count += fee.count;
        else state.fees[output++] = fee;
      }
      state.fees.resize(output);
      state.reader = {};
      state.logical_reserver = {};
      state.local_handle.reset();
      state.options = {};
      state.trace.reads.clear();
      state.trace.metadata_bytes = state.trace.raw_data_bytes = 0;
      it->busy = false; it->used = ++clock;
      return;
    }
  }
};
Hdf5NativeSessionPool::Hdf5NativeSessionPool() : impl_(std::make_shared<Impl>()) {
  impl_->entries.reserve(Impl::max_files);
}
Hdf5NativeSessionPool::~Hdf5NativeSessionPool() = default;
Hdf5NativeSessionPool::Stats Hdf5NativeSessionPool::stats() const {
  auto lock = native_library_lock();
  Stats result;
  result.files = impl_->entries.size(); result.datasets = impl_->dataset_count();
  result.accounted_bytes = impl_->bytes();
  for (const auto &entry : impl_->entries) result.busy += entry.busy;
  return result;
}
Id Hdf5SourceDriver::Impl::dataset(const std::string &path, bool metadata_only) {
  auto owner = pool.lock();
  if (owner) {
    auto found = datasets.find(path);
    if (found != datasets.end()) {
      found->second.used = ++owner->clock;
      query_profile_count("hdf5.native.dataset.hits");
      return Id(found->second.id, nullptr);
    }
  }
  if (!owner && metadata_only) {
    Id opened(H5Dopen2(file, path.c_str(), H5P_DEFAULT), H5Dclose);
    checked(opened.value, *state, "cannot open HDF5 dataset metadata");
    query_profile_count("hdf5.native.dataset.opens");
    return Id(std::exchange(opened.value, -1), H5Dclose);
  }
  Id dapl(H5Pcreate(H5P_DATASET_ACCESS), H5Pclose);
  checked(dapl.value, *state, "cannot create dataset access settings", QueryError::Internal);
  checked(H5Pset_chunk_cache(dapl, state->options.raw_chunk_cache_bytes ? 521 : 1,
      state->options.raw_chunk_cache_bytes, state->options.raw_chunk_cache_bytes ? 0.75 : 0), *state,
      "cannot bound dataset chunk cache", QueryError::Internal);
  Id opened(H5Dopen2(file, path.c_str(), dapl), H5Dclose);
  checked(opened.value, *state, "cannot open HDF5 dataset metadata");
  query_profile_count("hdf5.native.dataset.opens");
  // Include native raw-cache slots and a conservative handle/management credit.
  const auto charge = state->options.raw_chunk_cache_bytes + path.size() +
      sizeof(Dataset) + 16384ULL;
  if (owner && charge <= Hdf5NativeSessionPool::Impl::max_bytes &&
      owner->dataset_room(this, charge)) {
    auto inserted = datasets.emplace(path, Dataset{opened.value, ++owner->clock, charge});
    opened.value = -1;
    return Id(inserted.first->second.id, nullptr);
  }
  return Id(std::exchange(opened.value, -1), H5Dclose);
}
Hdf5SourceDriver::Hdf5SourceDriver(std::shared_ptr<Impl> p)
    : impl_(std::move(p)) {}
Hdf5SourceDriver::~Hdf5SourceDriver() {
  auto lock = native_library_lock();
  if (pool_) pool_->impl_->release(impl_);
  impl_.reset();
}
QueryResult<std::unique_ptr<Hdf5SourceDriver>>
Hdf5SourceDriver::open(std::shared_ptr<const QuerySource> source,
                       Hdf5SourceOptions options) {
  auto lock = native_library_lock();
  try {
    if (!source)
      throw Failure{
          failure(QueryError::InvalidQuery, "HDF5 QuerySource is null")};
    const auto manifest = source->manifest();
    if (!manifest.published || manifest.phase != StructuredBlobPhase::Published)
      throw Failure{failure(QueryError::SourceNotPublished,
                            "HDF5 source version is unpublished")};
    auto state = std::make_shared<State>();
    state->size = manifest.payload_size;
    state->options = std::move(options);
    state->reader = [source = std::move(source)](auto offset, auto length) {
      return source->read_payload(offset, length);
    };
    return QueryResult<std::unique_ptr<Hdf5SourceDriver>>::success(
        std::unique_ptr<Hdf5SourceDriver>(
            new Hdf5SourceDriver(std::make_unique<Impl>(std::move(state)))));
  } catch (const Failure &f) {
    return QueryResult<std::unique_ptr<Hdf5SourceDriver>>::failure(f.status);
  } catch (const std::bad_alloc &) {
    return QueryResult<std::unique_ptr<Hdf5SourceDriver>>::failure(failure(
        QueryError::ResourceExhausted, "HDF5 session allocation failed"));
  } catch (...) {
    return QueryResult<std::unique_ptr<Hdf5SourceDriver>>::failure(
        failure(QueryError::ReadFailure, "HDF5 source open threw"));
  }
}
QueryResult<std::unique_ptr<Hdf5SourceDriver>>
Hdf5SourceDriver::open_local(std::shared_ptr<const QuerySource> source,
                             Hdf5SourceOptions options,
                             std::shared_ptr<Hdf5NativeSessionPool> pool) {
  auto lock = native_library_lock();
  using Result = QueryResult<std::unique_ptr<Hdf5SourceDriver>>;
  try {
    if (!source)
      throw Failure{failure(QueryError::InvalidQuery, "HDF5 QuerySource is null")};
    const auto &manifest = source->manifest();
    if (!manifest.published || manifest.phase != StructuredBlobPhase::Published)
      throw Failure{failure(QueryError::SourceNotPublished,
                            "HDF5 source version is unpublished")};
    query_profile_count("hdf5.native.session.cache.enabled", pool ? 1 : 0);
    if (pool) {
      options.metadata_cache_bytes = 2ULL * 1024 * 1024;
      options.raw_chunk_cache_bytes = 256ULL * 1024;
    }
    options_valid(options);
    auto state = std::make_shared<State>();
    state->options = options;
    check_cancel(*state);
    std::string key;
    std::uint64_t charge = 0;
    if (pool) {
      auto identity = source->native_file_cache_identity();
      if (!identity.ok()) return Result::failure(identity.status());
      if (identity.value() && !options.native_selection_identity.empty()) {
        key = *identity.value();
        key += ":" + std::to_string(options.native_selection_identity.size()) + ":" + options.native_selection_identity;
        // Capacity and behavior are fixed for the lifetime of a library entry;
        // request limits are part of the key rather than silently enlarged.
        for (auto value : {std::uint64_t(options.max_read_calls), options.max_read_bytes,
            options.max_single_read_bytes, options.max_result_bytes,
            options.max_decoded_chunk_bytes, options.metadata_cache_bytes,
            options.raw_chunk_cache_bytes}) key += ":" + std::to_string(value);
        auto &entries = pool->impl_->entries;
        for (auto it = entries.begin(); it != entries.end(); ++it) {
          if (it->key != key || it->busy) continue;
          auto session = it->session;
          // Establish RAII ownership before revalidation or request binding.
          auto driver = std::unique_ptr<Hdf5SourceDriver>(new Hdf5SourceDriver(session));
          driver->pool_ = pool;
          it->busy = true;
          session->reusable = false;
          auto rebound = source->reuse_native_file(session->cached_handle);
          if (!rebound.ok()) { entries.erase(it); return Result::failure(rebound.status()); }
          if (!rebound.value() || rebound.value()->size() != manifest.payload_size) {
            entries.erase(it);
            return Result::failure(failure(QueryError::CorruptPayload,
              "cached HDF5 file differs from sealed size"));
          }
          state = session->state;
          state->options = std::move(options);
          state->error = QueryStatus::success(); state->fallback = QueryError::None;
          state->trace.reads.clear();
          state->trace.metadata_bytes = state->trace.raw_data_bytes = 0;
          state->local_handle = std::move(rebound.value());
          session->reusable = false; // A failed rebind must never return to idle.
          validate_local(*state);
          state->logical_bytes = state->logical_calls = 0;
          for (auto &fee : state->fees) {
            if (fee.length > state->options.max_single_read_bytes ||
                fee.count > state->options.max_read_calls - state->logical_calls ||
                fee.length > (state->options.max_read_bytes - state->logical_bytes) / fee.count)
              throw Failure{failure(QueryError::ResourceExhausted, "HDF5 cached logical demand exceeds request limits")};
            state->logical_calls += fee.count; state->logical_bytes += fee.length * fee.count;
            fee.remaining = fee.count;
          }
          for (const auto &fee : state->fees) for (std::uint32_t n = 0; n < fee.count; ++n) {
            check_cancel(*state);
            auto replay = source->reserve_logical_payload(fee.length);
            if (!replay.ok()) throw Failure{replay};
          }
          query_profile_count("hdf5.native.logical.calls", state->logical_calls);
          query_profile_count("hdf5.native.logical.bytes", state->logical_bytes);
          query_profile_count("hdf5.native.logical.replays");
          state->logical_reserver = [source](auto length) { return source->reserve_logical_payload(length); };
          state->reader = [handle = state->local_handle](auto offset, auto length) {
            query_profile_count("hdf5.native.filesystem.calls");
            query_profile_count("hdf5.native.filesystem.bytes", length);
            QueryProfileTimer timer("hdf5.native.filesystem.read");
            auto bytes = handle->read(offset, length);
            if (!bytes.ok()) return QueryResult<StructuredBlobBytes>::failure(native_file_failure(bytes.status()));
            return QueryResult<StructuredBlobBytes>::success(std::move(bytes.value()));
          };
          session->reusable = true;
          it->busy = true; it->used = ++pool->impl_->clock;
          query_profile_count("hdf5.native.session.hits");
          query_profile_count("hdf5.native.session.rebinds");
          query_profile_count("hdf5.native.session.leases");
          return Result::success(std::move(driver));
        }
        query_profile_count("hdf5.native.session.misses");
        // The two resident vectors reserve exact maximum capacities, covering
        // physical traces and compact logical demand/counts. A failed reservation uses the bounded
        // request-local implementation rather than weakening execution limits.
        charge = options.metadata_cache_bytes + 2 * key.size() +
            options.max_read_calls * (sizeof(Hdf5SourceRead) + sizeof(State::LogicalFee)) +
            sizeof(Impl) + sizeof(State) + 65536;
        while ((pool->impl_->entries.size() >= Hdf5NativeSessionPool::Impl::max_files ||
                charge > Hdf5NativeSessionPool::Impl::max_bytes ||
                pool->impl_->bytes() > Hdf5NativeSessionPool::Impl::max_bytes - charge) &&
               pool->impl_->evict_file()) {}
        if (charge > Hdf5NativeSessionPool::Impl::max_bytes ||
            pool->impl_->entries.size() >= Hdf5NativeSessionPool::Impl::max_files ||
            pool->impl_->bytes() > Hdf5NativeSessionPool::Impl::max_bytes - charge) key.clear();
      }
      if (key.empty()) query_profile_count("hdf5.native.session.fallbacks");
    }
    auto opened = source->open_native_file();
    if (!opened.ok()) return Result::failure(opened.status());
    auto handle = std::move(opened.value());
    if (!handle || handle->size() != manifest.payload_size)
      throw Failure{failure(QueryError::CorruptPayload,
                            "HDF5 managed file differs from sealed size")};
    state->size = handle->size();
    state->local_handle = handle;
    validate_local(*state);
    query_profile_count("hdf5.native.filesystem.opens");
    if (!key.empty()) {
      try {
        // Exact capacities avoid geometric slack in both resident vectors.
        state->trace.reads.reserve(options.max_read_calls);
        state->fees.reserve(options.max_read_calls);
      } catch (const std::bad_alloc &) {
        key.clear();
        std::vector<Hdf5SourceRead>().swap(state->trace.reads);
        std::vector<State::LogicalFee>().swap(state->fees);
        query_profile_count("hdf5.native.session.fallbacks");
      }
    }
    state->logical_reserver = [source](auto length) { return source->reserve_logical_payload(length); };
    state->reader = [handle](auto offset, auto length) {
      query_profile_count("hdf5.native.filesystem.calls");
      query_profile_count("hdf5.native.filesystem.bytes", length);
      QueryProfileTimer timer("hdf5.native.filesystem.read");
      auto bytes = handle->read(offset, length);
      if (!bytes.ok()) return QueryResult<StructuredBlobBytes>::failure(native_file_failure(bytes.status()));
      return QueryResult<StructuredBlobBytes>::success(std::move(bytes.value()));
    };
    auto session = std::make_shared<Impl>(std::move(state));
    auto driver = std::unique_ptr<Hdf5SourceDriver>(new Hdf5SourceDriver(session));
    if (!key.empty()) {
      // Failure to allocate optional bookkeeping does not fail a valid read.
      try {
        session->cached_handle = handle->cache_descriptor();
        pool->impl_->entries.push_back({std::move(key), session, true, ++pool->impl_->clock, charge});
        session->pool = pool->impl_;
        driver->pool_ = std::move(pool);
        query_profile_count("hdf5.native.session.leases");
      } catch (const std::bad_alloc &) {
        session->cached_handle.reset();
        query_profile_count("hdf5.native.session.fallbacks");
      }
    }
    return Result::success(std::move(driver));
  } catch (const Failure &f) {
    return Result::failure(f.status);
  } catch (const std::bad_alloc &) {
    return Result::failure(failure(QueryError::ResourceExhausted,
                                  "HDF5 filesystem session allocation failed"));
  } catch (...) {
    return Result::failure(failure(QueryError::ReadFailure,
                                  "HDF5 filesystem source open threw"));
  }
}

QueryStatus with_hdf5_ingestion_source(
    const FormatSource &source, Hdf5SourceOptions options,
    const std::function<QueryStatus(const Hdf5SourceDriver &)> &operation) {
  auto lock = native_library_lock();
  try {
    if (!source.read || !operation)
      throw Failure{
          failure(QueryError::InvalidQuery,
                  "HDF5 synchronous ingestion callbacks are missing")};
    auto state = std::make_shared<State>();
    state->size = source.size;
    state->options = std::move(options);
    state->reader = [&source](auto offset, auto length) {
      auto bytes = source.read(offset, length);
      if (!bytes)
        return QueryResult<StructuredBlobBytes>::failure(failure(
            QueryError::ReadFailure, "HDF5 ingestion source read failed"));
      return QueryResult<StructuredBlobBytes>::success(std::move(*bytes));
    };
    Hdf5SourceDriver driver(
        std::make_unique<Hdf5SourceDriver::Impl>(std::move(state)));
    return operation(driver);
  } catch (const Failure &f) {
    return f.status;
  } catch (const std::bad_alloc &) {
    return failure(QueryError::ResourceExhausted,
                   "HDF5 ingestion allocation failed");
  } catch (...) {
    return failure(QueryError::ReadFailure, "HDF5 ingestion callback threw");
  }
}
QueryResult<Hdf5DatasetDescriptor>
Hdf5SourceDriver::describe(const std::string &path) const {
  auto lock = native_library_lock();
  QueryProfileTimer timer("hdf5.native.describe");
  auto &s = *impl_->state;
  try {
    environment_policy();
    ErrorScope errors;
    policy();
    s.error = QueryStatus::success();
    s.fallback = QueryError::None;
    validate_local(s);
    path_links(impl_->file, s, path);
    auto dataset = impl_->dataset(path, true);
    checked(dataset.value, s, "cannot open HDF5 dataset metadata");
    Id dcpl(H5Dget_create_plist(dataset), H5Pclose);
    checked(dcpl.value, s, "cannot inspect HDF5 dataset");
    auto profile = inspect(dataset, dcpl, s, path);
    validate_local(s);
    return QueryResult<Hdf5DatasetDescriptor>::success(
        std::move(profile.descriptor));
  } catch (const Failure &f) {
    impl_->reusable = false;
    return QueryResult<Hdf5DatasetDescriptor>::failure(f.status);
  } catch (const std::bad_alloc &) {
    impl_->reusable = false;
    return QueryResult<Hdf5DatasetDescriptor>::failure(failure(
        QueryError::ResourceExhausted, "HDF5 metadata allocation failed"));
  } catch (...) {
    impl_->reusable = false;
    return QueryResult<Hdf5DatasetDescriptor>::failure(
        failure(QueryError::ReadFailure, "HDF5 metadata callback threw"));
  }
}
QueryResult<Hdf5NativeSubset>
Hdf5SourceDriver::read(const std::string &path,
                       const std::vector<std::uint64_t> &origin,
                       const std::vector<std::uint64_t> &shape) const {
  auto lock = native_library_lock();
  QueryProfileTimer timer("hdf5.native.read");
  auto &s = *impl_->state;
  try {
    environment_policy();
    ErrorScope errors;
    policy();
    s.error = QueryStatus::success();
    s.fallback = QueryError::None;
    validate_local(s);
    path_links(impl_->file, s, path);
    auto dataset = impl_->dataset(path);
    checked(dataset.value, s, "cannot open HDF5 dataset metadata");
    Id dcpl(H5Dget_create_plist(dataset), H5Pclose);
    checked(dcpl.value, s, "cannot inspect dataset");
    auto profile = inspect(dataset, dcpl, s, path);
    if (!profile.descriptor.supported)
      throw Failure{failure(QueryError::UnsupportedCapability,
                            profile.descriptor.unsupported_reason)};
    if (origin.size() != profile.descriptor.shape.size() ||
        shape.size() != origin.size())
      throw Failure{failure(QueryError::InvalidDomainSlice,
                            "HDF5 native selection rank mismatch")};
    for (size_t i = 0; i < shape.size(); ++i)
      if (origin[i] > profile.descriptor.shape[i] ||
          shape[i] > profile.descriptor.shape[i] - origin[i])
        throw Failure{failure(QueryError::InvalidDomainSlice,
                              "HDF5 native selection exceeds extent")};
    const bool f32 = profile.descriptor.value_type == LogicalTypeId::Float32;
    const bool i16 = profile.descriptor.value_type == LogicalTypeId::Int32;
    auto bytes =
        product(shape, (f32 || i16) ? 4 : 8, s.options.max_result_bytes);
    auto count = bytes / ((f32 || i16) ? 4 : 8);
    Hdf5NativeSubset result{
        profile.descriptor,
        origin,
        shape,
        {{profile.descriptor.value_type, false, std::nullopt},
         std::vector<float>{},
         {}}};
    if (i16)
      result.values.values = std::vector<std::int32_t>(count);
    else if (f32)
      result.values.values = std::vector<float>(count);
    else
      result.values.values = std::vector<double>(count);
    if (count) {
      const bool undefined_fill = profile.fill == H5D_FILL_VALUE_UNDEFINED ||
                                  profile.fill_time == H5D_FILL_TIME_NEVER;
      if (profile.layout == H5D_CONTIGUOUS) {
        if (undefined_fill) {
          H5D_space_status_t allocation{};
          checked(H5Dget_space_status(dataset, &allocation), s,
                  "cannot inspect contiguous allocation");
          if (allocation == H5D_SPACE_STATUS_NOT_ALLOCATED)
            throw Failure{failure(QueryError::UnsupportedCapability,
                                  "unallocated HDF5 data has undefined fill")};
        }
      } else {
        std::vector<hsize_t> first(shape.size()), last(shape.size()),
            where(shape.size());
        std::uint64_t candidates = 1;
        for (size_t i = 0; i < shape.size(); ++i) {
          first[i] = (origin[i] / profile.chunks[i]) * profile.chunks[i];
          last[i] = ((origin[i] + shape[i] - 1) / profile.chunks[i]) *
                    profile.chunks[i];
          where[i] = first[i];
          auto n = (last[i] - first[i]) / profile.chunks[i] + 1;
          if (n > s.options.max_read_calls / candidates)
            throw Failure{failure(
                QueryError::ResourceExhausted,
                "HDF5 selected chunk preflight exceeds candidate budget")};
          candidates *= n;
        }
        query_profile_count("hdf5.native.selected_chunk.visits", candidates);
        for (std::uint64_t n = 0; n < candidates; ++n) {
          check_cancel(s);
          unsigned mask = 0;
          haddr_t addr = HADDR_UNDEF;
          hsize_t encoded = 0;
          checked(H5Dget_chunk_info_by_coord(dataset, where.data(), &mask,
                                             &addr, &encoded),
                  s, "cannot validate selected chunk allocation");
          if (addr == HADDR_UNDEF || !encoded) {
            if (undefined_fill)
              throw Failure{
                  failure(QueryError::UnsupportedCapability,
                          "unallocated HDF5 chunk has undefined fill")};
          } else {
            if (encoded > s.options.max_single_read_bytes)
              throw Failure{
                  failure(QueryError::ResourceExhausted,
                          "encoded HDF5 chunk exceeds single read bound")};
            if (addr > s.size || encoded > s.size - addr)
              throw Failure{failure(QueryError::ReadFailure,
                                    "HDF5 chunk range exceeds source EOF")};
          }
          for (size_t i = shape.size(); i-- > 0;) {
            if (where[i] < last[i]) {
              where[i] += profile.chunks[i];
              break;
            }
            where[i] = first[i];
          }
        }
      }
      Id space(H5Dget_space(dataset), H5Sclose);
      checked(space.value, s, "cannot open dataset dataspace");
      std::vector<hsize_t> start(origin.begin(), origin.end()),
          length(shape.begin(), shape.end());
      checked(H5Sselect_hyperslab(space, H5S_SELECT_SET, start.data(), nullptr,
                                  length.data(), nullptr),
              s, "cannot select native hyperslab");
      Id memory(H5Screate_simple(length.size(), length.data(), nullptr),
                H5Sclose);
      checked(memory.value, s, "cannot create native memory space");
      void *output =
          i16 ? static_cast<void *>(
                    std::get<std::vector<std::int32_t>>(result.values.values)
                        .data())
          : f32
              ? static_cast<void *>(
                    std::get<std::vector<float>>(result.values.values).data())
              : static_cast<void *>(
                    std::get<std::vector<double>>(result.values.values).data());
      Id transfer(H5Pcreate(H5P_DATASET_XFER), H5Pclose);
      checked(transfer.value, s, "cannot create HDF5 transfer properties",
              QueryError::Internal);
      checked(H5Pset_buffer(transfer, s.trace.conversion_buffer_bytes, nullptr,
                            nullptr),
              s, "cannot bound HDF5 type conversion buffer",
              QueryError::Internal);
      checked(H5Dread(dataset,
                      i16   ? H5T_NATIVE_INT32
                      : f32 ? H5T_NATIVE_FLOAT
                            : H5T_NATIVE_DOUBLE,
                      memory, space, transfer, output),
              s, "native HDF5 subset read failed");
    }
    validate_local(s);
    return QueryResult<Hdf5NativeSubset>::success(std::move(result));
  } catch (const Failure &f) {
    impl_->reusable = false;
    return QueryResult<Hdf5NativeSubset>::failure(f.status);
  } catch (const std::bad_alloc &) {
    impl_->reusable = false;
    return QueryResult<Hdf5NativeSubset>::failure(failure(
        QueryError::ResourceExhausted, "HDF5 native read allocation failed"));
  } catch (...) {
    impl_->reusable = false;
    return QueryResult<Hdf5NativeSubset>::failure(
        failure(QueryError::ReadFailure, "HDF5 native read threw"));
  }
}
namespace {
struct InventoryBudget {
  const Hdf5LayoutLimits &limits;
  std::uint64_t directory_bytes{0}, chunk_bytes{0}, chunk_count{0};
  void directory(std::uint64_t bytes) {
    const auto cap =
        std::min(limits.max_directory_bytes, limits.max_layout_bytes);
    if (directory_bytes > cap || bytes > cap - directory_bytes)
      throw Failure{failure(QueryError::ResourceExhausted,
                            "HDF5 inventory directory budget exhausted")};
    directory_bytes += bytes;
  }
  bool chunks(std::uint64_t count, std::size_t rank) {
    const auto width = sizeof(Hdf5ChunkRecord) + rank * sizeof(std::uint64_t);
    const auto cap = limits.max_layout_bytes > limits.max_directory_bytes
                         ? limits.max_layout_bytes - limits.max_directory_bytes
                         : 0;
    if (chunk_bytes > cap || count > (cap - chunk_bytes) / width ||
        chunk_count > limits.max_chunks ||
        count > limits.max_chunks - chunk_count)
      return false;
    chunk_bytes += count * width;
    chunk_count += count;
    return true;
  }
  void string(const std::string &value) {
    if (value.size() > limits.max_string_bytes)
      throw Failure{failure(QueryError::ResourceExhausted,
                            "HDF5 inventory string limit exhausted")};
    directory(value.size());
  }
};
std::string inventory_token(hid_t file, const H5O_token_t &token, State &state,
                            const Hdf5LayoutLimits &limits) {
  char *text = nullptr;
  const auto status = H5Otoken_to_str(file, &token, &text);
  struct Free {
    char *p;
    ~Free() {
      if (p)
        H5free_memory(p);
    }
  } cleanup{text};
  checked(status, state, "cannot serialize native HDF5 object identity");
  if (!text)
    throw Failure{
        failure(QueryError::CorruptPayload, "HDF5 object identity is null")};
  const auto length = std::strlen(text);
  if (length > limits.max_string_bytes)
    throw Failure{failure(QueryError::ResourceExhausted,
                          "HDF5 native object identity exceeds string limit")};
  return std::string(text, length);
}
struct MetadataBudget {
  InventoryBudget &inventory;
  std::uint64_t bytes{0}, attributes{0}, candidates{0};
  bool reserve(std::uint64_t n) {
    const auto &l = inventory.limits;
    const auto cap = std::min(l.max_directory_bytes, l.max_layout_bytes);
    if (bytes > l.max_metadata_bytes || n > l.max_metadata_bytes - bytes ||
        inventory.directory_bytes > cap || n > cap - inventory.directory_bytes)
      return false;
    bytes += n;
    inventory.directory_bytes += n;
    return true;
  }
};
void optional_read_allowance(State &state) {
  check_cancel(state);
  checked(0, state, "HDF5 optional metadata source access failed");
  const auto used = state.trace.metadata_bytes + state.trace.raw_data_bytes;
  if (state.trace.reads.size() >= state.options.max_read_calls ||
      used >= state.options.max_read_bytes)
    throw Failure{failure(QueryError::ResourceExhausted,
                          "optional HDF5 metadata read allowance exhausted")};
}
bool optional_native(std::int64_t result, State &state) {
  // A malformed optional attribute is observable but does not erase chunks.
  // Actual QuerySource read failures and cancellation remain fatal.
  checked(0, state, "HDF5 optional metadata source access failed");
  check_cancel(state);
  return result >= 0;
}
void attribute_failure(Hdf5Attribute &a, Hdf5AttributeState status,
                       const char *reason) {
  a.state = status;
  a.reason = reason;
  a.values.clear();
  a.strings.clear();
}
void inventory_attribute_value(hid_t attr, State &state, Hdf5Attribute &a,
                               MetadataBudget &budget) {
  const auto &l = budget.inventory.limits;
  Id type(H5Aget_type(attr), H5Tclose), space(H5Aget_space(attr), H5Sclose);
  if (!optional_native(type.value, state) ||
      !optional_native(space.value, state)) {
    attribute_failure(a, Hdf5AttributeState::InvalidValue,
                      "invalid_attribute_type_or_space");
    return;
  }
  const auto cls = H5Tget_class(type);
  if (!optional_native(cls, state)) {
    attribute_failure(a, Hdf5AttributeState::InvalidValue,
                      "invalid_attribute_type");
    return;
  }
  a.native_type_class = static_cast<std::uint32_t>(cls);
  a.native_element_bytes = H5Tget_size(type);
  const auto space_kind = H5Sget_simple_extent_type(space);
  if (!optional_native(space_kind, state)) {
    attribute_failure(a, Hdf5AttributeState::InvalidValue,
                      "invalid_attribute_space");
    return;
  }
  a.null_dataspace = space_kind == H5S_NULL;
  const auto rank = H5Sget_simple_extent_ndims(space);
  if (!optional_native(rank, state) || rank > 42) {
    attribute_failure(a, Hdf5AttributeState::InvalidValue,
                      "invalid_attribute_rank");
    return;
  }
  std::vector<hsize_t> dims(static_cast<std::size_t>(rank));
  if (rank &&
      !optional_native(H5Sget_simple_extent_dims(space, dims.data(), nullptr),
                       state)) {
    attribute_failure(a, Hdf5AttributeState::InvalidValue,
                      "invalid_attribute_shape");
    return;
  }
  a.shape.assign(dims.begin(), dims.end());
  if (!budget.reserve(a.shape.size() * 8)) {
    a.shape.clear();
    attribute_failure(a, Hdf5AttributeState::TooLarge, "metadata_byte_limit");
    return;
  }
  const auto native_count = H5Sget_simple_extent_npoints(space);
  if (!optional_native(native_count, state)) {
    attribute_failure(a, Hdf5AttributeState::InvalidValue,
                      "invalid_attribute_cardinality");
    return;
  }
  const auto count = static_cast<std::uint64_t>(native_count);
  if (count > l.max_attribute_elements) {
    attribute_failure(a, Hdf5AttributeState::TooLarge,
                      "attribute_element_limit");
    return;
  }
  const auto variable = H5Tis_variable_str(type);
  if (!optional_native(variable, state)) {
    attribute_failure(a, Hdf5AttributeState::InvalidValue,
                      "invalid_attribute_string_type");
    return;
  }
  if (variable > 0 || cls == H5T_VLEN) {
    attribute_failure(a, Hdf5AttributeState::UnsupportedVariableLength,
                      "variable_length_attribute");
    return;
  }
  hid_t memory_type = -1;
  if (cls == H5T_INTEGER && a.native_element_bytes <= 8 &&
      a.native_element_bytes && H5Tget_precision(type) <= 64 &&
      H5Tget_offset(type) == 0) {
    const auto sign = H5Tget_sign(type);
    if (sign == H5T_SGN_NONE) {
      a.type = LogicalTypeId::UInt64;
      memory_type = H5T_STD_U64LE;
    } else if (sign == H5T_SGN_2) {
      a.type = LogicalTypeId::Int64;
      memory_type = H5T_STD_I64LE;
    }
  } else if (cls == H5T_FLOAT && (H5Tequal(type, H5T_IEEE_F32LE) > 0 ||
                                  H5Tequal(type, H5T_IEEE_F32BE) > 0 ||
                                  H5Tequal(type, H5T_IEEE_F64LE) > 0 ||
                                  H5Tequal(type, H5T_IEEE_F64BE) > 0)) {
    a.type = LogicalTypeId::Float64;
    memory_type = H5T_IEEE_F64LE;
  } else if (cls == H5T_STRING) {
    a.type = LogicalTypeId::Utf8;
    memory_type = type;
  }
  if (memory_type < 0) {
    attribute_failure(a, Hdf5AttributeState::UnsupportedType,
                      "unsupported_attribute_type");
    return;
  }
  const auto width = a.type == LogicalTypeId::Utf8 ? a.native_element_bytes : 8;
  if (!width ||
      (count && (width > l.max_attribute_bytes / count ||
                 a.native_element_bytes > l.max_attribute_bytes / count))) {
    attribute_failure(a, Hdf5AttributeState::TooLarge, "attribute_byte_limit");
    return;
  }
  const auto storage = H5Aget_storage_size(attr);
  checked(0, state, "cannot inspect HDF5 attribute storage");
  const auto bytes = count * width;
  if (a.type == LogicalTypeId::Utf8 &&
      count > UINT64_MAX / sizeof(std::string)) {
    attribute_failure(a, Hdf5AttributeState::TooLarge, "attribute_byte_limit");
    return;
  }
  const auto strings_overhead =
      a.type == LogicalTypeId::Utf8 ? count * sizeof(std::string) : 0;
  const auto scratch = a.type == LogicalTypeId::Utf8 ? bytes : 0;
  if (storage > l.max_attribute_bytes ||
      bytes > UINT64_MAX - strings_overhead ||
      scratch > UINT64_MAX - bytes - strings_overhead ||
      !budget.reserve(bytes + strings_overhead + scratch)) {
    attribute_failure(a, Hdf5AttributeState::TooLarge, "attribute_byte_limit");
    return;
  }
  BinaryValue data(static_cast<std::size_t>(bytes));
  if (count &&
      !optional_native(H5Aread(attr, memory_type, data.data()), state)) {
    attribute_failure(a, Hdf5AttributeState::InvalidValue,
                      "attribute_read_failed");
    return;
  }
  if (a.type == LogicalTypeId::Utf8) {
    const auto padding = H5Tget_strpad(type);
    const auto charset = H5Tget_cset(type);
    if ((padding != H5T_STR_NULLTERM && padding != H5T_STR_NULLPAD &&
         padding != H5T_STR_SPACEPAD) ||
        (charset != H5T_CSET_ASCII && charset != H5T_CSET_UTF8)) {
      attribute_failure(a, Hdf5AttributeState::UnsupportedType,
                        "unsupported_attribute_string_encoding");
      return;
    }
    for (std::uint64_t i = 0; i < count; ++i) {
      const char *start =
          reinterpret_cast<const char *>(data.data() + i * width);
      std::size_t length = static_cast<std::size_t>(width);
      if (padding == H5T_STR_SPACEPAD) {
        while (length && start[length - 1] == ' ')
          --length;
      } else if (padding == H5T_STR_NULLPAD) {
        while (length && start[length - 1] == '\0')
          --length;
      } else {
        const auto end =
            static_cast<const char *>(std::memchr(start, '\0', length));
        if (end)
          length = static_cast<std::size_t>(end - start);
      }
      if (length > l.max_string_bytes) {
        attribute_failure(a, Hdf5AttributeState::TooLarge,
                          "attribute_string_limit");
        return;
      }
      std::string value(start, length);
      const bool ascii = std::all_of(value.begin(), value.end(),
                                     [](unsigned char c) { return c < 128; });
      if (!valid_hdf5_metadata_text(value) ||
          (charset == H5T_CSET_ASCII && !ascii)) {
        attribute_failure(a, Hdf5AttributeState::InvalidValue,
                          "invalid_attribute_text");
        return;
      }
      a.strings.push_back(std::move(value));
    }
  } else
    a.values = std::move(data);
  a.state = Hdf5AttributeState::Complete;
  a.reason.clear();
}
void inventory_attributes(hid_t object_id, State &state,
                          Hdf5LayoutObject &object, MetadataBudget &budget) {
  const auto &l = budget.inventory.limits;
  H5O_info2_t info{};
  object.attribute_count_known = false;
  if (!optional_native(H5Oget_info3(object_id, &info, H5O_INFO_NUM_ATTRS),
                       state)) {
    object.attributes_complete = false;
    object.attributes_unavailable_reason = "attribute_count_unavailable";
    return;
  }
  object.attribute_count = info.num_attrs;
  object.attribute_count_known = true;
  if (!info.num_attrs)
    return;
  const auto remaining = budget.attributes <= l.max_attributes
                             ? l.max_attributes - budget.attributes
                             : 0;
  const auto count_limit =
      std::min<std::uint64_t>(l.max_attributes_per_object, remaining);
  if (!count_limit) {
    object.attributes_complete = false;
    object.attributes_unavailable_reason = "attribute_count_limit";
    return;
  }
  struct Names {
    MetadataBudget &budget;
    State &state;
    std::uint64_t cap;
    std::vector<std::string> values;
    const char *reason{nullptr};
    std::exception_ptr exception;
  } names{budget, state, count_limit, {}, nullptr, {}};
  const auto callback = [](hid_t, const char *name, const H5A_info_t *,
                           void *context) noexcept -> herr_t {
    auto &result = *static_cast<Names *>(context);
    try {
      optional_read_allowance(result.state);
      const auto &limits = result.budget.inventory.limits;
      const auto max_name = std::min<std::uint64_t>(
          {limits.max_string_bytes, limits.max_metadata_bytes,
           limits.max_directory_bytes, limits.max_layout_bytes,
           std::numeric_limits<std::size_t>::max() - 1,
           UINT64_MAX - sizeof(Hdf5Attribute) - 64});
      const auto length = strnlen(name, static_cast<std::size_t>(max_name) + 1);
      if (!length || length > max_name) {
        result.reason = "attribute_name_unavailable";
        return 1;
      }
      if (!result.budget.reserve(sizeof(Hdf5Attribute) + length + 64)) {
        result.reason = "metadata_byte_limit";
        return 1;
      }
      std::string value(name, length);
      if (!valid_hdf5_metadata_text(value)) {
        result.reason = "invalid_attribute_name";
        return 1;
      }
      result.values.push_back(std::move(value));
      ++result.budget.attributes;
      return result.values.size() == result.cap ? 1 : 0;
    } catch (...) {
      result.exception = std::current_exception();
      return 1;
    }
  };
  optional_read_allowance(state);
  hsize_t index = 0;
  // Native order walks the existing name-hash B-tree. INC/DEC first builds and
  // sorts a table of EVERY dense attribute, defeating bounded collection.
  const auto status = H5Aiterate2(object_id, H5_INDEX_NAME, H5_ITER_NATIVE,
                                  &index, callback, &names);
  if (names.exception)
    std::rethrow_exception(names.exception);
  if (!optional_native(status, state))
    names.reason = "attribute_iteration_failed";
  if (names.values.size() != info.num_attrs || names.reason) {
    object.attributes_complete = false;
    object.attributes_unavailable_reason =
        names.reason ? names.reason : "attribute_count_limit";
  }
  std::sort(names.values.begin(), names.values.end());
  for (auto &name : names.values) {
    optional_read_allowance(state);
    Id attr(H5Aopen(object_id, name.c_str(), H5P_DEFAULT), H5Aclose);
    if (!optional_native(attr.value, state)) {
      object.attributes_complete = false;
      object.attributes_unavailable_reason = "attribute_open_failed";
      break;
    }
    Hdf5Attribute value;
    value.name = std::move(name);
    inventory_attribute_value(attr, state, value, budget);
    object.attributes.push_back(std::move(value));
  }
}
void inventory_scale_candidates(
    hid_t object_id, State &state, Hdf5LayoutObject &object,
    const Hdf5LayoutDocument &document,
    const std::map<std::string, std::size_t> &directory_indices,
    MetadataBudget &budget) {
  if (!object.dataset)
    return;
  const auto unavailable = [&](const char *reason) {
    object.dimension_scales_complete = false;
    object.dimension_scales_unavailable_reason = reason;
  };
  if (!object.attributes_complete) {
    unavailable("attribute_directory_incomplete");
    return;
  }
  const auto cls =
      std::find_if(object.attributes.begin(), object.attributes.end(),
                   [](const auto &a) { return a.name == "CLASS"; });
  if (cls == object.attributes.end())
    return;
  if (cls->state != Hdf5AttributeState::Complete ||
      cls->type != LogicalTypeId::Utf8 || !cls->shape.empty() ||
      cls->null_dataspace || cls->strings.size() != 1) {
    unavailable("dimension_scale_class_unavailable");
    return;
  }
  if (cls->strings.front() != "DIMENSION_SCALE")
    return;
  optional_read_allowance(state);
  const auto present = H5Aexists(object_id, "REFERENCE_LIST");
  if (!optional_native(present, state)) {
    unavailable("dimension_scale_reference_unavailable");
    return;
  }
  if (!present)
    return;
  Id attr(H5Aopen(object_id, "REFERENCE_LIST", H5P_DEFAULT), H5Aclose);
  if (!optional_native(attr.value, state)) {
    unavailable("dimension_scale_reference_unavailable");
    return;
  }
  Id type(H5Aget_type(attr), H5Tclose), space(H5Aget_space(attr), H5Sclose);
  if (!optional_native(type.value, state) ||
      !optional_native(space.value, state) ||
      H5Tget_class(type) != H5T_COMPOUND || H5Tget_nmembers(type) != 2 ||
      H5Sget_simple_extent_ndims(space) != 1) {
    unavailable("unsupported_dimension_scale_reference_type");
    return;
  }
  const auto ref_index = H5Tget_member_index(type, "dataset"),
             dim_index = H5Tget_member_index(type, "dimension");
  if (ref_index < 0 || dim_index < 0) {
    unavailable("unsupported_dimension_scale_reference_type");
    return;
  }
  Id reference(H5Tget_member_type(type, ref_index), H5Tclose),
      dimension(H5Tget_member_type(type, dim_index), H5Tclose);
  if (!optional_native(reference.value, state) ||
      !optional_native(dimension.value, state) ||
      H5Tequal(reference, H5T_STD_REF_OBJ) <= 0 ||
      H5Tget_class(dimension) != H5T_INTEGER || H5Tget_size(dimension) != 4 ||
      H5Tget_sign(dimension) != H5T_SGN_NONE ||
      H5Tget_precision(dimension) != 32 || H5Tget_offset(dimension) != 0) {
    unavailable("unsupported_dimension_scale_reference_type");
    return;
  }
  const auto points = H5Sget_simple_extent_npoints(space);
  const auto &l = budget.inventory.limits;
  struct Reference {
    hobj_ref_t dataset;
    unsigned dimension;
  };
  if (!optional_native(points, state)) {
    unavailable("invalid_dimension_scale_reference_count");
    return;
  }
  const auto count = static_cast<std::uint64_t>(points);
  if (count > l.max_attribute_elements ||
      budget.candidates > l.max_dimension_scale_candidates ||
      count > l.max_dimension_scale_candidates - budget.candidates ||
      (count && (H5Tget_size(type) > l.max_attribute_bytes / count ||
                 sizeof(Reference) > l.max_attribute_bytes / count)) ||
      H5Aget_storage_size(attr) > l.max_attribute_bytes) {
    checked(0, state, "cannot inspect dimension-scale attribute storage");
    unavailable("dimension_scale_reference_limit");
    return;
  }
  if (!budget.reserve(count * sizeof(Reference))) {
    unavailable("metadata_byte_limit");
    return;
  }
  Id memory(H5Tcreate(H5T_COMPOUND, sizeof(Reference)), H5Tclose);
  checked(memory.value, state, "cannot create dimension scale memory type",
          QueryError::Internal);
  checked(H5Tinsert(memory, "dataset", offsetof(Reference, dataset),
                    H5T_STD_REF_OBJ),
          state, "cannot configure dimension scale reference type",
          QueryError::Internal);
  checked(H5Tinsert(memory, "dimension", offsetof(Reference, dimension),
                    H5T_NATIVE_UINT),
          state, "cannot configure dimension scale ordinal type",
          QueryError::Internal);
  std::vector<Reference> references(static_cast<std::size_t>(count));
  if (count &&
      !optional_native(H5Aread(attr, memory, references.data()), state)) {
    unavailable("invalid_dimension_scale_references");
    return;
  }
  std::set<std::pair<std::string, std::uint32_t>> seen;
  for (const auto &ref : references) {
    optional_read_allowance(state);
    // Only legacy local object references are admitted; modern references may
    // open external files and were rejected by the exact type test above.
    Id target(H5Rdereference2(object_id, H5P_DEFAULT, H5R_OBJECT, &ref.dataset),
              H5Oclose);
    if (!optional_native(target.value, state)) {
      unavailable("invalid_dimension_scale_target");
      continue;
    }
    H5O_info2_t info{};
    if (!optional_native(H5Oget_info3(target, &info, H5O_INFO_BASIC), state) ||
        info.type != H5O_TYPE_DATASET) {
      unavailable("invalid_dimension_scale_target");
      continue;
    }
    const auto id = inventory_token(target, info.token, state, l);
    const auto found = directory_indices.find(id);
    if (found == directory_indices.end() ||
        !document.objects[found->second].dataset ||
        ref.dimension >=
            document.objects[found->second].dataset->descriptor.shape.size()) {
      unavailable("dimension_scale_target_outside_directory");
      continue;
    }
    if (!seen.emplace(id, ref.dimension).second) {
      unavailable("duplicate_dimension_scale_reference");
      continue;
    }
    if (!budget.reserve(sizeof(Hdf5DimensionScaleCandidate) + id.size())) {
      unavailable("metadata_byte_limit");
      break;
    }
    ++budget.candidates;
    object.dimension_scales.push_back({id, ref.dimension});
  }
}
Hdf5DatasetLayout inventory_dataset(hid_t file, State &state,
                                    const std::string &path,
                                    InventoryBudget &budget) {
  Id dataset(H5Dopen2(file, path.c_str(), H5P_DEFAULT), H5Dclose);
  checked(dataset.value, state, "cannot inspect inventoried HDF5 dataset");
  Id dcpl(H5Dget_create_plist(dataset), H5Pclose);
  checked(dcpl.value, state, "cannot inspect dataset creation properties");
  auto profile = inspect(dataset, dcpl, state, path, false);
  Hdf5DatasetLayout result;
  result.descriptor = std::move(profile.descriptor);
  if (!make_hdf5_value_field(path).ok()) {
    result.descriptor.supported = false;
    result.descriptor.unsupported_reason =
        "dataset path is outside the canonical query path contract";
  }
  if (result.descriptor.supported) {
    try {
      product(result.descriptor.shape,
              result.descriptor.value_type == LogicalTypeId::Float32 ? 4
              : result.descriptor.value_type == LogicalTypeId::Int32 ? 2
                                                                     : 8,
              std::numeric_limits<std::uint64_t>::max());
    } catch (const Failure &error) {
      if (error.status.code() != QueryError::ResourceExhausted)
        throw;
      result.descriptor.supported = false;
      result.descriptor.unsupported_reason =
          "HDF5 logical array byte count overflows UInt64";
    }
  }
  budget.directory(sizeof(Hdf5DatasetLayout) +
                   result.descriptor.shape.size() * sizeof(std::uint64_t));
  budget.string(result.descriptor.path);
  budget.string(result.descriptor.unsupported_reason);
  switch (profile.layout) {
  case H5D_CONTIGUOUS:
    result.storage = Hdf5StorageLayout::Contiguous;
    break;
  case H5D_CHUNKED:
    result.storage = Hdf5StorageLayout::Chunked;
    break;
  case H5D_COMPACT:
    result.storage = Hdf5StorageLayout::Compact;
    break;
  case H5D_VIRTUAL:
    result.storage = Hdf5StorageLayout::Virtual;
    break;
  default:
    result.storage = Hdf5StorageLayout::Other;
    break;
  }
  Id type(H5Dget_type(dataset), H5Tclose);
  checked(type.value, state, "cannot inspect dataset datatype");
  const auto type_class = H5Tget_class(type);
  checked(type_class, state, "cannot inspect datatype class");
  result.native_type_class = static_cast<std::uint32_t>(type_class);
  result.element_bytes = H5Tget_size(type);
  if (!result.element_bytes)
    throw Failure{
        failure(QueryError::CorruptPayload, "HDF5 datatype has zero size")};
  const auto order = H5Tget_order(type);
  result.byte_order = order == H5T_ORDER_LE   ? Hdf5ByteOrder::Little
                      : order == H5T_ORDER_BE ? Hdf5ByteOrder::Big
                                              : Hdf5ByteOrder::Other;
  result.fill_status =
      profile.fill == H5D_FILL_VALUE_UNDEFINED ? Hdf5FillStatus::Undefined
      : profile.fill == H5D_FILL_VALUE_DEFAULT ? Hdf5FillStatus::Default
                                               : Hdf5FillStatus::Explicit;
  if (profile.fill != H5D_FILL_VALUE_UNDEFINED &&
      (result.descriptor.value_type == LogicalTypeId::Float32 ||
       result.descriptor.value_type == LogicalTypeId::Float64 ||
       result.descriptor.value_type == LogicalTypeId::Int32)) {
    result.fill_value.resize(
        result.descriptor.value_type == LogicalTypeId::Float32 ? 4
        : result.descriptor.value_type == LogicalTypeId::Int32 ? 2
                                                               : 8);
    checked(H5Pget_fill_value(dcpl,
                              result.fill_value.size() == 4   ? H5T_IEEE_F32LE
                              : result.fill_value.size() == 2 ? H5T_STD_I16LE
                                                              : H5T_IEEE_F64LE,
                              result.fill_value.data()),
            state, "cannot inspect canonical HDF5 fill value");
  }
  H5D_alloc_time_t allocation_time{};
  H5D_space_status_t allocation_status{};
  checked(H5Pget_alloc_time(dcpl, &allocation_time), state,
          "cannot inspect allocation time");
  checked(H5Dget_space_status(dataset, &allocation_status), state,
          "cannot inspect allocation status");
  result.allocation_time = static_cast<std::uint32_t>(allocation_time);
  result.allocation_status = static_cast<std::uint32_t>(allocation_status);
  result.fill_time = static_cast<std::uint32_t>(profile.fill_time);
  const auto filters = H5Pget_nfilters(dcpl);
  checked(filters, state, "cannot inspect filter count");
  for (int i = 0; i < filters; ++i) {
    unsigned flags = 0;
    std::size_t count = 0;
    auto id =
        H5Pget_filter2(dcpl, i, &flags, &count, nullptr, 0, nullptr, nullptr);
    checked(id, state, "cannot inspect filter parameter count");
    if (count > budget.limits.max_filter_parameters) {
      throw Failure{failure(QueryError::ResourceExhausted,
                            "HDF5 inventory filter parameter limit exhausted")};
    }
    budget.directory(sizeof(Hdf5FilterDescription) +
                     count * sizeof(std::uint32_t));
    std::vector<unsigned> parameters(count);
    auto supplied = count;
    auto repeated = H5Pget_filter2(dcpl, i, &flags, &supplied,
                                   parameters.data(), 0, nullptr, nullptr);
    checked(repeated, state, "cannot inspect filter parameters");
    if (repeated != id || supplied != count)
      throw Failure{
          failure(QueryError::CorruptPayload, "HDF5 filter metadata changed")};
    result.filters.push_back(
        {static_cast<std::uint32_t>(id), flags,
         std::vector<std::uint32_t>(parameters.begin(), parameters.end())});
  }
  if (profile.layout == H5D_CONTIGUOUS) {
    const auto external = H5Pget_external_count(dcpl);
    checked(external, state, "cannot inspect external storage declaration");
    if (external) {
      result.index_unavailable_reason = result.descriptor.unsupported_reason;
      return result;
    }
    // HDF5 1.14.6 H5Dget_offset adds the user-block base even to
    // HADDR_UNDEF; use allocation metadata before interpreting that address.
    const auto offset = allocation_status == H5D_SPACE_STATUS_NOT_ALLOCATED
                            ? HADDR_UNDEF
                            : H5Dget_offset(dataset);
    const auto size = H5Dget_storage_size(dataset);
    if (offset != HADDR_UNDEF) {
      if (offset > state.size || size > state.size - offset)
        throw Failure{failure(QueryError::CorruptPayload,
                              "HDF5 contiguous allocation exceeds source")};
      result.contiguous_offset = offset;
      result.contiguous_size = size;
    } else if (size)
      throw Failure{
          failure(QueryError::CorruptPayload,
                  "unallocated HDF5 contiguous storage has nonzero size")};
    result.index_complete = result.descriptor.supported;
  } else if (profile.layout == H5D_CHUNKED) {
    unsigned chunk_options = 0;
    checked(H5Pget_chunk_opts(dcpl, &chunk_options), state,
            "cannot inspect HDF5 chunk options");
    result.chunk_options = chunk_options;
    if (chunk_options & ~H5D_CHUNK_DONT_FILTER_PARTIAL_CHUNKS) {
      result.descriptor.supported = false;
      result.descriptor.unsupported_reason = "unsupported HDF5 chunk options";
    }
    result.chunk_shape = std::move(profile.chunks);
    if (result.descriptor.supported) {
      try {
        product(result.chunk_shape, result.element_bytes,
                std::numeric_limits<std::uint64_t>::max());
      } catch (const Failure &error) {
        if (error.status.code() != QueryError::ResourceExhausted)
          throw;
        result.descriptor.supported = false;
        result.descriptor.unsupported_reason =
            "HDF5 logical chunk byte count overflows UInt64";
      }
    }
    budget.directory(result.chunk_shape.size() * sizeof(std::uint64_t));
    Id space(H5Dget_space(dataset), H5Sclose);
    checked(space.value, state, "cannot inspect chunk dataspace");
    hsize_t allocated = 0;
    checked(H5Dget_num_chunks(dataset, space, &allocated), state,
            "cannot count allocated HDF5 chunks");
    result.allocated_chunk_count = allocated;
    if (result.descriptor.supported) {
      if (allocated > budget.limits.max_chunks ||
          !budget.chunks(allocated, result.chunk_shape.size())) {
        result.index_unavailable_reason =
            "resource_limit: allocated chunk index";
        return result;
      }
      result.chunks.reserve(static_cast<std::size_t>(allocated));
      struct ChunkIteration {
        State &state;
        Hdf5DatasetLayout &result;
        std::exception_ptr error;
        static herr_t visit(const hsize_t *coordinates, unsigned mask,
                            haddr_t offset, hsize_t encoded,
                            void *data) noexcept {
          auto &context = *static_cast<ChunkIteration *>(data);
          try {
            auto &state = context.state;
            auto &result = context.result;
            check_cancel(state);
            if (result.chunks.size() >= result.allocated_chunk_count)
              throw Failure{failure(QueryError::CorruptPayload,
                                    "allocated HDF5 chunk count changed")};
            if (offset == HADDR_UNDEF || !encoded || offset > state.size ||
                encoded > state.size - offset)
              throw Failure{failure(QueryError::CorruptPayload,
                                    "invalid allocated HDF5 chunk byte range")};
            for (std::size_t axis = 0; axis < result.chunk_shape.size(); ++axis)
              if (!result.chunk_shape[axis] ||
                  coordinates[axis] >= result.descriptor.shape[axis] ||
                  coordinates[axis] % result.chunk_shape[axis])
                throw Failure{
                    failure(QueryError::CorruptPayload,
                            "invalid allocated HDF5 chunk coordinate")};
            if (result.filters.size() < 32 && (mask >> result.filters.size()))
              throw Failure{failure(QueryError::CorruptPayload,
                                    "invalid HDF5 chunk filter mask")};
            result.chunks.push_back(
                {std::vector<std::uint64_t>(
                     coordinates, coordinates + result.chunk_shape.size()),
                 offset, encoded, mask});
            return 0;
          } catch (...) {
            context.error = std::current_exception();
            return -1;
          }
        }
      } iteration{state, result, {}};
      // HDF5 1.14.6 adds H5F_BASE_ADDR before invoking this callback, so
      // addresses remain absolute even in files with a user block. Unlike
      // repeated H5Dget_chunk_info(index), this walks the native index once.
      const auto iterated = H5Dchunk_iter(dataset, H5P_DEFAULT,
                                          ChunkIteration::visit, &iteration);
      if (iteration.error)
        std::rethrow_exception(iteration.error);
      checked(iterated, state, "cannot iterate allocated HDF5 chunks");
      if (result.chunks.size() != result.allocated_chunk_count)
        throw Failure{failure(QueryError::CorruptPayload,
                              "allocated HDF5 chunk count is incomplete")};
      std::sort(
          result.chunks.begin(), result.chunks.end(),
          [](const auto &a, const auto &b) { return a.origin < b.origin; });
      for (std::size_t i = 1; i < result.chunks.size(); ++i)
        if (result.chunks[i - 1].origin == result.chunks[i].origin)
          throw Failure{failure(QueryError::CorruptPayload,
                                "duplicate allocated HDF5 chunk coordinate")};
      result.index_complete = true;
    }
  }
  if (!result.index_complete && result.index_unavailable_reason.empty())
    result.index_unavailable_reason =
        result.descriptor.unsupported_reason.empty()
            ? "unsupported storage profile"
            : result.descriptor.unsupported_reason;
  return result;
}
} // namespace

QueryResult<Hdf5LayoutDocument>
Hdf5SourceDriver::inventory(const Hdf5LayoutLimits &limits) const {
  auto lock = native_library_lock();
  QueryProfileTimer timer("hdf5.native.inventory");
  auto &state = *impl_->state;
  const auto original_read_budget = state.options.max_read_bytes;
  struct Restore {
    State &state;
    std::uint64_t bytes;
    ~Restore() { state.options.max_read_bytes = bytes; }
  } restore{state, original_read_budget};
  const auto marker = [&](const std::string &reason) {
    Hdf5LayoutDocument doc;
    doc.source_size = state.size;
    doc.directory_complete = false;
    doc.index_unavailable_reason = reason;
    return QueryResult<Hdf5LayoutDocument>::success(std::move(doc));
  };
  try {
    if (!limits.max_layout_bytes || !limits.max_directory_bytes ||
        !limits.max_read_bytes || !limits.max_objects || !limits.max_links ||
        !limits.max_string_bytes)
      throw Failure{
          failure(QueryError::InvalidQuery, "invalid HDF5 inventory limits")};
    environment_policy();
    ErrorScope errors;
    policy();
    state.error = QueryStatus::success();
    state.fallback = QueryError::None;
    check_cancel(state);
    const auto consumed =
        state.trace.metadata_bytes + state.trace.raw_data_bytes;
    if (limits.max_read_bytes <=
        std::numeric_limits<std::uint64_t>::max() - consumed)
      state.options.max_read_bytes =
          std::min(original_read_budget, consumed + limits.max_read_bytes);
    Hdf5LayoutDocument document;
    document.source_size = state.size;
    InventoryBudget budget{limits};
    budget.directory(sizeof(Hdf5LayoutDocument));
    H5O_info2_t root{};
    checked(H5Oget_info_by_name3(impl_->file, "/", &root, H5O_INFO_BASIC,
                                 H5P_DEFAULT),
            state, "cannot inspect HDF5 root");
    if (root.type != H5O_TYPE_GROUP)
      throw Failure{
          failure(QueryError::CorruptPayload, "HDF5 root is not a group")};
    document.root_object_id =
        inventory_token(impl_->file, root.token, state, limits);
    std::map<std::string, std::size_t> visited;
    std::vector<std::string> object_paths;
    struct Pending {
      std::string path, id;
      std::size_t depth;
    };
    std::deque<Pending> pending;
    auto add_object = [&](const std::string &path, const H5O_info2_t &info,
                          const std::string &token, std::size_t depth) {
      if (visited.count(token))
        return;
      if (document.objects.size() >= limits.max_objects ||
          depth > limits.max_depth)
        throw Failure{failure(QueryError::ResourceExhausted,
                              "HDF5 object/depth limit exhausted")};
      budget.directory(sizeof(Hdf5LayoutObject));
      budget.string(token);
      budget.string(path);
      Hdf5LayoutObject object;
      object.id = token;
      object.canonical_path = path;
      if (info.type == H5O_TYPE_GROUP) {
        object.kind = Hdf5ObjectKind::Group;
        pending.push_back({path, token, depth});
      } else if (info.type == H5O_TYPE_DATASET) {
        object.kind = Hdf5ObjectKind::Dataset;
        object.dataset = inventory_dataset(impl_->file, state, path, budget);
      } else
        object.kind = Hdf5ObjectKind::Other;
      visited.emplace(token, document.objects.size());
      document.objects.push_back(std::move(object));
      object_paths.push_back(path);
    };
    add_object("/", root, document.root_object_id, 0);
    while (!pending.empty()) {
      auto parent = std::move(pending.front());
      pending.pop_front();
      check_cancel(state);
      Id group(H5Gopen2(impl_->file, parent.path.c_str(), H5P_DEFAULT),
               H5Gclose);
      checked(group.value, state, "cannot inspect HDF5 group");
      H5G_info_t group_info{};
      checked(H5Gget_info(group, &group_info), state,
              "cannot inspect HDF5 group link count");
      if (group_info.nlinks > limits.max_links - document.links.size())
        throw Failure{failure(QueryError::ResourceExhausted,
                              "HDF5 link limit exhausted")};
      for (hsize_t index = 0; index < group_info.nlinks; ++index) {
        check_cancel(state);
        const auto length =
            H5Lget_name_by_idx(group, ".", H5_INDEX_NAME, H5_ITER_INC, index,
                               nullptr, 0, H5P_DEFAULT);
        checked(length, state, "cannot inspect HDF5 link name length");
        if (static_cast<std::uint64_t>(length) > limits.max_string_bytes)
          throw Failure{failure(QueryError::ResourceExhausted,
                                "HDF5 link name exceeds string limit")};
        std::vector<char> name(static_cast<std::size_t>(length) + 1);
        const auto actual =
            H5Lget_name_by_idx(group, ".", H5_INDEX_NAME, H5_ITER_INC, index,
                               name.data(), name.size(), H5P_DEFAULT);
        checked(actual, state, "cannot inspect HDF5 link name");
        if (actual != length || !length)
          throw Failure{failure(QueryError::CorruptPayload,
                                "HDF5 link name changed or is empty")};
        H5L_info2_t link_info{};
        checked(H5Lget_info_by_idx2(group, ".", H5_INDEX_NAME, H5_ITER_INC,
                                    index, &link_info, H5P_DEFAULT),
                state, "cannot inspect HDF5 link");
        Hdf5LayoutLink link;
        link.parent_id = parent.id;
        link.name.assign(name.data(), length);
        budget.directory(sizeof(Hdf5LayoutLink));
        budget.string(link.parent_id);
        budget.string(link.name);
        if (link_info.type == H5L_TYPE_HARD) {
          link.kind = Hdf5LinkKind::Hard;
          H5O_info2_t object_info{};
          checked(H5Oget_info_by_name3(group, link.name.c_str(), &object_info,
                                       H5O_INFO_BASIC, H5P_DEFAULT),
                  state, "cannot inspect hard-link target");
          link.target_id =
              inventory_token(impl_->file, object_info.token, state, limits);
          budget.string(link.target_id);
          const auto path = parent.path == "/" ? parent.path + link.name
                                               : parent.path + "/" + link.name;
          if (path.size() > limits.max_string_bytes)
            throw Failure{failure(QueryError::ResourceExhausted,
                                  "HDF5 inventory path exceeds string limit")};
          add_object(path, object_info, link.target_id, parent.depth + 1);
        } else {
          link.kind = link_info.type == H5L_TYPE_SOFT ? Hdf5LinkKind::Soft
                      : link_info.type == H5L_TYPE_EXTERNAL
                          ? Hdf5LinkKind::External
                          : Hdf5LinkKind::Other;
          link.unsupported_reason = "non-hard link is not followed";
          budget.string(link.unsupported_reason);
        }
        document.links.push_back(std::move(link));
      }
    }
    // Optional metadata follows the complete physical inventory and consumes
    // only remaining directory space; its own limits never erase chunk indexes.
    MetadataBudget metadata{budget};
    std::size_t metadata_position = 0;
    bool attribute_pass_complete = false;
    try {
      for (; metadata_position < document.objects.size(); ++metadata_position) {
        auto &entry = document.objects[metadata_position];
        entry.attribute_count_known = false;
        optional_read_allowance(state);
        Id object(H5Oopen(impl_->file, object_paths[metadata_position].c_str(),
                          H5P_DEFAULT),
                  H5Oclose);
        checked(object.value, state, "cannot open HDF5 metadata object");
        inventory_attributes(object, state, entry, metadata);
      }
      attribute_pass_complete = true;
      for (metadata_position = 0; metadata_position < document.objects.size();
           ++metadata_position) {
        if (!document.objects[metadata_position].dataset)
          continue;
        Id object(H5Oopen(impl_->file, object_paths[metadata_position].c_str(),
                          H5P_DEFAULT),
                  H5Oclose);
        checked(object.value, state, "cannot open HDF5 scale candidate object");
        inventory_scale_candidates(object, state,
                                   document.objects[metadata_position],
                                   document, visited, metadata);
      }
    } catch (const Failure &error) {
      if (error.status.code() != QueryError::ResourceExhausted)
        throw;
      // Optional native metadata I/O can exhaust the shared read allowance
      // after all physical records are complete. Preserve that useful index.
      for (std::size_t i = attribute_pass_complete ? metadata_position : 0;
           i < document.objects.size(); ++i) {
        auto &entry = document.objects[i];
        if (!attribute_pass_complete && i >= metadata_position) {
          if (i > metadata_position)
            entry.attribute_count_known = false;
          entry.attributes_complete = false;
          entry.attributes_unavailable_reason = "metadata_read_limit";
        }
        if (entry.dataset) {
          entry.dimension_scales_complete = false;
          entry.dimension_scales_unavailable_reason = "metadata_read_limit";
        }
      }
    }

    // Only a declared semantic profile can request bounded coordinate evidence.
    // Dataset-specific rules remain in format plugins, outside query core.
    const auto profiles = state.options.semantic_profiles.empty()
        ? default_hdf5_semantic_profiles() : state.options.semantic_profiles;
    const Hdf5SemanticProfile *profile = nullptr;
    if (state.options.enable_cf_inventory)
      for (const auto &candidate : profiles)
        if (candidate && candidate->declares(document)) {
          profile = candidate.get();
          break;
        }
    if (profile) {
      Hdf5ScaleWorkerLimits worker_limits;
      worker_limits.max_associations = limits.max_dimension_scale_candidates;
      const auto used = state.trace.metadata_bytes + state.trace.raw_data_bytes;
      worker_limits.max_read_bytes = used < state.options.max_read_bytes
                                         ? state.options.max_read_bytes - used
                                         : 0;
      worker_limits.max_read_calls =
          state.trace.reads.size() < state.options.max_read_calls
              ? state.options.max_read_calls - state.trace.reads.size()
              : 0;
      FormatSource source{
          state.size,
          [&](std::uint64_t offset,
              std::uint64_t length) -> std::optional<BinaryValue> {
            auto data = diagnostic_read_range(offset, length, false);
            if (!data.ok())
              return std::nullopt;
            return data.value();
          }};
      auto associations =
          read_hdf5_dimension_associations_isolated(source, worker_limits);
      if (!associations.ok()) {
        // A failed actual source read remains fatal, unlike optional helper
        // resource exhaustion or unavailable metadata.
        if (!state.error.ok() &&
            state.error.code() != QueryError::ResourceExhausted)
          throw Failure{state.error};
        for (auto &object : document.objects)
          if (object.dataset)
            object.axis_associations_reason = "cf_forward_evidence_unavailable";
      } else {
        std::uint64_t association_bytes = 0;
        for (const auto &a : associations.value())
          association_bytes +=
              sizeof(Hdf5AxisAssociation) + a.scale_id.size() + 24;
        bool valid = metadata.reserve(association_bytes);
        for (const auto &a : associations.value()) {
          auto target = visited.find(a.dataset_id),
               scale = visited.find(a.scale_id);
          valid = valid && target != visited.end() && scale != visited.end();
          if (valid)
            valid = document.objects[target->second].dataset &&
                    document.objects[scale->second].dataset &&
                    a.axis < document.objects[target->second]
                                 .dataset->descriptor.shape.size();
        }
        if (valid) {
          for (auto &object : document.objects)
            if (object.dataset) {
              object.axis_associations_complete = true;
              object.axis_associations_reason.clear();
            }
          for (const auto &a : associations.value())
            document.objects[visited.at(a.dataset_id)]
                .axis_associations.push_back(
                    {a.axis, a.scale_id, "forward_dimension_list/1"});
        } else
          for (auto &object : document.objects)
            if (object.dataset)
              object.axis_associations_reason =
                  "cf_association_limit_or_invalid_target";
      }
      std::uint64_t coordinate_bytes = 0;
      for (auto &object : document.objects) {
        auto candidate = profile->inspect_coordinate(object);
        if (!candidate.ok())
          continue;
        auto index = std::move(candidate.value());
        auto count = object.dataset->descriptor.shape[0];
        object.coordinate_index_reason = "coordinate_read_unavailable";
        if (count > limits.max_coordinate_elements ||
            coordinate_bytes > limits.max_coordinate_bytes ||
            count > (limits.max_coordinate_bytes - coordinate_bytes) / 8 ||
            !metadata.reserve(count * 8)) {
          object.coordinate_index_reason = "coordinate_limit";
          continue;
        }
        auto subset = read(object.dataset->descriptor.path, {0}, {count});
        if (!subset.ok()) {
          // netCDF/GEOS commonly stores its time dimension as signed Int32.
          // It is coordinate evidence, not a supported value dataset: read
          // only this bounded rank-one axis without widening the typed HDF5
          // value contract or marking arbitrary Int32 datasets queryable.
          if (subset.status().code() == QueryError::UnsupportedCapability) {
            Id dapl(H5Pcreate(H5P_DATASET_ACCESS), H5Pclose);
            checked(dapl.value, state, "cannot create coordinate access settings",
                    QueryError::Internal);
            checked(H5Pset_chunk_cache(dapl, 1,
                                       state.options.raw_chunk_cache_bytes, 0),
                    state, "cannot bound coordinate chunk cache",
                    QueryError::Internal);
            Id dataset(H5Dopen2(impl_->file,
                                object.dataset->descriptor.path.c_str(), dapl),
                       H5Dclose);
            checked(dataset.value, state, "cannot open coordinate dataset");
            Id type(H5Dget_type(dataset), H5Tclose);
            checked(type.value, state, "cannot inspect coordinate datatype");
            if (H5Tequal(type, H5T_STD_I32LE) > 0 ||
                H5Tequal(type, H5T_STD_I32BE) > 0) {
              Id dcpl(H5Dget_create_plist(dataset), H5Pclose);
              checked(dcpl.value, state, "cannot inspect coordinate storage");
              const auto layout = H5Pget_layout(dcpl);
              if (layout == H5D_CHUNKED) {
                hsize_t chunk = 0;
                checked(H5Pget_chunk(dcpl, 1, &chunk), state,
                        "cannot inspect coordinate chunk shape");
                if (!chunk ||
                    (count && 1 + (count - 1) / chunk >
                                  state.options.max_read_calls)) {
                  object.coordinate_index_reason = "coordinate_limit";
                  continue;
                }
                for (hsize_t at = 0; at < count; at += chunk) {
                  unsigned mask = 0;
                  haddr_t address = HADDR_UNDEF;
                  hsize_t encoded = 0;
                  checked(H5Dget_chunk_info_by_coord(dataset, &at, &mask,
                                                     &address, &encoded),
                          state, "cannot inspect coordinate chunk");
                  if (address == HADDR_UNDEF || !encoded ||
                      encoded > state.options.max_single_read_bytes) {
                    object.coordinate_index_reason = "coordinate_limit";
                    break;
                  }
                }
                if (object.coordinate_index_reason == "coordinate_limit")
                  continue;
              } else if (layout != H5D_CONTIGUOUS) {
                object.coordinate_index_reason = "coordinate_storage_unsupported";
                continue;
              }
              std::vector<std::int32_t> values(count);
              Id transfer(H5Pcreate(H5P_DATASET_XFER), H5Pclose);
              checked(transfer.value, state,
                      "cannot create coordinate transfer settings",
                      QueryError::Internal);
              checked(H5Pset_buffer(transfer,
                                    state.trace.conversion_buffer_bytes,
                                    nullptr, nullptr),
                      state, "cannot bound coordinate conversion buffer",
                      QueryError::Internal);
              checked(H5Dread(dataset, H5T_NATIVE_INT32, H5S_ALL, H5S_ALL,
                              transfer, values.data()),
                      state, "native HDF5 coordinate read failed");
              index.values.assign(values.begin(), values.end());
            }
          }
          if (index.values.empty()) {
            if (subset.status().code() == QueryError::ReadFailure ||
                subset.status().code() == QueryError::Cancelled)
              throw Failure{subset.status()};
            object.coordinate_index_reason =
                "coordinate_read_unavailable: " + subset.status().message();
            continue;
          }
        }
        if (subset.ok()) {
          if (auto v = std::get_if<std::vector<float>>(
                  &subset.value().values.values))
            index.values.assign(v->begin(), v->end());
          else if (auto v = std::get_if<std::vector<double>>(
                       &subset.value().values.values))
            index.values = *v;
          else if (auto v = std::get_if<std::vector<std::int32_t>>(
                       &subset.value().values.values))
            index.values.assign(v->begin(), v->end());
        }
        bool valid = index.values.size() == count;
        if (count > 1)
          index.ascending = index.values[1] > index.values[0];
        for (std::size_t i = 0; i < index.values.size(); ++i) {
          valid = valid && std::isfinite(index.values[i]);
          if (index.role == "latitude")
            valid = valid && index.values[i] >= -90 && index.values[i] <= 90;
          if (i)
            valid = valid &&
                    (index.ascending ? index.values[i] > index.values[i - 1]
                                     : index.values[i] < index.values[i - 1]);
        }
        if (!valid) {
          object.coordinate_index_reason =
              "coordinate_not_finite_strict_monotone";
          continue;
        }
        coordinate_bytes += count * 8;
        object.coordinate_index = std::move(index);
        object.coordinate_index_reason.clear();
      }
    }
    check_cancel(state);
    return QueryResult<Hdf5LayoutDocument>::success(std::move(document));
  } catch (const Failure &error) {
    if (error.status.code() == QueryError::ResourceExhausted)
      return marker("resource_limit: " + error.status.message());
    return QueryResult<Hdf5LayoutDocument>::failure(error.status);
  } catch (const std::bad_alloc &) {
    return marker("resource_limit");
  } catch (...) {
    return QueryResult<Hdf5LayoutDocument>::failure(
        failure(QueryError::ReadFailure, "HDF5 inventory operation threw"));
  }
}

Hdf5SourceTrace Hdf5SourceDriver::trace() const {
  auto lock = native_library_lock();
  return impl_->state->trace;
}
QueryResult<StructuredBlobBytes>
Hdf5SourceDriver::diagnostic_read_range(std::uint64_t offset,
                                        std::uint64_t length, bool raw) const {
  auto lock = native_library_lock();
  auto &s = *impl_->state;
  try {
    environment_policy();
    ErrorScope errors;
    policy();
    s.error = QueryStatus::success();
    s.fallback = QueryError::None;
    check_cancel(s);
    if (length > s.options.max_single_read_bytes ||
        length > s.options.max_result_bytes)
      throw Failure{failure(QueryError::ResourceExhausted,
                            "diagnostic range exceeds buffer budget")};
    if (!s.active)
      throw Failure{failure(QueryError::Internal, "HDF5 source VFD is closed")};
    StructuredBlobBytes bytes(length);
    checked(read_file(&s.active->base, raw ? H5FD_MEM_DRAW : H5FD_MEM_SUPER,
                      H5P_DEFAULT, offset, length, bytes.data()),
            s, "diagnostic source read failed");
    return QueryResult<StructuredBlobBytes>::success(std::move(bytes));
  } catch (const Failure &f) {
    return QueryResult<StructuredBlobBytes>::failure(f.status);
  } catch (const std::bad_alloc &) {
    return QueryResult<StructuredBlobBytes>::failure(failure(
        QueryError::ResourceExhausted, "diagnostic buffer allocation failed"));
  } catch (...) {
    return QueryResult<StructuredBlobBytes>::failure(
        failure(QueryError::ReadFailure, "diagnostic source read threw"));
  }
}

QueryResult<std::vector<Hdf5DimensionAssociation>>
inspect_hdf5_dimension_associations_native(const FormatSource &source,
                                           std::uint64_t max_associations) {
  using R = QueryResult<std::vector<Hdf5DimensionAssociation>>;
  std::vector<Hdf5DimensionAssociation> output;
  Hdf5SourceOptions options;
  options.enable_cf_inventory = false;
  const auto status = with_hdf5_ingestion_source(
      source, options, [&](const Hdf5SourceDriver &driver) {
        auto lock = native_library_lock();
        ErrorScope errors;
        auto &state = *driver.impl_->state;
        auto inventory = driver.inventory();
        if (!inventory.ok())
          return inventory.status();
        if (!inventory.value().directory_complete)
          return failure(QueryError::ResourceExhausted,
                         "scale worker object directory incomplete");
        try {
          for (const auto &object : inventory.value().objects) {
            if (!object.dataset)
              continue;
            Id dataset(H5Dopen2(driver.impl_->file,
                                object.dataset->descriptor.path.c_str(),
                                H5P_DEFAULT),
                       H5Dclose);
            checked(dataset.value, state, "cannot open scale worker dataset");
            auto exists = H5Aexists(dataset, "DIMENSION_LIST");
            checked(exists, state, "cannot inspect forward dimensions");
            if (!exists)
              continue;
            Id attr(H5Aopen(dataset, "DIMENSION_LIST", H5P_DEFAULT), H5Aclose);
            Id type(H5Aget_type(attr), H5Tclose),
                space(H5Aget_space(attr), H5Sclose);
            checked(type.value, state,
                    "cannot inspect forward dimension datatype");
            checked(space.value, state,
                    "cannot inspect forward dimension dataspace");
            if (H5Tget_class(type) != H5T_VLEN ||
                H5Sget_simple_extent_ndims(space) != 1)
              return failure(QueryError::UnsupportedCapability,
                             "forward dimensions have unsupported datatype");
            Id base(H5Tget_super(type), H5Tclose);
            if (H5Tequal(base, H5T_STD_REF_OBJ) <= 0)
              return failure(
                  QueryError::UnsupportedCapability,
                  "only legacy local scale references are supported");
            auto count = H5Sget_simple_extent_npoints(space);
            if (count < 0 ||
                std::uint64_t(count) !=
                    object.dataset->descriptor.shape.size() ||
                count > 32)
              return failure(
                  QueryError::CorruptPayload,
                  "forward dimension count differs from dataset rank");
            std::vector<hvl_t> refs(static_cast<std::size_t>(count));
            // This API is only used by the spawned, OS-memory-bounded worker.
            // H5Aread may allocate vlen storage before its return: never call
            // this native extractor from an ingestion/query daemon directly.
            const auto read_status = H5Aread(attr, type, refs.data());
            struct Reclaim {
              hid_t type, space;
              void *data;
              ~Reclaim() { H5Treclaim(type, space, H5P_DEFAULT, data); }
            } reclaim{type, space, refs.data()};
            checked(read_status, state, "cannot read forward dimensions");
            for (std::size_t axis = 0; axis < refs.size(); ++axis) {
              if (refs[axis].len > max_associations - output.size())
                return failure(QueryError::ResourceExhausted,
                               "forward association limit");
              auto *references = static_cast<hobj_ref_t *>(refs[axis].p);
              std::set<std::string> seen;
              for (std::size_t i = 0; i < refs[axis].len; ++i) {
                Id target(H5Rdereference2(dataset, H5P_DEFAULT, H5R_OBJECT,
                                          references + i),
                          H5Oclose);
                checked(target.value, state,
                        "cannot dereference local dimension scale");
                H5O_info2_t info{};
                checked(H5Oget_info3(target, &info, H5O_INFO_BASIC), state,
                        "cannot inspect scale identity");
                if (info.type != H5O_TYPE_DATASET)
                  return failure(
                      QueryError::CorruptPayload,
                      "dimension scale reference does not target a dataset");
                const auto id = inventory_token(target, info.token, state, {});
                if (!seen.insert(id).second)
                  return failure(QueryError::CorruptPayload,
                                 "duplicate dimension scale reference");
                output.push_back(
                    {object.id, static_cast<std::uint32_t>(axis), id});
              }
            }
          }
          return QueryStatus::success();
        } catch (const Failure &e) {
          return e.status;
        }
      });
  if (!status.ok())
    return R::failure(status);
  return R::success(std::move(output));
}
} // namespace iotdbfs
