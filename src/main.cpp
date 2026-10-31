#include "iotdbfs/query/core/QueryMetadataBudget.h"
// IoTDB-backed FUSE 文件系统（C++17）
// 功能要点（与当前实现保持一致）：
//   - inode 保存版本 manifest、POSIX 属性、format_id 与 typed metadata；
//     typed set 成员写入通用 blob_metadata_set 供精确 SQL 剪枝。
//     payload B 和 adapter-owned layout L 分别存入独立 Structured Blob 表。
//   - 每个 GRIB 文件根据 mount 请求中的查询参数暴露虚拟视图：
//       ?function=read,...?function=crop,...?function=interp,...
//         —— 先按 read 段选择 message，再可选 crop/interp，对输出做裁剪/插值/类型转换。
//     未携带查询参数时仅访问原始文件本体。
//   - 写句柄共享匿名磁盘暂存；通用 Ingestion Module 按块提交 payload，
//     派生 adapter-private layout/typed metadata 后发布不可变版本。
//   - 虚拟视图（binary/data）在写入期间被禁用，正常读取走 READ_OBJECT 分片 +
//     LRU 缓存；decoded data 利用 DecodedDataCache 复用裁剪/插值结果。

#define FUSE_USE_VERSION 26

#include "SessionDataSet.h"
#include "TableSession.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <fuse.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cctype>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include "iotdbfs/Filesystem.h"
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iotdbfs/InodeCache.h"
#include "iotdbfs/InodeTypes.h"
#include "iotdbfs/InodeRowParser.h"
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/formats/grib/EcCodesRuntime.h"
#include "iotdbfs/FeatureConfig.h"
#include "iotdbfs/FormatRegistry.h"
#include "iotdbfs/FuseSemantics.h"
#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/formats/grib/GribFormatAdapter.h"
#include "iotdbfs/Ingestion.h"
#include "iotdbfs/NamespaceWriterLock.h"
#include "iotdbfs/query/frontend/CatalogSourceAuthorization.h"
#include "iotdbfs/query/frontend/TypedAuthorizationCache.h"
#include "iotdbfs/IoTDBProfile.h"
#include "iotdbfs/PublicationProfile.h"
#include "iotdbfs/migration/MigrationCli.h"
#include "iotdbfs/migration/ArrowReingestionPolicy.h"
#include "iotdbfs/migration/Hdf5ReingestionPolicy.h"
#include "iotdbfs/Logging.h"
#include "iotdbfs/MetadataSerialization.h"
#include "iotdbfs/PathUtils.h"
#include "iotdbfs/StagedFile.h"
#include "iotdbfs/Platform.h"
#include "iotdbfs/QueryUtils.h"
#include "iotdbfs/CrossFileClient.h"
#include "iotdbfs/SemanticGrepClient.h"
#include "iotdbfs/formats/arrow/ArrowFormatQueryAdapter.h"
#include "iotdbfs/formats/grib/GribFormatQueryAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatQueryAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5SourceDriver.h"
#include "iotdbfs/formats/hdf5/Hdf5QueryMode.h"
#include "iotdbfs/query/core/QueryModule.h"
#include "iotdbfs/query/core/QueryRuntime.h"
#include "iotdbfs/query/frontend/FuseTypedQueryBridge.h"
#include "iotdbfs/query/frontend/QueryRegistrationSocket.h"
#include "iotdbfs/query/frontend/TypedQueryFrontend.h"
#include "iotdbfs/query/frontend/TypedQueryService.h"
#include "iotdbfs/query/frontend/TypedResultStore.h"
#include "iotdbfs/query/core/QueryProfile.h"

using path_utils::base;
using path_utils::norm;
using path_utils::parent_of;
using path_utils::split_path_and_query;
using path_utils::to_lower;

using namespace std;

static void ensure_inode_cache_configured();
static std::optional<Inode> load_inode_from_db(const std::string& path,
  std::optional<long long> row_time);
static bool insert_inode(const Inode& input);
static bool is_path_in_writing(const std::string& path);
static std::optional<std::string> read_layout_text(long long row_time,
  const std::string& path);
static bool delete_inode_by_path(const std::string& path,
  std::optional<long long> row_time);
static std::optional<Inode> get_inode(const std::string& path);

static std::string join_path(const std::string& parent,
  const std::string& name) {
  if (parent.empty() || parent == "/")
    return "/" + name;
  return parent + "/" + name;
}

struct VersionPathInfo {
  bool is_version{ false };
  std::string base_path;
  long long row_time{ 0 };
  std::string version_name;
};

static pair<string, string> split_name_suffix(const string& filename) {
  auto pos = filename.rfind('.');
  if (pos == string::npos || pos == 0)
    return { filename, string() };
  return { filename.substr(0, pos), filename.substr(pos + 1) };
}

static bool is_all_digits(const std::string& s) {
  return !s.empty() &&
    std::all_of(s.begin(), s.end(), [](unsigned char c) { return std::isdigit(c); });
}

static VersionPathInfo parse_version_path(const std::string& path) {
  VersionPathInfo info;
  std::string npath = norm(path);
  info.base_path = npath;
  info.version_name = base(npath);
  std::string fname = base(npath);
  if (fname.size() <= 1 || fname[0] != '.')
    return info;

  std::string rest = fname.substr(1);
  auto dot_pos = rest.rfind('.');
  std::string suffix = dot_pos == std::string::npos ? "" : rest.substr(dot_pos + 1);
  std::string body = dot_pos == std::string::npos ? rest : rest.substr(0, dot_pos);
  auto under_pos = body.rfind('_');
  if (under_pos == std::string::npos)
    return info;

  std::string ts_str = body.substr(under_pos + 1);
  if (!is_all_digits(ts_str))
    return info;

  std::string orig_stem = body.substr(0, under_pos);
  std::string orig_name = orig_stem;
  if (!suffix.empty())
    orig_name += "." + suffix;
  std::string parent = parent_of(npath);

  info.is_version = true;
  try {
    info.row_time = std::stoll(ts_str);
  }
  catch (...) {
    info.is_version = false;
    return info;
  }
  info.base_path = norm(join_path(parent, orig_name));
  info.version_name = fname;
  return info;
}

static std::string build_version_name(const std::string& base_name,
  const std::string& suffix, long long row_time) {
  std::ostringstream os;
  os << "." << base_name << "_" << row_time;
  if (!suffix.empty())
    os << "." << suffix;
  return os.str();
}

static optional<Inode> load_inode_version_aware(const std::string& path) {
  auto info = parse_version_path(path);
  auto ino = load_inode_from_db(
    info.base_path,
    info.is_version ? optional<long long>(info.row_time) : nullopt);
  if (ino && info.is_version)
    ino->name = info.version_name;
  return ino;
}

static long long next_version_time(long long previous = 0) {
  static std::mutex mu;
  static long long last = 0;
  std::lock_guard<std::mutex> lk(mu);
  last = std::max({now_ms(), previous + 1, last + 1});
  return last;
}

static optional<Inode> get_inode_maybe_version(const std::string& path) {
  auto info = parse_version_path(path);
  if (info.is_version)
    return load_inode_version_aware(path);
  return get_inode(path);
}


#ifndef USE_PREPARED
#define USE_PREPARED 0
#endif

class DecodedDataCache {
  struct Entry {
    std::string key;
    std::shared_ptr<std::vector<uint8_t>> data;
    size_t size{ 0 };
  };

public:
  static DecodedDataCache& instance() {
    static DecodedDataCache inst;
    return inst;
  }

  void set_max_bytes(size_t bytes) {
    std::lock_guard<std::mutex> lk(mu_);
    if (bytes == 0)
      bytes = 64ull * 1024 * 1024;
    max_bytes_ = bytes;
    prune(0);
  }

  std::shared_ptr<const std::vector<uint8_t>> get(const std::string& key) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = map_.find(key);
    if (it == map_.end())
      return nullptr;
    lru_.splice(lru_.begin(), lru_, it->second);
    return std::static_pointer_cast<const std::vector<uint8_t>>(it->second->data);
  }

  std::shared_ptr<const std::vector<uint8_t>> put(const std::string& key,
    std::vector<uint8_t>&& data) {
    auto ptr = std::make_shared<std::vector<uint8_t>>(std::move(data));
    return insert(key, std::move(ptr));
  }

  std::shared_ptr<const std::vector<uint8_t>> put(const std::string& key,
    std::shared_ptr<std::vector<uint8_t>> ptr) {
    return insert(key, std::move(ptr));
  }

private:
  std::shared_ptr<const std::vector<uint8_t>> insert(const std::string& key,
    std::shared_ptr<std::vector<uint8_t>> ptr) {
    if (!ptr)
      return nullptr;
    size_t sz = ptr->size();
    auto shared = std::static_pointer_cast<const std::vector<uint8_t>>(ptr);
    if (sz == 0)
      return shared;

    std::lock_guard<std::mutex> lk(mu_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      cur_bytes_ -= it->second->size;
      lru_.erase(it->second);
      map_.erase(it);
    }

    if (sz > max_bytes_) {
      return shared;
    }

    prune(sz);

    lru_.push_front(Entry{ key, std::move(ptr), sz });
    map_[key] = lru_.begin();
    cur_bytes_ += sz;
    return shared;
  }

  void prune(size_t incoming) {
    while (!lru_.empty() && cur_bytes_ + incoming > max_bytes_) {
      auto& back = lru_.back();
      cur_bytes_ -= back.size;
      map_.erase(back.key);
      lru_.pop_back();
    }
  }

  std::list<Entry> lru_;
  std::unordered_map<std::string, std::list<Entry>::iterator> map_;
  size_t max_bytes_{ 64ull * 1024 * 1024 };
  size_t cur_bytes_{ 0 };
  std::mutex mu_;
};

static std::string make_file_cache_key(long long row_time,
  const std::string& path) {
  return std::to_string(row_time) + "|" + norm(path);
}

class RawFileCache {
  struct Entry {
    std::string key;
    std::shared_ptr<std::vector<uint8_t>> data;
    size_t size{ 0 };
  };

  struct PendingLoad {
    bool ready{ false };
    std::shared_ptr<const std::vector<uint8_t>> data;
    std::condition_variable cv;
  };

public:
  static RawFileCache& instance() {
    static RawFileCache inst;
    return inst;
  }

  void set_max_bytes(size_t bytes) {
    std::lock_guard<std::mutex> lk(mu_);
    if (bytes == 0)
      bytes = 256ull * 1024 * 1024;
    max_bytes_ = bytes;
    prune(0);
  }

  std::shared_ptr<const std::vector<uint8_t>> get(long long row_time,
    const std::string& path) {
    return get(make_file_cache_key(row_time, path));
  }

  std::shared_ptr<const std::vector<uint8_t>> get(const std::string& key) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = map_.find(key);
    if (it == map_.end())
      return nullptr;
    lru_.splice(lru_.begin(), lru_, it->second);
    return std::static_pointer_cast<const std::vector<uint8_t>>(it->second->data);
  }

  std::shared_ptr<const std::vector<uint8_t>> put(long long row_time,
    const std::string& path, std::vector<uint8_t>&& data) {
    return put(make_file_cache_key(row_time, path), std::move(data));
  }

  std::shared_ptr<const std::vector<uint8_t>> put(const std::string& key,
    std::vector<uint8_t>&& data) {
    auto ptr = std::make_shared<std::vector<uint8_t>>(std::move(data));
    return insert(key, std::move(ptr));
  }

  std::shared_ptr<const std::vector<uint8_t>> put(const std::string& key,
    std::shared_ptr<std::vector<uint8_t>> ptr) {
    return insert(key, std::move(ptr));
  }

  template<typename Loader>
  std::shared_ptr<const std::vector<uint8_t>> get_or_load(
    const std::string& key, Loader&& loader) {
    std::shared_ptr<PendingLoad> pending;
    {
      std::unique_lock<std::mutex> lk(mu_);
      auto it = map_.find(key);
      if (it != map_.end()) {
        lru_.splice(lru_.begin(), lru_, it->second);
        return std::static_pointer_cast<const std::vector<uint8_t>>(it->second->data);
      }
      auto pit = pending_.find(key);
      if (pit != pending_.end()) {
        pending = pit->second;
        pending->cv.wait(lk, [&] { return pending->ready; });
        auto cached = map_.find(key);
        if (cached != map_.end()) {
          lru_.splice(lru_.begin(), lru_, cached->second);
          return std::static_pointer_cast<const std::vector<uint8_t>>(cached->second->data);
        }
        return pending->data;
      }
      pending = std::make_shared<PendingLoad>();
      pending_[key] = pending;
    }

    std::shared_ptr<const std::vector<uint8_t>> loaded;
    try {
      loaded = loader();
    }
    catch (const std::exception& error) {
      LOG_WARN(std::string("raw cache load failed: ") + error.what());
    }
    catch (...) {
      LOG_WARN("raw cache load failed: unknown error");
    }

    {
      std::lock_guard<std::mutex> lk(mu_);
      pending->data = loaded;
      pending->ready = true;
      pending_.erase(key);
    }
    pending->cv.notify_all();
    return loaded;
  }

  void invalidate(long long row_time, const std::string& path) {
    invalidate(make_file_cache_key(row_time, path));
  }

  void invalidate(const std::string& key) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = map_.find(key);
    if (it == map_.end())
      return;
    cur_bytes_ -= it->second->size;
    lru_.erase(it->second);
    map_.erase(it);
  }

private:
  std::shared_ptr<const std::vector<uint8_t>> insert(const std::string& key,
    std::shared_ptr<std::vector<uint8_t>> ptr) {
    if (!ptr)
      return nullptr;
    size_t sz = ptr->size();
    auto shared = std::static_pointer_cast<const std::vector<uint8_t>>(ptr);
    if (sz == 0)
      return shared;

    std::lock_guard<std::mutex> lk(mu_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      cur_bytes_ -= it->second->size;
      lru_.erase(it->second);
      map_.erase(it);
    }

    if (sz > max_bytes_) {
      return shared;
    }

    prune(sz);

    lru_.push_front(Entry{ key, std::move(ptr), sz });
    map_[key] = lru_.begin();
    cur_bytes_ += sz;
    return shared;
  }

  void prune(size_t incoming) {
    while (!lru_.empty() && cur_bytes_ + incoming > max_bytes_) {
      auto& back = lru_.back();
      cur_bytes_ -= back.size;
      map_.erase(back.key);
      lru_.pop_back();
    }
  }

  std::list<Entry> lru_;
  std::unordered_map<std::string, std::list<Entry>::iterator> map_;
  std::unordered_map<std::string, std::shared_ptr<PendingLoad>> pending_;
  size_t max_bytes_{ 256ull * 1024 * 1024 };
  size_t cur_bytes_{ 0 };
  std::mutex mu_;
};

class LayoutCache {
  struct TextEntry {
    std::string text;
    std::chrono::steady_clock::time_point last_access;
  };

public:
  static LayoutCache& instance() {
    static LayoutCache inst;
    return inst;
  }

  std::optional<std::string> get_text(long long row_time,
    const std::string& path) {
    const std::string key = file_key(row_time, path);
    std::lock_guard<std::mutex> lk(mu_);
    auto it = text_cache_.find(key);
    if (it == text_cache_.end())
      return std::nullopt;
    it->second.last_access = std::chrono::steady_clock::now();
    return it->second.text;
  }

  void put_text(long long row_time, const std::string& path,
    std::string text) {
    const auto now = std::chrono::steady_clock::now();
    const std::string key = file_key(row_time, path);
    std::lock_guard<std::mutex> lk(mu_);
    text_cache_[key] = TextEntry{ std::move(text), now };
    prune_under_lock();
  }

  void invalidate(long long row_time, const std::string& path) {
    const std::string prefix = file_key(row_time, path);
    std::lock_guard<std::mutex> lk(mu_);
    text_cache_.erase(prefix);
  }

private:
  static std::string file_key(long long row_time, const std::string& path) {
    return make_file_cache_key(row_time, path);
  }

  template<typename CacheMap>
  static void prune_map_under_lock(CacheMap& cache, size_t keep_limit) {
    while (cache.size() > keep_limit) {
      auto victim = std::min_element(cache.begin(), cache.end(),
        [](const auto& a, const auto& b) {
          return a.second.last_access < b.second.last_access;
        });
      if (victim == cache.end())
        break;
      cache.erase(victim);
    }
  }

  void prune_under_lock() {
    prune_map_under_lock(text_cache_, max_text_entries_);
  }

  std::mutex mu_;
  std::unordered_map<std::string, TextEntry> text_cache_;
  size_t max_text_entries_{ 256 };
};

static const char* env_or(const char* k, const char* d) {
  const char* v = getenv(k);
  return (v && *v) ? v : d;
}
struct PropertiesResult {
  std::unordered_map<std::string, std::string> values;
  bool loaded{ false };
};

static std::string trim_copy(const std::string& s) {
  size_t begin = 0;
  while (begin < s.size() && std::isspace(static_cast<unsigned char>(s[begin])))
    ++begin;
  size_t end = s.size();
  while (end > begin && std::isspace(static_cast<unsigned char>(s[end - 1])))
    --end;
  return s.substr(begin, end - begin);
}

static bool starts_with(const std::string& s, const char* prefix) {
  size_t n = std::strlen(prefix);
  return s.size() >= n && std::equal(prefix, prefix + n, s.begin());
}

static PropertiesResult load_properties_file(const std::string& path) {
  PropertiesResult res;
  std::ifstream in(path);
  if (!in)
    return res;
  res.loaded = true;
  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty() && line.back() == '\r')
      line.pop_back();
    std::string trimmed = trim_copy(line);
    if (trimmed.empty())
      continue;
    if (trimmed[0] == '#' || trimmed[0] == ';')
      continue;
    if (starts_with(trimmed, "//"))
      continue;
    size_t eq = trimmed.find('=');
    if (eq == std::string::npos)
      continue;
    std::string key = trim_copy(trimmed.substr(0, eq));
    std::string value = trim_copy(trimmed.substr(eq + 1));
    if (!key.empty())
      res.values[key] = value;
  }
  return res;
}

static int parse_int_or(const std::string& s, int fallback) {
  if (s.empty())
    return fallback;
  int value = 0;
  const char* first = s.data();
  const char* last = first + s.size();
  auto result = std::from_chars(first, last, value, 10);
  return result.ec == std::errc{} && result.ptr == last ? value : fallback;
}

static size_t parse_size_t_or(const std::string& s, size_t fallback) {
  if (s.empty())
    return fallback;
  size_t value = 0;
  const char* first = s.data();
  const char* last = first + s.size();
  auto result = std::from_chars(first, last, value, 10);
  return result.ec == std::errc{} && result.ptr == last ? value : fallback;
}

static bool parse_size_t_strict(const std::string& s, size_t& value) {
  if (s.empty())
    return false;
  const char* first = s.data();
  const char* last = first + s.size();
  auto result = std::from_chars(first, last, value, 10);
  return result.ec == std::errc{} && result.ptr == last;
}

static size_t parse_size_or(const std::string& s, size_t fallback) {
  if (s.empty())
    return fallback;
  std::string value = trim_copy(s);
  if (value.empty())
    return fallback;
  if (!value.empty() && (value.back() == 'b' || value.back() == 'B'))
    value.pop_back();
  if (value.empty())
    return fallback;
  size_t multiplier = 1;
  char suffix = value.back();
  if (suffix == 'k' || suffix == 'K') {
    multiplier = 1024ull;
    value.pop_back();
  }
  else if (suffix == 'm' || suffix == 'M') {
    multiplier = 1024ull * 1024ull;
    value.pop_back();
  }
  else if (suffix == 'g' || suffix == 'G') {
    multiplier = 1024ull * 1024ull * 1024ull;
    value.pop_back();
  }
  value = trim_copy(value);
  if (value.empty())
    return fallback;
  try {
    auto base = std::stoull(value);
    return static_cast<size_t>(base * multiplier);
  }
  catch (...) {
    return fallback;
  }
}

struct FsTuneConfig {
  size_t read_cache_chunk_size{ 128 * 1024 };
  size_t read_full_prefetch_max_bytes{ 32ull * 1024 * 1024 };
  size_t raw_file_cache_max_bytes{ 256ull * 1024 * 1024 };
  size_t decoded_cache_max_bytes{ 64ull * 1024 * 1024 };
  size_t writer_db_request_max_bytes{ 8 * 1024 * 1024 };
  size_t writer_transfer_chunk_bytes{ 1024 * 1024 };
  size_t writer_manifest_batch_max_bytes{ 8 * 1024 * 1024 };
  size_t inode_cache_capacity{ 2048 };
  size_t inode_cache_flush_interval_ms{ 1500 };
  size_t message_cache_max_entries{ 32 };
};

static IoTDBConfig g_cfg{};
static std::string g_log_dir = "logs";
static std::string g_config_path;
static bool g_config_loaded_from_file = false;
static bool g_config_invalid = false;
static size_t g_pool_size_override = 0;
static FsTuneConfig g_fs_tune_cfg{};
static iotdbfs::QueryRuntimeConfig g_query_runtime_config = [] {
  iotdbfs::QueryRuntimeConfig config;
  config.max_entries = 16384;
  config.max_inflight = 256;
  return config;
}();
static iotdbfs::BoundedResultSender::Config g_result_sender_config{};
static auto g_hdf5_query_mode = iotdbfs::Hdf5QueryMode::IoTDBIndexed;
static bool g_version_directory_owner = false;
static std::unique_ptr<iotdbfs::ScopedQueryMetadataBudget> owner_read_budget() {
  if (g_version_directory_owner && !iotdbfs::current_query_metadata_budget() &&
      iotdbfs::active_query_metadata_trace == nullptr)
    return std::make_unique<iotdbfs::ScopedQueryMetadataBudget>();
  return {};
}
static std::string g_version_directory_prefix;
static std::string g_version_directory_owner_directory;
static std::string g_native_file_root;
static bool g_version_directory_initialize = false;
static size_t g_version_directory_reader_pool_size = 4;
static bool g_version_directory_payload_read_coalescing = true;
static size_t g_version_directory_payload_cache_bytes = 8ULL * 1024 * 1024;
static iotdbfs::TrainingExecutionOptions g_training_execution;
static iotdbfs::Hdf5QueryKernelConfig g_hdf5_execution;
static iotdbfs::GribParsedIndexCacheConfig g_grib_query_index_cache;
static iotdbfs::GribDecodedFieldCacheConfig g_grib_query_decoded_cache;
static size_t g_query_frontend_max_registrations{1024};
static bool g_grib_extension_enabled = true;
static std::string g_log_level_name = "INFO";
static std::string g_mount_point = "/mnt/dfs";
static std::shared_ptr<const iotdbfs::MetadataConfig> g_metadata_config;
static constexpr size_t kMaxWriterDbRequestBytes = 100ull * 1024 * 1024;
static constexpr size_t kWriterChunkSize = 512 * 1024;

static void load_configuration() {
  if (std::getenv("IOTDBFS_EXPERIMENT_FROZEN_TYPED")) {
    g_config_invalid = true;
    LOG_ERROR("IOTDBFS_EXPERIMENT_FROZEN_TYPED was removed from production; "
      "historical experiments must use their frozen source/build");
  }
  // namespace fs definition handled by Filesystem.h
  const char* config_file_env = getenv("IOTDB_CONFIG_FILE");
  fs::path config_path;
  if (config_file_env && *config_file_env) {
    config_path = fs::path(config_file_env);
  }
  else {
    const char* conf_dir_env = getenv("IOTDB_CONF_DIR");
    std::string conf_dir =
      (conf_dir_env && *conf_dir_env) ? conf_dir_env : "resources";
    config_path = fs::path(conf_dir) / "config.properties";
  }

  PropertiesResult props_res = load_properties_file(config_path.string());
  g_config_path = config_path.string();
  g_config_loaded_from_file = props_res.loaded;

  auto get_prop = [&](const std::string& key) -> std::string {
    auto it = props_res.values.find(key);
    if (it != props_res.values.end())
      return it->second;
    return {};
    };

  if (props_res.values.count("query.hdf5.native_processes")) {
    g_config_invalid = true;
    LOG_ERROR("query.hdf5.native_processes was removed; Native HDF5 uses "
      "in-process execution. Remove this property, including a value of 0");
  }

  std::string host = get_prop("iotdb.host");
  if (host.empty())
    host = env_or("IOTDB_HOST", "127.0.0.1");
  g_cfg.host = host;

  std::string port_str = get_prop("iotdb.port");
  if (port_str.empty())
    port_str = env_or("IOTDB_PORT", "6667");
  g_cfg.port = parse_int_or(port_str, 6667);

  std::string user = get_prop("iotdb.user");
  if (user.empty())
    user = env_or("IOTDB_USER", "root");
  g_cfg.user = user;

  std::string pass = get_prop("iotdb.pass");
  if (pass.empty())
    pass = get_prop("iotdb.password");
  if (pass.empty())
    pass = env_or("IOTDB_PASS", "root");
  g_cfg.pass = pass;

  std::string db = get_prop("iotdb.db");
  if (db.empty())
    db = env_or("IOTDB_DB", "dfs");
  g_cfg.db = db;
  if (!iotdbfs::is_valid_database_name(g_cfg.db)) {
    g_config_invalid = true;
  }

  const auto directory_mode = get_prop("storage.version_directory.mode");
  g_version_directory_owner = directory_mode == "owner";
  g_version_directory_prefix = get_prop("storage.version_directory.table_prefix");
  g_version_directory_owner_directory = get_prop("storage.version_directory.owner_directory");
  const auto initialize = get_prop("storage.version_directory.initialize");
  if ((!directory_mode.empty() && directory_mode != "strict" && directory_mode != "owner") ||
      (!initialize.empty() && initialize != "true" && initialize != "false")) {
    g_config_invalid = true;
    LOG_ERROR("Invalid storage.version_directory mode or initialize value");
  }
  g_version_directory_initialize = initialize == "true";
  const auto owner_pool_size = props_res.values.find("storage.version_directory.reader_pool_size");
  if (owner_pool_size != props_res.values.end() &&
      (!parse_size_t_strict(owner_pool_size->second, g_version_directory_reader_pool_size) ||
       g_version_directory_reader_pool_size == 0 ||
       g_version_directory_reader_pool_size > iotdbfs::kMaxIoTDBPoolSize)) {
    g_config_invalid = true;
    LOG_ERROR("Invalid storage.version_directory.reader_pool_size: expected an integer in [1,256]");
  }
  const auto payload_coalescing = props_res.values.find("storage.version_directory.payload_read_coalescing");
  const auto payload_cache = props_res.values.find("storage.version_directory.payload_cache_bytes");
  if(payload_cache!=props_res.values.end()&&
     (!parse_size_t_strict(payload_cache->second,g_version_directory_payload_cache_bytes)||
      g_version_directory_payload_cache_bytes>8ULL*1024*1024)) {
    g_config_invalid=true;
    LOG_ERROR("Invalid storage.version_directory.payload_cache_bytes: expected an integer in [0,8388608]");
  }
  if (payload_coalescing != props_res.values.end()) {
    if (payload_coalescing->second != "true" && payload_coalescing->second != "false") {
      g_config_invalid = true;
      LOG_ERROR("Invalid storage.version_directory.payload_read_coalescing: expected true or false");
    } else g_version_directory_payload_read_coalescing = payload_coalescing->second == "true";
  }
  if (g_version_directory_owner) {
    if (g_version_directory_prefix.empty() || g_version_directory_owner_directory.empty() ||
        g_version_directory_owner_directory.front() != '/' ||
        (g_cfg.host != "127.0.0.1" && g_cfg.host != "::1" && g_cfg.host != "localhost")) {
      g_config_invalid = true;
      LOG_ERROR("Version directory owner requires table_prefix, absolute owner_directory and loopback IoTDB");
    }
  } else if (!g_version_directory_prefix.empty() || !g_version_directory_owner_directory.empty() ||
             g_version_directory_initialize || owner_pool_size != props_res.values.end() ||
             payload_cache != props_res.values.end() ||
             payload_coalescing != props_res.values.end()) {
    g_config_invalid = true;
    LOG_ERROR("Version directory settings require explicit owner mode");
  }

  std::string prepared_str = get_prop("iotdb.use_prepared");
  if (prepared_str.empty()) {
    const char* env_val = getenv("USE_PREPARED");
    if (env_val && *env_val)
      prepared_str = env_val;
  }
  g_cfg.use_prepared = parse_int_or(prepared_str, 0);

  std::string log_dir_prop = get_prop("log.dir");
  if (log_dir_prop.empty()) {
    const char* env_log = getenv("IOTDB_LOG_DIR");
    if (env_log && *env_log)
      log_dir_prop = env_log;
  }
  if (log_dir_prop.empty())
    log_dir_prop = "logs";
  g_log_dir = log_dir_prop;

  std::string log_level_prop = get_prop("log.level");
  if (log_level_prop.empty()) {
    const char* env_log_level = getenv("IOTDB_LOG_LEVEL");
    if (env_log_level && *env_log_level)
      log_level_prop = env_log_level;
  }
  if (log_level_prop.empty() && getenv("IOTDB_DEBUG"))
    log_level_prop = "DEBUG";
  if (log_level_prop.empty())
    log_level_prop = "INFO";
  LogLevel parsed_level = logging_level_from_string(log_level_prop);
  set_logging_level(parsed_level);
  g_log_level_name = logging_level_to_string(parsed_level);

  std::string pool_size_str = get_prop("iotdb.pool_size");
  if (pool_size_str.empty()) {
    const char* env_pool = getenv("IOTDB_POOL_SIZE");
    if (env_pool && *env_pool)
      pool_size_str = env_pool;
  }
  if (!pool_size_str.empty() &&
    !parse_size_t_strict(pool_size_str, g_pool_size_override)) {
    g_config_invalid = true;
    g_pool_size_override = 0;
  }
  if (g_pool_size_override > iotdbfs::kMaxIoTDBPoolSize)
    g_config_invalid = true;

  auto query_limit = [&](const char* key, auto& target) {
    const auto value = props_res.values.find(key);
    if (value == props_res.values.end()) return;
    size_t parsed = 0;
    if (!parse_size_t_strict(value->second, parsed) || parsed == 0) {
      g_config_invalid = true;
      LOG_ERROR(std::string("Invalid positive integer property: ") + key);
      return;
    }
    target = parsed;
  };
  query_limit("query.cache.max_entries", g_query_runtime_config.max_entries);
  query_limit("query.cache.max_inflight", g_query_runtime_config.max_inflight);
  query_limit("query.cache.worker_threads", g_query_runtime_config.worker_threads);
  query_limit("query.cache.memory_budget_bytes", g_query_runtime_config.memory_budget_bytes);
  query_limit("query.tasks.worker_threads", g_query_runtime_config.query_tasks.worker_threads);
  query_limit("query.tasks.max_queued_tasks", g_query_runtime_config.query_tasks.max_queued_tasks);
  if (props_res.values.find("query.tasks.max_batch_size") == props_res.values.end())
    g_query_runtime_config.query_tasks.max_batch_size = std::min<std::size_t>(
      4, g_query_runtime_config.query_tasks.worker_threads);
  query_limit("query.tasks.max_batch_size", g_query_runtime_config.query_tasks.max_batch_size);
  if (g_query_runtime_config.query_tasks.worker_threads > 256) {
    g_config_invalid = true;
    LOG_ERROR("Invalid query.tasks.worker_threads: expected an integer in [1,256]");
  }
  if (g_query_runtime_config.query_tasks.max_queued_tasks > 65536) {
    g_config_invalid = true;
    LOG_ERROR("Invalid query.tasks.max_queued_tasks: expected an integer in [1,65536]");
  }
  if (g_query_runtime_config.query_tasks.max_batch_size >
      g_query_runtime_config.query_tasks.worker_threads) {
    g_config_invalid = true;
    LOG_ERROR("Invalid query.tasks.max_batch_size: expected an integer between 1 and query.tasks.worker_threads");
  }
  query_limit("query.result_fetch.sender_threads", g_result_sender_config.sender_threads);
  if (g_result_sender_config.sender_threads > 2) {
    g_config_invalid = true;
    LOG_ERROR("Invalid query.result_fetch.sender_threads: expected an integer in [1,2]");
  }
  query_limit("query.training.read_parallelism", g_training_execution.read_parallelism);
  query_limit("query.hdf5.read_parallelism", g_hdf5_execution.read_parallelism);
  {
    auto parsed = iotdbfs::parse_hdf5_query_configuration(props_res.values);
    if (!parsed.ok()) {
      g_config_invalid = true;
      LOG_ERROR(parsed.error());
    } else g_hdf5_query_mode = parsed.value();
  }
  g_native_file_root = get_prop("storage.native_file_root");
  if ((!g_native_file_root.empty() &&
       (!g_version_directory_owner || g_native_file_root.front() != '/')) ||
      (g_hdf5_query_mode == iotdbfs::Hdf5QueryMode::NativeHdf5 &&
       (!g_version_directory_owner || g_native_file_root.empty()))) {
    g_config_invalid = true;
    LOG_ERROR("native-hdf5 requires owner mode and an absolute storage.native_file_root");
  }
  query_limit("query.grib.index_cache.max_versions", g_grib_query_index_cache.max_versions);
  query_limit("query.grib.index_cache.memory_budget_bytes",
    g_grib_query_index_cache.memory_budget_bytes);
  query_limit("query.grib.decoded_cache.max_fields", g_grib_query_decoded_cache.max_fields);
  query_limit("query.grib.decoded_cache.memory_budget_bytes",
    g_grib_query_decoded_cache.memory_budget_bytes);
  query_limit("query.frontend.max_registrations",
    g_query_frontend_max_registrations);
  query_limit("writer.transfer_chunk_bytes",
    g_fs_tune_cfg.writer_transfer_chunk_bytes);
  query_limit("writer.manifest_batch_max_bytes",
    g_fs_tune_cfg.writer_manifest_batch_max_bytes);
  auto query_switch = [&](const char* key, bool& target) {
    const auto found = props_res.values.find(key);
    if (found == props_res.values.end()) return;
    if (found->second == "true") target = true;
    else if (found->second == "false") target = false;
    else {
      g_config_invalid = true;
      LOG_ERROR(std::string("Invalid boolean property: ") + key);
    }
  };
  query_switch("query.training.reuse_prepared_validation", g_training_execution.reuse_prepared_validation);
  query_switch("query.training.compiled_transform", g_training_execution.compiled_transform);
  query_switch("query.hdf5.native_cache", g_hdf5_execution.native_session_cache_enabled);
  if (g_training_execution.read_parallelism > 4 ||
      g_query_runtime_config.worker_threads < g_training_execution.read_parallelism ||
      (g_training_execution.read_parallelism <= 4 &&
       g_query_runtime_config.max_inflight < 64 * g_training_execution.read_parallelism)) {
    g_config_invalid = true;
    LOG_ERROR("Invalid query limits: read_parallelism must be in [1,4], with at least one cache worker and 64 inflight fragments per wave");
  }
  if (g_hdf5_execution.read_parallelism != 1 &&
      g_hdf5_execution.read_parallelism != 2 &&
      g_hdf5_execution.read_parallelism != 4) {
    g_config_invalid = true;
    LOG_ERROR("Invalid query.hdf5.read_parallelism: expected 1, 2, or 4");
  }

  g_fs_tune_cfg.decoded_cache_max_bytes = parse_size_or(
    get_prop("cache.decoded.max_bytes"), 64ull * 1024 * 1024);
  g_fs_tune_cfg.read_cache_chunk_size = parse_size_or(
    get_prop("cache.read.chunk_size"), 128 * 1024);
  g_fs_tune_cfg.read_full_prefetch_max_bytes = parse_size_or(
    get_prop("cache.read.full_prefetch_max_bytes"), 32ull * 1024 * 1024);
  g_fs_tune_cfg.raw_file_cache_max_bytes = parse_size_or(
    get_prop("cache.raw_file.max_bytes"), 256ull * 1024 * 1024);
  g_fs_tune_cfg.writer_db_request_max_bytes = parse_size_or(
    get_prop("writer.db_request_max_bytes"), 8 * 1024 * 1024);
  g_fs_tune_cfg.inode_cache_capacity = parse_size_t_or(
    get_prop("inode.cache.capacity"), 2048);
  g_fs_tune_cfg.inode_cache_flush_interval_ms = parse_size_t_or(
    get_prop("inode.cache.flush_interval_ms"), 1500);
  g_fs_tune_cfg.message_cache_max_entries = parse_size_t_or(
    get_prop("grib.message_cache.max_entries"), 32);
  g_grib_extension_enabled = iotdbfs::grib_extension_enabled_from_env(
    iotdbfs::grib_extension_enabled_from_properties(props_res.values, true));
  const auto metadata_result = iotdbfs::load_metadata_config(config_path.string());
  if (!metadata_result.ok()) {
    g_config_invalid = true;
    LOG_ERROR("Invalid metadata configuration: " + metadata_result.error());
  } else if (!metadata_result.value().fields().empty()) {
    g_metadata_config = std::make_shared<iotdbfs::MetadataConfig>(metadata_result.value());
  }

  std::string mount_point_prop = get_prop("mount.point");
  if (mount_point_prop.empty()) {
    const char* env_mount = getenv("IOTDB_MOUNT_POINT");
    if (env_mount && *env_mount)
      mount_point_prop = env_mount;
  }
  if (mount_point_prop.empty())
    mount_point_prop = "/mnt/dfs";
  g_mount_point = mount_point_prop;

  if (g_fs_tune_cfg.decoded_cache_max_bytes == 0)
    g_fs_tune_cfg.decoded_cache_max_bytes = 64ull * 1024 * 1024;
  if (g_fs_tune_cfg.read_cache_chunk_size == 0)
    g_fs_tune_cfg.read_cache_chunk_size = 128 * 1024;
  if (g_fs_tune_cfg.read_full_prefetch_max_bytes == 0)
    g_fs_tune_cfg.read_full_prefetch_max_bytes = 32ull * 1024 * 1024;
  if (g_fs_tune_cfg.raw_file_cache_max_bytes == 0)
    g_fs_tune_cfg.raw_file_cache_max_bytes = 256ull * 1024 * 1024;
  if (g_fs_tune_cfg.writer_db_request_max_bytes == 0)
    g_fs_tune_cfg.writer_db_request_max_bytes = 8 * 1024 * 1024;
  g_fs_tune_cfg.writer_db_request_max_bytes = std::min(
    g_fs_tune_cfg.writer_db_request_max_bytes, kMaxWriterDbRequestBytes);
  if (g_fs_tune_cfg.writer_transfer_chunk_bytes > iotdbfs::kMaxPayloadWriteBytesPerBatch ||
      g_fs_tune_cfg.writer_transfer_chunk_bytes > g_fs_tune_cfg.writer_db_request_max_bytes) {
    g_config_invalid = true;
    LOG_ERROR("writer.transfer_chunk_bytes must not exceed 8 MiB or writer.db_request_max_bytes");
  }
  if (g_fs_tune_cfg.writer_manifest_batch_max_bytes <
        g_fs_tune_cfg.writer_transfer_chunk_bytes ||
      g_fs_tune_cfg.writer_manifest_batch_max_bytes > iotdbfs::kMaxPayloadWriteBytesPerBatch) {
    g_config_invalid = true;
    LOG_ERROR("writer.manifest_batch_max_bytes must contain one transfer chunk and not exceed 8 MiB");
  }
  if (g_fs_tune_cfg.inode_cache_capacity == 0)
    g_fs_tune_cfg.inode_cache_capacity = 2048;
  if (g_fs_tune_cfg.inode_cache_flush_interval_ms == 0)
    g_fs_tune_cfg.inode_cache_flush_interval_ms = 1500;
  if (g_fs_tune_cfg.message_cache_max_entries == 0)
    g_fs_tune_cfg.message_cache_max_entries = 32;

  RawFileCache::instance().set_max_bytes(g_fs_tune_cfg.raw_file_cache_max_bytes);
  DecodedDataCache::instance().set_max_bytes(g_fs_tune_cfg.decoded_cache_max_bytes);
  iotdbfs::configure_grib_message_cache(
    g_fs_tune_cfg.message_cache_max_entries);
}

static int grib_extension_disabled_exit(const char* command) {
  std::cerr << command << " requires GRIB extension support, but "
            << iotdbfs::kGribExtensionEnabledKey << "=false\n";
  return 2;
}

// ===================== 会话连接池（并发读写） =====================
// 初始化连接池（可通过 IOTDB_POOL_SIZE 环境变量配置大小）
static void ensure_session_pool_inited() {
  static std::once_flag once;
  std::call_once(once, []() {
    IoTDBConnector::instance().initialize(g_cfg,
      iotdbfs::normalize_iotdb_pool_size(g_pool_size_override));
    });
}

// 统一的池化执行函数
static bool pool_exec_nonquery(const std::string& sql) {
  ensure_session_pool_inited();
  return IoTDBConnector::instance().executeNonQuery(sql);
}

static std::shared_ptr<SessionDataSet>
pool_execute_query(const std::string& sql) {
  ensure_session_pool_inited();
  return IoTDBConnector::instance().executeQuery(sql);
}

static std::shared_ptr<iotdbfs::IoTDBStructuredBlobBackend>
structured_blob_backend() {
  static auto backend =
    std::make_shared<iotdbfs::IoTDBStructuredBlobBackend>();
  return backend;
}

static iotdbfs::StructuredBlobStorage& structured_blob_storage() {
  static iotdbfs::StructuredBlobStorage storage(structured_blob_backend());
  return storage;
}

struct TypedMountRuntime {
  std::shared_ptr<iotdbfs::QueryRuntime> runtime;
  std::shared_ptr<iotdbfs::Hdf5ValidatedLayoutCache> hdf5_layout_cache;
  std::vector<std::shared_ptr<const iotdbfs::Hdf5SemanticProfile>> hdf5_profiles;
  std::shared_ptr<const iotdbfs::StructuredBlobStorage> storage;
  std::shared_ptr<const iotdbfs::PublishedVersionCatalog> catalog;
  std::shared_ptr<const iotdbfs::QueryModule> query;
  std::shared_ptr<iotdbfs::TypedQueryFrontend> frontend;
  std::shared_ptr<iotdbfs::TypedResultStore> results;
  std::shared_ptr<iotdbfs::TypedQueryService> service;
  std::shared_ptr<iotdbfs::FuseTypedQueryBridge> bridge;
};
static std::unique_ptr<TypedMountRuntime> g_typed_mount;
static std::unique_ptr<iotdbfs::QueryRegistrationSocketServer>
  g_query_registration_socket;
static std::string g_query_mount_root;
static std::string g_query_socket_path;
static std::string g_query_socket_error;
static mode_t g_query_socket_mode = 0600;

static bool initialize_typed_mount() {
  if (g_hdf5_execution.native_session_cache_enabled)
    g_hdf5_execution.native_session_pool = std::make_shared<iotdbfs::Hdf5NativeSessionPool>();
  auto state = std::make_unique<TypedMountRuntime>();
  state->runtime = std::make_shared<iotdbfs::QueryRuntime>(g_query_runtime_config);
  state->hdf5_layout_cache = iotdbfs::make_hdf5_validated_layout_cache();
  state->hdf5_profiles = iotdbfs::default_hdf5_semantic_profiles();
  state->storage = std::make_shared<iotdbfs::StructuredBlobStorage>(
    structured_blob_backend());
  state->catalog = iotdbfs::make_iotdb_published_version_catalog(
    structured_blob_backend());
  std::vector<std::shared_ptr<const iotdbfs::FormatQueryAdapter>> adapters;
  adapters.push_back(std::make_shared<iotdbfs::ArrowFormatQueryAdapter>(
    iotdbfs::ArrowDecodeConfig{}, state->runtime));
  adapters.push_back(std::make_shared<iotdbfs::Hdf5FormatQueryAdapter>(
    g_hdf5_execution, state->runtime, iotdbfs::Hdf5SemanticConfig{},
    iotdbfs::Hdf5LayoutCacheConfig{}, state->hdf5_profiles,
    state->hdf5_layout_cache, g_hdf5_query_mode));
  if (g_grib_extension_enabled)
    adapters.push_back(std::make_shared<iotdbfs::GribFormatQueryAdapter>(
      g_grib_query_index_cache, g_grib_query_decoded_cache,
      state->runtime));
  auto module = iotdbfs::QueryModule::create(
    state->catalog, state->storage, std::move(adapters));
  if (!module.ok()) {
    LOG_ERROR("typed query module initialization failed: " +
      module.status().message());
    return false;
  }
  state->query = std::move(module.value());
  state->frontend = std::make_shared<iotdbfs::TypedQueryFrontend>(
    g_query_frontend_max_registrations);
  state->results = std::make_shared<iotdbfs::TypedResultStore>(
    iotdbfs::TypedResultStore::Config{});
  state->service = std::make_shared<iotdbfs::TypedQueryService>(
    state->query, state->frontend, state->results, 64ULL * 1024 * 1024);
  state->bridge = std::make_shared<iotdbfs::FuseTypedQueryBridge>(
    state->frontend, state->service);
  g_typed_mount = std::move(state);
  return true;
}

static iotdbfs::StructuredBlobVersionKey blob_key(
  long long row_time, const std::string& path) {
  return {norm(path), row_time};
}

// 记录“正在写入”的 path 集合
static std::mutex g_writers_mu;
static std::unordered_map<string, size_t> g_open_writers;

// ===================== SQL/转义 =====================
static string esc(const string& s) {
  string o;
  o.reserve(s.size() + 8);
  for (char c : s) {
    if (c == '\'')
      o += "''";
    else
      o.push_back(c);
  }
  return o;
}

static bool exec_sql(const string& sql) {
  try {
    // route to pool
    return pool_exec_nonquery(sql);
  }
  catch (const exception& e) {
    LOG_ERROR(string("SQL FAILED: ") + iotdbfs::redact_sql_for_log(sql) +
      " => " + e.what());
    return false;
  }
}

static std::shared_ptr<SessionDataSet> query(const string& sql) {
  try {
    // route to pool
    return pool_execute_query(sql);
  }
  catch (const exception& e) {
    LOG_ERROR(string("QUERY FAILED: ") + iotdbfs::redact_sql_for_log(sql) +
      " => " + e.what());
    return nullptr;
  }
}

// ===================== Structured Blob payload/layout access =====================
static optional<vector<uint8_t>> read_object_part(long long row_time,
  const string& path,
  uint64_t off, uint64_t len) {
  auto result = structured_blob_storage().read_payload_range(
    blob_key(row_time, path), off, len);
  if (!result.ok()) return nullopt;
  return result.value();
}
static optional<string> read_layout_text(long long row_time,
  const string& path) {
  // Owner reads must validate the exact binding before reusing immutable
  // layout pages. The backend owns that validation and content-ID cache.
  if (!g_version_directory_owner) {
    if (auto cached = LayoutCache::instance().get_text(row_time, path))
      return cached;
  }
  auto result = structured_blob_storage().load_layout(blob_key(row_time, path));
  if (!result.ok()) return nullopt;
  string value(result.value().begin(), result.value().end());
  if (!g_version_directory_owner)
    LayoutCache::instance().put_text(row_time, path, value);
  return value;
}

struct WriteSession {
  Inode inode;
  iotdbfs::StagedFile staged;
  bool dirty{false};
  int write_error{0};
  size_t writers{0};
  std::mutex mu;
};

struct OpenFileCtx {
  long long row_time = 0;
  string path;
  uint64_t max_pos = 0;
  uint64_t file_size_orig = 0;

  bool is_writer = false;
  std::shared_ptr<iotdbfs::TypedResultStore::Handle> typed_result;
  std::shared_ptr<WriteSession> write_session;

  struct ReadCache {
    uint64_t base = 0;
    vector<uint8_t> data;
    bool valid = false;
    void reset() {
      base = 0;
      valid = false;
      data.clear();
    }
  } read_cache;
  bool is_range_view = false;
  uint64_t view_off = 0;
  uint64_t view_len = 0;

  bool is_materialized_view = false;
  vector<uint8_t> materialized_cache;
  std::shared_ptr<const std::vector<uint8_t>> materialized_cache_shared;
  std::shared_ptr<const std::vector<uint8_t>> raw_file_cache_shared;
  mutable std::mutex mu;
  explicit OpenFileCtx(bool reserve_read_cache = true) {
    if (!reserve_read_cache) return;
    size_t chunk = g_fs_tune_cfg.read_cache_chunk_size;
    if (chunk == 0)
      chunk = 1;
    read_cache.data.reserve(chunk);
  }
  void invalidate_read_cache() { read_cache.reset(); }
  void invalidate_raw_file_cache() { raw_file_cache_shared.reset(); }
};

static std::recursive_mutex g_namespace_mu;
// Successful typed-route checks are shared across getattr/access/open for at
// most the FUSE attribute-cache interval. Mutations in this process invalidate
// them immediately; checks also reload source and parent metadata from IoTDB.
static iotdbfs::TypedAuthorizationCache g_typed_authorization_cache;
// Controlled build-time ablation: both arms retain identical normalization,
// authorization and publication behavior; only successful prefilling differs.
constexpr bool kPrefillPublishedAuthorization = true;

// Linux may issue a path-only GETATTR even for fstat on an open file. Keep
// immutable attributes until the last open handle is released, without owning
// an additional result handle. Access and open still check source permissions.
struct OpenTypedAttributes {
  struct Entry {
    std::uint64_t size;
    std::size_t count;
  };
  std::mutex mu;
  std::unordered_map<std::string, Entry> entries;

  int add(const std::string& route, std::uint64_t size) {
    std::lock_guard<std::mutex> lock(mu);
    auto found = entries.find(route);
    if (found != entries.end()) {
      if (found->second.size != size) return -ESTALE;
      if (found->second.count == std::numeric_limits<std::size_t>::max())
        return -EMFILE;
      ++found->second.count;
    } else {
      entries.emplace(route, Entry{size, 1});
    }
    return 0;
  }
  std::optional<std::uint64_t> size(const std::string& route) {
    std::lock_guard<std::mutex> lock(mu);
    auto found = entries.find(route);
    if (found == entries.end()) return std::nullopt;
    return found->second.size;
  }
  void remove(const std::string& route) {
    std::lock_guard<std::mutex> lock(mu);
    auto found = entries.find(route);
    if (found != entries.end() && --found->second.count == 0)
      entries.erase(found);
  }
};
static OpenTypedAttributes g_open_typed_attributes;

static void limit_typed_authorization_ttl(const std::string& fuse_options) {
  std::size_t begin = 0;
  while (begin < fuse_options.size()) {
    const auto end = fuse_options.find(',', begin);
    const auto option = fuse_options.substr(begin,
      end == std::string::npos ? end : end - begin);
    constexpr const char* prefix = "attr_timeout=";
    if (option.compare(0, std::strlen(prefix), prefix) == 0) {
      const char* value = option.c_str() + std::strlen(prefix);
      char* after = nullptr;
      const double seconds = std::strtod(value, &after);
      if (after != value && *after == '\0' && std::isfinite(seconds) &&
          seconds >= 0.0) {
        const auto millis = static_cast<long long>(
          std::min(seconds, 1.0) * 1000.0);
        g_typed_authorization_cache.ttl =
          std::chrono::milliseconds(millis);
      }
    }
    if (end == std::string::npos) break;
    begin = end + 1;
  }
}
static std::mutex g_writer_sessions_mu;
static std::unordered_map<std::string, size_t> g_open_handle_counts;

static bool has_open_handle(const std::string& path) {
  std::lock_guard<std::mutex> lk(g_writer_sessions_mu);
  return g_open_handle_counts.find(norm(path)) != g_open_handle_counts.end();
}

static void add_open_handle(const std::string& path) {
  std::lock_guard<std::mutex> lk(g_writer_sessions_mu);
  ++g_open_handle_counts[norm(path)];
}

static void remove_open_handle(const std::string& path) {
  std::lock_guard<std::mutex> lk(g_writer_sessions_mu);
  auto it = g_open_handle_counts.find(norm(path));
  if (it == g_open_handle_counts.end())
    return;
  if (--it->second == 0)
    g_open_handle_counts.erase(it);
}
static std::unordered_map<std::string, std::shared_ptr<WriteSession>> g_writer_sessions;

static std::shared_ptr<WriteSession> find_writer_session(const std::string& path) {
  std::lock_guard<std::mutex> lk(g_writer_sessions_mu);
  auto it = g_writer_sessions.find(norm(path));
  return it == g_writer_sessions.end() ? nullptr : it->second;
}

static void register_writer_session(const std::string& path,
  const std::shared_ptr<WriteSession>& session) {
  std::lock_guard<std::mutex> lk(g_writer_sessions_mu);
  g_writer_sessions[norm(path)] = session;
}

static void unregister_writer_session(const std::string& path,
  const std::shared_ptr<WriteSession>& session) {
  std::lock_guard<std::mutex> lk(g_writer_sessions_mu);
  auto it = g_writer_sessions.find(norm(path));
  if (it != g_writer_sessions.end() && it->second == session)
    g_writer_sessions.erase(it);
}

static inline size_t materialized_cache_size(const OpenFileCtx* ctx) {
  if (!ctx)
    return 0;
  if (ctx->materialized_cache_shared)
    return ctx->materialized_cache_shared->size();
  return ctx->materialized_cache.size();
}

static inline const uint8_t* materialized_cache_ptr(const OpenFileCtx* ctx) {
  if (!ctx)
    return nullptr;
  if (ctx->materialized_cache_shared)
    return ctx->materialized_cache_shared->data();
  return ctx->materialized_cache.empty()
    ? nullptr : ctx->materialized_cache.data();
}

static inline size_t raw_file_cache_size(const OpenFileCtx* ctx) {
  if (!ctx || !ctx->raw_file_cache_shared)
    return 0;
  return ctx->raw_file_cache_shared->size();
}

static inline const uint8_t* raw_file_cache_ptr(const OpenFileCtx* ctx) {
  if (!ctx || !ctx->raw_file_cache_shared)
    return nullptr;
  return ctx->raw_file_cache_shared->data();
}
static inline OpenFileCtx* fh2ctx(struct fuse_file_info* fi) {
  return reinterpret_cast<OpenFileCtx*>(static_cast<uintptr_t>(fi->fh));
}

static iotdbfs::GribWriteStreamer::Config current_grib_ingestion_config() {
  iotdbfs::GribWriteStreamer::Config config;
  config.chunk_size = kWriterChunkSize;
  config.process_threshold_bytes = 4 * 1024 * 1024;
  config.process_retry_bytes = 2 * 1024 * 1024;
  config.window_reserve_bytes = 8 * 1024 * 1024;
  config.detect_threshold_bytes = 2 * 1024 * 1024;
  config.metadata_config = g_metadata_config;
  return config;
}

static iotdbfs::FormatRegistry& format_registry() {
  static iotdbfs::FormatRegistry registry = [] {
    iotdbfs::FormatRegistry value;
    // Probe Arrow framing before GRIB scans arbitrary payload bytes. Arrow
    // ingestion is independent of the optional GRIB extension.
    if (!value.register_adapter(std::make_shared<iotdbfs::ArrowFormatAdapter>()))
      LOG_ERROR("failed to register Arrow format adapter");
    if (!value.register_adapter(std::make_shared<iotdbfs::Hdf5FormatAdapter>()))
      LOG_ERROR("failed to register HDF5 format adapter");
    if (g_grib_extension_enabled) {
      iotdbfs::GribFormatAdapter::Config config;
      config.ingestion = current_grib_ingestion_config();
      config.queries.read_object_part = [](long long row_time,
          const std::string& path, std::uint64_t off, std::uint64_t len) {
        return read_object_part(row_time, path, off, len);
      };
      config.queries.read_layout_text = [](long long row_time,
          const std::string& path) {
        return read_layout_text(row_time, path);
      };
      config.queries.is_path_in_writing = [](const std::string& path) {
        return is_path_in_writing(path);
      };
      config.queries.metadata_config = g_metadata_config;
      if (!value.register_adapter(
          std::make_shared<iotdbfs::GribFormatAdapter>(std::move(config))))
        LOG_ERROR("failed to register GRIB format adapter");
    }
    return value;
  }();
  return registry;
}

static iotdbfs::IngestionModule& ingestion_module() {
  static iotdbfs::IngestionModule module(
    structured_blob_storage(), format_registry(),
    { g_fs_tune_cfg.writer_transfer_chunk_bytes,
      g_fs_tune_cfg.writer_manifest_batch_max_bytes });
  return module;
}

static int check_inode_access_as(const Inode& inode, int mask,
    uid_t uid, gid_t gid, const std::vector<gid_t>& groups = {}) {
  return iotdbfs::check_query_posix_access(inode.mode, inode.uid, inode.gid,
    mask, uid, gid, groups);
}

static std::vector<gid_t> fuse_caller_groups() {
  std::vector<gid_t> groups;
#ifdef __linux__
  const int count = fuse_getgroups(0, nullptr);
  if (count <= 0 || count > 65536) return groups;
  groups.resize(static_cast<std::size_t>(count));
  const int read = fuse_getgroups(count, groups.data());
  if (read < 0 || read > count) return {};
  groups.resize(static_cast<std::size_t>(read));
#endif
  const auto* context = fuse_get_context();
  return iotdbfs::normalized_authorization_groups(
    context ? context->gid : ::getegid(), std::move(groups));
}

static int check_inode_access(const Inode& inode, int mask) {
  const struct fuse_context* context = fuse_get_context();
  if (!context) return 0;
  return check_inode_access_as(inode, mask, context->uid, context->gid,
    fuse_caller_groups());
}

static bool is_typed_namespace(const std::string& path) {
  return path == "/.iotdbfs" ||
    path.compare(0, std::string("/.iotdbfs/").size(), "/.iotdbfs/") == 0;
}

static bool is_typed_directory(const std::string& path) {
  return path == "/.iotdbfs" || path == "/.iotdbfs/query" ||
    path == "/.iotdbfs/query/v1";
}

static bool is_typed_route(const std::string& path) {
  const bool known_kind = path.compare(0, std::string("/.iotdbfs/query/v1/typed.ipc?").size(),
      "/.iotdbfs/query/v1/typed.ipc?") == 0 ||
    path.compare(0, std::string("/.iotdbfs/query/v1/windows.ipc?").size(),
      "/.iotdbfs/query/v1/windows.ipc?") == 0;
  if (!known_kind) return false;
  const auto marker = path.rfind("&result=");
  if (marker == std::string::npos || path.size() - marker != 72)
    return false;
  return std::all_of(path.begin() + marker + 8, path.end(), [](char byte) {
    return (byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f');
  });
}

// Namespace identity and POSIX permissions are read before content evidence.
// Call this only after any permission checks required by the operation.
static int hydrate_owner_inode(Inode& inode) {
  if (!g_version_directory_owner || inode.is_dir) return 0;
  const auto manifest = structured_blob_storage().load_manifest(
    blob_key(inode.row_time, inode.path));
  if (!manifest.ok())
    return manifest.status().code() == iotdbfs::StructuredBlobError::NotFound
      ? -ENOENT : -EIO;
  auto metadata = iotdbfs::serialize_metadata(manifest.value().metadata, 65536);
  if (!metadata.ok()) return -EIO;
  inode.metadata = std::move(metadata.value());
  return 0;
}

static iotdbfs::QueryStatus validate_owner_typed_content(
    const std::vector<iotdbfs::QuerySourceReference>& sources) {
  if (!g_version_directory_owner) return iotdbfs::QueryStatus::success();
  std::set<iotdbfs::StructuredBlobVersionKey> checked;
  for (const auto& source : sources) {
    const auto key = blob_key(source.version, source.path);
    if (!checked.insert(key).second) continue;
    const auto manifest = structured_blob_storage().load_manifest(key);
    if (!manifest.ok())
      return iotdbfs::QueryStatus::failure(
        manifest.status().code() == iotdbfs::StructuredBlobError::NotFound
          ? iotdbfs::QueryError::StalePlan : iotdbfs::QueryError::ReadFailure,
        iotdbfs::QueryPhase::Catalog, manifest.status().message(),
        iotdbfs::QueryErrorDetail{source});
  }
  return iotdbfs::QueryStatus::success();
}

static iotdbfs::QueryStatus authorize_typed_sources_as(uid_t uid, gid_t gid,
    const std::vector<gid_t>& groups,
    const std::vector<iotdbfs::QuerySourceReference>& sources,
    const iotdbfs::CatalogSelection* selection = nullptr) {
  if (selection && selection->read_context) {
    auto admitted = selection->read_context->admit({uid, gid, groups});
    return admitted.ok() ? iotdbfs::QueryStatus::success() : admitted.status();
  }
  const auto backend = structured_blob_backend();
  std::map<iotdbfs::StructuredBlobVersionKey,
    const iotdbfs::StructuredBlobManifest*> selected_manifests;
  if (selection)
    for (const auto& manifest : selection->versions())
      selected_manifests.emplace(manifest.key, &manifest);
  std::map<std::string, std::vector<std::string>> source_parents;
  std::set<std::string> unique_parent_paths;
  for (const auto& source : sources) {
    auto [entry, inserted] = source_parents.emplace(source.path, std::vector<std::string>{});
    if (!inserted) continue;
    entry->second = iotdbfs::query_source_parent_paths(source.path);
    unique_parent_paths.insert(entry->second.begin(), entry->second.end());
  }
  const std::vector<std::string> parent_paths(unique_parent_paths.begin(), unique_parent_paths.end());
  auto permission_status = iotdbfs::authorize_source_permissions_with_generation(
    [&] { return backend->metadata_generation(); },
    [&](bool use_catalog, std::uint64_t generation) {
      std::set<std::pair<std::string, std::int64_t>> checked_sources;
      for (const auto& source : sources) {
        if (!checked_sources.emplace(source.path, source.version).second) continue;
        const auto exact = [&]() -> std::optional<Inode> {
          const auto selected = selected_manifests.find({source.path, source.version});
          const auto* permission = iotdbfs::validated_catalog_source_permission(
            use_catalog ? selection : nullptr,
            selected == selected_manifests.end() ? nullptr : selected->second,
            source, backend->storage_namespace(), generation);
          if (permission) {
            iotdbfs::query_profile_count("authorization_source_catalog_reuse");
            Inode inode;
            inode.mode = permission->mode;
            inode.uid = permission->uid;
            inode.gid = permission->gid;
            inode.is_dir = permission->is_dir;
            return inode;
          }
          iotdbfs::QueryProfileTimer timer("authorization_source_metadata");
          iotdbfs::query_profile_count("authorization_source_metadata_calls");
          return load_inode_from_db(source.path, source.version);
        }();
        if (!exact || exact->is_dir)
          return iotdbfs::QueryStatus::failure(iotdbfs::QueryError::StalePlan,
            iotdbfs::QueryPhase::Catalog,
            "typed query source version is no longer available",
            iotdbfs::QueryErrorDetail{source});
        if (check_inode_access_as(*exact, R_OK, uid, gid, groups) != 0)
          return iotdbfs::QueryStatus::failure(iotdbfs::QueryError::PermissionDenied,
            iotdbfs::QueryPhase::Catalog, "source read permission denied",
            iotdbfs::QueryErrorDetail{source});
      }
      const auto* parent_hint = use_catalog && selection &&
          selection->storage_namespace == backend->storage_namespace()
        ? selection->source_permissions.get() : nullptr;
      auto resolved = iotdbfs::resolve_query_parent_permissions(parent_hint,
        parent_paths, backend->storage_namespace(), generation,
        [&](const std::vector<std::string>& missing) {
          iotdbfs::QueryProfileTimer timer("authorization_parent_metadata");
          iotdbfs::query_profile_count("authorization_parent_metadata_paths", missing.size());
          auto loaded = backend->load_latest_inode_permissions(missing);
          using Result = iotdbfs::QueryResult<std::shared_ptr<const iotdbfs::SourcePermissionSnapshot>>;
          if (loaded.ok()) return Result::success(std::move(loaded.value()));
          iotdbfs::QueryErrorDetail detail;
          detail.operation = "authorization-parent-metadata";
          return Result::failure(iotdbfs::QueryStatus::failure(
            loaded.status().code() == iotdbfs::StructuredBlobError::InvalidArgument
              ? iotdbfs::QueryError::ResourceExhausted : iotdbfs::QueryError::ReadFailure,
            iotdbfs::QueryPhase::Catalog, loaded.status().message(), std::move(detail)));
        });
      if (!resolved.ok()) return resolved.status();
      iotdbfs::query_profile_count("authorization_parent_catalog_reuse", resolved.value().catalog_reuse);
      std::unordered_set<std::string> checked_parents;
      for (const auto& source : sources) {
        for (const auto& parent : source_parents.at(source.path)) {
          if (!checked_parents.insert(parent).second) continue;
          const auto checked = iotdbfs::check_query_parent_permission(
            resolved.value().observations.at(parent), source, uid, gid, groups);
          if (!checked.ok()) return checked;
        }
      }
      return iotdbfs::QueryStatus::success();
    });
  if (!permission_status.ok()) return permission_status;
  return validate_owner_typed_content(sources);
}

// Independent, optional FUSE authorization diagnostics. This never writes to
// the registration profile stream and never changes permission decisions.
class TypedAuthorizationProfileSink {
public:
  TypedAuthorizationProfileSink() noexcept {
    try {
      const auto* path = std::getenv("IOTDBFS_TYPED_AUTHORIZATION_PROFILE_JSONL");
      if (path && *path) output_.open(path, std::ios::out | std::ios::app);
      if (output_.is_open())
        iotdbfs::iotdb_profile_counters.timing_enabled.store(true, std::memory_order_relaxed);
    } catch (...) {}
  }
  bool enabled() const noexcept { return output_.is_open(); }
  void append(const std::string& route, uid_t uid, gid_t gid,
      const std::vector<gid_t>& groups, const iotdbfs::QueryStatus& status,
      bool cache_hit, const iotdbfs::QueryProfileRecord& record,
      const iotdbfs::IoTDBProfileSnapshot& before, const iotdbfs::IoTDBProfileSnapshot& after,
      std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end) noexcept {
    try {
      std::ostringstream line;
      line << "{\"protocol\":\"iotdbfs-typed-authorization-profile/1\",\"pid\":" << ::getpid()
           << ",\"route\":" << std::quoted(route)
           << ",\"canonical_route\":" << std::quoted(iotdbfs::canonical_published_authorization_route(route))
           << ",\"uid\":" << uid << ",\"gid\":" << gid << ",\"groups\":[";
      for (std::size_t i = 0; i < groups.size(); ++i) {
        if (i) line << ',';
        line << groups[i];
      }
      line << "],\"ok\":" << (status.ok() ? "true" : "false")
           << ",\"error_code\":" << static_cast<int>(status.code())
           << ",\"cache_hit\":" << (cache_hit ? "true" : "false")
           << ",\"elapsed_ns\":" << std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count()
           << ",\"counters\":{";
      bool first = true;
      for (const auto& [name, count] : record.counters) {
        if (!first) line << ',';
        first = false;
        line << std::quoted(name) << ':' << count;
      }
      line << "},\"stages_ns\":{";
      first = true;
      for (const auto& [name, count] : record.nanoseconds) {
        if (!first) line << ',';
        first = false;
        line << std::quoted(name) << ':' << count;
      }
      line << "},\"iotdb_delta\":{";
      first = true;
#define AUTH_DELTA(name) \
      if (!first) line << ','; \
      first = false; \
      line << std::quoted(#name) << ':' << (after.name >= before.name ? after.name - before.name : 0);
      IOTDBFS_IOTDB_PROFILE_COUNTERS(AUTH_DELTA)
#undef AUTH_DELTA
      line << "}}";
      std::lock_guard<std::mutex> lock(mu_);
      output_ << line.str() << '\n';
      output_.flush();
    } catch (...) {}
  }
private:
  std::mutex mu_;
  std::ofstream output_;
};

static TypedAuthorizationProfileSink& typed_authorization_profile_sink() {
  static TypedAuthorizationProfileSink sink;
  return sink;
}

static iotdbfs::QueryStatus authorize_typed_sources(
    const std::string& route,
    const std::vector<iotdbfs::QuerySourceReference>& sources) {
  if (g_version_directory_owner && !structured_blob_backend()->load_inode("/"))
    return iotdbfs::QueryStatus::failure(iotdbfs::QueryError::ReadFailure,
      iotdbfs::QueryPhase::Catalog, "version directory owner root is unavailable");
  const auto* context = fuse_get_context();
  if (!context)
    return iotdbfs::QueryStatus::failure(
      iotdbfs::QueryError::PermissionDenied, iotdbfs::QueryPhase::Validation,
      "typed query caller identity is unavailable");
  auto groups = fuse_caller_groups();
  auto& sink = typed_authorization_profile_sink();
  const bool profiling = sink.enabled();
  iotdbfs::QueryProfileRecord record;
  const auto before = profiling ? iotdbfs::iotdb_profile_snapshot() : iotdbfs::IoTDBProfileSnapshot{};
  const auto start = profiling ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
  bool cache_hit = false;
  auto status = [&] {
    iotdbfs::ScopedQueryProfile scope(profiling ? &record : nullptr);
    iotdbfs::QueryProfileTimer timer("authorization_total");
    const auto key = iotdbfs::typed_authorization_cache_key(context->uid,
      context->gid, route, groups);
    for (int attempt = 0; attempt < 2; ++attempt) {
      const auto observed = g_typed_authorization_cache.capture();
      const auto generation = observed.generation;
      if (g_typed_authorization_cache.contains(key, generation)) {
        cache_hit = true;
        iotdbfs::query_profile_count("authorization_cache_hit");
        return iotdbfs::QueryStatus::success();
      }
      auto status = authorize_typed_sources_as(context->uid, context->gid,
        groups, sources);
      if (g_typed_authorization_cache.snapshot() != generation) continue;
      if (!status.ok()) return status;
      if (g_typed_authorization_cache.store(key, observed)) return status;
    }
    return iotdbfs::QueryStatus::failure(iotdbfs::QueryError::StalePlan,
      iotdbfs::QueryPhase::Catalog,
      "typed query source permissions changed during authorization");
  }();
  // A permission-cache hit is not evidence for a file's immutable graph.
  if (status.ok() && cache_hit) status = validate_owner_typed_content(sources);
  if (profiling) sink.append(route, context->uid, context->gid, groups, status,
    cache_hit, record, before, iotdbfs::iotdb_profile_snapshot(), start,
    std::chrono::steady_clock::now());
  return status;
}
static iotdbfs::QueryResult<std::shared_ptr<
    iotdbfs::TypedResultStore::Handle>> acquire_typed_route(
      const std::string& route) {
  std::unique_ptr<iotdbfs::ScopedQueryMetadataBudget> metadata_scope;
  if (g_version_directory_owner) metadata_scope = std::make_unique<iotdbfs::ScopedQueryMetadataBudget>();
  if (!g_typed_mount)
    return iotdbfs::QueryResult<std::shared_ptr<
      iotdbfs::TypedResultStore::Handle>>::failure(
        iotdbfs::QueryStatus::failure(iotdbfs::QueryError::AdapterUnavailable,
          iotdbfs::QueryPhase::Execution,
          "typed query runtime is unavailable"));
  const auto marker = route.rfind("&result=");
  std::optional<iotdbfs::QueryPrincipal> principal;
  if (const auto* caller = fuse_get_context())
    principal = iotdbfs::QueryPrincipal{caller->uid, caller->gid, fuse_caller_groups()};
  auto result = g_typed_mount->bridge->acquire_exact(
    route.substr(0, marker), route.substr(marker + 8),
    [&route](const std::vector<iotdbfs::QuerySourceReference>& sources) {
      return authorize_typed_sources(route, sources);
    }, principal);
  if (!result.ok()) {
    const auto& status = result.status();
    std::string diagnostic = "typed query route failed code=" +
      std::to_string(static_cast<int>(status.code())) + " phase=" +
      std::to_string(static_cast<int>(status.phase())) + " message=" +
      status.message();
    if (status.detail().source)
      diagnostic += " source=" + status.detail().source->path + "@" +
        std::to_string(status.detail().source->version);
    if (status.detail().field_id)
      diagnostic += " field=" + *status.detail().field_id;
    if (status.detail().operation)
      diagnostic += " operation=" + *status.detail().operation;
    LOG_WARN(diagnostic);
  }
  else if (g_log_level_name == "DEBUG") {
    const auto stats = g_typed_mount->runtime->stats();
    LOG_DEBUG("typed query runtime hits=" + std::to_string(stats.hits) +
      " misses=" + std::to_string(stats.misses) +
      " decodes=" + std::to_string(stats.decode_count));
  }
  return result;
}

static int op_access(const char* path, int mask) {
  if (!path || (mask & ~(R_OK | W_OK | X_OK)))
    return -EINVAL;
  string full = norm(path);
  if (is_typed_namespace(full)) {
    if (is_typed_directory(full))
      return (mask & W_OK) ? -EROFS : 0;
    if (!is_typed_route(full)) return -ENOENT;
    if (mask & (W_OK | X_OK)) return -EACCES;
    auto result = acquire_typed_route(full);
    return result.ok() ? 0 : -iotdbfs::query_status_errno(result.status());
  }
  auto metadata_scope = owner_read_budget();
  string base_path = full;
  string query;
  split_path_and_query(full, base_path, query);
  if (!query.empty())
    return -EACCES;
  auto inode = get_inode_maybe_version(base_path);
  if (!inode)
    return -ENOENT;
  return check_inode_access(*inode, mask);
}

static ssize_t read_with_cache(OpenFileCtx* ctx, uint64_t base_off,
  uint64_t avail_len, uint64_t rel_off, size_t len,
  char* buf) {
  if (!ctx || !len)
    return 0;
  if (ctx->raw_file_cache_shared) {
    const size_t raw_size = raw_file_cache_size(ctx);
    const uint8_t* raw_ptr = raw_file_cache_ptr(ctx);
    uint64_t abs_off = 0;
    if (!fuse_semantics::checked_add_u64(base_off, rel_off, abs_off))
      return -EOVERFLOW;
    if (raw_ptr && abs_off < raw_size) {
      size_t can_copy = std::min(len, raw_size - static_cast<size_t>(abs_off));
      if (can_copy) {
        std::memcpy(buf, raw_ptr + abs_off, can_copy);
        return can_copy;
      }
    }
  }
  auto& cache = ctx->read_cache;
  if (cache.valid) {
    uint64_t cache_end = 0;
    if (!fuse_semantics::checked_add_u64(cache.base,
      static_cast<uint64_t>(cache.data.size()), cache_end))
      cache_end = std::numeric_limits<uint64_t>::max();
    if (rel_off >= cache.base && rel_off < cache_end) {
      size_t offset_in_cache = (size_t)(rel_off - cache.base);
      size_t can_copy = std::min(len, cache.data.size() - offset_in_cache);
      if (can_copy) {
        std::memcpy(buf, cache.data.data() + offset_in_cache, can_copy);
        return can_copy;
      }
    }
  }

  if (rel_off >= avail_len)
    return 0;

  size_t chunk = g_fs_tune_cfg.read_cache_chunk_size;
  if (chunk == 0)
    chunk = 1;
  size_t fetch_len = std::max(len, chunk);
  const size_t full_prefetch_max = g_fs_tune_cfg.read_full_prefetch_max_bytes;
  const bool should_prefetch_full =
    rel_off == 0 &&
    full_prefetch_max > 0 &&
    avail_len <= static_cast<uint64_t>(full_prefetch_max) &&
    len >= std::min(chunk, static_cast<size_t>(avail_len));
  if (should_prefetch_full)
    fetch_len = static_cast<size_t>(avail_len);
  uint64_t remain = avail_len - rel_off;
  if ((uint64_t)fetch_len > remain)
    fetch_len = (size_t)remain;
  if (fetch_len == 0)
    return 0;

  const bool whole_file_fetch =
    should_prefetch_full && base_off == 0 && fetch_len == ctx->max_pos;
  if (whole_file_fetch) {
    uint64_t whole_abs_off = 0;
    if (!fuse_semantics::checked_add_u64(base_off, rel_off, whole_abs_off))
      return -EOVERFLOW;
    const std::string cache_key = make_file_cache_key(ctx->row_time, ctx->path);
    const uint64_t full_len = ctx->max_pos;
    auto shared = RawFileCache::instance().get_or_load(cache_key, [&]() {
      auto part = read_object_part(ctx->row_time, ctx->path, whole_abs_off,
        full_len);
      if (!part || part->size() != static_cast<size_t>(full_len)) {
        LOG_WARN("whole-file prefetch incomplete path=" + ctx->path +
          " requested=" + to_string(full_len) +
          " got=" + to_string(part ? part->size() : 0));
        return std::shared_ptr<const std::vector<uint8_t>>{};
      }
      auto vec_ptr = std::make_shared<std::vector<uint8_t>>(std::move(*part));
      auto cached = RawFileCache::instance().put(cache_key, vec_ptr);
      if (!cached) {
        cached = std::static_pointer_cast<const std::vector<uint8_t>>(vec_ptr);
      }
      return cached;
    });
    if (!shared) {
      uint64_t abs_off = 0;
      if (!fuse_semantics::checked_add_u64(base_off, rel_off, abs_off))
        return -EOVERFLOW;
      auto part = read_object_part(ctx->row_time, ctx->path, abs_off,
        static_cast<uint64_t>(fetch_len));
      if (!part) {
        cache.reset();
        return -EIO;
      }
      if (part->size() != fetch_len) {
        LOG_WARN("chunk fallback short read path=" + ctx->path +
          " off=" + to_string(abs_off) +
          " requested=" + to_string(fetch_len) +
          " got=" + to_string(part->size()));
        cache.reset();
        return -EIO;
      }
      cache.data = std::move(*part);
      cache.base = rel_off;
      cache.valid = true;

      size_t can_copy = std::min(len, cache.data.size());
      if (can_copy)
        std::memcpy(buf, cache.data.data(), can_copy);
      return can_copy;
    }
    ctx->raw_file_cache_shared = shared;
    cache.reset();
    size_t can_copy = std::min(len, shared->size());
    if (can_copy)
      std::memcpy(buf, shared->data(), can_copy);
    return can_copy;
  }

  uint64_t abs_off = 0;
  if (!fuse_semantics::checked_add_u64(base_off, rel_off, abs_off))
    return -EOVERFLOW;
  auto part = read_object_part(ctx->row_time, ctx->path, abs_off,
    (uint64_t)fetch_len);
  if (!part) {
    cache.reset();
    return -EIO;
  }
  if (part->size() != fetch_len) {
    LOG_WARN("chunk read short path=" + ctx->path +
      " off=" + to_string(abs_off) +
      " requested=" + to_string(fetch_len) +
      " got=" + to_string(part->size()));
    cache.reset();
    return -EIO;
  }

  cache.data = std::move(*part);
  cache.base = rel_off;
  cache.valid = true;

  size_t can_copy = std::min(len, cache.data.size());
  if (can_copy)
    std::memcpy(buf, cache.data.data(), can_copy);
  return can_copy;
}

// ===================== DDL =====================
static bool ensure_schema() {
  ensure_session_pool_inited();
  if (!exec_sql("CREATE DATABASE IF NOT EXISTS " + g_cfg.db))
    return false;
  return structured_blob_backend()->ensure_schema();
}

static optional<Inode> load_inode_from_db(const string& path,
  optional<long long> row_time = nullopt) {
  return structured_blob_backend()->load_inode(path, row_time);
}
static bool upsert_inode_db(const Inode& i) {
  return structured_blob_backend()->upsert_inode(i, i.is_dir);
}
static vector<Inode> load_children_from_db(const string& dir,
  bool latest_only = true) {
  return structured_blob_backend()->load_children(dir, latest_only);
}
static bool delete_inode_db(const string& path,
  optional<long long> row_time = nullopt) {
  return structured_blob_backend()->remove_path(path, row_time);
}
static bool delete_inode_tree_db(const string& dir) {
  return structured_blob_backend()->remove_tree(dir);
}

static void ensure_inode_cache_configured() {
  static std::once_flag once;
  std::call_once(once, []() {
    InodeCache::DbHooks hooks;
    hooks.load_by_path = [](const std::string& p) {
      return load_inode_version_aware(p);
      };
    hooks.load_children = [](const std::string& p) {
      return load_children_from_db(p, true);
      };
    hooks.upsert = [](const Inode& in) {
      return upsert_inode_db(in);
      };
    hooks.erase_one = [](const std::string& p,
      optional<long long> row_time) {
        return delete_inode_db(p, row_time);
      };
    hooks.erase_tree = [](const std::string& p) {
      return delete_inode_tree_db(p);
      };
    InodeCache::instance().configure(
      hooks, g_fs_tune_cfg.inode_cache_capacity,
      std::chrono::milliseconds(g_fs_tune_cfg.inode_cache_flush_interval_ms));
    });
}

static optional<Inode> get_inode(const string& path) {
  if (g_version_directory_owner) return load_inode_from_db(norm(path));
  ensure_inode_cache_configured();
  return InodeCache::instance().get(norm(path));
}
static bool insert_inode(const Inode& input) {
  Inode i = input;
  i.path = norm(i.path);
  if (i.parent.empty())
    i.parent = parent_of(i.path);
  if (i.name.empty())
    i.name = base(i.path);
  if (g_version_directory_owner) {
    if (hydrate_owner_inode(i) != 0) return false;
    if (!upsert_inode_db(i)) return false;
    g_typed_authorization_cache.invalidate();
    return true;
  }
  ensure_inode_cache_configured();
  auto existing = InodeCache::instance().get(i.path);
  InodeCache::instance().put(i, true);
  if (InodeCache::instance().flush(i.path)) {
    g_typed_authorization_cache.invalidate();
    return true;
  }

  // Avoid leaving a cache-only inode behind after a failed create/insert.
  if (!existing) {
    InodeCache::instance().discard(i.path);
  }
  else {
    InodeCache::instance().put(*existing, false);
  }
  return false;
}
static vector<Inode> list_dir_current(const string& dir) {
  if (g_version_directory_owner) return load_children_from_db(norm(dir), true);
  ensure_inode_cache_configured();
  return InodeCache::instance().list_children(norm(dir));
}
static vector<Inode> list_dir_versions(const string& dir) {
  vector<Inode> out;
  auto entries = load_children_from_db(dir, false);
  out.reserve(entries.size());
  std::unordered_set<std::string> seen_latest;
  std::unordered_set<std::string> seen_versions;
  for (auto& i : entries) {
    auto key = norm(i.path);
    auto version_key = key + "#" + std::to_string(i.row_time);
    if (seen_latest.find(key) == seen_latest.end()) {
      // First (newest) occurrence: treat as current version, not hidden.
      seen_latest.insert(key);
      seen_versions.insert(version_key);
      continue;
    }
    if (i.is_dir)
      continue;
    if (!seen_versions.insert(version_key).second)
      continue;
    auto name_parts = split_name_suffix(base(i.path));
    string vname = build_version_name(name_parts.first, name_parts.second,
      i.row_time);
    Inode v = i;
    v.name = vname;
    out.push_back(std::move(v));
  }
  return out;
}
static bool delete_inode_by_path(const string& path,
  optional<long long> row_time = nullopt) {
  if (g_version_directory_owner) {
    const bool removed = delete_inode_db(norm(path), row_time);
    if (removed) g_typed_authorization_cache.invalidate();
    return removed;
  }
  ensure_inode_cache_configured();
  const bool removed = InodeCache::instance().erase(norm(path), row_time);
  if (removed) g_typed_authorization_cache.invalidate();
  return removed;
}
// =============== 写入状态查询（写入期间禁用虚拟视图） ===============
static bool is_path_in_writing(const string& path) {
  auto info = parse_version_path(path);
  string key = norm(info.is_version ? info.base_path : path);
  lock_guard<mutex> lk(g_writers_mu);
  return g_open_writers.find(key) != g_open_writers.end();
}
static void writers_add(const string& path) {
  auto info = parse_version_path(path);
  string key = norm(info.is_version ? info.base_path : path);
  lock_guard<mutex> lk(g_writers_mu);
  ++g_open_writers[key];
}
static void writers_remove(const string& path) {
  auto info = parse_version_path(path);
  string key = norm(info.is_version ? info.base_path : path);
  lock_guard<mutex> lk(g_writers_mu);
  auto it = g_open_writers.find(key);
  if (it == g_open_writers.end())
    return;
  if (it->second <= 1)
    g_open_writers.erase(it);
  else
    --it->second;
}

// Optional publication diagnostics. Capture only calls belonging to this
// thread's publication, independently of daemon-wide query counters.
class PublicationProfileSink {
public:
  PublicationProfileSink() noexcept {
    try {
      const auto* path = std::getenv("IOTDBFS_PUBLICATION_PROFILE_JSONL");
      if (path && *path) output_.open(path, std::ios::out | std::ios::app);
    } catch (...) {}
  }
  bool enabled() const noexcept { return output_.is_open(); }
  void append(const std::string& line) noexcept {
    try {
      std::lock_guard<std::mutex> lock(mu_);
      output_ << line << '\n';
      output_.flush();
    } catch (...) {}
  }
private:
  std::mutex mu_;
  std::ofstream output_;
};
static PublicationProfileSink& publication_profile_sink() {
  static PublicationProfileSink sink;
  return sink;
}
static std::string publication_json_string(const std::string& input) {
  std::ostringstream result;
  result << '"';
  constexpr char hex[] = "0123456789abcdef";
  for (unsigned char c : input) {
    if (c == '"' || c == '\\') result << '\\' << c;
    else if (c < 0x20) result << "\\u00" << hex[c >> 4] << hex[c & 15];
    else result << c;
  }
  result << '"';
  return result.str();
}
class PublicationProfileOperation {
public:
  PublicationProfileOperation(const char* kind, const std::string& path,
      std::uint64_t bytes, std::int64_t version = 0) noexcept {
    try {
      if (!publication_profile_sink().enabled()) return;
      kind_ = kind; path_ = path; bytes_ = bytes; version_ = version;
      record_ = std::make_shared<iotdbfs::PublicationProfileRecord>();
      scope_ = std::make_unique<iotdbfs::PublicationProfileScope>(record_);
      start_ = std::chrono::steady_clock::now();
    } catch (...) { scope_.reset(); record_.reset(); }
  }
  ~PublicationProfileOperation() noexcept {
    try {
      if (!record_) return;
      const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - start_).count();
      scope_.reset();
      std::ostringstream line;
      line << "{\"protocol\":\"iotdbfs-publication-profile/3\",\"pid\":" << ::getpid()
           << ",\"kind\":" << publication_json_string(kind_)
           << ",\"path\":" << publication_json_string(path_)
           << ",\"version\":" << version_ << ",\"source_bytes\":" << bytes_
           << ",\"ok\":" << (ok_ ? "true" : "false")
           << ",\"elapsed_ns\":" << elapsed << ",\"profile\":" << record_->to_json() << '}';
      publication_profile_sink().append(line.str());
    } catch (...) {}
  }
  void version(std::int64_t value) noexcept { version_ = value; }
  void succeeded() noexcept { ok_ = true; }
private:
  std::shared_ptr<iotdbfs::PublicationProfileRecord> record_;
  std::unique_ptr<iotdbfs::PublicationProfileScope> scope_;
  std::chrono::steady_clock::time_point start_{};
  std::string kind_, path_;
  std::uint64_t bytes_{};
  std::int64_t version_{};
  bool ok_{};
};

// Caller holds the session mutex. Commit preserves the last metadata operation,
// including an explicit utimens performed after this content change.
static void mark_write_session_dirty(WriteSession& session) {
  session.dirty = true;
  session.inode.mtime = session.inode.ctime = now_ms();
}

static int attach_write_session(OpenFileCtx* ctx, const Inode& inode,
  bool truncate) {
  auto session = find_writer_session(inode.path);
  if (!session) {
    session = std::make_shared<WriteSession>();
    session->inode = inode;
    int rc = session->staged.create();
    if (rc != 0)
      return rc;
    if (!truncate && inode.size > 0) {
      rc = session->staged.load(static_cast<uint64_t>(inode.size),
        [&](uint64_t off, uint64_t len) {
          return read_object_part(inode.row_time, inode.path, off, len);
        });
      if (rc != 0)
        return rc;
    }
    register_writer_session(inode.path, session);
  }
  std::lock_guard<std::mutex> lk(session->mu);
  if (truncate) {
    int rc = session->staged.resize(0);
    if (rc != 0)
      return rc;
    mark_write_session_dirty(*session);
  }
  ++session->writers;
  ctx->write_session = session;
  ctx->is_writer = true;
  writers_add(inode.path);
  return 0;
}

// Caller holds the session mutex. Ingestion Module owns format detection,
// payload copying, derived artifacts, lifecycle ordering, and rollback.
static int commit_write_session(WriteSession& session) {
  if (session.write_error != 0)
    return session.write_error;
  if (!session.dirty)
    return 0;
  PublicationProfileOperation publication_profile("content_commit", session.inode.path,
    session.staged.size());
  int rc;
  {
    iotdbfs::PublicationStageScope stage("staged_sync");
    rc = session.staged.sync();
  }
  if (rc != 0)
    return rc;
  Inode next = session.inode;
  next.row_time = next_version_time(next.row_time);
  publication_profile.version(next.row_time);
  next.size = static_cast<long long>(session.staged.size());
  const auto key = blob_key(next.row_time, next.path);
  iotdbfs::FormatSource source;
  source.size = session.staged.size();
  source.read = [&](std::uint64_t offset, std::uint64_t length)
      -> std::optional<std::vector<std::uint8_t>> {
    if (length > static_cast<std::uint64_t>(
        std::numeric_limits<std::size_t>::max()))
      return std::nullopt;
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(length));
    const int read_rc = session.staged.read(bytes.data(), bytes.size(),
      offset);
    if (read_rc < 0 || static_cast<std::size_t>(read_rc) != bytes.size())
      return std::nullopt;
    return bytes;
  };
  auto ingested = ingestion_module().ingest(key, source,
    [&](const iotdbfs::IngestionOutcome& outcome) {
      next.format_id = outcome.format_id.value_or("");
      auto metadata = iotdbfs::serialize_metadata(outcome.metadata, 65536);
      if (!metadata.ok()) {
        LOG_ERROR("failed to serialize ingested metadata path=" + next.path +
          " => " + metadata.error());
        return false;
      }
      next.metadata = std::move(metadata.value());
      return upsert_inode_db(next);
    });
  if (!ingested.ok()) {
    LOG_ERROR("ingestion failed path=" + next.path + " => " +
      ingested.message());
    LayoutCache::instance().invalidate(next.row_time, next.path);
    return -EIO;
  }
  if (!g_version_directory_owner) InodeCache::instance().put(next, false);
  RawFileCache::instance().invalidate(next.row_time, next.path);
  session.inode = next;
  session.dirty = false;
  publication_profile.succeeded();
  return 0;
}

static bool copy_structured_blob_version(const Inode& source,
  const Inode& target, Inode* published_target = nullptr) {
  const auto source_key = blob_key(source.row_time, source.path);
  const auto target_key = blob_key(target.row_time, target.path);
  const auto fail = [&](const char* operation, const std::string& message) {
    LOG_ERROR(std::string("rename version copy failed at ") + operation +
      " source=" + source.path + "@" + std::to_string(source.row_time) +
      " target=" + target.path + "@" + std::to_string(target.row_time) +
      (message.empty() ? std::string() : " => " + message));
    return false;
  };
  auto source_manifest = structured_blob_storage().load_manifest(source_key);
  if (!source_manifest.ok())
    return fail("load manifest", source_manifest.status().message());
  auto status = structured_blob_storage().begin(target_key);
  if (!status.ok()) return fail("begin target", status.message());
  bool published = false;
  auto cleanup = [&]() {
    if (!published) structured_blob_storage().abort(target_key);
  };
  constexpr std::uint64_t kCopyChunk = 1024ull * 1024;
  for (std::uint64_t offset = 0;
       offset < source_manifest.value().payload_size;
       offset += kCopyChunk) {
    const auto length = std::min<std::uint64_t>(kCopyChunk,
      source_manifest.value().payload_size - offset);
    auto bytes = structured_blob_storage().read_payload_range(
      source_key, offset, length);
    if (!bytes.ok()) {
      cleanup();
      return fail("read payload", bytes.status().message());
    }
    status = structured_blob_storage().write_payload_range(
      target_key, offset, bytes.value());
    if (!status.ok()) {
      cleanup();
      return fail("write payload", status.message());
    }
  }
  status = structured_blob_storage().seal_payload(target_key);
  if (!status.ok()) {
    cleanup();
    return fail("seal payload", status.message());
  }
  auto layout = structured_blob_storage().load_layout(source_key);
  if (!layout.ok()) {
    cleanup();
    return fail("load layout", layout.status().message());
  }
  status = structured_blob_storage().write_layout(target_key, layout.value());
  if (!status.ok()) {
    cleanup();
    return fail("write layout", status.message());
  }
  std::optional<iotdbfs::DatasetCatalogArtifact> dataset_catalog;
  if (source_manifest.value().metadata.count("__iotdbfs.dataset_catalog.protocol")) {
    auto loaded = structured_blob_storage().load_dataset_catalog(source_key);
    if (!loaded.ok()) { cleanup(); return fail("load dataset catalog", loaded.status().message()); }
    dataset_catalog = std::move(loaded.value());
  }
  status = structured_blob_storage().write_metadata_and_format_identity(
    target_key, source_manifest.value().format_id,
    source_manifest.value().metadata, std::move(dataset_catalog),
    &source_manifest.value().metadata);
  auto copied_target = target;
  auto copied_metadata = iotdbfs::serialize_metadata(source_manifest.value().metadata, 65536);
  if (copied_metadata.ok()) copied_target.metadata = copied_metadata.value();
  if (!status.ok() || !copied_metadata.ok() ||
      !structured_blob_backend()->upsert_inode(copied_target, false)) {
    cleanup();
    return fail("write description",
      status.ok() ? "inode publication preparation failed" : status.message());
  }
  status = structured_blob_storage().publish(target_key);
  if (!status.ok()) {
    cleanup();
    return fail("publish target", status.message());
  }
  published = true;
  if (published_target) *published_target = std::move(copied_target);
  return true;
}

// ===================== FUSE 回调 =====================
static int fill_typed_result_stat(std::uint64_t size, struct stat* st) {
  if (!st) return -EINVAL;
  if (!fuse_semantics::fits_off_t(size)) return -EOVERFLOW;
  std::memset(st, 0, sizeof(*st));
  st->st_mode = S_IFREG | 0444;
  st->st_nlink = 1;
  st->st_size = static_cast<off_t>(size);
  return 0;
}

static int op_getattr(const char* path, struct stat* st) {
  if (!path || !st) return -EINVAL;
  const std::string typed_path = norm(path);
  if (is_typed_namespace(typed_path)) {
    std::memset(st, 0, sizeof(*st));
    st->st_uid = 0;
    st->st_gid = 0;
    if (is_typed_directory(typed_path)) {
      st->st_mode = S_IFDIR | 0555;
      st->st_nlink = 2;
      return 0;
    }
    if (!is_typed_route(typed_path)) return -ENOENT;
    if (auto size = g_open_typed_attributes.size(typed_path))
      return fill_typed_result_stat(*size, st);
    auto result = acquire_typed_route(typed_path);
    if (!result.ok()) return -iotdbfs::query_status_errno(result.status());
    return fill_typed_result_stat(result.value()->size(), st);
  }
  auto metadata_scope = owner_read_budget();
  std::lock_guard<std::recursive_mutex> namespace_lk(g_namespace_mu);
  memset(st, 0, sizeof(*st));
  string full = norm(path);
  string base_path = full;
  string query;
  split_path_and_query(full, base_path, query);
  string p = norm(base_path);

  auto ino = get_inode_maybe_version(p);
  if (!ino)
    return -ENOENT;
  string data_path = ino->path.empty() ? p : ino->path;

  auto fill_times = [&](const Inode& node) {
#ifdef __APPLE__
    st->st_mtimespec = {static_cast<time_t>(node.mtime / 1000),
      static_cast<long>((node.mtime % 1000) * 1000000)};
    st->st_ctimespec = {static_cast<time_t>(node.ctime / 1000),
      static_cast<long>((node.ctime % 1000) * 1000000)};
    st->st_atimespec = {static_cast<time_t>(node.atime / 1000),
      static_cast<long>((node.atime % 1000) * 1000000)};
#else
    st->st_mtim = {static_cast<time_t>(node.mtime / 1000),
      static_cast<long>((node.mtime % 1000) * 1000000)};
    st->st_ctim = {static_cast<time_t>(node.ctime / 1000),
      static_cast<long>((node.ctime % 1000) * 1000000)};
    st->st_atim = {static_cast<time_t>(node.atime / 1000),
      static_cast<long>((node.atime % 1000) * 1000000)};
#endif
    };

  if (query.empty()) {
    if (!g_version_directory_owner && !ino->is_dir && !parse_version_path(p).is_version) {
      if (auto session = find_writer_session(p)) {
        std::lock_guard<std::mutex> lk(session->mu);
        *ino = session->inode;
        ino->size = static_cast<long long>(session->staged.size());
      }
    }
    if (ino->is_dir) {
      st->st_mode = S_IFDIR | (ino->mode & 07777);
      st->st_nlink = 2;
      st->st_size = 0;
    }
    else {
      if (ino->size < 0 || !fuse_semantics::fits_off_t(
        static_cast<uint64_t>(ino->size)))
        return -EOVERFLOW;
      st->st_mode = S_IFREG | (ino->mode & 07777);
      st->st_nlink = 1;
      st->st_size = (off_t)ino->size;
    }
    st->st_uid = ino->uid;
    st->st_gid = ino->gid;
    fill_times(*ino);
    return 0;
  }

  if (ino->is_dir)
    return -EISDIR;
  if (int rc = hydrate_owner_inode(*ino); rc != 0) return rc;
  auto adapter = format_registry().resolve_by_id(ino->format_id);
  if (!adapter || !adapter->can_handle_virtual_query(data_path))
    return -ENOENT;

  auto spec = adapter->parse_query(query);
  if (!spec || spec->type == iotdbfs::FormatViewType::None)
    return -ENOENT;
  auto attr = adapter->get_virtual_attr(
    ino->row_time, data_path, *spec, query);
  if (!attr.ok)
    return -ENOENT;
  st->st_mode = S_IFREG | (ino->mode & 07777);
  st->st_uid = static_cast<uid_t>(ino->uid);
  st->st_gid = static_cast<gid_t>(ino->gid);
  st->st_nlink = 1;
  if (!fuse_semantics::fits_off_t(attr.size))
    return -EOVERFLOW;
  st->st_size = static_cast<off_t>(attr.size);
  fill_times(*ino);
  return 0;
}

static int op_fgetattr(const char* path, struct stat* st,
  struct fuse_file_info* fi) {
  if (!st) return -EINVAL;
  auto* ctx = fi ? fh2ctx(fi) : nullptr;
  // An already-authorized handle pins immutable result bytes. Kernel attribute
  // refreshes must use that same handle even after the source is unlinked or
  // its permissions change; new path lookups and opens still reauthorize.
  if (ctx && ctx->typed_result)
    return fill_typed_result_stat(ctx->typed_result->size(), st);
  return op_getattr(path, st);
}

#ifdef __APPLE__
static int op_getxattr(const char* path, const char* name, char* value,
  size_t size, uint32_t position) {
  (void)position;
#else
static int op_getxattr(const char* path, const char* name, char* value,
  size_t size) {
#endif
  if (!name)
    return -EINVAL;

  auto metadata_scope = owner_read_budget();

  string full = norm(path);
  string base_path = full;
  string query;
  split_path_and_query(full, base_path, query);
  if (!query.empty())
    return -ENODATA;

  auto ino = get_inode(norm(base_path));
  if (!ino)
    return -ENOENT;
  if (ino->is_dir)
    return -ENODATA;
  if (int rc = hydrate_owner_inode(*ino); rc != 0) return rc;

  const std::string data_path = ino->path.empty() ? base_path : ino->path;
  auto adapter = format_registry().resolve_by_id(ino->format_id);
  auto attr = adapter
    ? adapter->get_xattr(ino->row_time, data_path, *ino, name)
    : std::nullopt;
  if (!attr)
    return -ENODATA;

  const std::string& val = *attr;
  if (!value)
    return static_cast<int>(val.size());
  if (size < val.size())
    return -ERANGE;
  if (!val.empty())
    std::memcpy(value, val.data(), val.size());
  return static_cast<int>(val.size());
}

#ifdef __APPLE__
static int op_setxattr(const char*, const char*, const char*, size_t, int,
  uint32_t) { return -ENOTSUP; }
static int op_removexattr(const char*, const char*) { return -ENOTSUP; }
#else
static int op_setxattr(const char*, const char*, const char*, size_t, int) {
  return -ENOTSUP;
}
static int op_removexattr(const char*, const char*) { return -ENOTSUP; }
#endif

static int op_listxattr(const char* path, char* list, size_t size) {
  auto metadata_scope = owner_read_budget();
  string full = norm(path);
  string base_path = full;
  string query;
  split_path_and_query(full, base_path, query);
  if (!query.empty())
    return 0;

  auto ino = get_inode(norm(base_path));
  if (!ino)
    return -ENOENT;
  if (ino->is_dir)
    return 0;
  if (int rc = hydrate_owner_inode(*ino); rc != 0) return rc;

  const std::string data_path = ino->path.empty() ? base_path : ino->path;
  auto adapter = format_registry().resolve_by_id(ino->format_id);
  auto attrs = adapter
    ? adapter->list_xattrs(ino->row_time, data_path, *ino)
    : iotdbfs::FormatXattrList{};
  if (attrs.empty())
    return 0;

  size_t total = 0;
  for (const auto& kv : attrs)
    total += kv.first.size() + 1;

  if (!list)
    return static_cast<int>(total);
  if (size < total)
    return -ERANGE;

  char* ptr = list;
  for (const auto& kv : attrs) {
    if (!kv.first.empty()) {
      std::memcpy(ptr, kv.first.data(), kv.first.size());
      ptr += kv.first.size();
    }
    *ptr++ = '\0';
  }
  return static_cast<int>(total);
}

static int op_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
  off_t off, struct fuse_file_info*) {
  if (!fuse_semantics::valid_readdir_offset(off))
    return -EINVAL;
  string p = norm(path);
  if (is_typed_namespace(p)) {
    if (!is_typed_directory(p)) return -ENOENT;
    std::vector<std::string> names{".", ".."};
    if (p == "/.iotdbfs") names.emplace_back("query");
    else if (p == "/.iotdbfs/query") names.emplace_back("v1");
    for (std::size_t i = static_cast<std::size_t>(off);
         i < names.size(); ++i) {
      off_t cookie = 0;
      if (!fuse_semantics::readdir_cookie(i, cookie)) return -EOVERFLOW;
      if (filler(buf, names[i].c_str(), nullptr, cookie) != 0) break;
    }
    return 0;
  }
  auto ino_self = get_inode(p);
  if (!ino_self)
    return -ENOENT;
  if (!ino_self->is_dir)
    return -ENOTDIR;

  auto add_child_name = [&p](std::vector<std::string>& names,
                             std::unordered_set<std::string>& seen,
                             const std::string& name,
                             const std::string& source_path) {
    auto warn_skip = [&](const std::string& reason) {
      LOG_WARN("skip invalid readdir entry parent=" + p +
        " source_path=" + source_path + " name=" + name +
        " reason=" + reason);
    };
    if (name.empty()) {
      warn_skip("empty");
      return;
    }
    if (name == "." || name == "..") {
      warn_skip("reserved");
      return;
    }
    if (name.find('/') != std::string::npos) {
      warn_skip("contains-slash");
      return;
    }
    if (name.find('\0') != std::string::npos) {
      warn_skip("contains-nul");
      return;
    }
    if (name.size() > 255) {
      warn_skip("too-long");
      return;
    }
    if (!seen.insert(name).second)
      return;
    names.push_back(name);
  };

  std::vector<std::string> names;
  std::unordered_set<std::string> seen;
  names.emplace_back(".");
  names.emplace_back("..");
  seen.insert(".");
  seen.insert("..");
  if (p == "/") {
    names.emplace_back(".iotdbfs");
    seen.insert(".iotdbfs");
  }
  auto kids = list_dir_current(p);
  auto versions = list_dir_versions(p);
  for (auto& k : kids)
    add_child_name(names, seen, k.name, k.path);
  for (auto& v : versions)
    add_child_name(names, seen, v.name, v.path);

  const size_t start = static_cast<size_t>(off);
  if (start >= names.size())
    return 0;
  for (size_t index = start; index < names.size(); ++index) {
    off_t cookie = 0;
    if (!fuse_semantics::readdir_cookie(index, cookie))
      return -EOVERFLOW;
    if (filler(buf, names[index].c_str(), nullptr, cookie) != 0) {
      LOG_WARN("readdir filler stopped parent=" + p + " at name=" + names[index]);
      break;
    }
  }
  return 0;
}

static int op_truncate(const char* path, off_t size) {
  if (path && is_typed_namespace(norm(path))) return -EROFS;
  std::lock_guard<std::recursive_mutex> namespace_lk(g_namespace_mu);
  if (size < 0)
    return -EINVAL;
  string full = norm(path);
  string base_path = full;
  string query;
  split_path_and_query(full, base_path, query);
  if (!query.empty())
    return -EINVAL;

  auto vinfo = parse_version_path(base_path);
  string target_path = vinfo.is_version ? vinfo.base_path : base_path;
  if (vinfo.is_version)
    return -EPERM;

  auto ino = load_inode_version_aware(target_path);
  if (!ino)
    return -ENOENT;
  if (ino->is_dir)
    return -EISDIR;

  OpenFileCtx ctx;
  int rc = attach_write_session(&ctx, *ino, false);
  if (rc != 0)
    return rc;
  auto session = ctx.write_session;
  {
    std::lock_guard<std::mutex> lk(session->mu);
    rc = session->staged.resize(static_cast<uint64_t>(size));
    if (rc == 0) {
      mark_write_session_dirty(*session);
      rc = commit_write_session(*session);
    }
    --session->writers;
    if (session->writers == 0)
      unregister_writer_session(target_path, session);
  }
  writers_remove(target_path);
  return rc;
}

static int op_mkdir(const char* path, mode_t mode) {
  if (path && is_typed_namespace(norm(path))) return -EROFS;
  std::lock_guard<std::recursive_mutex> namespace_lk(g_namespace_mu);
  string full = norm(path);
  string base_path = full;
  string query;
  split_path_and_query(full, base_path, query);
  if (!query.empty())
    return -EINVAL;
  string p = norm(base_path);
  if (parse_version_path(p).is_version)
    return -EINVAL;
  if (get_inode(p))
    return -EEXIST;
  string par = parent_of(p);
  auto pi = get_inode(par);
  if (!pi || !pi->is_dir)
    return -ENOENT;
  if (int rc = check_inode_access(*pi, W_OK | X_OK); rc != 0)
    return rc;
  Inode i;
  i.row_time = now_ms();
  i.path = p;
  i.parent = par;
  i.name = base(p);
  i.is_dir = true;
  i.mode = mode;
  i.uid = fuse_get_context()->uid;
  i.gid = fuse_get_context()->gid;
  i.nlink = 2;
  i.ctime = i.mtime = i.atime = i.row_time;
  if (!insert_inode(i)) {
    LOG_ERROR(string("Failed to insert inode for ") + p);
    return -EIO;
  }
  return 0;
}

static int op_create(const char* path, mode_t mode, struct fuse_file_info* fi) {
  if (path && is_typed_namespace(norm(path))) return -EROFS;
  std::lock_guard<std::recursive_mutex> namespace_lk(g_namespace_mu);
  string full = norm(path);
  PublicationProfileOperation publication_profile("empty_create", full, 0);
  string base_path = full;
  string query;
  split_path_and_query(full, base_path, query);
  if (!query.empty())
    return -EINVAL;
  string p = norm(base_path);
  if (parse_version_path(p).is_version)
    return -EINVAL;
  if (get_inode(p))
    return -EEXIST;
  string par = parent_of(p);
  auto pi = get_inode(par);
  if (!pi || !pi->is_dir)
    return -ENOENT;
  if (int rc = check_inode_access(*pi, W_OK | X_OK); rc != 0)
    return rc;

  Inode i;
  i.row_time = next_version_time();
  publication_profile.version(i.row_time);
  i.path = p;
  i.parent = par;
  i.name = base(p);
  i.is_dir = false;
  i.size = 0;
  i.mode = mode;
  i.uid = fuse_get_context()->uid;
  i.gid = fuse_get_context()->gid;
  i.nlink = 1;
  i.ctime = i.mtime = i.atime = i.row_time;
  const auto key = blob_key(i.row_time, p);
  iotdbfs::FormatSource empty_source;
  const auto ingested = ingestion_module().ingest(key, empty_source,
    [&](const iotdbfs::IngestionOutcome& outcome) {
      i.format_id = outcome.format_id.value_or("");
      auto metadata = iotdbfs::serialize_metadata(outcome.metadata, 65536);
      if (!metadata.ok()) return false;
      i.metadata = std::move(metadata.value());
      return structured_blob_backend()->upsert_inode(i, false);
    });
  if (!ingested.ok()) {
    LOG_ERROR(string("Failed to ingest empty file ") + p + " => " +
      ingested.message());
    return -EIO;
  }
  if (!g_version_directory_owner) {
    ensure_inode_cache_configured();
    InodeCache::instance().put(i, false);
  }

  auto* ctx = new OpenFileCtx();
  ctx->row_time = i.row_time;
  ctx->path = p;
  ctx->max_pos = 0;
  if (fi && (fi->flags & O_ACCMODE) != O_RDONLY) {
    int rc = attach_write_session(ctx, i, false);
    if (rc != 0) {
      delete ctx;
      if (!delete_inode_by_path(p, i.row_time))
        LOG_ERROR(string("Failed to roll back inode after writer setup for ") + p);
      return rc;
    }
  }
  fi->fh = (uint64_t)(uintptr_t)ctx;
  fi->direct_io = 1;
  add_open_handle(ctx->path);
  publication_profile.succeeded();
  return 0;
}

static OpenFileCtx* ensure_ctx_from_inode(const string& npath,
  struct fuse_file_info* fi) {
  if (!fi)
    return nullptr;
  if (fi->fh)
    return fh2ctx(fi);
  auto ino_opt = get_inode_maybe_version(npath);
  if (!ino_opt || ino_opt->is_dir)
    return nullptr;
  auto* ctx = new OpenFileCtx();
  {
    std::lock_guard<std::mutex> lk(ctx->mu);
    ctx->row_time = ino_opt->row_time;
    ctx->path = ino_opt->path.empty() ? npath : ino_opt->path;
    ctx->file_size_orig = (uint64_t)(ino_opt->size > 0 ? ino_opt->size : 0);
    ctx->max_pos = ctx->file_size_orig;
    ctx->is_writer = fi && (fi->flags & (O_WRONLY | O_RDWR));
    ctx->invalidate_read_cache();
    ctx->invalidate_raw_file_cache();
    ctx->materialized_cache.clear();
    ctx->materialized_cache_shared.reset();
  }
  if (ctx->file_size_orig > 0) {
    std::lock_guard<std::mutex> lk(ctx->mu);
    ctx->raw_file_cache_shared =
      RawFileCache::instance().get(ctx->row_time, ctx->path);
  }
  fi->fh = (uint64_t)(uintptr_t)ctx;
  return ctx;
}

static int op_open(const char* path, struct fuse_file_info* fi) {
  if (!path || !fi) return -EINVAL;
  const std::string typed_path = norm(path);
  if (is_typed_namespace(typed_path)) {
    if (is_typed_directory(typed_path)) return -EISDIR;
    if (!is_typed_route(typed_path)) return -ENOENT;
    if ((fi->flags & O_ACCMODE) != O_RDONLY ||
        (fi->flags & O_TRUNC)) return -EROFS;
    auto result = acquire_typed_route(typed_path);
    if (!result.ok()) return -iotdbfs::query_status_errno(result.status());
    try {
      auto ctx = std::make_unique<OpenFileCtx>(false);
      ctx->typed_result = std::move(result.value());
      ctx->path = typed_path;
      // is_typed_route above requires the full trailing result SHA-256, so
      // changing the latest source cannot reuse these pinned attributes.
      const int status = g_open_typed_attributes.add(
        ctx->path, ctx->typed_result->size());
      if (status != 0) return status;
      fi->fh = reinterpret_cast<std::uintptr_t>(ctx.release());
    } catch (const std::bad_alloc&) {
      return -ENOMEM;
    }
    fi->direct_io = 0;
    fi->keep_cache = 1;
    return 0;
  }
  // A writer may stream the entire existing file into staging. Its individual
  // read calls remain bounded; do not turn that lifecycle into one query.
  auto metadata_scope = (fi->flags & O_ACCMODE) == O_RDONLY
    ? owner_read_budget() : nullptr;
  std::lock_guard<std::recursive_mutex> namespace_lk(g_namespace_mu);
  string full = norm(path);
  string base_path = full;
  string query;
  split_path_and_query(full, base_path, query);
  string p = norm(base_path);

  auto vinfo = parse_version_path(p);
  if (fi && (fi->flags & (O_WRONLY | O_RDWR)) && vinfo.is_version)
    return -EPERM;

  auto ino = get_inode_maybe_version(p);
  if (!ino)
    return -ENOENT;
  if (ino->is_dir)
    return -EISDIR;
  auto parent_inode = get_inode(parent_of(vinfo.is_version ? vinfo.base_path : p));
  if (!parent_inode)
    return -ENOENT;
  if (int rc = check_inode_access(*parent_inode, X_OK); rc != 0)
    return rc;
  if (fi) {
    const int mask = (fi->flags & (O_WRONLY | O_RDWR)) ? (R_OK | W_OK) : R_OK;
    if (int rc = check_inode_access(*ino, mask); rc != 0)
      return rc;
  }
  if (int rc = hydrate_owner_inode(*ino); rc != 0) return rc;

  const bool write_open = fi && (fi->flags & (O_WRONLY | O_RDWR));
  if (write_open && !query.empty())
    return -EROFS;

  iotdbfs::FormatViewType view_type = iotdbfs::FormatViewType::None;
  iotdbfs::FormatQueryPtr query_spec;
  std::shared_ptr<const iotdbfs::FormatAdapter> adapter;
  if (!query.empty()) {
    const std::string data_path = ino->path.empty() ? p : ino->path;
    adapter = format_registry().resolve_by_id(ino->format_id);
    if (!adapter || !adapter->can_handle_virtual_query(data_path))
      return -ENOENT;
    query_spec = adapter->parse_query(query);
    if (!query_spec || query_spec->type == iotdbfs::FormatViewType::None)
      return -ENOENT;
    view_type = query_spec->type;
  }

  auto* ctx = new OpenFileCtx();
  {
    std::lock_guard<std::mutex> lk(ctx->mu);
    ctx->row_time = ino->row_time;
    ctx->path = ino->path.empty() ? norm(p) : ino->path;
    ctx->file_size_orig = (uint64_t)max(0LL, ino->size);
    ctx->max_pos = ctx->file_size_orig;
    ctx->is_writer = fi && (fi->flags & (O_WRONLY | O_RDWR));
    ctx->invalidate_read_cache();
    ctx->invalidate_raw_file_cache();
  }
  if (view_type == iotdbfs::FormatViewType::Range) {
    auto opened = adapter->open_virtual(
      ctx->row_time, ctx->path, *query_spec, query);
    if (!opened.ok || opened.type != iotdbfs::FormatViewType::Range) {
      delete ctx;
      return -ENOENT;
    }
    std::lock_guard<std::mutex> lk(ctx->mu);
    ctx->is_range_view = true;
    ctx->view_off = opened.range.offset;
    ctx->view_len = opened.range.length;
  }
  else if (view_type == iotdbfs::FormatViewType::Materialized) {
    auto cache_key =
      std::to_string(ctx->row_time) + "|" + norm(ctx->path) + "|" + query;
    auto attr = adapter->get_virtual_attr(
      ctx->row_time, ctx->path, *query_spec, query);
    if (!attr.ok) {
      delete ctx;
      return -ENOENT;
    }
    if (auto cached = DecodedDataCache::instance().get(cache_key);
      cached && cached->size() == attr.size) {
      std::lock_guard<std::mutex> lk(ctx->mu);
      ctx->is_materialized_view = true;
      ctx->materialized_cache.clear();
      ctx->materialized_cache_shared = cached;
    }
    else {
      auto opened = adapter->open_virtual(
        ctx->row_time, ctx->path, *query_spec, query);
      if (!opened.ok ||
        opened.type != iotdbfs::FormatViewType::Materialized) {
        delete ctx;
        return -ENOENT;
      }
      auto vec_ptr = std::make_shared<std::vector<uint8_t>>(
        std::move(opened.data));
      auto shared = DecodedDataCache::instance().put(cache_key, vec_ptr);
      if (!shared)
        shared = std::static_pointer_cast<const std::vector<uint8_t>>(vec_ptr);
      std::lock_guard<std::mutex> lk(ctx->mu);
      ctx->is_materialized_view = true;
      ctx->materialized_cache_shared = shared;
      ctx->materialized_cache.clear();
    }
  }
  else if (ctx->file_size_orig > 0) {
    std::lock_guard<std::mutex> lk(ctx->mu);
    ctx->raw_file_cache_shared =
      RawFileCache::instance().get(ctx->row_time, ctx->path);
  }

  if (fi && query.empty())
    fi->direct_io = 1;
  if (write_open) {
    int rc = attach_write_session(ctx, *ino, (fi->flags & O_TRUNC) != 0);
    if (rc != 0) {
      delete ctx;
      return rc;
    }
  }
  else if (!g_version_directory_owner && query.empty() && !vinfo.is_version) {
    ctx->write_session = find_writer_session(p);
  }
  fi->fh = (uint64_t)(uintptr_t)ctx;
  add_open_handle(ctx->path);
  return 0;
}

static int op_read(const char* path, char* buf, size_t size, off_t off,
  struct fuse_file_info* fi) {
  if (!buf || size == 0)
    return 0;
  if (off < 0)
    return -EINVAL;
  if (fi) {
    auto* opened = fh2ctx(fi);
    if (opened && opened->typed_result) {
      auto bytes = opened->typed_result->read(
        static_cast<std::uint64_t>(off), size);
      if (!bytes.ok()) return -iotdbfs::query_status_errno(bytes.status());
      if (bytes.value().size() > static_cast<std::size_t>(
          std::numeric_limits<int>::max())) return -EOVERFLOW;
      if (!bytes.value().empty())
        std::memcpy(buf, bytes.value().data(), bytes.value().size());
      return static_cast<int>(bytes.value().size());
    }
  }
  if (path && is_typed_namespace(norm(path))) return -EBADF;
  auto metadata_scope = owner_read_budget();
  string full = norm(path);
  string base_path = full;
  string query;
  split_path_and_query(full, base_path, query);
  string npath = norm(base_path);

  if (!query.empty()) {
    auto query_inode = get_inode_maybe_version(npath);
    auto adapter = query_inode
      ? format_registry().resolve_by_id(query_inode->format_id) : nullptr;
    auto spec = adapter ? adapter->parse_query(query) : nullptr;
    if (!spec || spec->type == iotdbfs::FormatViewType::None)
      return -ENOENT;
  }

  OpenFileCtx* ctx = fi ? fh2ctx(fi) : nullptr;
  if (!ctx)
    ctx = ensure_ctx_from_inode(npath, fi);
  if (!ctx)
    return -ENOENT;

  // Even an exact-version/raw cache hit must reject an uncertain owner. Only
  // already opened immutable IPC handles bypass the namespace (above).
  std::optional<Inode> committed_inode;
  if (g_version_directory_owner) {
    committed_inode = get_inode_maybe_version(npath);
    if (!committed_inode) return -ENOENT;
    if (query.empty()) {
      if (int rc = hydrate_owner_inode(*committed_inode); rc != 0) return rc;
    }
  }
  auto live_session = ctx->is_writer ? ctx->write_session : nullptr;
  if (!g_version_directory_owner && query.empty() && !parse_version_path(npath).is_version) {
    if (auto current = find_writer_session(npath))
      live_session = current;
  }
  if (live_session && query.empty()) {
    std::lock_guard<std::mutex> session_lk(live_session->mu);
    return live_session->staged.read(buf, size, static_cast<uint64_t>(off));
  }

  std::unique_lock<std::mutex> lk(ctx->mu);
  if (g_version_directory_owner && !query.empty()) {
    // A virtual view stays bound to its opened version even if the current
    // path advances. Validate that exact binding before serving its caches.
    auto exact = load_inode_from_db(ctx->path, ctx->row_time);
    if (!exact) return -ENOENT;
    if (int rc = hydrate_owner_inode(*exact); rc != 0) return rc;
  }
  if (query.empty() && !parse_version_path(npath).is_version) {
    auto current = g_version_directory_owner ? committed_inode : get_inode(npath);
    if (current && current->row_time != ctx->row_time) {
      ctx->row_time = current->row_time;
      ctx->max_pos = static_cast<uint64_t>(std::max(0LL, current->size));
      ctx->invalidate_read_cache();
      ctx->invalidate_raw_file_cache();
    }
  }
  // Materialized view: serve slices from the adapter-produced cache.
  if (ctx->is_materialized_view) {
    size_t file_size = materialized_cache_size(ctx);
    if ((uint64_t)off >= file_size)
      return 0;
    size_t n = size;
    if (static_cast<uint64_t>(n) > file_size - static_cast<uint64_t>(off))
      n = (size_t)(file_size - (uint64_t)off);
    const uint8_t* ptr = materialized_cache_ptr(ctx);
    if (n && ptr)
      memcpy(buf, ptr + off, n);
    return (int)n;
  }

  // Range view: read the adapter-selected span from the stored payload.
  uint64_t file_size = ctx->max_pos, base_off = 0, avail_len = file_size;
  if (ctx->is_range_view) {
    base_off = ctx->view_off;
    if (ctx->view_len > 0)
      avail_len = ctx->view_len;
    else
      avail_len = (file_size > base_off) ? (file_size - base_off) : 0;
  }

  if (ctx->raw_file_cache_shared) {
    const size_t raw_size = raw_file_cache_size(ctx);
    const uint8_t* raw_ptr = raw_file_cache_ptr(ctx);
    uint64_t abs_off = 0;
    if (!fuse_semantics::checked_add_u64(base_off,
      static_cast<uint64_t>(off), abs_off))
      return -EOVERFLOW;
    if (raw_size != avail_len) {
      LOG_WARN("raw cache size mismatch path=" + ctx->path +
        " raw_size=" + to_string(raw_size) +
        " avail_len=" + to_string(avail_len) +
        " off=" + to_string(static_cast<uint64_t>(off)));
    }
    if (raw_ptr && abs_off < raw_size) {
      size_t n = size;
      uint64_t remain = avail_len - static_cast<uint64_t>(off);
      if (static_cast<uint64_t>(n) > remain)
        n = static_cast<size_t>(remain);
      size_t raw_remain = raw_size - static_cast<size_t>(abs_off);
      if (n > raw_remain)
        n = raw_remain;
      if (n > 0) {
        std::memcpy(buf, raw_ptr + abs_off, n);
        if (n < size && n < avail_len - static_cast<uint64_t>(off)) {
          LOG_WARN("short read from raw cache path=" + ctx->path +
            " off=" + to_string(static_cast<uint64_t>(off)) +
            " requested=" + to_string(size) +
            " returned=" + to_string(n) +
            " avail_len=" + to_string(avail_len) +
            " raw_size=" + to_string(raw_size));
        }
        return static_cast<int>(n);
      }
    }
  }

  uint64_t rel_off = (uint64_t)off;
  if (rel_off >= avail_len)
    return 0;
  size_t n = size;
  if (static_cast<uint64_t>(n) > avail_len - rel_off)
    n = (size_t)(avail_len - rel_off);

  size_t got_total = 0;
  while (got_total < n) {
    uint64_t request_off = 0;
    if (!fuse_semantics::checked_add_u64(rel_off,
      static_cast<uint64_t>(got_total), request_off))
      return -EOVERFLOW;
    ssize_t got =
      read_with_cache(ctx, base_off, avail_len, request_off,
        n - got_total, buf + got_total);
    if (got < 0)
      return static_cast<int>(got);
    if (got == 0)
      break;
    got_total += got;
  }
  if (got_total < n && got_total < avail_len - rel_off) {
    LOG_WARN("short read path=" + ctx->path +
      " off=" + to_string(rel_off) +
      " requested=" + to_string(n) +
      " returned=" + to_string(got_total) +
      " avail_len=" + to_string(avail_len) +
      " max_pos=" + to_string(ctx->max_pos));
  }
  if (got_total == 0 && rel_off < avail_len) {
    LOG_WARN("read returned zero before EOF path=" + ctx->path +
      " off=" + to_string(rel_off) +
      " requested=" + to_string(n) +
      " avail_len=" + to_string(avail_len) +
      " max_pos=" + to_string(ctx->max_pos) +
      " has_raw_cache=" + string(ctx->raw_file_cache_shared ? "yes" : "no") +
      " cache_valid=" + string(ctx->read_cache.valid ? "yes" : "no"));
  }
  return (int)got_total;
}

static int op_write(const char* path, const char* buf, size_t size, off_t off,
  struct fuse_file_info* fi) {
  if (path && is_typed_namespace(norm(path))) return -EROFS;
  if (fi && fh2ctx(fi) && fh2ctx(fi)->typed_result) return -EROFS;
  LOG_DEBUG(string("op_write: ") + path + " off=" + to_string(off) +
    " size=" + to_string(size));
  if (!buf || size == 0)
    return 0;
  if (off < 0)
    return -EINVAL;
  string full = norm(path);
  string base_path = full;
  string query;
  split_path_and_query(full, base_path, query);
  if (!query.empty())
    return -EROFS;
  string npath = norm(base_path);

  OpenFileCtx* ctx = fi ? fh2ctx(fi) : nullptr;
  if (!ctx)
    ctx = ensure_ctx_from_inode(npath, fi);
  if (!ctx)
    return -ENOENT;
  if (!ctx->is_writer || !ctx->write_session || !fi ||
    (fi->flags & O_ACCMODE) == O_RDONLY)
    return -EBADF;
  if (size > static_cast<size_t>(std::numeric_limits<int>::max()))
    return -EFBIG;
  auto session = ctx->write_session;
  std::lock_guard<std::mutex> lk(session->mu);
  uint64_t write_off = (fi->flags & O_APPEND)
    ? session->staged.size() : static_cast<uint64_t>(off);
  int rc = session->staged.write(buf, size, write_off);
  if (rc != 0)
    session->write_error = rc;
  else
    mark_write_session_dirty(*session);
  return rc != 0 ? rc : static_cast<int>(size);
}

static int op_flush(const char*, struct fuse_file_info* fi) {
  auto* ctx = fi ? fh2ctx(fi) : nullptr;
  if (ctx && ctx->typed_result) return 0;
  if (!ctx || !ctx->is_writer || !ctx->write_session)
    return 0;
  std::lock_guard<std::mutex> lk(ctx->write_session->mu);
  return commit_write_session(*ctx->write_session);
}

static int op_fsync(const char* path, int, struct fuse_file_info* fi) {
  return op_flush(path, fi);
}

static int op_ftruncate(const char*, off_t size, struct fuse_file_info* fi) {
  if (size < 0)
    return -EINVAL;
  auto* ctx = fi ? fh2ctx(fi) : nullptr;
  if (ctx && ctx->typed_result) return -EROFS;
  if (!ctx || !ctx->is_writer || !ctx->write_session)
    return -EBADF;
  std::lock_guard<std::mutex> lk(ctx->write_session->mu);
  int rc = ctx->write_session->staged.resize(static_cast<uint64_t>(size));
  if (rc == 0)
    mark_write_session_dirty(*ctx->write_session);
  return rc;
}

static int op_chmod(const char* path, mode_t mode) {
  if (path && is_typed_namespace(norm(path))) return -EROFS;
  std::lock_guard<std::recursive_mutex> namespace_lk(g_namespace_mu);
  string p = norm(path);
  auto session = find_writer_session(p);
  std::unique_lock<std::mutex> session_lk;
  if (session)
    session_lk = std::unique_lock<std::mutex>(session->mu);
  auto ino = get_inode_maybe_version(p);
  if (!ino)
    return -ENOENT;
  if (parse_version_path(p).is_version)
    return -EPERM;
  const auto* context = fuse_get_context();
  if (context && context->uid != 0 &&
    context->uid != static_cast<uid_t>(ino->uid))
    return -EPERM;
  ino->mode = static_cast<int>(mode & 07777);
  ino->ctime = now_ms();
  if (session) {
    if (!insert_inode(*ino))
      return -EIO;
    session->inode.mode = ino->mode;
    session->inode.ctime = ino->ctime;
    return 0;
  }
  return insert_inode(*ino) ? 0 : -EIO;
}

static int op_chown(const char* path, uid_t uid, gid_t gid) {
  if (path && is_typed_namespace(norm(path))) return -EROFS;
  std::lock_guard<std::recursive_mutex> namespace_lk(g_namespace_mu);
  string p = norm(path);
  auto session = find_writer_session(p);
  std::unique_lock<std::mutex> session_lk;
  if (session)
    session_lk = std::unique_lock<std::mutex>(session->mu);
  auto ino = get_inode_maybe_version(p);
  if (!ino)
    return -ENOENT;
  if (parse_version_path(p).is_version)
    return -EPERM;
  const auto* context = fuse_get_context();
  if (context && context->uid != 0)
    return -EPERM;
  if (uid != static_cast<uid_t>(-1))
    ino->uid = static_cast<int>(uid);
  if (gid != static_cast<gid_t>(-1))
    ino->gid = static_cast<int>(gid);
  ino->ctime = now_ms();
  if (session) {
    if (!insert_inode(*ino))
      return -EIO;
    session->inode.uid = ino->uid;
    session->inode.gid = ino->gid;
    session->inode.ctime = ino->ctime;
    return 0;
  }
  return insert_inode(*ino) ? 0 : -EIO;
}

static int op_utimens(const char* path, const struct timespec tv[2]) {
  if (path && is_typed_namespace(norm(path))) return -EROFS;
  std::lock_guard<std::recursive_mutex> namespace_lk(g_namespace_mu);
  string p = norm(path);
  auto session = find_writer_session(p);
  std::unique_lock<std::mutex> session_lk;
  if (session)
    session_lk = std::unique_lock<std::mutex>(session->mu);
  auto ino = get_inode_maybe_version(p);
  if (!ino)
    return -ENOENT;
  if (parse_version_path(p).is_version)
    return -EPERM;
  const auto* context = fuse_get_context();
  if (context && context->uid != 0 &&
    context->uid != static_cast<uid_t>(ino->uid))
    return -EPERM;
  if (session && !g_version_directory_owner) {
    ino->atime = session->inode.atime;
    ino->mtime = session->inode.mtime;
    ino->ctime = session->inode.ctime;
  }
  const long long timestamp = now_ms();
  if (!tv) {
    ino->atime = ino->mtime = timestamp;
  }
  else {
    for (size_t i = 0; i < 2; ++i) {
      if (tv[i].tv_nsec != UTIME_NOW && tv[i].tv_nsec != UTIME_OMIT &&
        (tv[i].tv_nsec < 0 || tv[i].tv_nsec >= 1000000000))
        return -EINVAL;
    }
    if (tv[0].tv_nsec == UTIME_OMIT && tv[1].tv_nsec == UTIME_OMIT)
      return 0;
    if (tv[0].tv_nsec == UTIME_NOW)
      ino->atime = timestamp;
    else if (tv[0].tv_nsec != UTIME_OMIT)
      ino->atime = static_cast<long long>(tv[0].tv_sec) * 1000 +
        tv[0].tv_nsec / 1000000;
    if (tv[1].tv_nsec == UTIME_NOW)
      ino->mtime = timestamp;
    else if (tv[1].tv_nsec != UTIME_OMIT)
      ino->mtime = static_cast<long long>(tv[1].tv_sec) * 1000 +
        tv[1].tv_nsec / 1000000;
  }
  ino->ctime = timestamp;
  if (session) {
    if (!insert_inode(*ino))
      return -EIO;
    if (!g_version_directory_owner || !tv || tv[0].tv_nsec != UTIME_OMIT)
      session->inode.atime = ino->atime;
    if (!g_version_directory_owner || !tv || tv[1].tv_nsec != UTIME_OMIT)
      session->inode.mtime = ino->mtime;
    session->inode.ctime = ino->ctime;
    return 0;
  }
  return insert_inode(*ino) ? 0 : -EIO;
}

static int op_release(const char*, struct fuse_file_info* fi) {
  auto* typed_ctx = fi ? fh2ctx(fi) : nullptr;
  if (typed_ctx && typed_ctx->typed_result) {
    g_open_typed_attributes.remove(typed_ctx->path);
    delete typed_ctx;
    fi->fh = 0;
    return 0;
  }
  std::lock_guard<std::recursive_mutex> namespace_lk(g_namespace_mu);
  auto* ctx = fi ? fh2ctx(fi) : nullptr;
  if (!ctx)
    return 0;
  int rc = 0;
  if (ctx->is_writer && ctx->write_session) {
    auto session = ctx->write_session;
    std::lock_guard<std::mutex> lk(session->mu);
    try {
      rc = commit_write_session(*session);
    } catch (...) {
      // release must still retire this handle if an uncertain owner freezes
      // publication. The next operation reports the same unavailable owner.
      rc = -EIO;
    }
    if (session->writers > 0)
      --session->writers;
    if (session->writers == 0)
      unregister_writer_session(ctx->path, session);
    writers_remove(ctx->path);
  }
  remove_open_handle(ctx->path);
  delete ctx;
  fi->fh = 0;
  return rc;
}

static int op_unlink(const char* path) {
  if (path && is_typed_namespace(norm(path))) return -EROFS;
  std::lock_guard<std::recursive_mutex> namespace_lk(g_namespace_mu);
  string full = norm(path);
  string base_path = full;
  string query;
  split_path_and_query(full, base_path, query);
  if (!query.empty())
    return -EPERM;
  string p = norm(base_path);
  if (is_path_in_writing(p) || has_open_handle(p))
    return -EBUSY;
  auto ino = get_inode_maybe_version(p);
  if (!ino)
    return -ENOENT;
  if (ino->is_dir)
    return -EISDIR;
  auto vinfo = parse_version_path(p);
  auto parent_inode = get_inode(parent_of(vinfo.is_version ? vinfo.base_path : p));
  if (!parent_inode)
    return -ENOENT;
  if (int rc = check_inode_access(*parent_inode, W_OK | X_OK); rc != 0)
    return rc;
  string target_path = vinfo.is_version ? vinfo.base_path : p;
  optional<long long> target_row_time =
    vinfo.is_version ? optional<long long>(vinfo.row_time) : nullopt;

  if (!delete_inode_by_path(target_path, target_row_time)) {
    LOG_DEBUG("Failed to delete inode for " + p);
    return -EIO;
  }
  if (!g_version_directory_owner && vinfo.is_version) {
    ensure_inode_cache_configured();
    InodeCache::instance().erase(norm(p), target_row_time);
  }
  return 0;
}

static int op_rmdir(const char* path) {
  if (path && is_typed_namespace(norm(path))) return -EROFS;
  std::lock_guard<std::recursive_mutex> namespace_lk(g_namespace_mu);
  string full = norm(path);
  string base_path = full;
  string query;
  split_path_and_query(full, base_path, query);
  if (!query.empty())
    return -EINVAL;
  string p = norm(base_path);
  if (fuse_semantics::is_root_path(p))
    return -EBUSY;
  auto ino = get_inode(p);
  if (!ino)
    return -ENOENT;
  if (!ino->is_dir)
    return -ENOTDIR;
  if (auto parent_inode = get_inode(parent_of(p))) {
    if (int rc = check_inode_access(*parent_inode, W_OK | X_OK); rc != 0)
      return rc;
  }
  else {
    return -ENOENT;
  }
  auto kids = list_dir_current(p);
  auto versions = list_dir_versions(p);
  if (!kids.empty() || !versions.empty())
    return -ENOTEMPTY;
  if (!delete_inode_by_path(p)) {
    LOG_DEBUG(string("Failed to delete inode for ") + p);
    return -EIO;
  }
  return 0;
}
static int op_rename(const char* from, const char* to) {
  if ((from && is_typed_namespace(norm(from))) ||
      (to && is_typed_namespace(norm(to)))) return -EROFS;
  std::lock_guard<std::recursive_mutex> namespace_lk(g_namespace_mu);
  auto namespace_mutation = g_version_directory_owner
    ? structured_blob_backend()->begin_namespace_mutation() : nullptr;
  if (g_version_directory_owner && !namespace_mutation) return -EIO;
  // Rename copies versions directly through the backend before the ordinary
  // inode helpers run, so invalidate cached route permissions at the outset.
  g_typed_authorization_cache.invalidate();
  string sfrom, sto, q_from, q_to;
  split_path_and_query(norm(from), sfrom, q_from);
  split_path_and_query(norm(to), sto, q_to);
  sfrom = norm(sfrom);
  sto = norm(sto);
  if (!q_from.empty() || !q_to.empty())
    return -EPERM;
  if (parse_version_path(sfrom).is_version || parse_version_path(sto).is_version)
    return -EPERM;
  if (sfrom == sto)
    return 0;
  if (fuse_semantics::is_root_path(sfrom) || fuse_semantics::is_root_path(sto))
    return -EBUSY;
  auto source = get_inode(sfrom);
  if (!source)
    return -ENOENT;
  if (source->is_dir && fuse_semantics::is_strict_descendant_path(sto, sfrom))
    return -EINVAL;
  auto parent = get_inode(parent_of(sto));
  if (!parent)
    return -ENOENT;
  if (!parent->is_dir)
    return -ENOTDIR;
  auto source_parent = get_inode(parent_of(sfrom));
  if (!source_parent)
    return -ENOENT;
  if (int rc = check_inode_access(*source_parent, W_OK | X_OK); rc != 0)
    return rc;
  if (int rc = check_inode_access(*parent, W_OK | X_OK); rc != 0)
    return rc;
  {
    std::lock_guard<std::mutex> lk(g_writer_sessions_mu);
    for (const auto& handle : g_open_handle_counts) {
      if (handle.first == sfrom || handle.first == sto ||
        fuse_semantics::is_strict_descendant_path(handle.first, sfrom) ||
        fuse_semantics::is_strict_descendant_path(handle.first, sto))
        return -EBUSY;
    }
  }
  {
    std::lock_guard<std::mutex> lk(g_writer_sessions_mu);
    for (const auto& entry : g_writer_sessions) {
      if (entry.first == sfrom || entry.first == sto ||
        fuse_semantics::is_strict_descendant_path(entry.first, sfrom) ||
        fuse_semantics::is_strict_descendant_path(entry.first, sto))
        return -EBUSY;
    }
  }
  auto destination = get_inode(sto);
  if (destination) {
    const bool empty = !destination->is_dir ||
      (list_dir_current(sto).empty() && list_dir_versions(sto).empty());
    int rc = fuse_semantics::rename_target_error(
      source->is_dir, true, destination->is_dir, empty);
    if (rc != 0)
      return rc;
  }

  vector<pair<string, long long>> keys;
  if (g_version_directory_owner) {
    auto enumerated = structured_blob_backend()->enumerate_namespace_subtree(sfrom);
    if (!enumerated.ok()) {
      LOG_ERROR("rename owner enumeration failed: " + enumerated.status().message());
      return -EIO;
    }
    for (const auto& inode : enumerated.value()) {
      if (inode.path == sfrom || (source->is_dir &&
          fuse_semantics::is_strict_descendant_path(inode.path, sfrom)))
        keys.emplace_back(inode.path, inode.row_time);
    }
  } else try {
    auto ds = query("SELECT time,path FROM inode WHERE published=true "
      "AND (path='" + esc(sfrom) + "' OR " +
      query_utils::like_subtree_predicate(sfrom) + ")");
    if (!ds) {
      LOG_ERROR("rename enumeration query failed source=" + sfrom);
      return -EIO;
    }
    while (ds->hasNext()) {
      auto row = ds->next();
      if (row->fields.size() < 2 || !row->fields[0].longV ||
        !row->fields[1].stringV) {
        LOG_ERROR("rename enumeration returned a malformed time/path row");
        return -EIO;
      }
      const string p = norm(row->fields[1].stringV.value());
      if (p == sfrom || (source->is_dir &&
        fuse_semantics::is_strict_descendant_path(p, sfrom)))
        keys.emplace_back(p, row->fields[0].longV.value());
    }
  }
  catch (const std::exception& error) {
    LOG_ERROR("rename enumeration failed: " + string(error.what()));
    return -EIO;
  }
  if (keys.empty()) {
    LOG_ERROR("rename enumeration found no published versions source=" + sfrom);
    return -EIO;
  }

  // Release the enumeration result before loading individual inodes, so a
  // one-session connection pool can execute the complete operation.
  sort(keys.begin(), keys.end());
  struct RenameEntry { Inode source; Inode target; };
  vector<RenameEntry> entries;
  vector<pair<string, long long>> created;
  map<string, Inode> newest;
  set<string> old_paths;
  for (const auto& key : keys) {
    auto inode = load_inode_from_db(key.first, key.second);
    if (!inode) {
      LOG_ERROR("rename failed to load enumerated version source=" +
        key.first + "@" + std::to_string(key.second));
      return -EIO;
    }
    Inode target = *inode;
    target.path = sto + key.first.substr(sfrom.size());
    target.parent = parent_of(target.path);
    target.name = base(target.path);
    // Existing destination history remains readable; rebasing the incoming
    // version times prevents collisions and makes the source current.
    if (destination && key.first == sfrom)
      target.row_time = next_version_time(destination->row_time);
    entries.push_back({*inode, target});
  }
  auto rollback = [&]() {
    for (const auto& key : created) {
      if (!delete_inode_db(key.first, key.second))
        LOG_ERROR("rename rollback failed path=" + key.first);
      LayoutCache::instance().invalidate(key.second, key.first);
      InodeCache::instance().discard(key.first);
    }
  };
  for (auto& entry : entries) {
    auto& target = entry.target;
    created.emplace_back(target.path, target.row_time);
    target.ctime = now_ms();
    const bool copied = target.is_dir
      ? upsert_inode_db(target)
      : copy_structured_blob_version(entry.source, target, &target);
    if (!copied) {
      if (!g_version_directory_owner) rollback();
      return -EIO;
    }
    auto it = newest.find(target.path);
    if (it == newest.end() || it->second.row_time < target.row_time)
      newest[target.path] = target;
    old_paths.insert(entry.source.path);
  }
  for (const auto& p : old_paths) {
    if (!delete_inode_by_path(p)) {
      if (g_version_directory_owner) {
        LOG_ERROR("rename owner staging removal failed: " + p);
        return -EIO;
      }
      LOG_ERROR("rename source cleanup failed; rolling back destination: " + p);
      for (const auto& entry : entries) {
        if (entry.source.path != p && old_paths.count(entry.source.path) != 0) {
          const bool restored = entry.source.is_dir
            ? upsert_inode_db(entry.source)
            : copy_structured_blob_version(entry.target, entry.source);
          if (!restored)
            LOG_ERROR("rename source compensation failed path=" + entry.source.path);
        }
      }
      rollback();
      return -EIO;
    }
  }
  if (namespace_mutation) {
    if (!namespace_mutation->commit()) return -EIO;
    g_typed_authorization_cache.invalidate();
  } else {
    for (const auto& entry : newest)
      InodeCache::instance().put(entry.second, false);
  }
  return 0;
}

// ===================== 生命周期 =====================
static void* op_init(struct fuse_conn_info*) {
  try {
    if (!pool_exec_nonquery(
      (string("CREATE DATABASE IF NOT EXISTS ") + g_cfg.db).c_str()))
      return nullptr;
    if (!pool_exec_nonquery((string("USE ") + g_cfg.db).c_str()))
      return nullptr;
    LOG_DEBUG("IoTDB session opened: " + g_cfg.host + ":" +
      to_string(g_cfg.port) + ", DB=" + g_cfg.db);
  }
  catch (const exception& e) {
    LOG_ERROR(string("open session failed: ") + e.what());
    return nullptr;
  }
  if (!ensure_schema()) {
    LOG_ERROR("ensure_schema failed");
    return nullptr;
  }
  if (!get_inode("/")) {
    Inode root;
    root.row_time = now_ms();
    root.path = root.parent = root.name = "/";
    root.is_dir = true;
    root.mode = 0755;
    root.nlink = 2;
    root.uid = root.gid = 0;
    root.ctime = root.mtime = root.atime = root.row_time;
    if (!insert_inode(root))
      LOG_ERROR("Failed to persist root inode");
  }
  try {
    if (!initialize_typed_mount()) {
      const auto* context = fuse_get_context();
      if (context && context->fuse) fuse_exit(context->fuse);
      return nullptr;
    }
  } catch (const std::exception& error) {
    LOG_ERROR(std::string("typed query runtime initialization failed: ") +
      error.what());
    const auto* context = fuse_get_context();
    if (context && context->fuse) fuse_exit(context->fuse);
    return nullptr;
  }
  if (g_query_socket_path.empty()) {
    LOG_WARN("typed query registration unavailable: " + g_query_socket_error);
    return nullptr;
  }
  try {
    auto registration_socket =
      std::make_unique<iotdbfs::QueryRegistrationSocketServer>(
        g_query_socket_path, g_query_mount_root, g_typed_mount->frontend,
        g_typed_mount->catalog, g_typed_mount->storage,
        g_typed_mount->runtime, g_typed_mount->bridge,
        [](const iotdbfs::QueryRegistrationPeer& peer,
           const std::vector<iotdbfs::QuerySourceReference>& sources,
           const iotdbfs::CatalogSelection* selection) {
          return authorize_typed_sources_as(peer.uid, peer.gid,
            peer.supplementary_groups, sources, selection);
        }, g_query_socket_mode, g_training_execution,
        iotdbfs::PreparedTrainingRegistry::Config{}, g_hdf5_execution,
        [](const iotdbfs::DatasetCatalogRequest& request,
           const iotdbfs::DatasetCatalogAuthorizer& authorize,
           const std::shared_ptr<iotdbfs::HotQueryCache::Binding>& binding) {
          return structured_blob_backend()->discover_datasets(request, authorize, binding);
        }, g_typed_mount->hdf5_layout_cache, g_typed_mount->hdf5_profiles,
        [](const iotdbfs::QueryRegistrationPeer& peer,
           const iotdbfs::CatalogSelection* selected)
            -> iotdbfs::QueryRegistrationSocketServer::AuthorizationPublicationCommit {
          if (!kPrefillPublishedAuthorization) return {};
          auto observed = g_typed_authorization_cache.capture();
          const auto backend = structured_blob_backend();
          const auto backend_generation = backend->metadata_generation();
          if (selected && selected->source_permissions &&
              selected->source_permissions->storage_namespace == backend->storage_namespace())
            observed.observed_at = std::min(observed.observed_at,
              selected->source_permissions->observed_at);
          return [peer, observed, backend, backend_generation](std::string_view route) {
            if (backend->metadata_generation() != backend_generation) return false;
            const auto key = iotdbfs::typed_authorization_cache_key(peer.uid,
              peer.gid, std::string(route), peer.supplementary_groups);
            return g_typed_authorization_cache.prefill(key, observed);
          };
        },
        g_version_directory_owner, g_hdf5_query_mode, g_typed_mount->query,
        structured_blob_backend()->hot_query_cache(),
        [] { return structured_blob_backend()->prepare_selector_binding(); },
        [](const iotdbfs::SourceSelector& selector, std::size_t maximum,
           const std::shared_ptr<iotdbfs::HotQueryCache::Binding>& binding) {
          return structured_blob_backend()->scan_published_versions(selector, maximum, binding);
        }, g_result_sender_config);
    auto status = registration_socket->start();
    if (!status.ok()) {
      LOG_WARN("typed query registration unavailable at " + g_query_socket_path +
        ": " + status.message());
    } else {
      LOG_INFO("typed query registration socket: " + g_query_socket_path);
      g_query_registration_socket = std::move(registration_socket);
    }
  } catch (const std::exception& error) {
    LOG_WARN("typed query registration unavailable at " + g_query_socket_path +
      ": " + error.what());
  }
  return nullptr;
}
static void op_destroy(void*) {
  if (g_query_registration_socket) g_query_registration_socket->stop();
  g_query_registration_socket.reset();
  g_typed_mount.reset();
  g_hdf5_execution.native_session_pool.reset();
  try {
    if (!g_version_directory_owner) {
      ensure_inode_cache_configured();
      if (!InodeCache::instance().flush_all())
        LOG_ERROR("IoTDB inode flush failed during shutdown");
    }
    LOG_DEBUG("IoTDB session closed");
  }
  catch (...) {
  }
}

static bool initialize_backend() {
  try {
    ensure_session_pool_inited();
    if (!ensure_schema())
      return false;
    if (!get_inode("/")) {
      Inode root;
      root.row_time = now_ms();
      root.path = root.parent = root.name = "/";
      root.is_dir = true;
      root.mode = 0755;
      root.nlink = 2;
      root.uid = root.gid = 0;
      root.ctime = root.mtime = root.atime = root.row_time;
      if (!insert_inode(root))
        return false;
    }
    if (get_inode("/.iotdbfs")) {
      LOG_ERROR("reserved typed query namespace /.iotdbfs already exists");
      return false;
    }
    return true;
  }
  catch (const std::exception& error) {
    LOG_ERROR(std::string("filesystem backend initialization failed: ") + error.what());
    return false;
  }
  catch (...) {
    LOG_ERROR("filesystem backend initialization failed: unknown error");
    return false;
  }
}

// ===================== main =====================
// Never let a C++ storage/owner exception unwind into libfuse's C dispatcher.
// Existing errno returns pass through unchanged, including permission denials.
template<auto Callback> struct FuseCallbackBoundary;

template<typename Result, typename... Args, Result (*Callback)(Args...)>
struct FuseCallbackBoundary<Callback> {
  static Result invoke(Args... args) noexcept {
    try {
      return Callback(args...);
    } catch (const std::exception& error) {
      try { LOG_ERROR(std::string("FUSE callback failed: ") + error.what()); }
      catch (...) {}
    } catch (...) {
      try { LOG_ERROR("FUSE callback failed with an unknown storage error"); }
      catch (...) {}
    }
    if constexpr (std::is_void_v<Result>) {
      return;
    } else if constexpr (std::is_pointer_v<Result>) {
      // init cannot return errno; stop this mount instead of admitting reads.
      const auto* context = fuse_get_context();
      if (context && context->fuse) fuse_exit(context->fuse);
      return nullptr;
    } else {
      return -EIO;
    }
  }
};

int main(int argc, char* argv[]) {
  load_configuration();
  init_logging(g_log_dir);
  if (g_config_invalid) {
    LOG_ERROR("Invalid configuration: check database identifier, pool size, metadata and query limits");
    return 2;
  }
  std::string eccodes_msg;
  if (iotdbfs::configure_eccodes_runtime(argc > 0 ? argv[0] : nullptr,
    &eccodes_msg)) {
    LOG_INFO(eccodes_msg);
  }
  else {
    LOG_WARN(eccodes_msg);
  }
  if (g_config_loaded_from_file) {
    LOG_INFO(string("Loaded configuration from ") + g_config_path);
  }
  else {
    LOG_WARN(string("Configuration file not found at ") + g_config_path +
      ", using defaults/environment variables");
  }
  LOG_INFO(string("Log directory: ") + g_log_dir);
  LOG_INFO(string("Log level: ") + g_log_level_name);
  LOG_INFO(string("Mount point (configured default): ") + g_mount_point);
  LOG_INFO(string("Starting IOTDB-FS host=") + g_cfg.host +
    " port=" + to_string(g_cfg.port) + " db=" + g_cfg.db +
    (g_cfg.use_prepared ? " (prepared=ON)" : " (prepared=OFF)"));
  if (g_version_directory_owner && argc > 1 &&
      (std::string(argv[1]) == "cross-file-client" || std::string(argv[1]) == "grep" ||
       std::string(argv[1]) == "arrow-migrate" || std::string(argv[1]) == "hdf5-migrate")) {
    LOG_ERROR("Version directory owner mode requires the owner socket; legacy direct clients and migration are disabled");
    return 2;
  }
  if (argc > 1 && std::string(argv[1]) == "cross-file-client") {
    if (!g_grib_extension_enabled)
      return grib_extension_disabled_exit("cross-file-client");
    size_t pool_size = g_pool_size_override == 0 ? 4 : g_pool_size_override;
    return run_cross_file_client(argc - 1, argv + 1, g_cfg, g_mount_point, pool_size);
  }
  if (argc > 1 && std::string(argv[1]) == "grep") {
    if (!g_grib_extension_enabled)
      return grib_extension_disabled_exit("grep");
    size_t pool_size = g_pool_size_override == 0 ? 4 : g_pool_size_override;
    return run_semantic_grep_client(argc - 1, argv + 1, g_cfg, g_mount_point,
      pool_size);
  }
  if (argc > 1 && (std::string(argv[1]) == "arrow-migrate" ||
      std::string(argv[1]) == "hdf5-migrate")) {
    const size_t pool_size = g_pool_size_override == 0 ? 4 :
      g_pool_size_override;
    auto policy = std::string(argv[1]) == "arrow-migrate"
      ? iotdbfs::make_arrow_reingestion_policy()
      : iotdbfs::make_hdf5_reingestion_policy();
    return iotdbfs::run_migration_cli(argc - 1, argv + 1,
      g_cfg, pool_size, std::move(policy));
  }
  std::string writer_lock_error;
  std::optional<iotdbfs::NamespaceWriterLock> namespace_writer_lock;
  if (g_version_directory_owner) {
    try {
      ensure_session_pool_inited();
      iotdbfs::VersionDirectoryConfig config;
      config.table_prefix = g_version_directory_prefix;
      config.owner_directory = g_version_directory_owner_directory;
      config.native_file_root = g_native_file_root;
      config.initialize = g_version_directory_initialize;
      config.reader_pool_size = g_version_directory_reader_pool_size;
      config.payload_read_coalescing = g_version_directory_payload_read_coalescing;
      config.payload_cache_bytes = g_version_directory_payload_cache_bytes;
      config.artifact_validator = [](const std::string& format,
          const std::vector<std::uint8_t>& layout, std::uint64_t payload_size) {
        const auto adapter = format_registry().resolve_by_id(format);
        if (!adapter)
          return iotdbfs::StructuredBlobStatus::failure(
            iotdbfs::StructuredBlobError::InvalidState,
            "format adapter is unavailable for layout validation: " + format);
        const auto validated = adapter->validate_layout_artifact(layout, payload_size);
        return validated.ok() ? iotdbfs::StructuredBlobStatus::success()
          : iotdbfs::StructuredBlobStatus::failure(
              iotdbfs::StructuredBlobError::InvalidState, validated.error());
      };
      if (!structured_blob_backend()->configure_version_directory(config)) {
        LOG_ERROR("Refusing to mount: version directory owner initialization failed");
        return 1;
      }
    } catch (const std::exception& error) {
      LOG_ERROR(std::string("Refusing to mount: version directory owner initialization failed: ") + error.what());
      return 1;
    }
  } else {
    namespace_writer_lock = iotdbfs::NamespaceWriterLock::acquire(
      iotdbfs::namespace_writer_lock_path(g_cfg.host, g_cfg.port, g_cfg.db), &writer_lock_error);
    if (!namespace_writer_lock) {
      LOG_ERROR("Refusing to mount: " + writer_lock_error);
      return 1;
    }
  }
  if (argc > 1 && std::string(argv[1]) == "native-file-bind") {
    // Offline owner command: configure_version_directory has acquired the
    // normal single-owner journal/exclusion. No FUSE mount is created.
    if (!g_version_directory_owner || g_native_file_root.empty() ||
        g_version_directory_initialize) {
      std::cerr << "native-file-bind requires an existing owner namespace and storage.native_file_root\n";
      return 2;
    }
    std::string path, subtree;
    std::optional<long long> version;
    for (int i = 2; i < argc; ++i) {
      const std::string option = argv[i];
      if (i + 1 >= argc) { std::cerr << "native-file-bind option value missing\n"; return 2; }
      const std::string value = argv[++i];
      if (option == "--path" && path.empty()) path = value;
      else if (option == "--subtree" && subtree.empty()) subtree = value;
      else if (option == "--version" && !version) {
        long long parsed = 0;
        const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed);
        if (result.ec != std::errc{} || result.ptr != value.data() + value.size() || parsed < 0) {
          std::cerr << "native-file-bind version must be a nonnegative integer\n"; return 2;
        }
        version = parsed;
      } else { std::cerr << "unknown or repeated native-file-bind option\n"; return 2; }
    }
    if (path.empty() == subtree.empty() || (version && path.empty())) {
      std::cerr << "usage: iotdb_fs native-file-bind --path /logical/file --version TIMESTAMP\n"
                   "   or: iotdb_fs native-file-bind --subtree /logical/root\n";
      return 2;
    }
    const auto& scope = path.empty() ? subtree : path;
    try { (void)iotdbfs::versioning::exact_key(scope, 0); }
    catch (const std::exception&) { std::cerr << "native-file-bind requires a canonical absolute path\n"; return 2; }
    iotdbfs::SourceSelector selector;
    selector.paths = {{path.empty() ? iotdbfs::PathScopeKind::Subtree : iotdbfs::PathScopeKind::Exact, scope}};
    selector.versions.kind = version ? iotdbfs::VersionSelectionKind::Exact : iotdbfs::VersionSelectionKind::All;
    if (version) selector.versions.exact = *version;
    selector.formats.adapters = iotdbfs::AdapterSelectionKind::Listed;
    selector.formats.adapter_ids = {"hdf5"};
    selector.formats.include_byte_passthrough = false;
    const auto manifests = structured_blob_backend()->load_published_manifests(selector);
    if (!manifests.ok() || manifests.value().empty()) {
      std::cerr << "native-file-bind found no matching published HDF5 versions\n"; return 1;
    }
    for (const auto& manifest : manifests.value()) {
      const auto before = structured_blob_backend()->native_binding_source_identity(manifest.key);
      if (!before.ok()) { std::cerr << "cannot verify native binding source identity\n"; return 1; }
      const auto bound = structured_blob_backend()->bind_native_file(manifest.key);
      const auto after = structured_blob_backend()->native_binding_source_identity(manifest.key);
      const bool same = after.ok() && before.value() == after.value();
      std::cout << "{\"protocol\":\"iotdbfs-native-file-bind/1\",\"path\":"
                << publication_json_string(manifest.key.path) << ",\"version\":" << manifest.key.version
                << ",\"bytes\":" << manifest.payload_size << ",\"complete\":" << (bound.ok() && same ? "true" : "false")
                << ",\"source_identity_before\":" << publication_json_string(before.value())
                << ",\"source_identity_after\":" << publication_json_string(after.ok() ? after.value() : "")
                << ",\"same_scientific_identity\":" << (same ? "true" : "false")
                << ",\"scientific_publication\":false,\"message\":"
                << publication_json_string(bound.ok() ? "" : bound.message()) << "}\n";
      if (!bound.ok() || !same) return 1;
    }
    return 0;
  }
  if (!initialize_backend()) {
    LOG_ERROR("Refusing to mount: IoTDB backend is unavailable");
    return 1;
  }
  struct fuse_operations ops;
  memset(&ops, 0, sizeof(ops));
  ops.getattr = FuseCallbackBoundary<op_getattr>::invoke;
  ops.fgetattr = FuseCallbackBoundary<op_fgetattr>::invoke;
  ops.access = FuseCallbackBoundary<op_access>::invoke;
  ops.getxattr = FuseCallbackBoundary<op_getxattr>::invoke;
  ops.setxattr = FuseCallbackBoundary<op_setxattr>::invoke;
  ops.removexattr = FuseCallbackBoundary<op_removexattr>::invoke;
  ops.listxattr = FuseCallbackBoundary<op_listxattr>::invoke;
  ops.readdir = FuseCallbackBoundary<op_readdir>::invoke;
  ops.mkdir = FuseCallbackBoundary<op_mkdir>::invoke;
  ops.rmdir = FuseCallbackBoundary<op_rmdir>::invoke;
  ops.create = FuseCallbackBoundary<op_create>::invoke;
  ops.open = FuseCallbackBoundary<op_open>::invoke;
  ops.read = FuseCallbackBoundary<op_read>::invoke;
  ops.write = FuseCallbackBoundary<op_write>::invoke;
  ops.release = FuseCallbackBoundary<op_release>::invoke;
  ops.flush = FuseCallbackBoundary<op_flush>::invoke;
  ops.fsync = FuseCallbackBoundary<op_fsync>::invoke;
  ops.ftruncate = FuseCallbackBoundary<op_ftruncate>::invoke;
  ops.chmod = FuseCallbackBoundary<op_chmod>::invoke;
  ops.chown = FuseCallbackBoundary<op_chown>::invoke;
  ops.utimens = FuseCallbackBoundary<op_utimens>::invoke;
  ops.unlink = FuseCallbackBoundary<op_unlink>::invoke;
  ops.rename = FuseCallbackBoundary<op_rename>::invoke;
  ops.truncate = FuseCallbackBoundary<op_truncate>::invoke;
  ops.init = FuseCallbackBoundary<op_init>::invoke;
  ops.destroy = FuseCallbackBoundary<op_destroy>::invoke;
  std::vector<char*> fuse_argv;
  fuse_argv.reserve(static_cast<size_t>(argc > 1 ? argc : 2));
  std::vector<std::unique_ptr<char[]>> local_args;
  fuse_argv.push_back(argv[0]);
  bool has_kernel_cache = false;
  bool has_default_permissions = false;

  if (argc > 1) {
    LOG_INFO("Using mount parameters from command line");
    for (int i = 1; i < argc; ++i) {
      if (std::string(argv[i]) == "-o" && i + 1 < argc)
        limit_typed_authorization_ttl(argv[i + 1]);
      else if (std::string(argv[i]).compare(0, 2, "-o") == 0)
        limit_typed_authorization_ttl(std::string(argv[i]).substr(2));
      if (std::string(argv[i]) == "-o" && i + 1 < argc &&
        std::string(argv[i + 1]).find("kernel_cache") != std::string::npos) {
        const std::string option = argv[i + 1];
        has_kernel_cache = option.find("kernel_cache") != std::string::npos;
        has_default_permissions = option.find("default_permissions") != std::string::npos;
      }
      fuse_argv.push_back(argv[i]);
    }
  }
  else {
    std::string mp = g_mount_point.empty() ? std::string("/mnt/dfs") : g_mount_point;
    LOG_INFO(string("Using mount point from configuration: ") + mp);
    auto buf = std::make_unique<char[]>(mp.size() + 1);
    std::memcpy(buf.get(), mp.c_str(), mp.size() + 1);
    fuse_argv.push_back(buf.get());
    local_args.emplace_back(std::move(buf));
  }

  if (!has_kernel_cache) {
    auto opt_flag = std::make_unique<char[]>(3);
    std::memcpy(opt_flag.get(), "-o", 3);
    fuse_argv.push_back(opt_flag.get());
    local_args.emplace_back(std::move(opt_flag));

    const char* opt_value = "kernel_cache";
    auto opt_buf = std::make_unique<char[]>(std::strlen(opt_value) + 1);
    std::memcpy(opt_buf.get(), opt_value, std::strlen(opt_value) + 1);
    fuse_argv.push_back(opt_buf.get());
    local_args.emplace_back(std::move(opt_buf));
    LOG_INFO("Enabling FUSE option: kernel_cache");
  }
  if (!has_default_permissions) {
    auto opt_flag = std::make_unique<char[]>(3);
    std::memcpy(opt_flag.get(), "-o", 3);
    fuse_argv.push_back(opt_flag.get());
    local_args.emplace_back(std::move(opt_flag));
    const char* opt_value = "default_permissions";
    auto opt_buf = std::make_unique<char[]>(std::strlen(opt_value) + 1);
    std::memcpy(opt_buf.get(), opt_value, std::strlen(opt_value) + 1);
    fuse_argv.push_back(opt_buf.get());
    local_args.emplace_back(std::move(opt_buf));
    LOG_INFO("Enabling FUSE option: default_permissions");
  }

  std::string mount_root = g_mount_point.empty()
    ? std::string("/mnt/dfs") : g_mount_point;
  if (argc > 1) {
    for (int i = 1; i < argc; ++i) {
      const std::string argument(argv[i]);
      if (argument == "-o" && i + 1 < argc) { ++i; continue; }
      if (!argument.empty() && argument[0] != '-') mount_root = argument;
    }
  }
  if (!mount_root.empty() && mount_root[0] != '/') {
    char cwd[PATH_MAX];
    if (!getcwd(cwd, sizeof(cwd))) {
      LOG_ERROR("cannot resolve query mount root");
      return 1;
    }
    mount_root = std::string(cwd) + "/" + mount_root;
  }
  const char* socket_env = std::getenv("IOTDBFS_QUERY_SOCKET");
  const char* socket_mode_env = std::getenv("IOTDBFS_QUERY_SOCKET_MODE");
  if (socket_mode_env && *socket_mode_env) {
    if (std::strcmp(socket_mode_env, "0600") == 0)
      g_query_socket_mode = 0600;
    else if (std::strcmp(socket_mode_env, "0666") == 0)
      g_query_socket_mode = 0666;
    else {
      LOG_ERROR("IOTDBFS_QUERY_SOCKET_MODE must be 0600 or 0666");
      return 1;
    }
  }
  if (g_query_socket_mode == 0666 && (!socket_env || !*socket_env)) {
    LOG_ERROR("shared query registration requires IOTDBFS_QUERY_SOCKET "
      "in a directory searchable by intended users");
    return 1;
  }
  std::string socket_path;
  if (socket_env && *socket_env) {
    socket_path = socket_env;
  } else {
    const char* runtime_env = std::getenv("XDG_RUNTIME_DIR");
    auto selected = iotdbfs::default_query_registration_socket_path(
      mount_root, runtime_env ? runtime_env : "");
    if (selected.ok()) socket_path = std::move(selected.value());
    else g_query_socket_error = selected.status().message();
  }
  g_query_mount_root = std::move(mount_root);
  g_query_socket_path = std::move(socket_path);

  int fuse_argc = static_cast<int>(fuse_argv.size());
  const int result = fuse_main(fuse_argc, fuse_argv.data(), &ops, nullptr);
  return result;
}
