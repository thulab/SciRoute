#include "iotdbfs/Logging.h"
#include "iotdbfs/PathUtils.h"
#include "iotdbfs/QueryUtils.h"
#include "iotdbfs/formats/grib/gribUtils.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include "iotdbfs/Filesystem.h"
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

// namespace fs handled by Filesystem.h

struct Config {
  std::vector<std::string> srcs{
    "/mnt/local/paper_0.grib2"
    "?function=read&level=500&dtype=f64"
  };
  int num_tasks = 100;
  int concurrency = 10;
};

struct PreparedQuery {
  fs::path base_path;
  std::string normalized_src;
  bool has_query{ false };
  std::map<std::string, std::string> kv;
  std::shared_ptr<const std::vector<uint8_t>> file_bytes;
  grib_utils::MsgInfo msg;
  grib_utils::DataDType dtype{ grib_utils::DataDType::F32 };
  grib_utils::CropParams crop;
  grib_utils::InterpParams interp;
  bool raw_binary{ false };
};

namespace {

std::mutex g_print_mutex;

bool parse_positive_int(const std::string& text, int& value) {
  if (text.empty()) {
    return false;
  }

  try {
    const long parsed = std::stol(text);
    if (parsed <= 0 || parsed > std::numeric_limits<int>::max()) {
      return false;
    }
    value = static_cast<int>(parsed);
    return true;
  }
  catch (...) {
    return false;
  }
}

void print_usage(const char* prog) {
  const Config defaults;
  std::cout
    << "用法: " << prog << " [--src=FILE_OR_QUERY]... [--tasks=N] [--concurrency=N]\n"
    << "默认值:\n"
    << "  src=" << defaults.srcs.front() << "\n"
    << "  tasks=" << defaults.num_tasks << "\n"
    << "  concurrency=" << defaults.concurrency << "\n"
    << "说明: 直接从本地 GRIB2 文件读取消息内容，调用 src/formats/grib/gribUtils.cpp 的同一套\n"
    << "      消息匹配、解码、裁剪与插值逻辑，不走 FUSE。可重复传入多个\n"
    << "      --src，单个任务会按给定顺序依次处理这些源。\n";
}

bool parse_args(int argc, char* argv[], Config& cfg) {
  bool saw_custom_src = false;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "-h" || arg == "--help") {
      print_usage(argv[0]);
      std::exit(0);
    }

    if (arg.rfind("--", 0) != 0) {
      std::cerr << "未知参数: " << arg << "\n";
      return false;
    }

    const auto eq = arg.find('=');
    if (eq == std::string::npos || eq <= 2) {
      std::cerr << "参数格式应为 --key=value: " << arg << "\n";
      return false;
    }

    const std::string key = arg.substr(2, eq - 2);
    const std::string value = arg.substr(eq + 1);
    if (key == "src" || key == "source" || key == "file") {
      if (!saw_custom_src) {
        cfg.srcs.clear();
        saw_custom_src = true;
      }
      cfg.srcs.push_back(value);
    }
    else if (key == "tasks") {
      if (!parse_positive_int(value, cfg.num_tasks)) {
        std::cerr << "无效的 tasks 数值: " << value << "\n";
        return false;
      }
    }
    else if (key == "concurrency" || key == "threads") {
      if (!parse_positive_int(value, cfg.concurrency)) {
        std::cerr << "无效的 concurrency 数值: " << value << "\n";
        return false;
      }
    }
    else {
      std::cerr << "未知参数: " << key << "\n";
      return false;
    }
  }

  return true;
}

std::string normalize_query_path(const std::string& src) {
  std::string out;
  out.reserve(src.size());
  bool seen_first_q = false;
  for (char ch : src) {
    if (ch == '?') {
      if (!seen_first_q) {
        seen_first_q = true;
        out.push_back(ch);
      }
      else {
        out.push_back('&');
      }
    }
    else {
      out.push_back(ch);
    }
  }
  return out;
}

std::optional<std::vector<uint8_t>> read_entire_file(const fs::path& path) {
  std::ifstream ifs(path, std::ios::binary | std::ios::ate);
  if (!ifs) {
    std::cerr << "[ERR] 无法打开文件: " << path << "\n";
    return std::nullopt;
  }

  std::streamsize size = ifs.tellg();
  if (size < 0) {
    std::cerr << "[ERR] 获取文件大小失败: " << path << "\n";
    return std::nullopt;
  }
  ifs.seekg(0, std::ios::beg);

  std::vector<uint8_t> data(static_cast<size_t>(size));
  if (size > 0 && !ifs.read(reinterpret_cast<char*>(data.data()), size)) {
    std::cerr << "[ERR] 读取文件失败: " << path << "\n";
    return std::nullopt;
  }
  return data;
}

struct HandleHolder {
  codes_handle* handle{ nullptr };
  explicit HandleHolder(codes_handle* h = nullptr)
    : handle(h) {
  }
  HandleHolder(const HandleHolder&) = delete;
  HandleHolder& operator=(const HandleHolder&) = delete;
  ~HandleHolder() {
    if (handle) {
      codes_handle_delete(handle);
    }
  }
  codes_handle* get() const { return handle; }
};

bool get_long_safe(codes_handle* h, const char* key, long& out) {
  return codes_get_long(h, key, &out) == 0;
}

bool get_string_safe(codes_handle* h, const char* key, std::string& out) {
  size_t need = 0;
  int err = codes_get_length(h, key, &need);
  if (err != 0 || need == 0) {
    out.clear();
    return false;
  }
  std::string buf(need, '\0');
  size_t got = need;
  err = codes_get_string(h, key, buf.data(), &got);
  if (err != 0 || got == 0) {
    out.clear();
    return false;
  }
  out.assign(buf.data(), got > 0 ? got - 1 : 0);
  return true;
}

bool get_units(codes_handle* h, std::string& units) {
  if (get_string_safe(h, "units", units)) {
    return true;
  }
  return get_string_safe(h, "unit", units);
}

bool get_step_range(codes_handle* h, std::string& sr) {
  if (get_string_safe(h, "stepRange", sr)) {
    return true;
  }
  long s = 0;
  long e = 0;
  const bool hs = get_long_safe(h, "startStep", s);
  const bool he = get_long_safe(h, "endStep", e);
  if (hs && he) {
    sr = std::to_string(s) + "-" + std::to_string(e);
    return true;
  }
  if (get_long_safe(h, "step", e)) {
    sr = std::to_string(e);
    return true;
  }
  return false;
}

bool get_ni_nj(codes_handle* h, long& ni, long& nj) {
  bool ok1 = get_long_safe(h, "Ni", ni);
  bool ok2 = get_long_safe(h, "Nj", nj);
  if (ok1 && ok2) {
    return true;
  }
  ok1 = get_long_safe(h, "numberOfPointsAlongParallel", ni);
  ok2 = get_long_safe(h, "numberOfPointsAlongMeridian", nj);
  return ok1 && ok2;
}

std::string canonical_match_key(const std::string& key) {
  if (key == "sn") return "shortName";
  if (key == "sr") return "stepRange";
  if (key == "tl") return "typeOfLevel";
  if (key == "lv") return "level";
  if (key == "st") return "step";
  if (key == "u") return "unit";
  if (key == "gd") return "globalDomain";
  if (key == "np") return "numberOfDataPoints";
  if (key == "ed") return "edition";
  return key;
}

bool parse_i64_strict(const std::string& text, long long& out) {
  if (text.empty()) {
    return false;
  }
  char* endp = nullptr;
  const long long value = std::strtoll(text.c_str(), &endp, 10);
  if (endp == text.c_str() || *endp != '\0') {
    return false;
  }
  out = value;
  return true;
}

bool handle_matches_kv(codes_handle* h, int edition,
  const std::map<std::string, std::string>& kv) {
  for (const auto& it : kv) {
    const std::string key = canonical_match_key(it.first);
    const std::string& want = it.second;

    if (key == "shortName" || key == "typeOfLevel" || key == "stepRange" ||
      key == "unit" || key == "globalDomain") {
      std::string got;
      bool ok = false;
      if (key == "shortName") {
        ok = get_string_safe(h, "shortName", got);
      }
      else if (key == "typeOfLevel") {
        ok = get_string_safe(h, "typeOfLevel", got);
      }
      else if (key == "stepRange") {
        ok = get_step_range(h, got);
      }
      else if (key == "unit") {
        ok = get_units(h, got);
      }
      else if (key == "globalDomain") {
        ok = get_string_safe(h, "globalDomain", got);
      }
      if (!ok || got != want) {
        return false;
      }
      continue;
    }

    if (key == "level" || key == "step" || key == "ni" || key == "nj" ||
      key == "edition" || key == "numberOfDataPoints") {
      long long want_num = 0;
      if (!parse_i64_strict(want, want_num)) {
        return false;
      }
      long long got_num = 0;
      if (key == "edition") {
        got_num = edition;
      }
      else if (key == "ni" || key == "nj") {
        long ni = 0;
        long nj = 0;
        if (!get_ni_nj(h, ni, nj)) {
          return false;
        }
        got_num = (key == "ni") ? static_cast<long long>(ni)
                                : static_cast<long long>(nj);
      }
      else if (key == "numberOfDataPoints") {
        long np = 0;
        if (!get_long_safe(h, "numberOfDataPoints", np)) {
          return false;
        }
        got_num = static_cast<long long>(np);
      }
      else {
        long v = 0;
        if (!get_long_safe(h, key.c_str(), v)) {
          return false;
        }
        got_num = static_cast<long long>(v);
      }
      if (got_num != want_num) {
        return false;
      }
      continue;
    }

    std::string got_s;
    if (get_string_safe(h, key.c_str(), got_s)) {
      if (got_s != want) {
        return false;
      }
      continue;
    }

    long long want_num = 0;
    long got_l = 0;
    if (parse_i64_strict(want, want_num) && get_long_safe(h, key.c_str(), got_l)) {
      if (static_cast<long long>(got_l) != want_num) {
        return false;
      }
      continue;
    }
    return false;
  }
  return true;
}

grib_utils::MsgInfo find_msg_info_in_bytes(const std::vector<uint8_t>& bytes,
  const std::map<std::string, std::string>& kv_match) {
  grib_utils::MsgInfo mi{};
  const auto msgs = grib_utils::find_messages(bytes.data(), bytes.size());
  for (const auto& msg : msgs) {
    HandleHolder holder(grib_utils::new_handle_from_message_guarded(
      bytes.data() + msg.pos, msg.len));
    if (!holder.get()) {
      holder.handle = grib_utils::new_handle_from_message_copy_guarded(
        bytes.data() + msg.pos, msg.len);
    }
    if (!holder.get()) {
      continue;
    }
    if (!handle_matches_kv(holder.get(), msg.edition, kv_match)) {
      continue;
    }

    mi.off = static_cast<uint64_t>(msg.pos);
    mi.len = static_cast<uint64_t>(msg.len);
    mi.ok = true;

    long ni = 0;
    long nj = 0;
    if (get_ni_nj(holder.get(), ni, nj)) {
      mi.ni = ni;
      mi.nj = nj;
    }
    long np = 0;
    if (get_long_safe(holder.get(), "numberOfDataPoints", np)) {
      mi.numberOfDataPoints = np;
    }
    return mi;
  }
  return mi;
}

std::optional<std::vector<uint8_t>> read_memory_object_part(
  const std::shared_ptr<const std::vector<uint8_t>>& bytes,
  uint64_t off, uint64_t len) {
  if (!bytes) {
    return std::nullopt;
  }
  const uint64_t size = static_cast<uint64_t>(bytes->size());
  if (off > size || len > size - off) {
    return std::nullopt;
  }
  std::vector<uint8_t> out(static_cast<size_t>(len));
  if (len > 0) {
    std::memcpy(out.data(), bytes->data() + static_cast<size_t>(off),
      static_cast<size_t>(len));
  }
  return out;
}

std::optional<PreparedQuery> prepare_query(const std::string& raw_src) {
  PreparedQuery prepared;
  prepared.normalized_src = normalize_query_path(raw_src);

  std::string base_path_str;
  std::string query;
  path_utils::split_path_and_query(prepared.normalized_src, base_path_str, query);
  prepared.base_path = base_path_str;
  prepared.has_query = !query.empty();

  if (!fs::exists(prepared.base_path)) {
    std::cerr << "[ERR] 文件不存在: " << prepared.base_path << "\n";
    return std::nullopt;
  }

  if (!prepared.has_query) {
    return prepared;
  }

  auto bytes = read_entire_file(prepared.base_path);
  if (!bytes) {
    return std::nullopt;
  }
  prepared.file_bytes = std::make_shared<const std::vector<uint8_t>>(std::move(*bytes));

  prepared.kv = query_utils::parse_kv_query(query);
  prepared.dtype = grib_utils::parse_dtype_from_kv(prepared.kv);
  prepared.crop = grib_utils::parse_crop_params_from_kv(prepared.kv);
  prepared.interp = grib_utils::parse_interp_params_from_kv(prepared.kv);

  auto type_it = prepared.kv.find("type");
  if (type_it != prepared.kv.end() &&
    path_utils::to_lower(type_it->second) == "binary") {
    prepared.raw_binary = true;
  }

  auto kv_match = query_utils::filter_index_match_kv(prepared.kv);
  grib_utils::strip_crop_keys(kv_match);
  prepared.msg = find_msg_info_in_bytes(*prepared.file_bytes, kv_match);
  if (!prepared.msg.ok) {
    std::cerr << "[ERR] 未匹配到消息: " << raw_src << "\n";
    return std::nullopt;
  }

  return prepared;
}

struct TaskResult {
  bool ok{ false };
  uint64_t source_bytes{ 0 };
  uint64_t output_bytes{ 0 };
};

TaskResult run_one_src(const std::string& raw_src) {
  TaskResult result;
  std::optional<PreparedQuery> prepared = prepare_query(raw_src);
  if (!prepared) {
    return result;
  }

  grib_utils::ReadObjectPartFunc reader =
    [&](uint64_t off, uint64_t len) -> std::optional<std::vector<uint8_t>> {
      return read_memory_object_part(prepared->file_bytes, off, len);
    };

  std::optional<std::vector<uint8_t>> data;
  if (!prepared->has_query) {
    data = read_entire_file(prepared->base_path);
  }
  else if (prepared->raw_binary) {
    data = reader(prepared->msg.off, prepared->msg.len);
  }
  else if (prepared->interp.enabled ||
    prepared->interp.outNi > 0 || prepared->interp.outNj > 0 ||
    prepared->interp.dlon > 0.0 || prepared->interp.dlat > 0.0) {
    data = grib_utils::decode_crop_and_bilinear_resample(
      prepared->msg, prepared->dtype, prepared->crop, prepared->interp, reader);
  }
  else if (prepared->crop.enabled) {
    data = grib_utils::decode_and_maybe_crop_values(
      prepared->msg, prepared->dtype, prepared->crop, reader);
  }
  else {
    data = grib_utils::decode_values_as_dtype(
      prepared->msg, prepared->dtype, reader);
  }

  if (!data) {
    return result;
  }

  result.ok = true;
  result.source_bytes = prepared->has_query
    ? prepared->msg.len
    : static_cast<uint64_t>(data->size());
  result.output_bytes = static_cast<uint64_t>(data->size());
  return result;
}

TaskResult run_one_task(const std::vector<std::string>& raw_srcs) {
  TaskResult result;
  result.ok = true;
  for (const auto& raw_src : raw_srcs) {
    TaskResult one = run_one_src(raw_src);
    if (!one.ok) {
      result.ok = false;
      return result;
    }
    result.source_bytes += one.source_bytes;
    result.output_bytes += one.output_bytes;
  }
  return result;
}

}  // namespace

int main(int argc, char* argv[]) {
  Config cfg;
  if (!parse_args(argc, argv, cfg)) {
    print_usage(argv[0]);
    return 1;
  }

  set_logging_level(LogLevel::WARN);

  if (cfg.srcs.empty()) {
    std::cerr << "[ERR] 未指定有效的源文件路径。\n";
    return 1;
  }

  std::vector<std::string> normalized_srcs;
  std::vector<fs::path> base_paths;
  std::vector<uint64_t> file_sizes;
  normalized_srcs.reserve(cfg.srcs.size());
  base_paths.reserve(cfg.srcs.size());
  file_sizes.reserve(cfg.srcs.size());

  bool any_has_query = false;
  bool all_have_query = true;
  uint64_t total_file_size = 0;

  for (const auto& src : cfg.srcs) {
    const std::string normalized_src = normalize_query_path(src);
    std::string base_path_str;
    std::string query;
    path_utils::split_path_and_query(normalized_src, base_path_str, query);
    const fs::path base_path = base_path_str;
    const bool has_query = !query.empty();

    if (base_path.empty()) {
      std::cerr << "[ERR] 未指定有效的源文件路径: " << src << "\n";
      return 1;
    }

    if (!fs::exists(base_path)) {
      std::cerr << "[ERR] 文件不存在: " << base_path << "\n";
      return 1;
    }

    uintmax_t file_size = 0;
    try {
      file_size = fs::file_size(base_path);
    }
    catch (const std::exception& e) {
      std::cerr << "[ERR] 获取文件大小失败: " << base_path
                << " : " << e.what() << "\n";
      return 1;
    }

    normalized_srcs.push_back(normalized_src);
    base_paths.push_back(base_path);
    file_sizes.push_back(static_cast<uint64_t>(file_size));
    total_file_size += static_cast<uint64_t>(file_size);
    any_has_query = any_has_query || has_query;
    all_have_query = all_have_query && has_query;
  }

  std::cout << "源文件数: " << base_paths.size() << "\n";
  for (size_t i = 0; i < base_paths.size(); ++i) {
    std::cout << "源文件[" << i << "]: " << base_paths[i] << "\n";
    if (normalized_srcs[i].find('?') != std::string::npos) {
      std::cout << "查询路径[" << i << "]: " << normalized_srcs[i] << "\n";
    }
    std::cout << "文件大小[" << i << "]: " << file_sizes[i] << " bytes\n";
  }
  std::cout << "源文件总大小: " << total_file_size << " bytes\n";
  std::cout << "任务总数: " << cfg.num_tasks
            << "  并发数: " << cfg.concurrency << "\n";

  // Warm up eccodes on the main thread before launching worker concurrency.
  // Each worker still rebuilds the index for its own query, but this avoids
  // races in the library's first-time parser/definition initialization.
  if (any_has_query) {
    for (const auto& normalized_src : normalized_srcs) {
      if (normalized_src.find('?') == std::string::npos) {
        continue;
      }
      auto warmup = prepare_query(normalized_src);
      if (!warmup) {
        std::cerr << "[ERR] 预热查询失败: " << normalized_src << "\n";
        return 1;
      }
    }
  }

  std::atomic<int> next_task{0};
  std::atomic<int> success_count{0};
  std::atomic<int> fail_count{0};
  std::atomic<uint64_t> total_source_bytes{0};
  std::atomic<uint64_t> total_output_bytes{0};
  std::atomic<uint64_t> total_latency_ns{0};
  std::atomic<bool> fatal_error{false};

  const auto start = std::chrono::steady_clock::now();

  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(cfg.concurrency));
  for (int t = 0; t < cfg.concurrency; ++t) {
    workers.emplace_back([&]() {
      for (;;) {
        if (fatal_error.load(std::memory_order_relaxed)) {
          break;
        }

        const int task_index = next_task.fetch_add(1, std::memory_order_relaxed);
        if (task_index >= cfg.num_tasks) {
          break;
        }

        const auto op_begin = std::chrono::steady_clock::now();
        TaskResult task = run_one_task(cfg.srcs);
        const auto op_end = std::chrono::steady_clock::now();
        const uint64_t latency_ns = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_begin).count());

        if (!task.ok) {
          fail_count.fetch_add(1, std::memory_order_relaxed);
          fatal_error.store(true, std::memory_order_relaxed);
          {
            std::lock_guard<std::mutex> lock(g_print_mutex);
            std::cerr << "[ERR] 任务 " << task_index << " 查询失败。\n";
          }
          break;
        }

        success_count.fetch_add(1, std::memory_order_relaxed);
        total_source_bytes.fetch_add(task.source_bytes, std::memory_order_relaxed);
        total_output_bytes.fetch_add(task.output_bytes, std::memory_order_relaxed);
        total_latency_ns.fetch_add(latency_ns, std::memory_order_relaxed);

      }
    });
  }

  for (auto& worker : workers) {
    worker.join();
  }

  const auto end = std::chrono::steady_clock::now();
  const auto elapsed_ns =
    std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  const double elapsed_ms = elapsed_ns / 1e6;
  const double elapsed_seconds = elapsed_ns / 1e9;
  const double total_source_mib = total_source_bytes.load() / (1024.0 * 1024.0);
  const double total_output_mib = total_output_bytes.load() / (1024.0 * 1024.0);
  const double source_throughput =
    elapsed_seconds > 0.0 ? total_source_mib / elapsed_seconds : 0.0;
  const double output_throughput =
    elapsed_seconds > 0.0 ? total_output_mib / elapsed_seconds : 0.0;

  std::cout << "结果:\n";
  std::cout << "成功任务: " << success_count.load() << "\n";
  std::cout << "失败任务: " << fail_count.load() << "\n";
  std::cout << "总耗时: " << std::fixed << std::setprecision(3) << elapsed_ms
            << " ms (" << std::fixed << std::setprecision(3)
            << elapsed_seconds << " s)\n";
  if (all_have_query) {
    std::cout << "源消息读取总量: " << total_source_bytes.load() << " bytes ("
              << std::fixed << std::setprecision(2) << total_source_mib << " MiB)\n";
    std::cout << "查询结果总量: " << total_output_bytes.load() << " bytes ("
              << std::fixed << std::setprecision(2) << total_output_mib << " MiB)\n";
    std::cout << "源消息读取吞吐: " << std::fixed << std::setprecision(2)
              << source_throughput << " MiB/s\n";
    std::cout << "查询结果吞吐: " << std::fixed << std::setprecision(2)
              << output_throughput << " MiB/s\n";
  }
  else if (!any_has_query) {
    std::cout << "读取总量: " << total_output_bytes.load() << " bytes ("
              << std::fixed << std::setprecision(2) << total_output_mib << " MiB)\n";
    std::cout << "平均吞吐: " << std::fixed << std::setprecision(2)
              << output_throughput << " MiB/s\n";
  }
  else {
    std::cout << "源读取总量: " << total_source_bytes.load() << " bytes ("
              << std::fixed << std::setprecision(2) << total_source_mib << " MiB)\n";
    std::cout << "结果总量: " << total_output_bytes.load() << " bytes ("
              << std::fixed << std::setprecision(2) << total_output_mib << " MiB)\n";
    std::cout << "源读取吞吐: " << std::fixed << std::setprecision(2)
              << source_throughput << " MiB/s\n";
    std::cout << "结果吞吐: " << std::fixed << std::setprecision(2)
              << output_throughput << " MiB/s\n";
  }

  if (success_count.load() > 0) {
    const double avg_latency_ms =
      static_cast<double>(total_latency_ns.load()) / 1e6 / success_count.load();
    std::cout << "平均查询延迟(成功任务): " << std::fixed << std::setprecision(3)
              << avg_latency_ms << " ms\n";
  }

  if (fail_count.load() > 0 || fatal_error.load()) {
    std::cerr << "[WARN] 存在失败或异常终止的任务，请检查查询路径和源文件内容。\n";
    return 2;
  }

  return 0;
}
