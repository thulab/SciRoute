#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include "iotdbfs/Filesystem.h"
#include <eccodes.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// namespace fs handled by Filesystem.h

struct Config {
  fs::path src_file = "/root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2";
  std::string ftp_url = "ftp://127.0.0.1/upload";
  std::string ftp_user;
  std::string ftp_pass;
  std::string prefix = "upload_";
  int num_tasks = 100;
  int concurrency = 10;
};

namespace {

bool parse_int(const std::string& s, int& out) {
  if (s.empty()) return false;
  try {
    long v = std::stol(s);
    if (v <= 0 || v > std::numeric_limits<int>::max())
      return false;
    out = static_cast<int>(v);
    return true;
  }
  catch (...) {
    return false;
  }
}

void print_usage(const char* prog) {
  Config def;
  std::cout << "用法: " << prog
            << " [--src=FILE] [--ftp-url=URL] [--ftp-user=USER] [--ftp-pass=PASS]"
               " [--prefix=NAME_PREFIX] [--tasks=N] [--concurrency=N]\n"
            << "默认值: src=" << def.src_file
            << " ftp-url=" << def.ftp_url << " prefix=" << def.prefix
            << " tasks=" << def.num_tasks << " concurrency=" << def.concurrency << '\n'
            << "说明: 需要系统已安装 curl，程序将通过 curl --ftp-create-dirs 上传文件。\n";
}

bool parse_args(int argc, char* argv[], Config& cfg) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-h" || arg == "--help") {
      print_usage(argv[0]);
      std::exit(0);
    }
    if (arg.rfind("--", 0) != 0) {
      std::cerr << "未知参数: " << arg << "\n";
      return false;
    }
    auto eq = arg.find('=');
    if (eq == std::string::npos || eq <= 2) {
      std::cerr << "参数格式应为 --key=value: " << arg << "\n";
      return false;
    }
    std::string key = arg.substr(2, eq - 2);
    std::string value = arg.substr(eq + 1);

    if (key == "src" || key == "source") {
      cfg.src_file = value;
    }
    else if (key == "ftp-url" || key == "ftp") {
      cfg.ftp_url = value;
    }
    else if (key == "ftp-user" || key == "user") {
      cfg.ftp_user = value;
    }
    else if (key == "ftp-pass" || key == "password") {
      cfg.ftp_pass = value;
    }
    else if (key == "prefix") {
      cfg.prefix = value;
    }
    else if (key == "tasks") {
      int v = 0;
      if (!parse_int(value, v)) {
        std::cerr << "无效的 tasks 数值: " << value << "\n";
        return false;
      }
      cfg.num_tasks = v;
    }
    else if (key == "concurrency" || key == "threads") {
      int v = 0;
      if (!parse_int(value, v)) {
        std::cerr << "无效的 concurrency 数值: " << value << "\n";
        return false;
      }
      cfg.concurrency = v;
    }
    else {
      std::cerr << "未知参数: " << key << "\n";
      return false;
    }
  }
  return true;
}

std::string shell_quote(const std::string& s) {
  std::string quoted;
  quoted.reserve(s.size() + 2);
  quoted.push_back('\'');
  for (char ch : s) {
    if (ch == '\'') {
      quoted += "'\\''";
    }
    else {
      quoted.push_back(ch);
    }
  }
  quoted.push_back('\'');
  return quoted;
}

std::string shell_quote(const fs::path& path) {
  return shell_quote(path.string());
}

bool get_long(codes_handle* h, const char* key, long& out) {
  return codes_get_long(h, key, &out) == 0;
}

bool get_string(codes_handle* h, const char* key, std::string& out) {
  size_t len = 0;
  if (codes_get_length(h, key, &len) != 0 || len == 0) {
    out.clear();
    return false;
  }
  std::string buf(len, '\0');
  size_t got = len;
  if (codes_get_string(h, key, buf.data(), &got) != 0 || got == 0) {
    out.clear();
    return false;
  }
  out.assign(buf.data(), got > 0 ? got - 1 : 0);
  return true;
}

bool get_units(codes_handle* h, std::string& units) {
  if (get_string(h, "units", units))
    return true;
  return get_string(h, "unit", units);
}

bool get_step_range(codes_handle* h, std::string& sr) {
  if (get_string(h, "stepRange", sr))
    return true;
  long s = 0, e = 0;
  bool hs = get_long(h, "startStep", s);
  bool he = get_long(h, "endStep", e);
  if (hs && he) {
    sr = std::to_string(s) + "-" + std::to_string(e);
    return true;
  }
  if (get_long(h, "step", e)) {
    sr = std::to_string(e);
    return true;
  }
  return false;
}

bool get_ni_nj(codes_handle* h, long& ni, long& nj) {
  bool ok1 = get_long(h, "Ni", ni);
  bool ok2 = get_long(h, "Nj", nj);
  if (ok1 && ok2)
    return true;
  ok1 = get_long(h, "numberOfPointsAlongParallel", ni);
  ok2 = get_long(h, "numberOfPointsAlongMeridian", nj);
  return ok1 && ok2;
}

std::string build_index_line(codes_handle* h, uint64_t offset, uint64_t total_len) {
  std::string shortName;
  std::string stepRange;
  std::string typeOfLevel;
  std::string unit;
  std::string globalDomain;
  long level = 0;
  long step = 0;
  long Ni = 0;
  long Nj = 0;
  long numberOfDataPoints = 0;
  long edition = 0;

  bool has_level = get_long(h, "level", level);
  bool has_step = get_long(h, "step", step);
  bool has_ni_nj = get_ni_nj(h, Ni, Nj);
  bool has_num_points = get_long(h, "numberOfDataPoints", numberOfDataPoints);
  bool has_edition = get_long(h, "editionNumber", edition);

  get_string(h, "shortName", shortName);
  get_step_range(h, stepRange);
  get_string(h, "typeOfLevel", typeOfLevel);
  get_units(h, unit);
  get_string(h, "globalDomain", globalDomain);

  std::ostringstream os;
  os << "{";
  bool first = true;
  auto emit_str = [&](const char* k, const std::string& v) {
    if (v.empty())
      return;
    if (!first) os << ",";
    first = false;
    os << "\"" << k << "\":\"" << v << "\"";
    };
  auto emit_ll = [&](const char* k, long long v, bool has) {
    if (!has) return;
    if (!first) os << ",";
    first = false;
    os << "\"" << k << "\":" << v;
    };
  auto emit_u64 = [&](const char* k, uint64_t v) {
    if (!first) os << ",";
    first = false;
    os << "\"" << k << "\":" << v;
    };

  emit_str("sn", shortName);
  emit_str("sr", stepRange);
  emit_str("tl", typeOfLevel);
  emit_ll("lv", level, has_level);
  emit_ll("st", step, has_step);
  emit_str("u", unit);
  emit_str("gd", globalDomain);
  emit_ll("ni", Ni, has_ni_nj);
  emit_ll("nj", Nj, has_ni_nj);
  emit_ll("np", numberOfDataPoints, has_num_points);
  emit_u64("of", offset);
  emit_u64("ln", total_len);
  emit_ll("ed", edition, has_edition);
  os << "}\n";
  return os.str();
}

bool generate_grib_index(const fs::path& src, std::string& out) {
  FILE* fp = std::fopen(src.c_str(), "rb");
  if (!fp) {
    std::cerr << "[ERR] 无法打开 GRIB 文件生成索引: " << src << "\n";
    return false;
  }
  out.clear();
  int err = 0;
  while (true) {
    codes_handle* h = codes_handle_new_from_file(nullptr, fp, PRODUCT_GRIB, &err);
    if (!h) {
      if (err == CODES_SUCCESS || err == CODES_END_OF_FILE) {
        err = CODES_SUCCESS;
      }
      break;
    }
    off_t offset = 0;
    if (codes_get_message_offset(h, &offset) != 0) {
      codes_handle_delete(h);
      err = -1;
      break;
    }
    long total_len = 0;
    if (!get_long(h, "totalLength", total_len) || total_len < 0) {
      codes_handle_delete(h);
      err = -1;
      break;
    }
    out += build_index_line(h, static_cast<uint64_t>(offset),
      static_cast<uint64_t>(total_len));
    codes_handle_delete(h);
  }
  std::fclose(fp);
  if (err != CODES_SUCCESS) {
    std::cerr << "[ERR] 生成 GRIB 索引失败 (err=" << err << ")\n";
    return false;
  }
  return true;
}

bool upload_via_curl(const fs::path& src, const std::string& ftp_base,
  const std::string& remote_name, const std::string& user, const std::string& pass) {
  std::string url = ftp_base;
  if (!url.empty() && url.back() != '/')
    url.push_back('/');
  url += remote_name;

  std::string command = "curl -sS --ftp-create-dirs -T " + shell_quote(src) + " ";
  if (!user.empty() || !pass.empty()) {
    command += "--user " + shell_quote(user + ":" + pass) + " ";
  }
  command += shell_quote(url);

  int ret = std::system(command.c_str());
  if (ret != 0) {
    std::cerr << "[ERR] curl 上传失败: " << command << " (返回值 " << ret << ")\n";
    return false;
  }
  return true;
}

} // namespace

int main(int argc, char* argv[]) {
  Config cfg;
  if (!parse_args(argc, argv, cfg))
    return 1;

  if (!fs::exists(cfg.src_file)) {
    std::cerr << "[ERR] 源文件不存在: " << cfg.src_file << "\n";
    return 1;
  }

  uintmax_t src_size = 0;
  try {
    src_size = fs::file_size(cfg.src_file);
  }
  catch (const std::exception& e) {
    std::cerr << "[ERR] 获取源文件大小失败: " << e.what() << "\n";
    return 1;
  }

  std::cout << "源文件: " << cfg.src_file << "  大小: " << src_size << " bytes\n";
  std::cout << "FTP 目标: " << cfg.ftp_url << "\n";
  if (!cfg.ftp_user.empty())
    std::cout << "FTP 用户: " << cfg.ftp_user << "\n";
  std::cout << "任务总数: " << cfg.num_tasks << "  并发数: " << cfg.concurrency << "\n";

  std::atomic<int> next_index{ 0 };
  std::atomic<int> success_cnt{ 0 };
  std::atomic<int> fail_cnt{ 0 };
  std::atomic<uint64_t> sum_data_bytes{ 0 };
  std::atomic<uint64_t> sum_index_bytes{ 0 };
  std::atomic<uint64_t> sum_task_latency_ns{ 0 };

  const auto t0 = std::chrono::steady_clock::now();

  std::vector<std::thread> workers;
  workers.reserve(cfg.concurrency);
  const fs::path temp_dir = fs::temp_directory_path();

  for (int t = 0; t < cfg.concurrency; ++t) {
    workers.emplace_back([&, t]() {
      for (;;) {
        int i = next_index.fetch_add(1, std::memory_order_relaxed);
        if (i >= cfg.num_tasks)
          break;
        const auto task_start = std::chrono::steady_clock::now();
        const auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
        std::string remote_base =
          cfg.prefix + std::to_string(now_us) + "_" + std::to_string(t) + "_" + std::to_string(i);
        std::string remote_data_name = remote_base + ".grib2";
        std::string remote_index_name = remote_data_name + ".index";

        std::string index_content;
        const auto index_begin = std::chrono::steady_clock::now();
        if (!generate_grib_index(cfg.src_file, index_content)) {
          fail_cnt.fetch_add(1, std::memory_order_relaxed);
          continue;
        }
        const auto index_end = std::chrono::steady_clock::now();

        fs::path temp_index_path =
          temp_dir / (remote_index_name + ".tmp." + std::to_string(t) + "_" + std::to_string(i));
        {
          std::ofstream ofs(temp_index_path, std::ios::binary | std::ios::trunc);
          if (!ofs.is_open()) {
            std::cerr << "[ERR] 无法创建索引临时文件: " << temp_index_path << "\n";
            fs::remove(temp_index_path);
            fail_cnt.fetch_add(1, std::memory_order_relaxed);
            continue;
          }
          ofs.write(index_content.data(), static_cast<std::streamsize>(index_content.size()));
          if (!ofs) {
            std::cerr << "[ERR] 写索引临时文件失败: " << temp_index_path << "\n";
            fs::remove(temp_index_path);
            fail_cnt.fetch_add(1, std::memory_order_relaxed);
            continue;
          }
        }

        bool ok_data = upload_via_curl(cfg.src_file, cfg.ftp_url, remote_data_name,
          cfg.ftp_user, cfg.ftp_pass);
        bool ok_index = false;
        if (ok_data) {
          ok_index = upload_via_curl(temp_index_path, cfg.ftp_url, remote_index_name,
            cfg.ftp_user, cfg.ftp_pass);
        }

        fs::remove(temp_index_path);

        const auto task_end = std::chrono::steady_clock::now();
        const uint64_t task_ns = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(task_end - task_start).count());
        sum_task_latency_ns.fetch_add(task_ns, std::memory_order_relaxed);

        if (ok_data && ok_index) {
          success_cnt.fetch_add(1, std::memory_order_relaxed);
          sum_data_bytes.fetch_add(static_cast<uint64_t>(src_size), std::memory_order_relaxed);
          sum_index_bytes.fetch_add(static_cast<uint64_t>(index_content.size()), std::memory_order_relaxed);
        }
        else {
          fail_cnt.fetch_add(1, std::memory_order_relaxed);
        }
      }
      });
  }

  for (auto& th : workers) th.join();

  const auto t1 = std::chrono::steady_clock::now();
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

  const uint64_t total_data_bytes = sum_data_bytes.load();
  const uint64_t total_index_bytes = sum_index_bytes.load();
  const uint64_t total_bytes = total_data_bytes + total_index_bytes;
  const double seconds = ms / 1000.0;
  const double mib = total_bytes / (1024.0 * 1024.0);
  const double throughput_mibs = seconds > 0 ? (mib / seconds) : 0.0;

  std::cout << "\n====== FTP 写入统计 ======\n";
  std::cout << "成功: " << success_cnt.load() << "  失败: " << fail_cnt.load() << "\n";
  std::cout << "总耗时: " << ms << " ms (" << std::fixed << std::setprecision(3)
            << seconds << " s)\n";
  std::cout << "写入总量: " << total_bytes << " bytes ("
            << std::fixed << std::setprecision(2) << mib << " MiB)\n";
  std::cout << "平均吞吐: " << std::fixed << std::setprecision(2)
            << throughput_mibs << " MiB/s\n";
  if (success_cnt.load() > 0) {
    const double avg_task_ms =
      static_cast<double>(sum_task_latency_ns.load()) / 1e6 / success_cnt.load();
    std::cout << "其中数据写入: " << total_data_bytes << " bytes, 索引写入: "
              << total_index_bytes << " bytes\n";
    std::cout << "平均任务耗时(含索引生成): " << std::fixed << std::setprecision(3)
              << avg_task_ms << " ms\n";
  }

  if (fail_cnt.load() > 0) {
    std::cerr << "[WARN] 存在失败任务，请检查 curl 输出、FTP 权限或网络连通性。\n";
  }
  return 0;
}

