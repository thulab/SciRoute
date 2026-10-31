#include <atomic>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include "iotdbfs/Filesystem.h"
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <cerrno>
#include <cstring>

#include <eccodes.h>

// namespace fs handled by Filesystem.h

struct Config {
  fs::path src_file = "/root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2";
  std::string mysql_host = "127.0.0.1";
  int mysql_port = 3306;
  std::string mysql_user = "root";
  std::string mysql_pass;
  std::string mysql_db = "iotdb_fs_perf";
  std::string mysql_bin = "mysql";
  std::string inode_table = "inode";
  std::string blob_table = "blob";
  std::string base_path = "/perf";
  std::string prefix = "mysql_";
  int num_tasks = 50;
  int concurrency = 5;
};

namespace {

  std::string shell_quote(const std::string& s) {
    std::string quoted;
    quoted.reserve(s.size() + 2);
    quoted.push_back('\'');
    for (char ch : s) {
      if (ch == '\'')
        quoted += "'\\''";
      else
        quoted.push_back(ch);
    }
    quoted.push_back('\'');
    return quoted;
  }

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
      << " [--src=FILE] [--host=HOST] [--port=PORT] [--user=USER] [--pass=PASS]\n"
      "             [--db=DB] [--inode-table=NAME] [--blob-table=NAME]\n"
      "             [--mysql-bin=PATH] [--base-path=/prefix] [--prefix=name_]\n"
      "             [--tasks=N] [--concurrency=N]\n"
      "说明: 并发解析 GRIB 索引并写入 MySQL inode/blob 表（索引仅生成不入库）。\n";
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
      else if (key == "host") {
        cfg.mysql_host = value;
      }
      else if (key == "port") {
        int v = 0;
        if (!parse_int(value, v)) {
          std::cerr << "无效的 port 数值: " << value << "\n";
          return false;
        }
        cfg.mysql_port = v;
      }
      else if (key == "user") {
        cfg.mysql_user = value;
      }
      else if (key == "pass") {
        cfg.mysql_pass = value;
      }
      else if (key == "db" || key == "database") {
        cfg.mysql_db = value;
      }
      else if (key == "mysql-bin" || key == "mysql") {
        cfg.mysql_bin = value;
      }
      else if (key == "inode-table") {
        cfg.inode_table = value;
      }
      else if (key == "blob-table") {
        cfg.blob_table = value;
      }
      else if (key == "base-path") {
        cfg.base_path = value;
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

  std::string escape_sql(const std::string& s) {
    std::string out;
    out.reserve(s.size() * 2);
    for (char ch : s) {
      if (ch == '\'')
        out += "''";
      else
        out.push_back(ch);
    }
    return out;
  }

  std::string summarize_sql(const std::string& sql) {
    constexpr size_t kMaxPreview = 400;
    if (sql.size() <= kMaxPreview)
      return sql;
    std::ostringstream oss;
    oss << sql.substr(0, kMaxPreview) << "... (total " << sql.size() << " chars)";
    return oss.str();
  }

  std::string quote_identifier(const std::string& name) {
    std::string out;
    out.reserve(name.size() + 2);
    out.push_back('`');
    for (char ch : name) {
      if (ch == '`')
        out += "``";
      else
        out.push_back(ch);
    }
    out.push_back('`');
    return out;
  }

  bool run_mysql(const Config& cfg, const std::string& sql, bool use_db = true,
    const std::string* explain_tag = nullptr) {
    std::ostringstream cmd;
    cmd << shell_quote(cfg.mysql_bin) << " --batch --silent --raw"
      << " --host=" << shell_quote(cfg.mysql_host)
      << " --port=" << cfg.mysql_port
      << " --user=" << shell_quote(cfg.mysql_user);
    if (!cfg.mysql_pass.empty())
      cmd << " --password=" << shell_quote(cfg.mysql_pass);
    if (use_db)
      cmd << " " << shell_quote(cfg.mysql_db);
    cmd << " -e " << shell_quote(sql) << " 2>&1";

    FILE* pipe = popen(cmd.str().c_str(), "r");
    if (!pipe) {
      std::cerr << "[ERR] 无法启动 mysql 命令 (errno=" << errno << ": " << std::strerror(errno) << ")\n";
      std::cerr << "      Command: " << cmd.str() << "\n";
      return false;
    }
    std::string output;
    std::array<char, 4096> buffer{};
    while (true) {
      size_t n = fread(buffer.data(), 1, buffer.size(), pipe);
      if (n > 0)
        output.append(buffer.data(), n);
      if (n < buffer.size()) {
        if (feof(pipe))
          break;
        if (ferror(pipe)) {
          output.append("[read error]");
          break;
        }
      }
      if (n == 0)
        break;
    }
    int ret = pclose(pipe);
    if (ret != 0) {
      std::cerr << "[ERR] mysql 命令执行失败 (返回值 " << ret << ")\n";
      if (explain_tag)
        std::cerr << "      SQL tag: " << *explain_tag << "\n";
      std::cerr << "      Command: " << cmd.str() << "\n"
        << "      SQL: " << summarize_sql(sql) << "\n";
      if (!output.empty())
        std::cerr << "      mysql 输出: " << output << "\n";
      return false;
    }
    return true;
  }

  bool ensure_schema(const Config& cfg) {
    std::ostringstream create_db;
    create_db << "CREATE DATABASE IF NOT EXISTS "
      << quote_identifier(cfg.mysql_db) << ";";
    const std::string tag_db = "create_db";
    if (!run_mysql(cfg, create_db.str(), false, &tag_db))
      return false;

    std::ostringstream create_inode;
    create_inode << "CREATE TABLE IF NOT EXISTS "
      << quote_identifier(cfg.inode_table) << " ("
      << "path VARCHAR(512) NOT NULL PRIMARY KEY,"
      << "size BIGINT NOT NULL,"
      << "ctime BIGINT NOT NULL,"
      << "mtime BIGINT NOT NULL,"
      << "atime BIGINT NOT NULL"
      << ");";
    const std::string tag_inode = "create_inode";
    if (!run_mysql(cfg, create_inode.str(), true, &tag_inode))
      return false;

    std::ostringstream create_blob;
    create_blob << "CREATE TABLE IF NOT EXISTS "
      << quote_identifier(cfg.blob_table) << " ("
      << "path VARCHAR(512) NOT NULL PRIMARY KEY,"
      << "data LONGBLOB,"
      << "size BIGINT,"
      << "mtime BIGINT DEFAULT 0"
      << ");";
    const std::string tag_blob = "create_blob";
    return run_mysql(cfg, create_blob.str(), true, &tag_blob);
  }

} // namespace

int main(int argc, char* argv[]) {
  Config cfg;
  if (!parse_args(argc, argv, cfg))
    return 1;

  uintmax_t src_size = 0;
  try {
    src_size = fs::file_size(cfg.src_file);
  }
  catch (const std::exception& e) {
    std::cerr << "[ERR] 获取源文件大小失败: " << e.what() << "\n";
    return 1;
  }

  std::cout << "源文件: " << cfg.src_file << "  大小: " << src_size << " bytes\n";
  std::cout << "MySQL: " << cfg.mysql_host << ":" << cfg.mysql_port
    << " DB=" << cfg.mysql_db << "\n";
  std::cout << "inode 表: " << cfg.inode_table
    << "  blob 表: " << cfg.blob_table << "\n";
  std::cout << "任务总数: " << cfg.num_tasks << "  并发数: " << cfg.concurrency << "\n";

  if (!ensure_schema(cfg)) {
    std::cerr << "[ERR] 初始化数据库/表结构失败，请检查 MySQL 连接或权限。\n";
    return 1;
  }

  const std::string src_sql_path = escape_sql(cfg.src_file.string());

  std::atomic<int> next_index{ 0 };
  std::atomic<int> success_cnt{ 0 };
  std::atomic<int> fail_cnt{ 0 };
  std::atomic<uint64_t> sum_data_bytes{ 0 };
  std::atomic<uint64_t> sum_index_bytes{ 0 };
  std::atomic<uint64_t> sum_latency_ns{ 0 };

  const auto t0 = std::chrono::steady_clock::now();

  std::vector<std::thread> workers;
  workers.reserve(cfg.concurrency);
  for (int t = 0; t < cfg.concurrency; ++t) {
    workers.emplace_back([&, t]() {
      while (true) {
        int idx = next_index.fetch_add(1, std::memory_order_relaxed);
        if (idx >= cfg.num_tasks)
          break;

        const auto op_begin = std::chrono::steady_clock::now();

        std::string index_text;
        if (!generate_grib_index(cfg.src_file, index_text)) {
          fail_cnt.fetch_add(1, std::memory_order_relaxed);
          continue;
        }

        const auto now = std::chrono::system_clock::now();
        const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
          now.time_since_epoch()).count();
        std::ostringstream path_ss;
        path_ss << cfg.base_path;
        if (!cfg.base_path.empty() && cfg.base_path.back() != '/')
          path_ss << '/';
        path_ss << cfg.prefix << now_ms << "_" << t << "_" << idx << ".grib2";
        std::string file_path = path_ss.str();

        std::string escaped_path = escape_sql(file_path);

        std::ostringstream sql;
        sql << "INSERT INTO " << quote_identifier(cfg.inode_table)
          << "(path,size,ctime,mtime,atime) VALUES('"
          << escaped_path << "'," << src_size << "," << now_ms << "," << now_ms << "," << now_ms
          << ") ON DUPLICATE KEY UPDATE size=VALUES(size),mtime=VALUES(mtime),atime=VALUES(atime);";
        sql << "INSERT INTO " << quote_identifier(cfg.blob_table)
          << "(path,data,size,mtime) VALUES('"
          << escaped_path << "',LOAD_FILE('" << src_sql_path << "'),"
          << src_size << "," << now_ms
          << ") ON DUPLICATE KEY UPDATE size=VALUES(size),mtime=VALUES(mtime);";

        std::string tag = "insert_task_" + std::to_string(idx);
        if (run_mysql(cfg, sql.str(), true, &tag)) {
          success_cnt.fetch_add(1, std::memory_order_relaxed);
          sum_data_bytes.fetch_add(static_cast<uint64_t>(src_size), std::memory_order_relaxed);
          sum_index_bytes.fetch_add(static_cast<uint64_t>(index_text.size()), std::memory_order_relaxed);

          const auto op_end = std::chrono::steady_clock::now();
          const uint64_t op_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_begin).count());
          sum_latency_ns.fetch_add(op_ns, std::memory_order_relaxed);
        }
        else {
          fail_cnt.fetch_add(1, std::memory_order_relaxed);
        }
      }
      });
  }

  for (auto& th : workers)
    th.join();

  const auto t1 = std::chrono::steady_clock::now();
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

  const uint64_t total_data_bytes = sum_data_bytes.load();
  const uint64_t total_index_bytes = sum_index_bytes.load();
  const double seconds = ms / 1000.0;
  const double mib_data = total_data_bytes / (1024.0 * 1024.0);
  const double throughput_mibs = seconds > 0 ? (mib_data / seconds) : 0.0;

  std::cout << "\n====== MySQL 写入 + 索引生成统计 ======\n";
  std::cout << "成功: " << success_cnt.load() << "  失败: " << fail_cnt.load() << "\n";
  std::cout << "总耗时: " << ms << " ms (" << std::fixed << std::setprecision(3)
    << seconds << " s)\n";
  std::cout << "写入数据量: " << total_data_bytes << " bytes ("
    << std::fixed << std::setprecision(2) << mib_data << " MiB)\n";
  std::cout << "平均吞吐: " << std::fixed << std::setprecision(2)
    << throughput_mibs << " MiB/s\n";
  std::cout << "索引生成总量(未写入数据库): " << total_index_bytes << " bytes\n";
  if (success_cnt.load() > 0) {
    const double avg_ms =
      static_cast<double>(sum_latency_ns.load()) / 1e6 / success_cnt.load();
    std::cout << "平均任务耗时(含索引生成 + 数据写入): "
      << std::fixed << std::setprecision(3)
      << avg_ms << " ms\n";
  }

  if (fail_cnt.load() > 0) {
    std::cerr << "[WARN] 存在失败任务，请检查 MySQL 连接、权限或 LOAD_FILE 权限。\n";
  }
  return (fail_cnt.load() == 0) ? 0 : 2;
}
