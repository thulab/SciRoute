#include "iotdbfs/CrossFileClient.h"

#include "iotdbfs/Logging.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/MetadataSetIndex.h"
#include "iotdbfs/PathUtils.h"
#include "iotdbfs/QueryUtils.h"
#include "iotdbfs/formats/grib/gribUtils.h"

#include <cerrno>
#include <cctype>
#include <cstdlib>
#include "iotdbfs/Filesystem.h"
#include <fstream>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

// namespace fs definition handled by Filesystem.h

struct CrossFileConfig {
  std::string host = "127.0.0.1";
  int port = 6667;
  std::string user = "root";
  std::string pass = "root";
  std::string db = "dfs";
  size_t pool_size = 4;

  std::string inner_query = "level=500";
  std::string file_query;
  std::string file_short_name;
  std::string path_like;
  std::string path_prefix = "/mnt/dfs";
  std::string path_contains;
  std::string path_suffix = ".grib2";

  std::string out_dir;
  size_t max_files = 0;
  bool latest_only = true;
};

namespace {

std::shared_ptr<iotdbfs::IoTDBStructuredBlobBackend> blob_backend() {
  static auto backend =
    std::make_shared<iotdbfs::IoTDBStructuredBlobBackend>();
  return backend;
}

iotdbfs::StructuredBlobStorage& blob_storage() {
  static iotdbfs::StructuredBlobStorage storage(blob_backend());
  return storage;
}

bool parse_int(const std::string& s, int& out) {
  if (s.empty())
    return false;
  try {
    long v = std::stol(s);
    if (v < 0 || v > std::numeric_limits<int>::max())
      return false;
    out = static_cast<int>(v);
    return true;
  }
  catch (...) {
    return false;
  }
}

bool parse_size(const std::string& s, size_t& out) {
  if (s.empty())
    return false;
  try {
    unsigned long long v = std::stoull(s);
    out = static_cast<size_t>(v);
    return true;
  }
  catch (...) {
    return false;
  }
}

void print_usage(const char* prog) {
  std::cout
    << "用法: " << prog << " [options]\n"
    << "必需: IoTDB 中已存在 inode 表。\n\n"
    << "常用选项:\n"
    << "  --host=IP             IoTDB 地址 (默认 127.0.0.1)\n"
    << "  --port=PORT           IoTDB 端口 (默认 6667)\n"
    << "  --user=USER           用户名 (默认 root)\n"
    << "  --pass=PASS           密码 (默认 root)\n"
    << "  --db=DB               数据库 (默认 dfs)\n"
    << "  --pool=N              连接池大小 (默认 4)\n"
    << "  --inner-query=KV      消息级别筛选 (默认 level=500)\n"
    << "                         例: level=500\n"
    << "  --file-query=KV       文件级别筛选 (SQL 过滤)，例如 shortName=clwmr\n"
    << "  --short-name=NAME     等价于 --file-query=shortName=NAME\n"
    << "  --path-like=PATTERN   SQL LIKE 过滤 path\n"
    << "  --path-prefix=PREFIX  过滤 path 前缀 (默认 /mnt/dfs)\n"
    << "  --path-contains=STR   过滤 path 包含\n"
    << "  --path-suffix=SUF     过滤 path 后缀 (默认 .grib2)\n"
    << "  --out-dir=DIR         输出目录 (默认不写文件，仅打印大小)\n"
    << "  --max-files=N         限制处理文件数 (0 表示不限)\n"
    << "  --all-versions        不做最新版本去重\n";
}

bool parse_args(int argc, char** argv, CrossFileConfig& cfg) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-h" || arg == "--help") {
      print_usage(argv[0]);
      std::exit(0);
    }
    if (arg == "--all-versions") {
      cfg.latest_only = false;
      continue;
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

    if (key == "host") {
      cfg.host = value;
    }
    else if (key == "port") {
      int v = 0;
      if (!parse_int(value, v)) {
        std::cerr << "无效的 port 数值: " << value << "\n";
        return false;
      }
      cfg.port = v;
    }
    else if (key == "user") {
      cfg.user = value;
    }
    else if (key == "pass" || key == "password") {
      cfg.pass = value;
    }
    else if (key == "db") {
      cfg.db = value;
    }
    else if (key == "pool") {
      size_t v = 0;
      if (!parse_size(value, v) || v == 0) {
        std::cerr << "无效的 pool 数值: " << value << "\n";
        return false;
      }
      cfg.pool_size = v;
    }
    else if (key == "inner-query") {
      cfg.inner_query = value;
    }
    else if (key == "file-query") {
      cfg.file_query = value;
    }
    else if (key == "short-name") {
      cfg.file_short_name = value;
    }
    else if (key == "path-like") {
      cfg.path_like = value;
    }
    else if (key == "path-prefix") {
      cfg.path_prefix = value;
    }
    else if (key == "path-contains") {
      cfg.path_contains = value;
    }
    else if (key == "path-suffix") {
      cfg.path_suffix = value;
    }
    else if (key == "out-dir") {
      cfg.out_dir = value;
    }
    else if (key == "max-files") {
      size_t v = 0;
      if (!parse_size(value, v)) {
        std::cerr << "无效的 max-files 数值: " << value << "\n";
        return false;
      }
      cfg.max_files = v;
    }
    else {
      std::cerr << "未知参数: " << key << "\n";
      return false;
    }
  }
  return true;
}

struct InodeRow {
  long long row_time{ 0 };
  std::string path;
};

std::vector<InodeRow> load_inode_rows(const CrossFileConfig& cfg) {
  std::vector<InodeRow> out;
  std::unordered_set<std::string> seen;
  auto ds = IoTDBConnector::instance().executeQuery(
    iotdbfs::build_grib_cross_file_inode_sql(
      cfg.latest_only, cfg.file_short_name));
  while (ds && ds->hasNext()) {
    auto r = ds->next();
    auto& f = r->fields;
    if (f.size() < 2)
      continue;
    long long row_time = f[0].longV ? f[0].longV.value() : 0;
    std::string path = f[1].stringV ? f[1].stringV.value() : "";
    if (path.empty())
      continue;
    if (cfg.latest_only) {
      if (!seen.insert(path).second)
        continue;
    }
    out.push_back({ row_time, path });
    if (cfg.max_files > 0 && out.size() >= cfg.max_files)
      break;
  }
  return out;
}

std::optional<std::string> fetch_index(long long row_time,
  const std::string& path) {
  auto layout = blob_storage().load_layout(
    {path_utils::norm(path), row_time});
  if (!layout.ok()) return std::nullopt;
  return std::string(layout.value().begin(), layout.value().end());
}

std::optional<std::vector<uint8_t>> read_object_part(long long row_time,
  const std::string& path, uint64_t off, uint64_t len) {
  auto bytes = blob_storage().read_payload_range(
    {path_utils::norm(path), row_time}, off, len);
  if (!bytes.ok()) return std::nullopt;
  return bytes.value();
}

std::string sanitize_filename(const std::string& path) {
  std::string out;
  out.reserve(path.size());
  for (char c : path) {
    if (c == '/' || c == '\\')
      out.push_back('_');
    else if (std::isalnum(static_cast<unsigned char>(c)) || c == '.' || c == '-' || c == '_')
      out.push_back(c);
    else
      out.push_back('_');
  }
  if (!out.empty() && out[0] == '_')
    out.erase(out.begin());
  if (out.empty())
    out = "grib";
  return out;
}

bool write_binary_file(const fs::path& path,
  const std::vector<uint8_t>& data) {
  std::ofstream ofs(path, std::ios::binary);
  if (!ofs) {
    std::cerr << "[ERR] 无法打开输出文件: " << path << " : "
              << std::strerror(errno) << "\n";
    return false;
  }
  ofs.write(reinterpret_cast<const char*>(data.data()),
    static_cast<std::streamsize>(data.size()));
  if (!ofs) {
    std::cerr << "[ERR] 写入失败: " << path << "\n";
    return false;
  }
  return true;
}

} // namespace

int run_cross_file_client(int argc, char** argv,
  const IoTDBConfig& default_cfg,
  const std::string& default_mount_point,
  size_t default_pool_size) {
  CrossFileConfig cfg;
  cfg.host = default_cfg.host;
  cfg.port = default_cfg.port;
  cfg.user = default_cfg.user;
  cfg.pass = default_cfg.pass;
  cfg.db = default_cfg.db;
  if (default_pool_size > 0)
    cfg.pool_size = default_pool_size;
  if (!default_mount_point.empty())
    cfg.path_prefix = default_mount_point;

  if (!parse_args(argc, argv, cfg))
    return 1;

  IoTDBConnector::instance().initialize(
    IoTDBConfig{ cfg.host, cfg.port, cfg.user, cfg.pass, cfg.db, 0 },
    cfg.pool_size);

  std::optional<fs::path> out_dir;
  if (!cfg.out_dir.empty()) {
    fs::path dir(cfg.out_dir);
    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec) {
      std::cerr << "[ERR] 无法创建输出目录: " << dir << " : "
                << ec.message() << "\n";
      return 1;
    }
    out_dir = dir;
  }

  if (!cfg.file_query.empty() && cfg.file_short_name.empty()) {
    auto fk = query_utils::parse_kv_query(cfg.file_query);
    auto it = fk.find("shortName");
    if (it == fk.end())
      it = fk.find("shortname");
    if (it != fk.end())
      cfg.file_short_name = it->second;
  }

  auto kv = query_utils::parse_kv_query(cfg.inner_query);
  auto kv_match = query_utils::filter_index_match_kv(kv);

  std::vector<InodeRow> files = load_inode_rows(cfg);
  if (files.empty()) {
    std::cout << "[INFO] 未找到匹配的 GRIB 文件。\n";
    return 0;
  }

  size_t success = 0;
  size_t fail = 0;
  for (const auto& file : files) {
    auto idx_opt = fetch_index(file.row_time, file.path);
    if (!idx_opt || idx_opt->empty()) {
      std::cerr << "[WARN] 缺少索引: " << file.path << "\n";
      ++fail;
      continue;
    }
    auto matches = grib_utils::find_all_msg_matches(*idx_opt, kv_match, 2);
    if (matches.empty()) {
      std::cerr << "[WARN] 未匹配到消息: " << file.path << "\n";
      ++fail;
      continue;
    }
    if (matches.size() > 1) {
      std::cerr << "[WARN] 消息匹配不唯一，请补充 step、stepRange 或其他字段: "
                << file.path << "\n";
      ++fail;
      continue;
    }
    const auto& mi = matches.front().info;
    grib_utils::ReadObjectPartFunc reader =
      [&](uint64_t off, uint64_t len) {
        return read_object_part(file.row_time, file.path, off, len);
      };
    auto data = grib_utils::decode_values_as_dtype(mi,
      grib_utils::DataDType::F32, reader);
    if (!data) {
      std::cerr << "[WARN] 解码失败: " << file.path << "\n";
      ++fail;
      continue;
    }

    std::cout << "[OK] time=" << file.row_time << " path=" << file.path
              << " bytes=" << data->size() << "\n";

    if (out_dir) {
      std::string base = sanitize_filename(file.path);
      fs::path out_path = *out_dir /
        (base + "__t" + std::to_string(file.row_time) + ".f32");
      if (!write_binary_file(out_path, *data)) {
        ++fail;
        continue;
      }
    }
    ++success;
  }

  std::cout << "[DONE] success=" << success << " fail=" << fail << "\n";
  return (fail == 0) ? 0 : 2;
}
