#include "iotdbfs/SemanticGrepClient.h"

#include "iotdbfs/PathUtils.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/MetadataSetIndex.h"
#include "iotdbfs/MetadataSerialization.h"
#include "iotdbfs/formats/grib/gribUtils.h"
#include "iotdbfs/QueryUtils.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include "iotdbfs/Filesystem.h"
#include <iostream>
#include <map>
#include <optional>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

// namespace fs definition handled by Filesystem.h

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

enum class GrepResultMode { AUTO, FILE_ONLY, BLOCK_ONLY };

struct GrepConfig {
  std::string host{ "127.0.0.1" };
  int port{ 6667 };
  std::string user{ "root" };
  std::string pass{ "root" };
  std::string db{ "dfs" };
  size_t pool_size{ 4 };
  std::string mount_point{ "/mnt/dfs" };

  std::string raw_query;
  std::vector<std::string> folders;
  size_t limit{ 100 };
  size_t scan_limit{ 500 };
  size_t debug_limit{ 10 };
  bool latest_only{ true };
  bool debug{ false };
  GrepResultMode mode{ GrepResultMode::AUTO };
};

struct SemanticPlan {
  std::string raw_query;
  std::map<std::string, std::string> file_filters;
  std::map<std::string, std::string> message_filters;
  std::vector<std::string> path_terms;
  std::vector<std::string> reasons;
  bool explicit_file_mode{ false };
  bool explicit_block_mode{ false };
};

struct CandidateFile {
  long long row_time{ 0 };
  std::string path;
  std::string name;
  std::set<std::string> short_names;
  std::set<std::string> type_of_levels;
  std::set<std::string> units;
  std::set<std::string> global_domains;
  std::set<long long> numbers_of_data_points;
};

struct ResultRow {
  bool is_block{ false };
  int score{ 0 };
  long long row_time{ 0 };
  std::string path;
  std::string query_path;
  std::string summary;
};

struct FilePlanCheck {
  bool matched{ true };
  std::vector<std::string> reasons;
};

std::string strip_mount_prefix(const std::string& mount_point,
  const std::string& path);
std::string to_mounted_path(const std::string& mount_point,
  const std::string& logical_path);
std::string describe_filters(const std::map<std::string, std::string>& filters);

std::string trim_copy(const std::string& s) {
  size_t begin = 0;
  while (begin < s.size() &&
    std::isspace(static_cast<unsigned char>(s[begin]))) {
    ++begin;
  }
  size_t end = s.size();
  while (end > begin &&
    std::isspace(static_cast<unsigned char>(s[end - 1]))) {
    --end;
  }
  return s.substr(begin, end - begin);
}

std::string sql_escape(const std::string& s) {
  return query_utils::sql_escape(s);
}

bool parse_int(const std::string& s, int& out) {
  if (s.empty())
    return false;
  try {
    out = std::stoi(s);
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
    out = static_cast<size_t>(std::stoull(s));
    return true;
  }
  catch (...) {
    return false;
  }
}

void print_usage(const char* prog) {
  std::cout
    << "用法: " << prog << " [options] \"自然语言描述\" [folder ...]\n"
    << "示例:\n"
    << "  " << prog << " \"500百帕温度场\" /mnt/dfs\n"
    << "  " << prog << " --mode=file \"全球降水\" gfs ecmwf\n"
    << "\n常用选项:\n"
    << "  --host=IP             IoTDB 地址 (默认 127.0.0.1)\n"
    << "  --port=PORT           IoTDB 端口 (默认 6667)\n"
    << "  --user=USER           用户名 (默认 root)\n"
    << "  --pass=PASS           密码 (默认 root)\n"
    << "  --db=DB               数据库 (默认 dfs)\n"
    << "  --pool=N              连接池大小 (默认 4)\n"
    << "  --query=TEXT          自然语言检索描述\n"
    << "  --mode=auto|file|block  返回文件或子数据块 (默认 auto)\n"
    << "  --limit=N             最大返回结果数 (默认 100)\n"
    << "  --scan-limit=N        最大扫描候选文件数 (默认 500)\n"
    << "  --debug               输出候选过滤和匹配原因\n"
    << "  --debug-limit=N       调试输出的样本上限 (默认 10)\n"
    << "  --all-versions        不做最新版本去重\n";
}

std::string canonicalize_key(std::string key) {
  key = path_utils::to_lower(trim_copy(key));
  if (key == "shortname" || key == "short-name" || key == "short_name" ||
    key == "var" || key == "variable")
    return "shortName";
  if (key == "typeoflevel" || key == "type-level" || key == "leveltype")
    return "typeOfLevel";
  if (key == "typeofprocesseddata")
    return "typeOfProcessedData";
  if (key == "typeofgeneratingprocess")
    return "typeOfGeneratingProcess";
  if (key == "globaldomain" || key == "domain")
    return "globalDomain";
  if (key == "generatingprocessidentifier")
    return "generatingProcessIdentifier";
  if (key == "parametercategory")
    return "parameterCategory";
  if (key == "parameternumber")
    return "parameterNumber";
  if (key == "numberofdatapoints" || key == "numpoints" || key == "np")
    return "numberOfDataPoints";
  if (key == "totallength")
    return "totalLength";
  return key;
}

bool supports_file_filter_key(const std::string& key) {
  static const std::set<std::string> keys{
    "shortName", "typeOfProcessedData", "typeOfGeneratingProcess",
    "typeOfLevel", "centre", "generatingProcessIdentifier", "discipline",
    "parameterCategory", "parameterNumber", "unit", "globalDomain",
    "Ni", "Nj", "numberOfDataPoints"
  };
  return keys.find(key) != keys.end();
}

bool supports_message_filter_key(const std::string& key) {
  static const std::set<std::string> keys{
    "shortName", "typeOfLevel", "stepRange", "unit", "globalDomain",
    "level", "step", "edition", "numberOfDataPoints", "offset",
    "totalLength", "ni", "nj"
  };
  return keys.find(key) != keys.end();
}

void add_filter(SemanticPlan& plan, const std::string& key,
  const std::string& value, bool file_level, bool message_level,
  const std::string& reason) {
  if (value.empty())
    return;
  if (file_level && supports_file_filter_key(key) &&
    plan.file_filters.find(key) == plan.file_filters.end()) {
    plan.file_filters[key] = value;
  }
  if (message_level && supports_message_filter_key(key) &&
    plan.message_filters.find(key) == plan.message_filters.end()) {
    plan.message_filters[key] = value;
  }
  if (!reason.empty())
    plan.reasons.push_back(reason);
}

bool contains_any(const std::string& lower_text,
  const std::vector<std::string>& needles) {
  for (const auto& needle : needles) {
    if (!needle.empty() && lower_text.find(needle) != std::string::npos)
      return true;
  }
  return false;
}

void parse_explicit_kv(const std::string& text, SemanticPlan& plan) {
  static const std::regex kv_re(
    "([A-Za-z][A-Za-z0-9_-]*)\\s*=\\s*([^\\s,;]+)");
  auto begin = std::sregex_iterator(text.begin(), text.end(), kv_re);
  auto end = std::sregex_iterator();
  for (auto it = begin; it != end; ++it) {
    const std::string key = canonicalize_key((*it)[1].str());
    const std::string value = trim_copy((*it)[2].str());
    add_filter(plan, key, value, true, true,
      "显式约束 " + key + "=" + value);
  }
}

void parse_level_and_step(const std::string& lower_text, SemanticPlan& plan) {
  std::smatch m;
  static const std::regex hpa_re("(\\d{2,4})\\s*(hpa|mb|百帕)");
  if (std::regex_search(lower_text, m, hpa_re)) {
    const std::string level = m[1].str();
    add_filter(plan, "typeOfLevel", "isobaricInhPa", true, true,
      "识别到等压面语义");
    add_filter(plan, "level", level, false, true,
      "识别到 level=" + level);
  }

  static const std::regex step_re("(\\d{1,4})\\s*(小时|hour|hours|hr|h)");
  if (std::regex_search(lower_text, m, step_re) &&
    contains_any(lower_text, { "预报", "forecast", "step", "时效" })) {
    add_filter(plan, "step", m[1].str(), false, true,
      "识别到预报时效 step=" + m[1].str());
  }

  static const std::regex meter_re("(\\d{1,3})\\s*(m|米)");
  if (plan.message_filters.find("level") == plan.message_filters.end() &&
    std::regex_search(lower_text, m, meter_re) &&
    contains_any(lower_text, { "地面", "高度", "high", "above", "风" })) {
    add_filter(plan, "typeOfLevel", "heightAboveGround", true, true,
      "识别到地面高度层语义");
    add_filter(plan, "level", m[1].str(), false, true,
      "识别到高度层 level=" + m[1].str());
  }
}

void parse_aliases(const std::string& lower_text, SemanticPlan& plan) {
  struct AliasRule {
    std::vector<std::string> needles;
    std::string key;
    std::string value;
    bool file_level{ true };
    bool message_level{ true };
    const char* reason{ nullptr };
  };
  static const std::vector<AliasRule> rules{
    { { "temperature", "tmp", "温度", "气温" }, "shortName", "tmp", true, true,
      "识别到温度变量" },
    { { "humidity", "relative humidity", "rh", "湿度", "相对湿度" },
      "shortName", "r", true, true, "识别到湿度变量" },
    { { "u wind", "u-wind", "zonal wind", "u风", "纬向风" },
      "shortName", "u", true, true, "识别到 U 风分量" },
    { { "v wind", "v-wind", "meridional wind", "v风", "经向风" },
      "shortName", "v", true, true, "识别到 V 风分量" },
    { { "wind speed", "ws", "风速" }, "shortName", "ws", true, true,
      "识别到风速变量" },
    { { "geopotential", "geopotential height", "gh", "位势高度", "高度场" },
      "shortName", "gh", true, true, "识别到位势高度变量" },
    { { "precipitation", "total precipitation", "tp", "降水", "总降水" },
      "shortName", "tp", true, true, "识别到降水变量" },
    { { "cloud water", "clwmr", "云水" }, "shortName", "clwmr", true, true,
      "识别到云水变量" },
    { { "surface", "地面", "地表" }, "typeOfLevel", "surface", true, true,
      "识别到 surface 层" },
    { { "sea level", "mean sea level", "海平面" }, "typeOfLevel", "meanSea",
      true, true, "识别到 meanSea 层" },
    { { "global", "全球" }, "globalDomain", "global", true, true,
      "识别到 globalDomain=global" }
  };

  for (const auto& rule : rules) {
    if (!contains_any(lower_text, rule.needles))
      continue;
    add_filter(plan, rule.key, rule.value, rule.file_level,
      rule.message_level, rule.reason ? rule.reason : "");
  }

  if (contains_any(lower_text, { "海平面气压", "mslp", "prmsl" })) {
    add_filter(plan, "shortName", "prmsl", true, true,
      "识别到海平面气压变量");
    add_filter(plan, "typeOfLevel", "meanSea", true, true,
      "识别到 meanSea 层");
  }
}

void parse_mode_intent(const std::string& lower_text, SemanticPlan& plan) {
  if (contains_any(lower_text, { "子块", "子数据块", "message", "block", "数据块" }))
    plan.explicit_block_mode = true;
  if (contains_any(lower_text, { "文件", "files", "file only" }))
    plan.explicit_file_mode = true;
}

bool is_ignored_path_token(const std::string& token_lower) {
  static const std::set<std::string> ignored{
    "grep", "file", "files", "block", "blocks", "message", "messages",
    "data", "query", "search", "find", "forecast", "surface", "global",
    "hpa", "mb", "hour", "hours", "hr", "tmp", "rh", "gh", "tp",
    "u", "v", "ws"
  };
  if (ignored.find(token_lower) != ignored.end())
    return true;
  if (!token_lower.empty() &&
    std::all_of(token_lower.begin(), token_lower.end(),
      [](unsigned char c) { return std::isdigit(c); })) {
    return true;
  }
  return false;
}

void parse_path_terms(const std::string& raw_query, SemanticPlan& plan) {
  static const std::regex ascii_token_re("([A-Za-z0-9._/-]+)");
  std::set<std::string> seen;
  auto begin = std::sregex_iterator(raw_query.begin(), raw_query.end(),
    ascii_token_re);
  auto end = std::sregex_iterator();
  for (auto it = begin; it != end; ++it) {
    std::string token = trim_copy((*it)[1].str());
    std::string token_lower = path_utils::to_lower(token);
    if (token.size() < 2 || is_ignored_path_token(token_lower))
      continue;
    if (seen.insert(token_lower).second)
      plan.path_terms.push_back(token);
  }

  if (!plan.file_filters.empty() || !plan.message_filters.empty() ||
    !plan.path_terms.empty()) {
    return;
  }

  std::string trimmed = trim_copy(raw_query);
  if (!trimmed.empty())
    plan.path_terms.push_back(trimmed);
}

SemanticPlan build_semantic_plan(const std::string& raw_query) {
  SemanticPlan plan;
  plan.raw_query = trim_copy(raw_query);
  std::string lower_text = path_utils::to_lower(plan.raw_query);

  parse_explicit_kv(plan.raw_query, plan);
  parse_level_and_step(lower_text, plan);
  parse_aliases(lower_text, plan);
  parse_mode_intent(lower_text, plan);
  parse_path_terms(plan.raw_query, plan);
  return plan;
}

bool parse_args(int argc, char** argv, GrepConfig& cfg,
  const std::string& default_mount_point) {
  cfg.mount_point = default_mount_point.empty()
    ? std::string("/mnt/dfs")
    : path_utils::norm(default_mount_point);
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-h" || arg == "--help") {
      print_usage(argv[0]);
      std::exit(0);
    }
    if (arg == "--debug") {
      cfg.debug = true;
      continue;
    }
    if (arg == "--all-versions") {
      cfg.latest_only = false;
      continue;
    }
    if (arg.rfind("--", 0) == 0) {
      auto eq = arg.find('=');
      if (eq == std::string::npos || eq <= 2) {
        std::cerr << "参数格式应为 --key=value: " << arg << "\n";
        return false;
      }
      const std::string key = arg.substr(2, eq - 2);
      const std::string value = arg.substr(eq + 1);
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
      else if (key == "query") {
        cfg.raw_query = value;
      }
      else if (key == "limit") {
        size_t v = 0;
        if (!parse_size(value, v) || v == 0) {
          std::cerr << "无效的 limit 数值: " << value << "\n";
          return false;
        }
        cfg.limit = v;
      }
      else if (key == "scan-limit") {
        size_t v = 0;
        if (!parse_size(value, v) || v == 0) {
          std::cerr << "无效的 scan-limit 数值: " << value << "\n";
          return false;
        }
        cfg.scan_limit = v;
      }
      else if (key == "debug-limit") {
        size_t v = 0;
        if (!parse_size(value, v) || v == 0) {
          std::cerr << "无效的 debug-limit 数值: " << value << "\n";
          return false;
        }
        cfg.debug_limit = v;
      }
      else if (key == "mode") {
        std::string v = path_utils::to_lower(value);
        if (v == "auto")
          cfg.mode = GrepResultMode::AUTO;
        else if (v == "file")
          cfg.mode = GrepResultMode::FILE_ONLY;
        else if (v == "block")
          cfg.mode = GrepResultMode::BLOCK_ONLY;
        else {
          std::cerr << "无效的 mode: " << value << "\n";
          return false;
        }
      }
      else {
        std::cerr << "未知参数: " << key << "\n";
        return false;
      }
      continue;
    }

    if (cfg.raw_query.empty()) {
      cfg.raw_query = arg;
    }
    else {
      cfg.folders.push_back(arg);
    }
  }

  if (cfg.raw_query.empty()) {
    std::cerr << "缺少自然语言查询描述。\n";
    return false;
  }

  if (cfg.folders.empty())
    cfg.folders.push_back(cfg.mount_point);
  for (auto& folder : cfg.folders) {
    if (folder.empty())
      folder = cfg.mount_point;
    else if (folder[0] != '/')
      folder = path_utils::norm(cfg.mount_point + "/" + folder);
    else
      folder = path_utils::norm(folder);
    folder = strip_mount_prefix(cfg.mount_point, folder);
  }
  return true;
}

std::string join_strings(const std::vector<std::string>& items,
  const char* sep) {
  std::ostringstream oss;
  for (size_t i = 0; i < items.size(); ++i) {
    if (i)
      oss << sep;
    oss << items[i];
  }
  return oss.str();
}

std::string strip_mount_prefix(const std::string& mount_point,
  const std::string& path) {
  std::string mp = path_utils::norm(mount_point);
  std::string p = path_utils::norm(path);
  if (mp == "/")
    return p;
  if (p == mp)
    return "/";
  if (p.size() > mp.size() &&
    p.compare(0, mp.size(), mp) == 0 &&
    p[mp.size()] == '/') {
    return p.substr(mp.size());
  }
  return p;
}

std::string to_mounted_path(const std::string& mount_point,
  const std::string& logical_path) {
  std::string mp = path_utils::norm(mount_point);
  std::string lp = path_utils::norm(logical_path);
  if (mp == "/")
    return lp;
  if (lp == "/")
    return mp;
  return mp + lp;
}

std::string build_folder_clause(const std::vector<std::string>& folders,
  const std::string& qualifier = {}) {
  std::vector<std::string> parts;
  parts.reserve(folders.size());
  const std::string column = qualifier.empty() ? "path" : qualifier + ".path";
  for (const auto& folder : folders) {
    const std::string normalized = path_utils::norm(folder);
    const std::string escaped = sql_escape(normalized);
    if (normalized == "/")
      parts.push_back(column + " LIKE '/%'");
    else
      parts.push_back("(" + column + "='" + escaped + "' OR " + column +
        " LIKE '" + escaped + "/%')");
  }
  if (parts.empty())
    return {};
  return "(" + join_strings(parts, " OR ") + ")";
}

std::string build_folder_only_where(const GrepConfig& cfg) {
  std::vector<std::string> clauses;
  clauses.push_back("published=true");
  clauses.push_back("is_dir=false");
  clauses.push_back("format_id='grib'");
  std::string folder_clause = build_folder_clause(cfg.folders);
  if (!folder_clause.empty())
    clauses.push_back(folder_clause);
  return join_strings(clauses, " AND ");
}

std::optional<long long> parse_ll_maybe(const std::string& value) {
  try {
    size_t pos = 0;
    long long v = std::stoll(value, &pos, 10);
    if (pos != value.size())
      return std::nullopt;
    return v;
  }
  catch (...) {
    return std::nullopt;
  }
}

std::string build_sql_where(const GrepConfig& cfg, const SemanticPlan& plan,
  const std::string& qualifier = {}) {
  std::vector<std::string> clauses;
  const std::string prefix = qualifier.empty() ? std::string() : qualifier + ".";
  clauses.push_back(prefix + "published=true");
  clauses.push_back(prefix + "is_dir=false");
  clauses.push_back(prefix + "format_id='grib'");

  std::string folder_clause = build_folder_clause(cfg.folders, qualifier);
  if (!folder_clause.empty())
    clauses.push_back(folder_clause);

  for (const auto& term : plan.path_terms) {
    const std::string escaped = sql_escape(term);
    clauses.push_back("(" + prefix + "path LIKE '%" + escaped + "%' OR " +
      prefix + "name LIKE '%" + escaped + "%')");
  }

  return join_strings(clauses, " AND ");
}

void apply_typed_metadata(CandidateFile& row, const std::string& document) {
  auto parsed = iotdbfs::deserialize_metadata(document);
  if (!parsed.ok()) return;
  const auto strings = [&](const char* key) {
    auto found = parsed.value().find(key);
    if (found == parsed.value().end()) return std::set<std::string>{};
    if (found->second.type() == iotdbfs::MetadataValueType::StringSet)
      return std::get<iotdbfs::MetadataStringSet>(found->second.value());
    if (found->second.type() == iotdbfs::MetadataValueType::String)
      return std::set<std::string>{
        std::get<std::string>(found->second.value())};
    return std::set<std::string>{};
  };
  const auto integers = [&](const char* key) {
    std::set<long long> values;
    auto found = parsed.value().find(key);
    if (found == parsed.value().end()) return values;
    if (found->second.type() == iotdbfs::MetadataValueType::Int64Set) {
      const auto& stored = std::get<iotdbfs::MetadataInt64Set>(
        found->second.value());
      values.insert(stored.begin(), stored.end());
    } else if (found->second.type() == iotdbfs::MetadataValueType::Int64) {
      values.insert(std::get<std::int64_t>(found->second.value()));
    }
    return values;
  };
  row.short_names = strings("shortName");
  row.type_of_levels = strings("typeOfLevel");
  row.units = strings("unit");
  row.global_domains = strings("globalDomain");
  row.numbers_of_data_points = integers("numberOfDataPoints");
}

std::vector<CandidateFile> load_candidates(const GrepConfig& cfg,
  const SemanticPlan& plan) {
  std::ostringstream oss;
  const auto short_name = plan.file_filters.find("shortName");
  if (short_name == plan.file_filters.end()) {
    oss << "SELECT time,path,name,metadata FROM inode WHERE "
        << build_sql_where(cfg, plan) << " ORDER BY time DESC";
  } else {
    oss << "SELECT table1.time,table1.path,table1.name,table1.metadata "
        << "FROM inode table1 JOIN blob_metadata_set set1 ON "
        << "table1.time=set1.time AND table1.path=set1.path WHERE "
        << build_sql_where(cfg, plan, "table1")
        << " AND set1.present=true AND set1.field_id='shortName'"
        << " AND set1.member_type='string' AND set1.member_value="
        << query_utils::sql_literal(short_name->second)
        << " ORDER BY table1.time DESC";
  }
  if (cfg.scan_limit > 0) {
    const size_t limit = cfg.latest_only ? cfg.scan_limit * 4 : cfg.scan_limit;
    oss << " LIMIT " << limit;
  }

  std::vector<CandidateFile> out;
  std::unordered_set<std::string> seen_paths;
  auto ds = IoTDBConnector::instance().executeQuery(oss.str());
  while (ds && ds->hasNext()) {
    auto r = ds->next();
    auto& f = r->fields;
    if (f.size() < 4)
      continue;
    CandidateFile row;
    row.row_time = f[0].longV ? f[0].longV.value() : 0;
    row.path = f[1].stringV ? f[1].stringV.value() : "";
    row.name = f[2].stringV ? f[2].stringV.value() : "";
    apply_typed_metadata(
      row, f[3].stringV ? f[3].stringV.value() : "");
    if (row.path.empty())
      continue;
    if (cfg.latest_only && !seen_paths.insert(row.path).second)
      continue;
    out.push_back(std::move(row));
    if (cfg.scan_limit > 0 && out.size() >= cfg.scan_limit)
      break;
  }
  return out;
}

std::vector<CandidateFile> load_debug_samples(const GrepConfig& cfg) {
  std::ostringstream oss;
  oss << "SELECT time,path,name,metadata FROM inode WHERE "
      << build_folder_only_where(cfg) << " ORDER BY time DESC";
  size_t sample_limit = std::max(cfg.debug_limit * 8, static_cast<size_t>(32));
  oss << " LIMIT " << sample_limit;

  std::vector<CandidateFile> out;
  std::unordered_set<std::string> seen_paths;
  auto ds = IoTDBConnector::instance().executeQuery(oss.str());
  while (ds && ds->hasNext()) {
    auto r = ds->next();
    auto& f = r->fields;
    if (f.size() < 4)
      continue;
    CandidateFile row;
    row.row_time = f[0].longV ? f[0].longV.value() : 0;
    row.path = f[1].stringV ? f[1].stringV.value() : "";
    row.name = f[2].stringV ? f[2].stringV.value() : "";
    apply_typed_metadata(
      row, f[3].stringV ? f[3].stringV.value() : "");
    if (row.path.empty())
      continue;
    if (cfg.latest_only && !seen_paths.insert(row.path).second)
      continue;
    out.push_back(std::move(row));
    if (out.size() >= cfg.debug_limit)
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

bool contains_ci(const std::string& haystack, const std::string& needle) {
  return path_utils::to_lower(haystack).find(path_utils::to_lower(needle)) !=
    std::string::npos;
}

std::string string_set_text(const std::set<std::string>& values) {
  return values.empty() ? std::string() :
    iotdbfs::metadata_value_text(iotdbfs::MetadataValue(
      iotdbfs::MetadataStringSet(values.begin(), values.end())));
}

std::string int64_set_text(const std::set<long long>& values) {
  if (values.empty()) return {};
  iotdbfs::MetadataInt64Set typed(values.begin(), values.end());
  return iotdbfs::metadata_value_text(iotdbfs::MetadataValue(
    std::move(typed)));
}

int score_candidate_file(const CandidateFile& file, const SemanticPlan& plan) {
  int score = 0;
  auto score_if_member = [&](const std::string& key,
                              const std::set<std::string>& actual) {
    auto it = plan.file_filters.find(key);
    if (it != plan.file_filters.end() && actual.count(it->second) != 0)
      score += 20;
    };
  score_if_member("shortName", file.short_names);
  score_if_member("typeOfLevel", file.type_of_levels);
  score_if_member("unit", file.units);
  score_if_member("globalDomain", file.global_domains);
  auto np_it = plan.file_filters.find("numberOfDataPoints");
  if (np_it != plan.file_filters.end() &&
    file.numbers_of_data_points.count(
      parse_ll_maybe(np_it->second).value_or(-1)) != 0) {
    score += 20;
  }
  for (const auto& term : plan.path_terms) {
    if (contains_ci(file.path, term) || contains_ci(file.name, term))
      score += 8;
  }
  return score;
}

FilePlanCheck analyze_file_plan(const CandidateFile& file,
  const SemanticPlan& plan) {
  FilePlanCheck out;
  auto check_member = [&](const char* key,
                           const std::set<std::string>& actual) {
    auto it = plan.file_filters.find(key);
    if (it == plan.file_filters.end())
      return;
    if (actual.count(it->second) != 0)
      out.reasons.push_back(std::string(key) + " contains " + it->second);
    else {
      out.matched = false;
      out.reasons.push_back(std::string(key) + " expected " + it->second +
        " but got " + (actual.empty() ? "<empty>" : string_set_text(actual)));
    }
    };
  check_member("shortName", file.short_names);
  check_member("typeOfLevel", file.type_of_levels);
  check_member("unit", file.units);
  check_member("globalDomain", file.global_domains);

  auto np_it = plan.file_filters.find("numberOfDataPoints");
  if (np_it != plan.file_filters.end()) {
    auto want = parse_ll_maybe(np_it->second).value_or(-1);
    if (file.numbers_of_data_points.count(want) != 0)
      out.reasons.push_back("numberOfDataPoints matched " +
        std::to_string(want));
    else {
      out.matched = false;
      out.reasons.push_back("numberOfDataPoints expected " + np_it->second +
        " but got " + (file.numbers_of_data_points.empty() ? "<empty>" :
          int64_set_text(file.numbers_of_data_points)));
    }
  }

  for (const auto& term : plan.path_terms) {
    if (contains_ci(file.path, term) || contains_ci(file.name, term))
      out.reasons.push_back("path term matched " + term);
    else {
      out.matched = false;
      out.reasons.push_back("path term missing " + term);
    }
  }
  return out;
}

std::string build_block_query_path(const CandidateFile& file,
  const grib_utils::MsgMatch& match, const std::string& mount_point) {
  std::ostringstream oss;
  oss << to_mounted_path(mount_point, file.path)
      << "?function=read&type=binary&offset=" << match.info.off;
  return oss.str();
}

std::string format_block_summary(const CandidateFile& file,
  const grib_utils::MsgMatch& match) {
  std::ostringstream oss;
  bool first = true;
  auto emit = [&](const std::string& label, const std::string& value) {
    if (value.empty())
      return;
    if (!first)
      oss << " ";
    first = false;
    oss << label << "=" << value;
    };
  emit("fileShortName", string_set_text(file.short_names));
  auto it = match.labels.find("shortName");
  if (it != match.labels.end())
    emit("shortName", it->second);
  it = match.labels.find("typeOfLevel");
  if (it != match.labels.end())
    emit("typeOfLevel", it->second);
  it = match.labels.find("level");
  if (it != match.labels.end())
    emit("level", it->second);
  it = match.labels.find("step");
  if (it != match.labels.end())
    emit("step", it->second);
  it = match.labels.find("stepRange");
  if (it != match.labels.end())
    emit("stepRange", it->second);
  emit("offset", std::to_string(match.info.off));
  emit("len", std::to_string(match.info.len));
  return oss.str();
}

std::vector<ResultRow> collect_file_results(const std::vector<CandidateFile>& files,
  const SemanticPlan& plan, const std::string& mount_point, size_t limit) {
  std::vector<ResultRow> out;
  out.reserve(files.size());
  for (const auto& file : files) {
    ResultRow row;
    row.is_block = false;
    row.score = score_candidate_file(file, plan);
    row.row_time = file.row_time;
    row.path = to_mounted_path(mount_point, file.path);
    std::ostringstream summary;
    if (!file.short_names.empty())
      summary << "shortName=" << string_set_text(file.short_names) << " ";
    if (!file.type_of_levels.empty())
      summary << "typeOfLevel=" << string_set_text(file.type_of_levels) << " ";
    if (!file.global_domains.empty())
      summary << "globalDomain=" << string_set_text(file.global_domains) << " ";
    if (!file.units.empty())
      summary << "unit=" << string_set_text(file.units) << " ";
    if (!file.numbers_of_data_points.empty())
      summary << "numberOfDataPoints=" <<
        int64_set_text(file.numbers_of_data_points);
    row.summary = trim_copy(summary.str());
    out.push_back(std::move(row));
  }

  std::sort(out.begin(), out.end(),
    [](const ResultRow& a, const ResultRow& b) {
      if (a.score != b.score)
        return a.score > b.score;
      if (a.row_time != b.row_time)
        return a.row_time > b.row_time;
      return a.path < b.path;
    });
  if (limit > 0 && out.size() > limit)
    out.resize(limit);
  return out;
}

std::vector<ResultRow> collect_block_results(const std::vector<CandidateFile>& files,
  const SemanticPlan& plan, const std::string& mount_point, size_t limit,
  bool debug, size_t debug_limit) {
  std::vector<ResultRow> out;
  size_t debug_lines = 0;
  for (const auto& file : files) {
    if (!path_utils::has_grib_ext(file.path))
      continue;
    auto index_text = fetch_index(file.row_time, file.path);
    if (!index_text || index_text->empty()) {
      if (debug && debug_lines < debug_limit) {
        std::cout << "[DEBUG] reject path="
                  << to_mounted_path(mount_point, file.path)
                  << " reason=missing index\n";
        ++debug_lines;
      }
      continue;
    }
    size_t remaining = 0;
    if (limit > 0) {
      if (out.size() >= limit)
        break;
      remaining = limit - out.size();
    }
    auto matches = grib_utils::find_all_msg_matches(*index_text,
      plan.message_filters, remaining);
    if (debug && debug_lines < debug_limit) {
      std::cout << "[DEBUG] candidate path="
                << to_mounted_path(mount_point, file.path)
                << " message_matches=" << matches.size()
                << " filters=" << describe_filters(plan.message_filters) << "\n";
      ++debug_lines;
    }
    for (const auto& match : matches) {
      ResultRow row;
      row.is_block = true;
      row.score = score_candidate_file(file, plan) + 30;
      row.row_time = file.row_time;
      row.path = to_mounted_path(mount_point, file.path);
      row.query_path = build_block_query_path(file, match, mount_point);
      row.summary = format_block_summary(file, match);
      out.push_back(std::move(row));
      if (limit > 0 && out.size() >= limit)
        break;
    }
  }

  std::sort(out.begin(), out.end(),
    [](const ResultRow& a, const ResultRow& b) {
      if (a.score != b.score)
        return a.score > b.score;
      if (a.row_time != b.row_time)
        return a.row_time > b.row_time;
      if (a.path != b.path)
        return a.path < b.path;
      return a.query_path < b.query_path;
    });
  if (limit > 0 && out.size() > limit)
    out.resize(limit);
  return out;
}

void explain_empty_candidates(const GrepConfig& cfg, const SemanticPlan& plan) {
  auto samples = load_debug_samples(cfg);
  if (samples.empty()) {
    std::cout << "[DEBUG] 没有可用于解释的样本文件。\n";
    return;
  }
  std::cout << "[DEBUG] 候选为 0，下面展示目录内样本文件的过滤原因:\n";
  for (const auto& sample : samples) {
    auto check = analyze_file_plan(sample, plan);
    std::cout << "[DEBUG] sample path="
              << to_mounted_path(cfg.mount_point, sample.path)
              << " shortName=" << (sample.short_names.empty() ? "<empty>" :
                string_set_text(sample.short_names))
              << " typeOfLevel="
              << (sample.type_of_levels.empty() ? "<empty>" :
                string_set_text(sample.type_of_levels))
              << " => "
              << (check.matched ? "matched file filters" : "rejected")
              << " reasons=" << join_strings(check.reasons, " | ")
              << "\n";
  }
}

std::string describe_filters(const std::map<std::string, std::string>& filters) {
  std::vector<std::string> items;
  items.reserve(filters.size());
  for (const auto& it : filters)
    items.push_back(it.first + "=" + it.second);
  return join_strings(items, ", ");
}

void print_plan(const SemanticPlan& plan, const GrepConfig& cfg,
  GrepResultMode effective_mode, size_t candidate_count) {
  std::cout << "[PLAN] query=" << plan.raw_query << "\n";
  std::vector<std::string> mounted_folders;
  mounted_folders.reserve(cfg.folders.size());
  for (const auto& folder : cfg.folders)
    mounted_folders.push_back(to_mounted_path(cfg.mount_point, folder));
  std::cout << "[PLAN] folders=" << join_strings(mounted_folders, ", ") << "\n";
  std::cout << "[PLAN] mode="
            << (effective_mode == GrepResultMode::FILE_ONLY ? "file" :
              effective_mode == GrepResultMode::BLOCK_ONLY ? "block" : "auto")
            << " latest_only=" << (cfg.latest_only ? "true" : "false")
            << " scan_limit=" << cfg.scan_limit
            << " candidates=" << candidate_count << "\n";
  if (cfg.debug)
    std::cout << "[PLAN] debug=true debug_limit=" << cfg.debug_limit << "\n";
  if (!plan.file_filters.empty())
    std::cout << "[PLAN] file_filters="
              << describe_filters(plan.file_filters) << "\n";
  if (!plan.message_filters.empty())
    std::cout << "[PLAN] message_filters="
              << describe_filters(plan.message_filters) << "\n";
  if (!plan.path_terms.empty())
    std::cout << "[PLAN] path_terms="
              << join_strings(plan.path_terms, ", ") << "\n";
  if (!plan.reasons.empty()) {
    std::vector<std::string> uniq;
    std::set<std::string> seen;
    for (const auto& reason : plan.reasons) {
      if (seen.insert(reason).second)
        uniq.push_back(reason);
    }
    std::cout << "[PLAN] reasoning=" << join_strings(uniq, " | ") << "\n";
  }
}

} // namespace

int run_semantic_grep_client(int argc, char** argv,
  const IoTDBConfig& default_cfg,
  const std::string& default_mount_point,
  size_t default_pool_size) {
  GrepConfig cfg;
  cfg.host = default_cfg.host;
  cfg.port = default_cfg.port;
  cfg.user = default_cfg.user;
  cfg.pass = default_cfg.pass;
  cfg.db = default_cfg.db;
  if (default_pool_size > 0)
    cfg.pool_size = default_pool_size;

  if (!parse_args(argc, argv, cfg, default_mount_point))
    return 1;

  SemanticPlan plan = build_semantic_plan(cfg.raw_query);
  GrepResultMode effective_mode = cfg.mode;
  if (effective_mode == GrepResultMode::AUTO) {
    if (plan.explicit_file_mode)
      effective_mode = GrepResultMode::FILE_ONLY;
    else if (plan.explicit_block_mode || !plan.message_filters.empty())
      effective_mode = GrepResultMode::BLOCK_ONLY;
    else
      effective_mode = GrepResultMode::FILE_ONLY;
  }

  if (effective_mode == GrepResultMode::BLOCK_ONLY &&
    plan.message_filters.empty()) {
    std::cerr << "[ERR] 当前查询没有解析出 message 级约束，无法稳定返回子数据块。\n";
    std::cerr << "      请补充如 level/shortName/step 等语义，例如：\"500百帕温度场\"。\n";
    return 2;
  }

  IoTDBConnector::instance().initialize(
    IoTDBConfig{ cfg.host, cfg.port, cfg.user, cfg.pass, cfg.db, 0 },
    cfg.pool_size);

  auto candidates = load_candidates(cfg, plan);
  print_plan(plan, cfg, effective_mode, candidates.size());
  if (candidates.empty()) {
    std::cout << "[INFO] 未找到候选文件。\n";
    if (cfg.debug)
      explain_empty_candidates(cfg, plan);
    return 0;
  }

  std::vector<ResultRow> results =
    (effective_mode == GrepResultMode::BLOCK_ONLY)
    ? collect_block_results(candidates, plan, cfg.mount_point, cfg.limit,
      cfg.debug, cfg.debug_limit)
    : collect_file_results(candidates, plan, cfg.mount_point, cfg.limit);

  if (results.empty()) {
    std::cout << "[INFO] 没有结果满足最终匹配条件。\n";
    return 0;
  }

  for (const auto& row : results) {
    if (row.is_block) {
      std::cout << "[BLOCK] score=" << row.score
                << " time=" << row.row_time
                << " query_path=" << row.query_path;
    }
    else {
      std::cout << "[FILE] score=" << row.score
                << " time=" << row.row_time
                << " path=" << row.path;
    }
    if (!row.summary.empty())
      std::cout << " " << row.summary;
    std::cout << "\n";
  }
  std::cout << "[DONE] results=" << results.size() << "\n";
  return 0;
}
