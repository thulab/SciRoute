#include "iotdbfs/migration/MigrationJournal.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <charconv>
#include <limits>
#include <set>
#include <sstream>

namespace iotdbfs {
namespace {
constexpr std::size_t kMaximumFile = 64ull * 1024 * 1024;
constexpr std::size_t kMaximumLine = 1024 * 1024;
constexpr const char* kReport = "IOTDBFS-MIGRATION-REPORT/1";
constexpr const char* kInventory = "IOTDBFS-MIGRATION-INVENTORY/1";
constexpr const char* kJournal = "IOTDBFS-MIGRATION-JOURNAL/1";
void fail(std::string* error, const std::string& text) { if (error) *error = text; }
std::string hex(const std::string& text) {
  constexpr char digits[] = "0123456789abcdef";
  std::string output;
  for (unsigned char c : text) { output += digits[c >> 4]; output += digits[c & 15]; }
  return output;
}
std::optional<std::string> unhex(const std::string& text) {
  if (text.size() % 2) return std::nullopt;
  auto digit = [](char c) { return c >= '0' && c <= '9' ? c - '0' :
    c >= 'a' && c <= 'f' ? c - 'a' + 10 : -1; };
  std::string result;
  for (std::size_t i = 0; i < text.size(); i += 2) {
    int a = digit(text[i]), b = digit(text[i + 1]);
    if (a < 0 || b < 0) return std::nullopt;
    result += static_cast<char>((a << 4) | b);
  }
  return result;
}
bool valid_path(const std::string& path) {
  if (path.empty() || path.front() != '/' || path == "/" ||
      path.back() == '/' || path.find('\0') != std::string::npos) return false;
  std::istringstream in(path.substr(1)); std::string part;
  while (std::getline(in, part, '/'))
    if (part.empty() || part == "." || part == "..") return false;
  return true;
}
template<class T> bool integer(const std::string& text, T& value) {
  if (text.empty() || text.front() == '-' || text.front() == '+') return false;
  auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
  return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}
std::vector<std::string> tokens(const std::string& line) {
  if (line.size() > kMaximumLine) return {};
  std::istringstream in(line); std::vector<std::string> result; std::string part;
  while (in >> part) result.push_back(std::move(part));
  return result;
}
bool known_state(const std::string& state) {
  return state == "started" || state == "failed" || state == "conflict" ||
    state == "skipped" || state == "published_unverified" || state == "verified";
}
std::optional<MigrationKind> parse_kind(const std::string& name) {
  for (auto kind : {MigrationKind::Candidate, MigrationKind::HistoricalVersion,
      MigrationKind::AlreadyIndexed, MigrationKind::RequiresReingestion,
      MigrationKind::Unsupported, MigrationKind::OtherFormat,
      MigrationKind::NotFormat, MigrationKind::Error})
    if (name == migration_kind_name(kind)) return kind;
  return std::nullopt;
}
std::string digest(const std::string& bytes) {
  return migration_sha256(StructuredBlobBytes(bytes.begin(), bytes.end()));
}
bool write_all(int fd, const std::string& bytes) {
  std::size_t at = 0;
  while (at < bytes.size()) {
    auto n = ::write(fd, bytes.data() + at, bytes.size() - at);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) return false;
    at += static_cast<std::size_t>(n);
  }
  return true;
}
bool sync_directory(const std::filesystem::path& path) {
  int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) return false;
  bool okay = ::fsync(fd) == 0; ::close(fd); return okay;
}
std::optional<std::string> read_file(const std::filesystem::path& path,
    bool absent_ok = false) {
  int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
  if (fd < 0) return absent_ok && errno == ENOENT ?
    std::optional<std::string>("") : std::nullopt;
  struct stat st {};
  if (::fstat(fd, &st) || !S_ISREG(st.st_mode) || st.st_size < 0 ||
      static_cast<std::uint64_t>(st.st_size) > kMaximumFile) {
    ::close(fd); return std::nullopt;
  }
  std::string output(static_cast<std::size_t>(st.st_size), '\0');
  std::size_t at = 0;
  while (at < output.size()) {
    auto n = ::read(fd, output.data() + at, output.size() - at);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) { ::close(fd); return std::nullopt; }
    at += static_cast<std::size_t>(n);
  }
  ::close(fd); return output;
}
bool atomic_write(const std::filesystem::path& path, const std::string& bytes) {
  if (bytes.size() > kMaximumFile) return false;
  auto pattern = path.string() + ".tmp.XXXXXX";
  std::vector<char> name(pattern.begin(), pattern.end()); name.push_back('\0');
  int fd = ::mkstemp(name.data()); if (fd < 0) return false;
  bool okay = write_all(fd, bytes) && ::fsync(fd) == 0;
  ::close(fd);
  if (!okay || ::rename(name.data(), path.c_str())) {
    ::unlink(name.data()); return false;
  }
  return sync_directory(path.parent_path());
}
bool valid_entry(const MigrationInventoryEntry& item) {
  if (!valid_path(item.source.path) || item.source.version <= 0) return false;
  if (!item.sha256.empty() && !migration_valid_sha256(item.sha256)) return false;
  if (!item.layout_sha256.empty() && !migration_valid_sha256(item.layout_sha256)) return false;
  return item.kind != MigrationKind::Candidate ||
    (migration_valid_sha256(item.sha256) && migration_valid_sha256(item.layout_sha256));
}
bool valid_event(const MigrationResult& event) {
  if (!valid_path(event.source.path) || event.source.version <= 0 ||
      !migration_valid_sha256(event.source_sha256) ||
      !migration_valid_sha256(event.layout_sha256) ||
      !known_state(event.state) || event.adapter_id.empty() || event.layout_version.empty()) return false;
  if (!event.destination_sha256.empty() && !migration_valid_sha256(event.destination_sha256)) return false;
  if (event.destination.version == 0)
    return (event.destination.path.empty() || event.destination.path == event.source.path) &&
      (event.state == "failed" || event.state == "skipped" || event.state == "conflict");
  return event.destination.path == event.source.path &&
    event.destination.version > event.source.version &&
    (event.state != "verified" || event.destination_sha256 == event.source_sha256);
}
std::string manifest_text(const MigrationReportBinding& binding, const std::string& hash) {
  std::ostringstream out;
  out << kReport << '\n' << hex(binding.host) << ' ' << binding.port << ' '
      << hex(binding.database) << ' ' << hex(binding.adapter_id) << ' '
      << hex(binding.layout_protocol) << ' ' << hash << '\n';
  return out.str();
}
std::string event_line(const MigrationResult& event) {
  std::ostringstream out;
  out << kJournal << ' ' << hex(event.source.path) << ' ' << event.source.version
      << ' ' << event.destination.version << ' ' << event.bytes << ' '
      << event.source_sha256 << ' ' << event.state << ' '
      << (event.destination_sha256.empty() ? "-" : event.destination_sha256) << ' '
      << event.layout_bytes << ' ' << event.layout_sha256 << ' '
      << hex(event.adapter_id) << ' ' << hex(event.layout_version) << ' '
      << (event.error.empty() ? "-" : hex(event.error)) << '\n';
  return out.str();
}
std::optional<MigrationResult> parse_event(const std::string& line) {
  auto parts = tokens(line);
  if (parts.size() != 13 || parts[0] != kJournal) return std::nullopt;
  auto path = unhex(parts[1]), adapter = unhex(parts[10]), layout = unhex(parts[11]);
  auto error = parts[12] == "-" ? std::optional<std::string>("") : unhex(parts[12]);
  MigrationResult result;
  if (!path || !adapter || !layout || !error ||
      !integer(parts[2], result.source.version) ||
      !integer(parts[3], result.destination.version) ||
      !integer(parts[4], result.bytes) || !integer(parts[8], result.layout_bytes)) return std::nullopt;
  result.source.path = result.destination.path = *path;
  result.source_sha256 = parts[5]; result.state = parts[6];
  result.destination_sha256 = parts[7] == "-" ? "" : parts[7];
  result.layout_sha256 = parts[9]; result.adapter_id = *adapter;
  result.layout_version = *layout; result.error = *error;
  return valid_event(result) ? std::optional<MigrationResult>(std::move(result)) : std::nullopt;
}
} // namespace

std::string migration_source_identity(const StructuredBlobVersionKey& key) {
  return key.path + '\0' + std::to_string(key.version);
}
std::string migration_inventory_json(const MigrationInventoryEntry& entry) {
  std::ostringstream out;
  out << "{\"path\":" << migration_json_quote(entry.source.path)
      << ",\"source_version\":" << entry.source.version << ",\"bytes\":" << entry.bytes
      << ",\"sha256\":" << migration_json_quote(entry.sha256)
      << ",\"layout_sha256\":" << migration_json_quote(entry.layout_sha256)
      << ",\"kind\":" << migration_json_quote(migration_kind_name(entry.kind))
      << ",\"detail\":" << migration_json_quote(entry.detail)
      << ",\"partially_queryable\":" << (entry.partially_queryable ? "true" : "false")
      << ",\"profile\":" << entry.profile_json << '}';
  return out.str();
}
std::string migration_result_json(const MigrationResult& result) {
  std::ostringstream out;
  out << "{\"path\":" << migration_json_quote(result.source.path)
      << ",\"source_version\":" << result.source.version
      << ",\"destination_version\":" << result.destination.version
      << ",\"bytes\":" << result.bytes
      << ",\"source_sha256\":" << migration_json_quote(result.source_sha256)
      << ",\"destination_sha256\":" << migration_json_quote(result.destination_sha256)
      << ",\"layout_sha256\":" << migration_json_quote(result.layout_sha256)
      << ",\"layout_bytes\":" << result.layout_bytes
      << ",\"adapter_id\":" << migration_json_quote(result.adapter_id)
      << ",\"layout_version\":" << migration_json_quote(result.layout_version)
      << ",\"state\":" << migration_json_quote(result.state)
      << ",\"error\":" << migration_json_quote(result.error) << '}';
  return out.str();
}

bool save_migration_inventory(const std::filesystem::path& report,
    const MigrationReportBinding& binding,
    const std::vector<MigrationInventoryEntry>& entries, std::string* error) {
  if (binding.host.empty() || binding.database.empty() || binding.port <= 0 ||
      binding.port > 65535 || binding.adapter_id.empty() || binding.layout_protocol.empty()) {
    fail(error, "invalid report namespace/policy binding"); return false;
  }
  for (const char* name : {"report.manifest", "inventory.tsv", "inventory.jsonl", "journal.log", "results.jsonl"})
    if (std::filesystem::exists(report / name)) {
      fail(error, "report is not fresh; use a new inventory directory (legacy A12 reports are unsupported)"); return false;
    }
  std::set<std::string> seen; std::ostringstream machine, human;
  machine << kInventory << '\n';
  for (const auto& entry : entries) {
    if (!valid_entry(entry) || !seen.insert(migration_source_identity(entry.source)).second) {
      fail(error, "invalid or duplicate inventory entry"); return false;
    }
    machine << hex(entry.source.path) << ' ' << entry.source.version << ' ' << entry.bytes
        << ' ' << migration_kind_name(entry.kind) << ' ' << (entry.sha256.empty() ? "-" : entry.sha256)
        << ' ' << (entry.layout_sha256.empty() ? "-" : entry.layout_sha256) << '\n';
    human << migration_inventory_json(entry) << '\n';
  }
  auto frozen = machine.str();
  if (!atomic_write(report / "inventory.tsv", frozen) ||
      !atomic_write(report / "inventory.jsonl", human.str()) ||
      !atomic_write(report / "report.manifest", manifest_text(binding, digest(frozen))) ||
      !sync_directory(report.parent_path())) {
    fail(error, "cannot durably commit inventory and report manifest"); return false;
  }
  return true;
}

std::optional<MigrationReportState> load_migration_report(
    const std::filesystem::path& report, const MigrationReportBinding& binding,
    std::string* error) {
  auto manifest = read_file(report / "report.manifest");
  auto frozen = read_file(report / "inventory.tsv");
  if (!manifest || !frozen || *manifest != manifest_text(binding, digest(*frozen))) {
    fail(error, "report manifest absent, namespace/policy mismatch, or inventory digest changed; legacy A12 requires new inventory");
    return std::nullopt;
  }
  MigrationReportState state;
  std::map<std::string, MigrationInventoryEntry> candidates;
  std::set<std::string> sources; std::istringstream lines(*frozen); std::string line;
  if (!std::getline(lines, line) || line != kInventory || frozen->empty() || frozen->back() != '\n') {
    fail(error, "invalid inventory protocol/framing"); return std::nullopt;
  }
  while (std::getline(lines, line)) {
    auto parts = tokens(line);
    if (parts.size() != 6) { fail(error, "invalid inventory row"); return std::nullopt; }
    auto path = unhex(parts[0]); auto kind = parse_kind(parts[3]);
    MigrationInventoryEntry entry;
    if (!path || !kind || !integer(parts[1], entry.source.version) || !integer(parts[2], entry.bytes)) {
      fail(error, "invalid inventory identity/kind"); return std::nullopt;
    }
    entry.source.path = *path; entry.kind = *kind;
    entry.sha256 = parts[4] == "-" ? "" : parts[4]; entry.layout_sha256 = parts[5] == "-" ? "" : parts[5];
    auto identity = migration_source_identity(entry.source);
    if (!valid_entry(entry) || !sources.insert(identity).second) {
      fail(error, "invalid or duplicate inventory source"); return std::nullopt;
    }
    if (entry.kind == MigrationKind::Candidate) candidates.emplace(identity, entry);
    state.inventory.push_back(std::move(entry));
  }
  auto journal = read_file(report / "journal.log", true);
  if (!journal) { fail(error, "journal is unreadable or exceeds limit"); return std::nullopt; }
  std::size_t intact = journal->size();
  if (!journal->empty() && journal->back() != '\n') {
    auto last = journal->find_last_of('\n'); intact = last == std::string::npos ? 0 : last + 1;
  }
  std::istringstream events(journal->substr(0, intact));
  std::map<std::string, std::int64_t> reserved;
  while (std::getline(events, line)) {
    auto event = parse_event(line);
    if (!event) { fail(error, "invalid journal event"); return std::nullopt; }
    auto identity = migration_source_identity(event->source); auto source = candidates.find(identity);
    if (source == candidates.end() || event->adapter_id != binding.adapter_id ||
        event->layout_version != binding.layout_protocol || event->bytes != source->second.bytes ||
        event->source_sha256 != source->second.sha256 || event->layout_sha256 != source->second.layout_sha256) {
      fail(error, "journal event does not match frozen inventory/policy"); return std::nullopt;
    }
    if (event->destination.version > 0) {
      if (event->destination.version < reserved[identity]) {
        fail(error, "journal destination versions moved backwards"); return std::nullopt;
      }
      reserved[identity] = event->destination.version;
      state.max_reserved_version = std::max(state.max_reserved_version, event->destination.version);
    }
    state.journal[identity] = std::move(*event);
  }
  // Never mutate a report until both the envelope and every complete row have
  // been validated. Only an incomplete final append is repairable.
  if (intact != journal->size()) {
    int fd = ::open((report / "journal.log").c_str(), O_WRONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) { fail(error, "cannot open torn journal for repair"); return std::nullopt; }
    bool okay = ::ftruncate(fd, static_cast<off_t>(intact)) == 0 && ::fsync(fd) == 0;
    ::close(fd);
    if (!okay) { fail(error, "cannot durably repair torn journal"); return std::nullopt; }
  }
  return state;
}

bool append_migration_event(const std::filesystem::path& report, const MigrationResult& event) {
  if (!valid_event(event)) return false;
  auto line = event_line(event); if (line.size() > kMaximumLine) return false;
  int fd = ::open((report / "journal.log").c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, 0600);
  if (fd < 0) return false;
  struct stat st {};
  bool okay = ::fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_size >= 0 &&
    static_cast<std::uint64_t>(st.st_size) <= kMaximumFile - line.size() &&
    write_all(fd, line) && ::fsync(fd) == 0;
  ::close(fd);
  // A newly created journal is not durable until its directory entry is too.
  return okay && sync_directory(report);
}
bool save_migration_results(const std::filesystem::path& report,
    const std::map<std::string, MigrationResult>& results) {
  std::ostringstream out;
  for (const auto& entry : results) out << migration_result_json(entry.second) << '\n';
  return atomic_write(report / "results.jsonl", out.str());
}
} // namespace iotdbfs
