// Real IoTDB persistence with explicit fault wrappers. This driver never changes
// server configuration and never creates a database. Wrapper ACK loss is not a
// claim about a real network failure (the V00 probe supplies that evidence).
#include "iotdbfs/storage/versioning/IoTDBVersionStore.h"
#include "iotdbfs/storage/versioning/VersionNamespaceRecord.h"
#include "iotdbfs/NamespaceWriterLock.h"
#include "AbstractSessionBuilder.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <map>
#include <sstream>
#include <set>
#include <string>
#include <vector>

using namespace iotdbfs::versioning;
namespace {
void check(bool okay, const char* message) {
  if (!okay) throw Error(ErrorCode::Corrupt, message);
}
std::string env(const char* name) {
  const auto* value = std::getenv(name);
  if (!value || !*value) throw Error(ErrorCode::Invalid, std::string("missing environment variable: ") + name);
  return value;
}
std::string json_string(const std::string& value) {
  std::ostringstream out;
  out << '"';
  const char* hex = "0123456789abcdef";
  for (unsigned char c : value) {
    if (c == '"' || c == '\\') out << '\\' << c;
    else if (c < 32) out << "\\u00" << hex[c >> 4] << hex[c & 15];
    else out << c;
  }
  out << '"';
  return out.str();
}
std::string head_json(const Head& head) {
  return "{\"namespace\":" + json_string(head.identity.namespace_id) +
    ",\"incarnation\":" + json_string(head.identity.incarnation) +
    ",\"seq\":" + std::to_string(head.sequence) + ",\"commit\":" + json_string(head.commit) +
    ",\"root\":" + json_string(head.root) + "}";
}
Bytes bytes(const std::string& text) { return {text.begin(), text.end()}; }
constexpr std::array<Table, 4> tables{Table::Current, Table::Exact, Table::Liveness, Table::Permission};

void inspect(const IoTDBClientConfig& config, const std::string& prefix) {
  IoTDBVersionStore store(config, prefix); // Read-only diagnosis also works after WAL config restoration.
  const auto raw_head = store.read_head();
  check(raw_head.has_value(), "namespace head is absent");
  const auto head = decode_head(*raw_head);
  std::uint64_t charged = raw_head->size();
  auto get = [&](const ObjectRef& ref) {
    auto value = store.get_object(head.identity, ref);
    check(value.size() <= kMetadataBytes - charged, "inspect metadata budget exceeded");
    charged += value.size();
    return value;
  };
  const auto roots = decode_roots(get({"catalog-root", "iotdbfs-catalog-root/1", head.root}));
  std::vector<NamespaceRecord> records;
  std::set<ContentId> visited;
  unsigned page_count = 0;
  std::function<void(const ContentId&, std::optional<unsigned>)> visit;
  visit = [&](const ContentId& id, std::optional<unsigned> level) {
    if (id.empty()) return;
    check(visited.insert(id).second, "inspect page cycle or duplicate reference");
    const auto page = decode_page(get({"catalog-page", "iotdbfs-catalog-page/1", id}));
    ++page_count;
    check(!level || page.level == *level, "inspect child level mismatch");
    for (const auto& entry : page.entries) {
      if (page.level) visit(entry.value, page.level - 1);
      else {
        check(records.size() < kScanEntries, "inspect record budget exceeded");
        auto record = decode_namespace_record(get({"catalog-record", "iotdbfs-catalog-record/1", entry.value}));
        check(entry.key == exact_key(record.inode.path, record.inode.row_time), "inspect exact binding mismatch");
        records.push_back(std::move(record));
      }
    }
  };
  visit(roots[static_cast<unsigned>(Table::Exact)], {});
  std::cout << "{\"completed\":true,\"command\":\"inspect\",\"head\":" << head_json(head)
    << ",\"exact_pages\":" << page_count << ",\"metadata_bytes\":" << charged << ",\"records\":[";
  for (std::size_t i = 0; i < records.size(); ++i) {
    const auto& r = records[i];
    std::cout << (i ? "," : "") << "{\"path\":" << json_string(r.inode.path)
      << ",\"version\":" << r.inode.row_time << ",\"format\":" << json_string(r.inode.format_id)
      << ",\"size\":" << r.inode.size << ",\"is_dir\":" << (r.inode.is_dir ? "true" : "false") << '}';
  }
  std::cout << "]}\n";
}

void stats(const IoTDBClientConfig& config, const std::string& prefix) {
  check(prefix.size() >= 4 && prefix.size() <= 51 && prefix.substr(0, 3) == "vd_" &&
    std::all_of(prefix.begin(), prefix.end(), [](char c) {
      return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
    }), "invalid namespace prefix");
  check(iotdbfs::is_valid_database_name(config.db), "invalid database name");
  AbstractSessionBuilder builder;
  builder.host = config.host; builder.rpcPort = config.port;
  builder.username = config.user; builder.password = config.pass;
  builder.sqlDialect = "table";
  builder.enableAutoFetch = false; builder.enableRedirections = false;
  builder.maxRetries = 0; builder.retryDelayMs = 0;
  builder.connectTimeoutMs = 5000;
  builder.fetchSize = 1;
  TableSession session(std::make_shared<Session>(&builder));
  std::unique_ptr<SessionDataSet> rows;
  struct Counts { std::uint64_t rows{0}, value_bytes{0}; } total;
  std::map<std::string, Counts> by_type;
  constexpr std::uint64_t row_limit = 65536, byte_limit = 256ULL * 1024 * 1024;
  try {
    session.open(false);
    session.executeNonQueryStatement("USE " + config.db);
    rows = session.executeQueryStatement("SELECT object_type,value FROM " + prefix +
      "_objects LIMIT 65537", 5000);
    check(bool(rows), "stats query returned no dataset");
    while (rows->hasNext()) {
      check(total.rows < row_limit, "stats row budget exceeded");
      const auto row = rows->next();
      check(row && row->fields.size() == 2 && row->fields[0].stringV.is_initialized() &&
        row->fields[1].dataType == TSDataType::BLOB && row->fields[1].stringV.is_initialized(),
        "stats row is malformed");
      const auto& type = row->fields[0].stringV.value();
      const auto size = row->fields[1].stringV.value().size();
      check(!type.empty() && type.size() <= 128, "stats object type is invalid");
      check(size <= byte_limit - total.value_bytes, "stats value-byte budget exceeded");
      ++total.rows; total.value_bytes += size;
      auto& item = by_type[type]; ++item.rows; item.value_bytes += size;
    }
    rows->closeOperationHandle(); rows.reset(); session.close();
  } catch (...) {
    if (rows) { try { rows->closeOperationHandle(); } catch (...) {} }
    try { session.close(); } catch (...) {}
    throw;
  }
  std::cout << "{\"completed\":true,\"command\":\"stats\",\"prefix\":" << json_string(prefix)
    << ",\"measurement\":\"stored_object_blob_value_bytes_including_unreachable_objects\",\"total_rows\":"
    << total.rows << ",\"total_value_bytes\":" << total.value_bytes
    << ",\"row_limit\":" << row_limit << ",\"value_byte_limit\":" << byte_limit << ",\"by_type\":{";
  bool first = true;
  for (const auto& item : by_type) {
    std::cout << (first ? "" : ",") << json_string(item.first) << ":{\"rows\":" << item.second.rows
      << ",\"value_bytes\":" << item.second.value_bytes << '}';
    first = false;
  }
  std::cout << "}}\n";
}

struct Counters {
  unsigned object_puts{0}, head_attempts{0}, head_sent{0}, reconciliations{0}, journal_saves{0};
  unsigned object_batches{0};
};
class FaultStore final : public Store {
public:
  enum class HeadFault { None, AckLost, NotSent };
  std::shared_ptr<IoTDBVersionStore> real;
  std::shared_ptr<Counters> counters;
  HeadFault head_fault{HeadFault::None};
  bool fail_object{false}, fail_commit_object{false};
  FaultStore(std::shared_ptr<IoTDBVersionStore> store, std::shared_ptr<Counters> stats)
    : real(std::move(store)), counters(std::move(stats)) {}
  std::optional<Bytes> read_head() override { return real->read_head(); }
  Bytes get_object(const NamespaceIdentity& ns, const ObjectRef& ref) override { return real->get_object(ns, ref); }
  std::vector<Bytes> get_objects(const NamespaceIdentity& ns,
      const std::vector<ObjectReadRequest>& requests) override {
    ++counters->object_batches;
    return real->get_objects(ns, requests);
  }
  void put_object(const NamespaceIdentity& ns, const ObjectRef& ref, const Bytes& value) override {
    ++counters->object_puts;
    if (fail_object || (fail_commit_object && ref.type == "commit")) {
      fail_object = false; fail_commit_object = false;
      throw Error(ErrorCode::Unavailable, "injected failure before immutable object RPC");
    }
    real->put_object(ns, ref, value);
  }
  HeadWriteResult write_head(const Bytes& value) override {
    ++counters->head_attempts;
    const auto fault = head_fault;
    head_fault = HeadFault::None;
    if (fault == HeadFault::NotSent) return HeadWriteResult::Indeterminate;
    ++counters->head_sent;
    const auto result = real->write_head(value);
    if (fault == HeadFault::AckLost) {
      check(result == HeadWriteResult::Acknowledged, "real head write must ACK before wrapper drops acknowledgement");
      return HeadWriteResult::Indeterminate;
    }
    return result;
  }
  void accept_reconciled_head() override { ++counters->reconciliations; real->accept_reconciled_head(); }
};
class FaultJournal final : public Journal {
public:
  std::shared_ptr<FileJournal> real;
  std::shared_ptr<Counters> counters;
  bool fail_clear_pending{false};
  FaultJournal(std::shared_ptr<FileJournal> journal, std::shared_ptr<Counters> stats)
    : real(std::move(journal)), counters(std::move(stats)) {}
  std::optional<JournalState> load() override { return real->load(); }
  void save(const JournalState& state) override {
    ++counters->journal_saves;
    if (fail_clear_pending && !state.pending && state.committed) {
      fail_clear_pending = false;
      throw Error(ErrorCode::Unavailable, "injected failure before committed journal fsync");
    }
    real->save(state);
  }
};
struct Session {
  std::shared_ptr<FaultStore> store;
  std::shared_ptr<FaultJournal> journal;
  std::unique_ptr<Owner> owner;
};
Session open(const IoTDBClientConfig& config, const std::string& prefix,
    const std::string& directory, const std::shared_ptr<Counters>& stats, bool fresh) {
  auto real_store = std::make_shared<IoTDBVersionStore>(config, prefix);
  if (fresh) real_store->create_schema(); else real_store->verify_capabilities();
  Session result;
  result.store = std::make_shared<FaultStore>(real_store, stats);
  result.journal = std::make_shared<FaultJournal>(std::make_shared<FileJournal>(directory), stats);
  result.owner = std::make_unique<Owner>(result.store, result.journal);
  return result;
}
void put_all(Owner& owner, const std::string& key, const Bytes& value, const std::string& operation) {
  auto tx = owner.begin(operation);
  for (auto table : tables) tx->put(table, key, value);
  tx->commit();
}
void check_all(Owner& owner, const std::string& key, const Bytes& value) {
  const auto view = owner.view();
  for (auto table : tables) {
    const auto got = owner.get(view, table, key);
    check(got && *got == value, "four-tree durable record mismatch");
  }
}
template<class Action> void expect_error(Action action) {
  bool failed = false;
  try { action(); } catch (const Error&) { failed = true; }
  check(failed, "injected fault unexpectedly succeeded");
}
void faults(const IoTDBClientConfig& config, const std::string& prefix, const std::string& directory,
    std::string& stage) {
  stage = "faults_fresh_namespace";
  check(!directory.empty() && directory.front() == '/', "journal must be absolute");
  check(!std::filesystem::exists(directory), "fault probe requires a new journal directory");
  const auto scope = std::string("127.0.0.1:") + std::to_string(config.port) + '/' + config.db + '/' + prefix;
  const auto lock_id = content_id("owner-scope", "iotdbfs-owner-scope/1", bytes(scope));
  std::string lock_error;
  auto exclusion = iotdbfs::NamespaceWriterLock::acquire("/tmp/iotdbfs-v1-" + lock_id + ".lock", &lock_error);
  check(exclusion.has_value(), "canonical namespace writer lock unavailable");
  const auto stats = std::make_shared<Counters>();
  auto session = open(config, prefix, directory, stats, true);
  session.owner->initialize({random_uuid(), random_uuid()});
  std::vector<std::string> cases{"initialize"};
  const auto key = exact_key("/v01-probe", 1);
  stage = "faults_four_trees";
  put_all(*session.owner, key, bytes("initial"), "probe-four-trees");
  check_all(*session.owner, key, bytes("initial"));
  auto expected = session.owner->view().head;
  cases.push_back("four_trees_atomic_commit");

  stage = "faults_fresh_recovery";
  // Releasing every wrapper also releases FileJournal's process-local flock.
  session = Session{};
  session = open(config, prefix, directory, stats, false);
  session.owner->recover();
  check(session.owner->view().head == expected, "fresh owner changed head identity");
  check_all(*session.owner, key, bytes("initial"));
  cases.push_back("fresh_owner_recovers_four_trees");

  stage = "faults_ack_loss";
  session.store->head_fault = FaultStore::HeadFault::AckLost;
  put_all(*session.owner, key, bytes("ack-lost"), "probe-wrapper-ack-loss");
  check_all(*session.owner, key, bytes("ack-lost"));
  check(stats->reconciliations == 1 && !session.owner->frozen(), "target head was not reconciled");
  expected = session.owner->view().head;
  cases.push_back("real_head_committed_wrapper_ack_loss_reconciled");

  stage = "faults_object_failure";
  session.store->fail_object = true;
  expect_error([&] { put_all(*session.owner, key, bytes("failed-object"), "probe-object-failure"); });
  check(session.owner->view().head == expected && decode_head(*session.store->read_head()) == expected,
    "object failure changed committed head");
  check_all(*session.owner, key, bytes("ack-lost"));
  cases.push_back("object_rpc_not_sent_preserves_old_root");

  stage = "faults_before_head_failure";
  session.store->fail_commit_object = true;
  expect_error([&] { put_all(*session.owner, key, bytes("failed-commit-object"), "probe-before-head-failure"); });
  check(session.owner->view().head == expected && decode_head(*session.store->read_head()) == expected,
    "before-head commit-object failure changed committed head");
  check_all(*session.owner, key, bytes("ack-lost"));
  cases.push_back("commit_object_exception_before_head_preserves_old_root");

  stage = "faults_after_head_journal_failure";
  session.journal->fail_clear_pending = true;
  expect_error([&] { put_all(*session.owner, key, bytes("journal-recovery"), "probe-after-head-journal-failure"); });
  check(session.owner->frozen(), "journal failure did not freeze owner");
  const auto pending = session.journal->load();
  check(pending && pending->pending && session.store->read_head() == pending->pending,
    "journal fault did not preserve recoverable target head");
  expected = decode_head(*pending->pending);
  session = Session{};
  session = open(config, prefix, directory, stats, false);
  session.owner->recover();
  check(session.owner->view().head == expected, "journal recovery adopted wrong target");
  check_all(*session.owner, key, bytes("journal-recovery"));
  check(!session.journal->load()->pending, "journal recovery did not clear pending");
  cases.push_back("after_head_journal_fault_fresh_owner_recovers_target");

  stage = "faults_unknown_old_head";
  session.store->head_fault = FaultStore::HeadFault::NotSent;
  expect_error([&] { put_all(*session.owner, key, bytes("unknown-not-sent"), "probe-unknown-old-head"); });
  check(session.owner->frozen(), "unknown head outcome did not freeze owner");
  expect_error([&] { (void)session.owner->view(); });
  check(decode_head(*session.store->read_head()) == expected, "not-sent fault changed database head");
  auto unresolved = session.journal->load();
  check(unresolved && unresolved->pending && unresolved->pending != unresolved->committed,
    "unresolved journal evidence is missing");
  session = Session{};
  session = open(config, prefix, directory, stats, false);
  expect_error([&] { session.owner->recover(); });
  check(session.owner->frozen() && session.journal->load()->pending == unresolved->pending,
    "fresh recovery incorrectly cleared unknown pending");
  check(decode_head(*session.store->read_head()) == expected, "fresh recovery wrote database head");
  cases.push_back("unknown_not_sent_frozen_and_fresh_recovery_refuses_old_head");

  std::cout << "{\"completed\":true,\"command\":\"faults\",\"fault_injection\":\"in_process_wrappers_over_real_iotdb\","
    << "\"prefix\":" << json_string(prefix) << ",\"journal\":" << json_string(directory)
    << ",\"expected_final_frozen\":true,\"head\":" << head_json(expected)
    << ",\"pending_head_hash\":" << json_string(content_id("probe-evidence", "iotdbfs-v01-probe/1", *unresolved->pending))
    << ",\"case_count\":" << cases.size() << ",\"cases\":[";
  for (std::size_t i = 0; i < cases.size(); ++i) std::cout << (i ? "," : "") << json_string(cases[i]);
  std::cout << "],\"counts\":{\"object_puts\":" << stats->object_puts << ",\"head_attempts\":" << stats->head_attempts
    << ",\"head_sent\":" << stats->head_sent << ",\"reconciliations\":" << stats->reconciliations
    << ",\"journal_saves\":" << stats->journal_saves << "}}\n";
}
} // namespace
int main(int argc, char** argv) {
  std::string stage = "arguments";
  try {
    std::string command, prefix, directory;
    for (int i = 1; i < argc; ++i) {
      const std::string arg = argv[i];
      if ((arg == "head" || arg == "faults" || arg == "stats" || arg == "inspect") && command.empty()) command = arg;
      else if (arg == "--command" && i + 1 < argc) command = argv[++i];
      else if (arg == "--prefix" && i + 1 < argc) prefix = argv[++i];
      else if (arg == "--journal" && i + 1 < argc) directory = argv[++i];
      else throw Error(ErrorCode::Invalid, "usage: v01_store_probe head|faults|stats|inspect --prefix vd_NAME [--journal /absolute/new-directory]");
    }
    check((command == "head" || command == "faults" || command == "stats" || command == "inspect") && !prefix.empty(), "command and prefix are required");
    stage = "configuration";
    IoTDBClientConfig config;
    config.host = env("IOTDB_HOST"); config.user = env("IOTDB_USER");
    config.pass = env("IOTDB_PASS"); config.db = env("IOTDB_DB");
    const auto port = env("IOTDB_PORT");
    check(!port.empty() && port.size() <= 5 && port.find_first_not_of("0123456789") == std::string::npos,
      "invalid port");
    config.port = std::stoi(port);
    check(config.port > 0 && config.port <= 65535, "invalid port");
    check(config.host == "127.0.0.1" || config.host == "localhost", "controlled loopback endpoint required");
    config.host = "127.0.0.1";
    check(std::none_of(config.db.begin(), config.db.end(), [](unsigned char c) { return std::isupper(c); }),
      "database name must be canonical lowercase");
    config.auto_create_db = false;
    stage = command;
    if (command == "head") {
      IoTDBVersionStore store(config, prefix);
      store.verify_capabilities();
      const auto value = store.read_head();
      check(value.has_value(), "namespace head is absent");
      std::cout << head_json(decode_head(*value)) << '\n';
    } else if (command == "inspect") inspect(config, prefix);
    else if (command == "stats") stats(config, prefix);
    else faults(config, prefix, directory, stage);
    return 0;
  } catch (const Error& error) {
    // Core errors contain protocol/test assertions, never SDK connection strings.
    std::cout << "{\"completed\":false,\"stage\":" << json_string(stage)
      << ",\"error\":" << json_string(error.what()) << "}\n";
    return 1;
  } catch (const std::exception&) {
    // Do not emit arbitrary SDK exception text: it may contain connection data.
    std::cout << "{\"completed\":false,\"stage\":" << json_string(stage)
      << ",\"error\":\"probe rejected or assertion failed; no automatic retry\"}\n";
    return 1;
  }
}
