// Read-only mechanism benchmark over an existing immutable namespace. No owner
// recovery, publication, schema/configuration changes or query timing claims.
#include "iotdbfs/storage/versioning/IoTDBVersionStore.h"
#include "iotdbfs/query/core/QueryStorageReadControl.h"
#include "AbstractSessionBuilder.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <future>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace iotdbfs;
using namespace iotdbfs::versioning;
namespace {
void check(bool value, const char* message) { if (!value) throw std::runtime_error(message); }
std::string env(const char* name) {
  const auto* value = std::getenv(name);
  check(value && *value, "required private environment is missing");
  return value;
}
std::size_t number(const std::string& text) {
  check(!text.empty() && std::all_of(text.begin(), text.end(), [](char c) {
    return c >= '0' && c <= '9'; }), "invalid integer");
  return std::stoull(text);
}
std::string text(const Field& field) {
  check(field.stringV.is_initialized(), "incomplete oracle field");
  return field.stringV.value();
}
struct Oracle {
  std::vector<ObjectReadRequest> requests;
  std::vector<Bytes> values;
};
Oracle oracle(const IoTDBClientConfig& config, const std::string& prefix, const Head& head) {
  AbstractSessionBuilder builder;
  builder.host = config.host; builder.rpcPort = config.port;
  builder.username = config.user; builder.password = config.pass;
  builder.sqlDialect = "table"; builder.enableAutoFetch = false;
  builder.enableRedirections = false; builder.connectTimeoutMs = 5000;
  builder.maxRetries = 0; builder.retryDelayMs = 0;
  TableSession session(std::make_shared<Session>(&builder));
  struct Close { TableSession& session; ~Close() { try { session.close(); } catch (...) {} } } close{session};
  session.open(false);
  session.executeNonQueryStatement("USE " + config.db);
  // Prefix and UUIDs were validated by the production store before this SQL.
  const auto sql = "SELECT object_type,object_protocol,content_id,value FROM " + prefix +
    "_objects WHERE time=0 AND namespace_id='" + head.identity.namespace_id +
    "' AND incarnation_id='" + head.identity.incarnation +
    "' AND object_type='payload-block' ORDER BY content_id LIMIT 64";
  auto rows = session.executeQueryStatement(sql, 5000);
  check(bool(rows), "oracle result missing");
  struct CloseRows { SessionDataSet& rows; ~CloseRows() { try { rows.closeOperationHandle(); } catch (...) {} } } close_rows{*rows};
  Oracle result;
  while (rows->hasNext()) {
    const auto row = rows->next();
    check(row && row->fields.size() == 4, "oracle row shape differs");
    ObjectRef ref{text(row->fields[0]), text(row->fields[1]), text(row->fields[2])};
    const auto raw = text(row->fields[3]);
    Bytes bytes(raw.begin(), raw.end());
    check(!bytes.empty() && bytes.size() <= kPayloadBlockBytes &&
      content_id(ref.type, ref.protocol, bytes) == ref.id, "oracle object digest differs");
    result.requests.push_back({std::move(ref), bytes.size()});
    result.values.push_back(std::move(bytes));
  }
  check(result.requests.size() >= 4, "at least four existing payload objects are required");
  // Stable object order across pool sizes, independent of SQL result ordering.
  std::vector<std::size_t> order(result.requests.size());
  for (std::size_t i = 0; i < order.size(); ++i) order[i] = i;
  std::sort(order.begin(), order.end(), [&](auto a, auto b) { return result.requests[a].ref.id < result.requests[b].ref.id; });
  Oracle sorted;
  for (auto i : order) {
    sorted.requests.push_back(std::move(result.requests[i]));
    sorted.values.push_back(std::move(result.values[i]));
  }
  return sorted;
}
bool rejected(const std::function<void()>& action) {
  try { action(); return false; } catch (const std::exception&) { return true; }
}
void correctness(IoTDBVersionStore& store, const Head& head, const Oracle& expected) {
  // Unordered batches must preserve the caller's exact requested order.
  std::vector<ObjectReadRequest> reversed{expected.requests[3], expected.requests[1], expected.requests[2], expected.requests[0]};
  const auto values = store.get_objects(head.identity, reversed);
  check(values == std::vector<Bytes>({expected.values[3], expected.values[1], expected.values[2], expected.values[0]}),
    "unordered batch output differs");
  check(store.get_object(head.identity, expected.requests[0].ref) == expected.values[0], "scalar oracle differs");
  check(rejected([&] { store.get_objects(head.identity, {}); }), "empty batch accepted");
  check(rejected([&] { store.get_objects(head.identity, {expected.requests[0], expected.requests[0]}); }), "duplicate accepted");
  auto wrong = expected.requests[0]; ++wrong.size;
  check(rejected([&] { store.get_objects(head.identity, {wrong}); }), "incorrect length accepted");
  auto missing = expected.requests[0].ref; missing.id.assign(64, '0');
  check(rejected([&] { store.get_object(head.identity, missing); }), "missing object accepted");
  auto other = head.identity; other.incarnation = "00000000-0000-0000-0000-000000000000";
  check(rejected([&] { store.get_objects(other, {expected.requests[0]}); }), "wrong incarnation accepted");
  std::size_t checkpoints = 0;
  const auto before = store.stats();
  {
    ScopedQueryStorageReadControl control([&] { ++checkpoints; throw std::runtime_error("cancelled"); }, {});
    check(rejected([&] { store.get_objects(head.identity, {expected.requests[0]}); }), "cancelled request accepted");
  }
  check(checkpoints > 0 && store.stats().query_attempts == before.query_attempts, "cancelled request issued SQL");
  check(store.get_objects(head.identity, {expected.requests[0]})[0] == expected.values[0], "reader did not recover after rejection");
}
}

int main(int argc, char** argv) {
  std::string stage = "arguments";
  try {
    std::string prefix;
    std::size_t pool_size = 1, clients = 4, operations = 32, batch = 1;
    for (int i = 1; i < argc; ++i) {
      check(i + 1 < argc, "missing argument");
      const std::string key = argv[i], value = argv[++i];
      if (key == "--prefix") prefix = value;
      else if (key == "--pool-size") pool_size = number(value);
      else if (key == "--clients") clients = number(value);
      else if (key == "--operations") operations = number(value);
      else if (key == "--batch") batch = number(value);
      else throw std::runtime_error("unknown option");
    }
    check((pool_size == 1 || pool_size == 2 || pool_size == 4) && clients > 0 && clients <= 4 &&
      operations >= clients && operations <= 4096 && batch > 0 && batch <= 8, "benchmark limits invalid");
    IoTDBClientConfig config;
    config.host = env("IOTDB_HOST"); config.user = env("IOTDB_USER");
    config.pass = env("IOTDB_PASS"); config.db = env("IOTDB_DB");
    config.port = number(env("IOTDB_PORT")); config.auto_create_db = false;
    check(config.port > 0 && config.port <= 65535, "invalid private port");
    IoTDBVersionStore::Options options; options.reader_pool_size = pool_size;
    stage = "read_only_open";
    IoTDBVersionStore store(config, prefix, options);
    const auto before_head = store.read_head(); check(bool(before_head), "head missing");
    const auto head = decode_head(*before_head);
    stage = "independent_oracle";
    const auto expected = oracle(config, prefix, head);
    Bytes oracle_ids;
    for (const auto& request : expected.requests)
      oracle_ids.insert(oracle_ids.end(), request.ref.id.begin(), request.ref.id.end());
    const auto oracle_identity = content_id("reader-pool-oracle", "iotdbfs-reader-pool-oracle/1", oracle_ids);
    stage = "correctness"; correctness(store, head, expected);
    std::vector<std::vector<ObjectReadRequest>> workload;
    std::uint64_t bytes = 0;
    for (std::size_t op = 0; op < operations; ++op) {
      std::vector<ObjectReadRequest> request;
      for (std::size_t j = 0; j < std::min(batch, expected.requests.size()); ++j) {
        const auto index = (op * batch + j) % expected.requests.size();
        request.push_back(expected.requests[index]); bytes += expected.requests[index].size;
      }
      workload.push_back(std::move(request));
    }
    // Populate every intended connection outside timing, without relying on
    // server speed to make lazy connection creation overlap.
    stage = "warmup";
    std::vector<std::future<void>> warmup;
    std::mutex warm_mutex; std::condition_variable warm_cv; std::size_t warm_ready = 0;
    for (std::size_t i = 0; i < pool_size; ++i) warmup.push_back(std::async(std::launch::async, [&] {
      std::size_t checkpoints = 0;
      ScopedQueryStorageReadControl control([&] {
        // The pool's second checkpoint follows exclusive connection grant.
        // Hold all grants here to deterministically open the desired readers.
        if (++checkpoints == 2) {
          std::unique_lock<std::mutex> lock(warm_mutex); ++warm_ready; warm_cv.notify_all();
          check(warm_cv.wait_for(lock, std::chrono::seconds(10), [&] { return warm_ready == pool_size; }),
            "warmup reader barrier timed out");
        }
      }, {});
      for (int j = 0; j < 4; ++j) store.get_objects(head.identity, workload[j % workload.size()]);
    }));
    for (auto& task : warmup) task.get();
    std::mutex mutex; std::condition_variable cv;
    std::size_t ready = 0; bool start = false;
    std::atomic<std::size_t> next{0};
    std::vector<Bytes> outputs(operations * std::min(batch, expected.requests.size()));
    std::vector<std::future<void>> tasks;
    stage = "measurement";
    for (std::size_t client = 0; client < clients; ++client) tasks.push_back(std::async(std::launch::async, [&] {
      { std::unique_lock<std::mutex> lock(mutex); ++ready; cv.notify_all(); cv.wait(lock, [&] { return start; }); }
      for (;;) {
        const auto op = next.fetch_add(1); if (op >= workload.size()) break;
        auto result = store.get_objects(head.identity, workload[op]);
        check(result.size() == workload[op].size(), "benchmark result count differs");
        for (std::size_t j = 0; j < result.size(); ++j) outputs[op * result.size() + j] = std::move(result[j]);
      }
    }));
    { std::unique_lock<std::mutex> lock(mutex); cv.wait(lock, [&] { return ready == clients; }); }
    const auto counters_before = store.stats();
    const auto begin = std::chrono::steady_clock::now();
    { std::lock_guard<std::mutex> lock(mutex); start = true; cv.notify_all(); }
    for (auto& task : tasks) task.get();
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - begin).count();
    const auto counters_after = store.stats();
    stage = "output_and_head_verification";
    for (std::size_t op = 0; op < operations; ++op) {
      for (std::size_t j = 0; j < workload[op].size(); ++j) {
        const auto index = (op * batch + j) % expected.requests.size();
        check(outputs[op * workload[op].size() + j] == expected.values[index], "measured bytes differ from independent oracle");
      }
    }
    check(store.read_head() == before_head, "namespace head changed");
    check(counters_after.query_attempts - counters_before.query_attempts == operations &&
      counters_after.returned_blob_bytes - counters_before.returned_blob_bytes == bytes, "measured SQL or bytes differ");
    check(counters_after.reader_pool_created == counters_before.reader_pool_created &&
      counters_after.reader_pool_active == 0 && counters_after.reader_pool_peak == pool_size &&
      counters_after.reader_pool_acquire_timeouts == counters_before.reader_pool_acquire_timeouts,
      "unexpected connection creation, capacity or timeout during measurement");
    std::cout << "{\"complete\":true,\"correctness\":true,\"head_unchanged\":true,\"pool_size\":" << pool_size
      << ",\"clients\":" << clients << ",\"operations\":" << operations << ",\"batch\":" << batch
      << ",\"bytes\":" << bytes << ",\"wall_ns\":" << ns << ",\"oracle_objects\":" << expected.requests.size()
      << ",\"reader_pool_peak\":" << counters_after.reader_pool_peak
      << ",\"reader_pool_waits\":" << counters_after.reader_pool_waits - counters_before.reader_pool_waits
      << ",\"reader_pool_created_during_measurement\":" << counters_after.reader_pool_created - counters_before.reader_pool_created
      << ",\"oracle_identity\":\"" << oracle_identity << '"'
      << ",\"namespace\":\"" << head.identity.namespace_id << "\",\"incarnation\":\"" << head.identity.incarnation
      << "\",\"head_commit\":\"" << head.commit << "\",\"head_sequence\":" << head.sequence << "}\n";
    return 0;
  } catch (...) {
    // SDK exception text can contain SQL or private connection parameters.
    std::cerr << "owner reader probe failed at " << stage << '\n';
    return 1;
  }
}
