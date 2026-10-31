#include "iotdbfs/IoTDBClient.h"
#include "iotdbfs/Logging.h"
#include "iotdbfs/IoTDBProfile.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

namespace {

using namespace std::chrono_literals;

void require(bool condition, const std::string& message) {
  if (!condition)
    throw std::runtime_error(message);
}

struct Options {
  IoTDBClientConfig config{ "127.0.0.1", 6667, "root", "root", "iotdb_client_test", 0 };
  std::string table_prefix{ "concurrent" };
  int writers{ 3 };
  int readers{ 2 };
  int operations{ 24 };
  std::vector<size_t> pool_sizes{ 1, 4 };
  bool concurrency_only{ false };
  bool help{ false };
};

bool asciiLetter(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

bool asciiDigit(char c) {
  return c >= '0' && c <= '9';
}

void validatePrefix(const std::string& value, const std::string& name) {
  require(!value.empty() && value.size() <= 40 && asciiLetter(value.front()) &&
    std::all_of(value.begin(), value.end(), [](char c) {
      return asciiLetter(c) || asciiDigit(c) || c == '_';
    }), name + " prefix must start with a letter and contain only ASCII letters, digits and underscores (max 40)");
}

int parseNumber(const std::string& value, const std::string& option, int minimum, int maximum) {
  const std::string error = option + " must be an integer in [" +
    std::to_string(minimum) + ", " + std::to_string(maximum) + "]";
  require(!value.empty(), error);
  int result = 0;
  for (char c : value) {
    require(asciiDigit(c), error);
    const int digit = c - '0';
    require(result <= (maximum - digit) / 10, error);
    result = result * 10 + digit;
  }
  require(result >= minimum && result <= maximum, error);
  return result;
}

Options parseOptions(int argc, char** argv) {
  Options options;
  if (const char* password = std::getenv("IOTDB_PASS"))
    options.config.pass = password;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      options.help = true;
      continue;
    }
    if (arg == "--concurrency-only") {
      options.concurrency_only = true;
      continue;
    }
    require(i + 1 < argc, "Missing value for " + arg);
    const std::string value = argv[++i];
    if (arg == "--host") options.config.host = value;
    else if (arg == "--user") options.config.user = value;
    else if (arg == "--pass") options.config.pass = value;
    else if (arg == "--db") options.config.db = value;
    else if (arg == "--table") options.table_prefix = value;
    else if (arg == "--port") options.config.port = parseNumber(value, arg, 1, 65535);
    else if (arg == "--writers") options.writers = parseNumber(value, arg, 1, 256);
    else if (arg == "--readers") options.readers = parseNumber(value, arg, 0, 256);
    else if (arg == "--ops") options.operations = parseNumber(value, arg, 1, 1000000);
    else if (arg == "--pool") options.pool_sizes = { static_cast<size_t>(parseNumber(value, arg, 1, 256)) };
    else throw std::runtime_error("Unknown option: " + arg);
  }
  require(!options.config.host.empty(), "Host must not be empty");
  require(!options.config.user.empty(), "User must not be empty");
  validatePrefix(options.config.db, "Database");
  validatePrefix(options.table_prefix, "Table");
  require(options.writers + options.readers <= 256, "Total writers and readers must not exceed 256");
  require(static_cast<int64_t>(options.writers) * options.operations <= 1000000,
    "Total written rows (writers * ops) must not exceed 1000000");
  return options;
}

std::shared_ptr<SessionDataSet> query(IoTDBClient& client, const std::string& sql) {
  auto result = client.executeQuery(sql);
  require(static_cast<bool>(result), "Query failed: " + sql);
  return result;
}

void execute(IoTDBClient& client, const std::string& sql) {
  require(client.executeNonQuery(sql), "SQL failed: " + sql);
}

void verifySeed(const std::shared_ptr<SessionDataSet>& result) {
  auto rows = result->getIterator();
  require(rows.next(), "Seed row missing");
  auto value = rows.getLong("value");
  require(value && value.value() == 42, "Seed row value differs");
  require(!rows.next(), "Unexpected duplicate seed row");
}

void verifyUninitialized() {
  IoTDBClient client;
  require(!client.isInitialized() && client.poolSize() == 0, "Empty client reports a pool");
  require(!client.executeNonQuery("SHOW DATABASES"), "Uninitialized nonquery succeeded");
  require(!client.executeQuery("SHOW DATABASES"), "Uninitialized query succeeded");
}

void verifyFailedInitialization(const IoTDBClientConfig& valid) {
  for (int failure = 0; failure < 3; ++failure) {
    IoTDBClient client;
    auto invalid = valid;
    if (failure == 0) {
      invalid.db += "_absent";
      invalid.auto_create_db = false;
    }
    else if (failure == 1) {
      invalid.db = "invalid database name";
      invalid.auto_use_db = false;
    }
    else invalid.pass += "_invalid";
    bool failed = false;
    try {
      client.initialize(invalid, 3);
    }
    catch (const std::exception&) {
      failed = true;
    }
    require(failed, "Failed CREATE/USE/authentication was accepted");
    require(!client.isInitialized() && client.poolSize() == 0,
      "Failed initialization published a partial pool");
    client.initialize(valid, 2);
    require(client.isInitialized() && client.poolSize() == 2, "Initialization retry failed");
    verifySeed(query(client, "SELECT value FROM seed"));
    // Successful initialization is idempotent, even with a subsequent bad config.
    client.initialize(invalid, 5);
    require(client.poolSize() == 2, "Successful pool was reinitialized");
  }
}

void verifyDatabaseControls(const IoTDBClientConfig& valid) {
  auto config = valid;
  config.db += "_absent";
  config.auto_create_db = false;
  config.auto_use_db = false;
  IoTDBClient without_database(config, 1);
  verifySeed(query(without_database, "SELECT value FROM " + valid.db + ".seed"));
  require(!without_database.executeQuery("SELECT value FROM seed"),
    "auto_use_db=false still selected a database");

  config = valid;
  config.db.clear();
  IoTDBClient empty_database(config, 1);
  verifySeed(query(empty_database, "SELECT value FROM " + valid.db + ".seed"));

  config = valid;
  config.auto_create_db = false;
  IoTDBClient existing_database(config, 1);
  verifySeed(query(existing_database, "SELECT value FROM seed"));
}

void verifySingleConnectionLeases(const IoTDBClientConfig& config) {
  IoTDBClient client(config, 1);
  iotdbfs::iotdb_profile_counters.timing_enabled.store(true, std::memory_order_relaxed);
  const auto metrics_before = iotdbfs::iotdb_profile_snapshot();
  auto result = query(client, "SELECT value FROM seed");
  require(iotdbfs::iotdb_profile_snapshot().session_leases_active == metrics_before.session_leases_active + 1,
    "Session metric did not retain the live query result lease");
  auto copy = result;
  std::weak_ptr<SessionDataSet> weak_result = result;
  std::promise<void> started;
  auto ready = started.get_future();
  auto pending = std::async(std::launch::async, [&] {
    started.set_value();
    verifySeed(query(client, "SELECT value FROM seed"));
  });
  ready.wait();
  const bool held = pending.wait_for(100ms) == std::future_status::timeout;
  result.reset();
  const bool copy_held = pending.wait_for(100ms) == std::future_status::timeout;
  copy.reset();
  const bool returned = pending.wait_for(5s) == std::future_status::ready;
  if (!returned) {
    std::cerr << "FAIL: single connection did not return after dataset destruction\n";
    std::_Exit(1);
  }
  pending.get();
  const auto metrics_after = iotdbfs::iotdb_profile_snapshot();
  require(metrics_after.session_leases_active == metrics_before.session_leases_active,
    "Session metric did not release the completed query lease");
  require(metrics_after.session_acquire_calls - metrics_before.session_acquire_calls == 2 &&
      metrics_after.query_calls - metrics_before.query_calls == 2,
    "Session/query attempt metrics differ from the two real queries");
  require(metrics_after.session_acquire_nanoseconds > metrics_before.session_acquire_nanoseconds,
    "Session acquisition timing was not recorded");
  require(held && copy_held, "Connection was reused while a dataset still owned it");
  require(weak_result.expired(), "Dataset remained alive after all strong references were reset");
  require(!client.executeQuery("SELECT nonexistent FROM seed"), "Invalid query succeeded");
  require(!client.executeNonQuery("INSERT INTO seed (time, value) VALUES (1, 'not-an-int')"),
    "Invalid insert succeeded");
  verifySeed(query(client, "SELECT value FROM seed"));
}

void verifyDatasetOutlivesClient(const IoTDBClientConfig& config) {
  constexpr int64_t row_count = 10017;
  std::shared_ptr<SessionDataSet> result;
  {
    IoTDBClient client(config, 1);
    execute(client, "CREATE TABLE lifetime (value INT64 FIELD)");
    Tablet tablet("lifetime", {{"value", TSDataType::INT64}},
      {ColumnCategory::FIELD}, row_count);
    for (int64_t row = 0; row < row_count; ++row) {
      tablet.addTimestamp(row, row + 1);
      tablet.addValue(0, row, row * 19);
    }
    require(client.insertTablet(tablet), "Lifetime fixture insert failed");
    result = query(client, "SELECT value FROM lifetime ORDER BY time");
    require(result->getFetchSize() < row_count, "Lifetime fixture does not require another page");
  }
  {
    auto rows = result->getIterator();
    int64_t count = 0;
    while (rows.next()) {
      auto value = rows.getLong("value");
      require(value && value.value() == count * 19,
        "Dataset content changed after client destruction");
      ++count;
    }
    require(count == row_count, "Dataset lost rows after client destruction");
  }
  result.reset();
}

constexpr int64_t kTimestampBase = 1700000000000LL;

void verifyRows(const std::shared_ptr<SessionDataSet>& result, const Options& options, bool complete) {
  std::vector<bool> seen(static_cast<size_t>(options.writers) * options.operations, false);
  size_t count = 0;
  auto rows = result->getIterator();
  while (rows.next()) {
    auto seq = rows.getLong("seq");
    auto stamp = rows.getTimestamp("time");
    auto worker = rows.getString("worker");
    auto payload = rows.getString("payload");
    require(seq && seq.value() >= 0 &&
      seq.value() < static_cast<int64_t>(seen.size()), "Invalid row id");
    const size_t id = static_cast<size_t>(seq.value());
    require(!seen[id], "Duplicate row id");
    require(stamp && stamp.value() == kTimestampBase + seq.value(),
      "Timestamp differs");
    require(worker && worker.value() ==
      "writer_" + std::to_string(id / options.operations),
      "Writer tag differs");
    require(payload && payload.value() ==
      "payload_" + std::to_string(seq.value() * 17 + 9),
      "Payload differs");
    seen[id] = true;
    ++count;
  }
  require(!complete || count == seen.size(), "Concurrent writes lost rows");
}

void verifyConcurrency(const Options& options, size_t pool_size) {
  IoTDBClient client(options.config, pool_size);
  const std::string table = options.table_prefix + "_" + std::to_string(pool_size);
  execute(client, "CREATE TABLE " + table + " (worker STRING TAG, seq INT64 FIELD, payload STRING FIELD)");
  const std::string select = "SELECT time, worker, seq, payload FROM " + table + " ORDER BY time";
  std::promise<void> signal;
  auto start = signal.get_future().share();
  std::mutex error_mutex;
  std::vector<std::string> errors;
  auto run = [&](auto operation) {
    start.wait();
    try {
      operation();
    }
    catch (const std::exception& e) {
      std::lock_guard<std::mutex> lock(error_mutex);
      errors.push_back(e.what());
    }
  };
  std::vector<std::thread> threads;
  threads.reserve(static_cast<size_t>(options.writers + options.readers));
  try {
  for (int writer = 0; writer < options.writers; ++writer) {
    threads.emplace_back([&, writer] {
      run([&] {
        for (int i = 0; i < options.operations; ++i) {
          const int64_t id = static_cast<int64_t>(writer) * options.operations + i;
          const std::string worker = "writer_" + std::to_string(writer);
          const std::string payload = "payload_" + std::to_string(id * 17 + 9);
          if (i % 2 == 0) {
            Tablet tablet(table, {{"worker", TSDataType::STRING}, {"seq", TSDataType::INT64},
                                  {"payload", TSDataType::STRING}},
              {ColumnCategory::TAG, ColumnCategory::FIELD, ColumnCategory::FIELD}, 1);
            tablet.addTimestamp(0, kTimestampBase + id);
            tablet.addValue(0, 0, worker);
            tablet.addValue(1, 0, id);
            tablet.addValue(2, 0, payload);
            require(client.insertTablet(tablet), "Concurrent tablet insert failed");
          }
          else {
            execute(client, "INSERT INTO " + table + " (time, worker, seq, payload) VALUES (" +
              std::to_string(kTimestampBase + id) + ", '" + worker + "', " +
              std::to_string(id) + ", '" + payload + "')");
          }
        }
      });
    });
  }
  for (int reader = 0; reader < options.readers; ++reader) {
    threads.emplace_back([&] {
      run([&] {
        for (int i = 0; i < options.operations; ++i)
          verifyRows(query(client, select), options, false);
      });
    });
  }
  }
  catch (...) {
    signal.set_value();
    for (auto& thread : threads)
      thread.join();
    throw;
  }
  const auto started = std::chrono::steady_clock::now();
  signal.set_value();
  for (auto& thread : threads)
    thread.join();
  const auto elapsed = std::chrono::duration<double, std::milli>(
    std::chrono::steady_clock::now() - started).count();
  require(errors.empty(), errors.empty() ? "" : errors.front());
  verifyRows(query(client, select), options, true);
  std::cout << "PASS concurrent contents table=" << table << " pool=" << pool_size
            << " writers=" << options.writers << " readers=" << options.readers
            << " ops=" << options.operations
            << " writes=" << static_cast<int64_t>(options.writers) * options.operations
            << " reads=" << static_cast<int64_t>(options.readers) * options.operations
            << " elapsed_ms=" << std::fixed << std::setprecision(3) << elapsed << '\n';
}

} // namespace

int main(int argc, char** argv) {
  std::unique_ptr<IoTDBClient> admin;
  std::string database;
  bool database_created = false;
  try {
    auto options = parseOptions(argc, argv);
    if (options.help) {
      std::cout << "Usage: " << argv[0] << " [--host HOST] [--port PORT] [--user USER]"
        " [--pass PASS] [--db PREFIX]\n"
        "       [--table PREFIX] [--writers N] [--readers N] [--ops N] [--pool N]\n"
        "       [--concurrency-only]\n"
        "Creates and drops a unique database on an isolated IoTDB table server.\n"
        "--db is a prefix, never an existing database (default: iotdb_client_test).\n"
        "--table is a prefix; each run uses PREFIX_POOL (default: concurrent).\n"
        "Defaults: 3 writers, 2 readers, 24 operations per worker, pools 1 and 4.\n"
        "--pool selects one concurrency pool; other lifecycle checks retain their own pools.\n"
        "--concurrency-only skips lifecycle checks, retaining all row-content checks.\n"
        "Limits: writers 1..256, readers 0..256, total workers <=256, ops 1..1000000,\n"
        "        total written rows <=1000000, pool 1..256. Readers validate full queries.\n"
        "Password: --pass overrides IOTDB_PASS; otherwise the default is root.\n"
        "elapsed_ms covers concurrent operations and their content checks; it excludes\n"
        "database/table setup, the final completeness query, and database cleanup.\n";
      return 0;
    }
    set_logging_level(LogLevel::WARN);
    const auto suffix = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
    database = options.config.db + "_" + std::to_string(getpid()) + "_" + std::to_string(suffix);
    options.config.db = database;
    auto admin_config = options.config;
    admin_config.db.clear();
    admin = std::make_unique<IoTDBClient>(admin_config, 1);
    execute(*admin, "CREATE DATABASE " + database);
    database_created = true;
    std::cout << "Test database: " << database << '\n';
    if (!options.concurrency_only) {
    {
      IoTDBClient setup(options.config, 1);
      execute(setup, "CREATE TABLE seed (value INT64 FIELD)");
      execute(setup, "INSERT INTO seed (time, value) VALUES (1, 42)");
    }
    verifyUninitialized();
    std::cout << "PASS uninitialized operations\n";
    verifyFailedInitialization(options.config);
    std::cout << "PASS failed CREATE/USE/authentication and retry\n";
    verifyDatabaseControls(options.config);
    std::cout << "PASS optional database setup\n";
    verifySingleConnectionLeases(options.config);
    std::cout << "PASS single connection ownership and failed-query recovery\n";
    verifyDatasetOutlivesClient(options.config);
    std::cout << "PASS dataset outlives client\n";
    }
    for (size_t pool_size : options.pool_sizes)
      verifyConcurrency(options, pool_size);
    execute(*admin, "DROP DATABASE " + database);
    database_created = false;
    database.clear();
    std::cout << "PASS IoTDBClient integration\n";
    return 0;
  }
  catch (const std::exception& e) {
    std::cerr << "FAIL: " << e.what() << '\n';
    if (admin && database_created && !admin->executeNonQuery("DROP DATABASE " + database))
      std::cerr << "Test database retained: " << database << '\n';
    return 1;
  }
}
