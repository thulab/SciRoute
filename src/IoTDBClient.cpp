#include "iotdbfs/IoTDBClient.h"
#include "iotdbfs/Logging.h"
#include "iotdbfs/IoTDBProfile.h"
#include "iotdbfs/PublicationProfile.h"

#include "AbstractSessionBuilder.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <sstream>
#include <stdexcept>
#include <typeinfo>
#include <utility>
#include <vector>

namespace {

const char* to_type_string(TSDataType::TSDataType t) {
  using namespace TSDataType;
  switch (t) {
  case BOOLEAN: return "BOOLEAN";
  case INT32: return "INT32";
  case INT64: return "INT64";
  case FLOAT: return "FLOAT";
  case DOUBLE: return "DOUBLE";
  case TEXT: return "TEXT";
  case VECTOR: return "VECTOR";
  case UNKNOWN: return "UNKNOWN";
  case TIMESTAMP: return "TIMESTAMP";
  case DATE: return "DATE";
  case BLOB: return "BLOB";
  case STRING: return "STRING";
  case OBJECT: return "OBJECT";
  default: return "UNSPECIFIED";
  }
}

const char* to_column_category_string(ColumnCategory c) {
  switch (c) {
  case ColumnCategory::TAG: return "TAG";
  case ColumnCategory::FIELD: return "FIELD";
  case ColumnCategory::ATTRIBUTE: return "ATTRIBUTE";
  default: return "UNKNOWN";
  }
}

std::string summarizeTablet(const Tablet& tablet) {
  std::ostringstream oss;
  oss << "tablet{device=" << tablet.deviceId << ", rows=" << tablet.rowSize
      << ", maxRows=" << tablet.maxRowNumber << ", schema=[";
  for (size_t i = 0; i < tablet.schemas.size(); ++i) {
    if (i)
      oss << ", ";
    oss << tablet.schemas[i].first << ":" << to_type_string(tablet.schemas[i].second);
    if (i < tablet.columnTypes.size())
      oss << "(" << to_column_category_string(tablet.columnTypes[i]) << ")";
  }
  oss << "]";
  if (!tablet.timestamps.empty() && tablet.rowSize > 0) {
    size_t sample = std::min<size_t>(tablet.rowSize, static_cast<size_t>(3));
    oss << ", tsSample=[";
    for (size_t i = 0; i < sample; ++i) {
      if (i)
        oss << ", ";
      oss << tablet.timestamps[i];
    }
    if (tablet.rowSize > sample)
      oss << ", ...";
    oss << "]";
  }
  oss << "}";
  return oss.str();
}

void executeInitializationSql(TableSession& session, const std::string& sql) {
  try {
    session.executeNonQueryStatement(sql);
    LOG_DEBUG(std::string("SQL OK: ") +
      iotdbfs::redact_sql_for_log(sql));
  }
  catch (const std::exception& e) {
    LOG_ERROR(std::string("SQL FAILED: ") +
      iotdbfs::redact_sql_for_log(sql) + " => " + e.what());
    throw;
  }
}

} // namespace

struct IoTDBClient::SessionPool : std::enable_shared_from_this<SessionPool> {
  struct Lease {
    std::shared_ptr<SessionPool> pool;
    size_t idx;

    Lease(std::shared_ptr<SessionPool> p, size_t i)
      : pool(std::move(p)), idx(i) {}
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    Lease(Lease&& other) noexcept
      : pool(std::move(other.pool)), idx(other.idx) {}
    Lease& operator=(Lease&&) = delete;
    ~Lease() {
      if (pool) {
        {
          std::lock_guard<std::mutex> lock(pool->mu);
          pool->free.push_back(idx);
          iotdbfs::iotdb_profile_counters.session_leases_active.fetch_sub(1, std::memory_order_relaxed);
        }
        pool->cv.notify_one();
      }
    }
    TableSession* operator->() const { return pool->sessions[idx].get(); }
  };

  ~SessionPool() {
    for (auto& session : sessions) {
      try {
        session->close();
      }
      catch (const std::exception& e) {
        LOG_WARN(std::string("Session close failed: ") + e.what());
      }
      catch (...) {
        LOG_WARN("Session close failed: unknown error");
      }
    }
  }

  static std::shared_ptr<SessionPool> create(const IoTDBClientConfig& cfg, size_t n) {
    auto pool = std::make_shared<SessionPool>();
    pool->acquire_timeout = cfg.acquire_timeout.count() > 0
      ? cfg.acquire_timeout : std::chrono::milliseconds(5000);
    pool->sessions.reserve(n);
    pool->free.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      // Own the Session before open(): this SDK's builder opens a raw Session
      // before returning it, leaking it if connection setup throws.
      AbstractSessionBuilder builder;
      builder.host = cfg.host;
      builder.rpcPort = cfg.port;
      builder.username = cfg.user;
      builder.password = cfg.pass;
      builder.sqlDialect = "table";
      // Pool connections target the configured endpoint. SDK background node
      // discovery also has an unsafe destructor after authentication failure.
      builder.enableAutoFetch = false;
      auto session = std::make_shared<Session>(&builder);
      auto table_session = std::make_unique<TableSession>(std::move(session));
      pool->sessions.push_back(std::move(table_session));
      auto& table = *pool->sessions.back();
      table.open(false);
      if (!cfg.db.empty() && cfg.auto_create_db && i == 0)
        executeInitializationSql(table, "CREATE DATABASE IF NOT EXISTS " + cfg.db);
      if (!cfg.db.empty() && cfg.auto_use_db)
        executeInitializationSql(table, "USE " + cfg.db);
      pool->free.push_back(i);
    }
    return pool;
  }

  Lease acquire(std::chrono::milliseconds timeout) {
    iotdbfs::PublicationOperationTimer publication_timer(
      iotdbfs::PublicationOperation::SessionAcquire);
    iotdbfs::iotdb_profile_counters.session_acquire_calls.fetch_add(1, std::memory_order_relaxed);
    iotdbfs::IoTDBProfileTimer timer(iotdbfs::iotdb_profile_counters.session_acquire_nanoseconds);
    std::unique_lock<std::mutex> lock(mu);
    if (!cv.wait_for(lock, timeout, [&] { return !free.empty(); })) {
      iotdbfs::iotdb_profile_counters.session_acquire_timeouts.fetch_add(1, std::memory_order_relaxed);
      throw std::runtime_error("IoTDB session pool acquire timed out");
    }
    size_t idx = free.back();
    free.pop_back();
    iotdbfs::iotdb_profile_lease_acquired();
    publication_timer.finish(true);
    return Lease(shared_from_this(), idx);
  }

  std::vector<std::unique_ptr<TableSession>> sessions;
  std::vector<size_t> free;
  std::chrono::milliseconds acquire_timeout{ 5000 };
  std::mutex mu;
  std::condition_variable cv;
};

IoTDBClient::IoTDBClient(const IoTDBClientConfig& cfg, size_t pool_size_hint) {
  initialize(cfg, pool_size_hint);
}

void IoTDBClient::initialize(const IoTDBClientConfig& cfg, size_t pool_size_hint) {
  std::call_once(init_flag_, [this, &cfg, pool_size_hint] {
    try {
      if (!cfg.db.empty() && !iotdbfs::is_valid_database_name(cfg.db))
        throw std::invalid_argument("invalid IoTDB database name");
      const size_t pool_size = iotdbfs::normalize_iotdb_pool_size(pool_size_hint);
      auto pool = SessionPool::create(cfg, pool_size);
      const size_t size = pool->sessions.size();
      LOG_INFO("SessionPool initialized with " + std::to_string(size) + " connections");
      std::atomic_store_explicit(&pool_, std::move(pool), std::memory_order_release);
    }
    catch (const std::exception& e) {
      LOG_ERROR(std::string("SessionPool initialization failed: ") + e.what());
      throw;
    }
  });
}

bool IoTDBClient::isInitialized() const {
  return static_cast<bool>(std::atomic_load_explicit(&pool_, std::memory_order_acquire));
}

size_t IoTDBClient::poolSize() const {
  auto pool = std::atomic_load_explicit(&pool_, std::memory_order_acquire);
  return pool ? pool->sessions.size() : 0;
}

std::shared_ptr<IoTDBClient::SessionPool> IoTDBClient::initializedPool() const {
  auto pool = std::atomic_load_explicit(&pool_, std::memory_order_acquire);
  if (!pool)
    throw std::runtime_error("IoTDBClient used before initialize()");
  return pool;
}

bool IoTDBClient::executeNonQuery(const std::string& sql) {
  iotdbfs::PublicationOperationTimer publication_timer(iotdbfs::PublicationOperation::NonQuery);
  try {
    auto pool = initializedPool();
    auto lease = pool->acquire(pool->acquire_timeout);
    lease->executeNonQueryStatement(sql);
    publication_timer.finish(true);
    LOG_DEBUG(std::string("SQL OK: ") + iotdbfs::redact_sql_for_log(sql));
    return true;
  }
  catch (const std::exception& e) {
    publication_timer.finish(false);
    LOG_ERROR(std::string("SQL FAILED: ") +
      iotdbfs::redact_sql_for_log(sql) + " => " + e.what());
  }
  catch (...) {
    publication_timer.finish(false);
    LOG_ERROR(std::string("SQL FAILED: ") +
      iotdbfs::redact_sql_for_log(sql) + " => unknown error");
  }
  return false;
}

std::shared_ptr<SessionDataSet> IoTDBClient::executeQuery(const std::string& sql) {
  iotdbfs::iotdb_profile_counters.query_calls.fetch_add(1, std::memory_order_relaxed);
  iotdbfs::IoTDBProfileTimer query_timer(iotdbfs::iotdb_profile_counters.query_nanoseconds);
  iotdbfs::PublicationOperationTimer publication_timer(iotdbfs::PublicationOperation::QuerySubmit);
  try {
    auto pool = initializedPool();
    auto lease = pool->acquire(pool->acquire_timeout);
    LOG_DEBUG(std::string("QUERY: ") + iotdbfs::redact_sql_for_log(sql));
    auto holder = std::make_shared<SessionPool::Lease>(std::move(lease));
    auto ds = (*holder)->executeQueryStatement(sql);
    if (!ds)
      throw std::runtime_error("IoTDB returned a null query result");
    publication_timer.finish(true);
    const auto publication_context = publication_timer.deferred_context();
    return std::shared_ptr<SessionDataSet>(ds.release(),
      [holder = std::move(holder), publication_context](SessionDataSet* p) mutable {
        if (p) {
          const iotdbfs::PublicationProfileContext disabled_context;
          iotdbfs::PublicationOperationTimer close_timer(
            iotdbfs::PublicationOperation::QueryClose,
            publication_context ? *publication_context : disabled_context);
          try {
            p->closeOperationHandle();
            close_timer.finish(true);
          }
          catch (const std::exception& e) {
            close_timer.finish(false);
            LOG_WARN(std::string("Query close failed: ") + e.what());
          }
          catch (...) {
            close_timer.finish(false);
            LOG_WARN("Query close failed: unknown error");
          }
          delete p;
        }
        // Weak dataset references can retain the deleter after this call.
        holder.reset();
      });
  }
  catch (const std::exception& e) {
    publication_timer.finish(false);
    LOG_ERROR(std::string("QUERY FAILED: ") +
      iotdbfs::redact_sql_for_log(sql) + " => " + e.what());
  }
  catch (...) {
    publication_timer.finish(false);
    LOG_ERROR(std::string("QUERY FAILED: ") +
      iotdbfs::redact_sql_for_log(sql) + " => unknown error");
  }
  iotdbfs::iotdb_profile_counters.query_failures.fetch_add(1, std::memory_order_relaxed);
  return nullptr;
}

bool IoTDBClient::insertTablet(Tablet& tablet) {
  iotdbfs::PublicationOperationTimer publication_timer(iotdbfs::PublicationOperation::Tablet);
  try {
    auto pool = initializedPool();
    auto lease = pool->acquire(pool->acquire_timeout);
    {
      iotdbfs::PublicationOperationTimer sdk_timer(
        iotdbfs::PublicationOperation::SdkInsert);
      lease->insert(tablet);
      sdk_timer.finish(true);
    }
    publication_timer.finish(true);
    return true;
  }
  catch (const BatchExecutionException& e) {
    publication_timer.finish(false);
    std::ostringstream oss;
    oss << "INSERT FAILED (BatchExecutionException): " << e.what();
    if (!e.statusList.empty()) {
      oss << " statusList=[";
      for (size_t i = 0; i < e.statusList.size(); ++i) {
        const auto& st = e.statusList[i];
        if (i)
          oss << "; ";
        oss << "{code=" << st.code << ", msg='" << st.message << "'}";
      }
      oss << "]";
    }
    LOG_ERROR(oss.str());
    LOG_ERROR(std::string("INSERT FAILED tablet detail: ") + summarizeTablet(tablet));
    return false;
  }
  catch (const ExecutionException& e) {
    publication_timer.finish(false);
    std::ostringstream oss;
    oss << "INSERT FAILED (ExecutionException): " << e.what();
    oss << " [code=" << e.status.code << "]";
    if (!e.status.message.empty())
      oss << " message='" << e.status.message << "'";
    LOG_ERROR(oss.str());
    LOG_ERROR(std::string("INSERT FAILED tablet detail: ") + summarizeTablet(tablet));
    return false;
  }
  catch (const std::exception& e) {
    publication_timer.finish(false);
    std::ostringstream oss;
    oss << "INSERT FAILED: " << e.what() << " (type=" << typeid(e).name() << ")";
    LOG_ERROR(oss.str());
    LOG_ERROR(std::string("INSERT FAILED tablet detail: ") + summarizeTablet(tablet));
    try {
      std::rethrow_if_nested(e);
    }
    catch (const std::exception& nested) {
      LOG_ERROR(std::string("INSERT FAILED NESTED: ") + nested.what());
    }
    catch (...) {
      LOG_ERROR("INSERT FAILED NESTED: <non-std exception>");
    }
  }
  catch (...) {
    publication_timer.finish(false);
    LOG_ERROR("INSERT FAILED: unknown error");
    LOG_ERROR(std::string("INSERT FAILED tablet detail: ") + summarizeTablet(tablet));
  }
  return false;
}
