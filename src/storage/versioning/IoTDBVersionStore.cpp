#include "iotdbfs/storage/versioning/IoTDBVersionStore.h"

#include "AbstractSessionBuilder.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/IoTDBProfile.h"
#include "iotdbfs/query/core/QueryStorageReadControl.h"
#include "iotdbfs/storage/versioning/detail/BoundedReaderPool.h"
#include "iotdbfs/storage/versioning/detail/OwnerReadTiming.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <mutex>
#include <stdexcept>
#include <tuple>
#include <utility>

namespace iotdbfs::versioning {
namespace {

constexpr std::size_t kHeadLimit = 4096;
constexpr std::size_t kObjectLimit = 64 * 1024 * 1024;
constexpr std::int64_t kQueryTimeoutMs = 5000;

void require(bool value, const char* message) {
  if (!value) throw std::runtime_error(message);
}

std::string quote(const std::string& value) {
  std::string result("'");
  for (char c : value) {
    require(c != '\0', "version store key contains NUL");
    if (c == '\'') result.push_back('\'');
    result.push_back(c);
  }
  return result + "'";
}

bool lower_hex(const std::string& text, std::size_t size) {
  return text.size() == size && std::all_of(text.begin(), text.end(), [](char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  });
}

bool uuid(const std::string& text) {
  if (text.size() != 36) return false;
  std::string digits;
  for (std::size_t i = 0; i < text.size(); ++i) {
    if (i == 8 || i == 13 || i == 18 || i == 23) {
      if (text[i] != '-') return false;
    } else digits.push_back(text[i]);
  }
  return lower_hex(digits, 32);
}

std::string hex(const Bytes& bytes) {
  constexpr char digits[] = "0123456789abcdef";
  std::string result;
  result.reserve(bytes.size() * 2);
  for (auto byte : bytes) {
    result.push_back(digits[byte >> 4]);
    result.push_back(digits[byte & 15]);
  }
  return result;
}

std::string framed_digest(const ObjectRef& ref, const Bytes& bytes) {
  return content_id(ref.type, ref.protocol, bytes);
}

std::string field_text(const Field& field) {
  require(field.stringV.is_initialized(), "version store expected a text result column");
  return field.stringV.value();
}

int field_int(const Field& field) {
  require(field.intV.is_initialized(), "version store expected an INT32 result column");
  return field.intV.value();
}

using Schema = std::vector<std::tuple<std::string, std::string, std::string>>;

} // namespace

struct IoTDBVersionStore::Impl {
  IoTDBClientConfig config;
  std::string head_table;
  std::string object_table;
  std::unique_ptr<TableSession> reader;
  std::unique_ptr<TableSession> writer;
  mutable std::mutex mutex;
  mutable std::mutex counters_mutex;
  Stats counters;
  bool verified{false};
  bool writer_uncertain{false};
  std::optional<Bytes> pending_head;

  struct ObjectReader {
    std::unique_ptr<TableSession> session;
    std::uint64_t id;
    explicit ObjectReader(std::unique_ptr<TableSession> value) : session(std::move(value)) {
      static std::atomic<std::uint64_t> next{0};
      id = next.fetch_add(1, std::memory_order_relaxed) + 1;
    }
    ~ObjectReader() { Impl::close(session); }
  };
  detail::BoundedReaderPool<ObjectReader> object_readers;

  explicit Impl(const IoTDBClientConfig& cfg, const std::string& prefix, Options options)
    : config(cfg), head_table(prefix + "_head"), object_table(prefix + "_objects"),
      object_readers(options.reader_pool_size, options.reader_acquire_timeout) {
    require(prefix.size() >= 4 && prefix.size() <= 51 && prefix.substr(0, 3) == "vd_" &&
      std::all_of(prefix.begin(), prefix.end(), [](char c) {
        return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
      }), "version store requires a bounded vd_ table prefix");
    require(iotdbfs::is_valid_database_name(config.db), "invalid version store database");
    require(config.host == "127.0.0.1" || config.host == "::1" || config.host == "localhost",
      "version store supports only a controlled loopback DataNode");
    reader = connect();
    writer = connect();
  }

  ~Impl() { close(reader); close(writer); }

  void count(std::uint64_t Stats::*member, std::uint64_t amount = 1) {
    std::lock_guard<std::mutex> lock(counters_mutex);
    counters.*member += amount;
  }

  static void close(std::unique_ptr<TableSession>& session) noexcept {
    if (session) { try { session->close(); } catch (...) {} session.reset(); }
  }

  std::unique_ptr<TableSession> connect() {
    AbstractSessionBuilder builder;
    builder.host = config.host;
    builder.rpcPort = config.port;
    builder.username = config.user;
    builder.password = config.pass;
    builder.sqlDialect = "table";
    builder.enableAutoFetch = false;
    builder.enableRedirections = false;
    builder.connectTimeoutMs = 5000;
    builder.maxRetries = 0;
    builder.retryDelayMs = 0;
    auto result = std::make_unique<TableSession>(std::make_shared<Session>(&builder));
    try {
      result->open(false);
      count(&Stats::nonquery_attempts);query_profile_count("version_store.sql_nonqueries");
      result->executeNonQueryStatement("USE " + config.db);
    } catch (...) {
      close(result);
      throw std::runtime_error("version store connection or existing database unavailable");
    }
    return result;
  }

  template<class Visitor>
  void query(const std::string& sql, Visitor visitor, std::size_t maximum_rows, bool batch = false) {
    if (!reader) reader = connect();
    try {
      query_on(*reader, sql, std::move(visitor), maximum_rows, batch, false);
    } catch (...) {
      // Failed control reads never retry this operation.
      close(reader);
      throw;
    }
  }

  auto acquire_object_reader(detail::OwnerReadTiming& timing) {
    timing.stage("acquire");
    timing.mark(timing.record.acquire_start_ns);
    try {
      auto result = object_readers.acquire([this, &timing] {
        timing.stage("connect");
        timing.mark(timing.record.connect_start_ns);
        try {
          auto reader = std::make_unique<ObjectReader>(connect());
          timing.mark(timing.record.connect_end_ns);
          timing.stage("acquire");
          return reader;
        } catch (...) {
          timing.mark(timing.record.connect_end_ns);
          throw;
        }
      }, [] { storage_read_checkpoint(); });
      timing.mark(timing.record.acquire_end_ns);
      timing.record.reader_id = result->id;
      return result;
    } catch (...) {
      timing.mark(timing.record.acquire_end_ns);
      throw;
    }
  }

  template<class Visitor>
  void query_on(TableSession& session, const std::string& sql, Visitor visitor,
      std::size_t maximum_rows, bool batch, bool cancellable, detail::OwnerReadTiming* timing = nullptr) {
    // Keep the caller's exclusive lease through fetch, validation, and close.
    std::unique_ptr<SessionDataSet> rows;
    try {
      if (timing) timing->stage("execute_checkpoint");
      if (cancellable) storage_read_checkpoint();
      count(&Stats::query_attempts);query_profile_count("version_store.sql_queries");
      iotdb_profile_counters.version_store_query_calls.fetch_add(1, std::memory_order_relaxed);
      if(batch) {
        count(&Stats::batch_query_attempts);query_profile_count("version_store.batch_sql_queries");
        iotdb_profile_counters.version_store_batch_query_calls.fetch_add(1, std::memory_order_relaxed);
      }
      if (timing) { timing->stage("execute"); timing->mark(timing->record.execute_start_ns); }
      rows = session.executeQueryStatement(sql, kQueryTimeoutMs);
      if (timing) timing->mark(timing->record.execute_end_ns);
      require(bool(rows), "version store query returned no dataset");
      std::size_t count = 0;
      for (;;) {
        if (timing) timing->stage("consume");
        if (cancellable) storage_read_checkpoint();
        if (!rows->hasNext()) break;
        require(++count <= maximum_rows, "version store query row bound exceeded");
        auto row = rows->next();
        require(row != nullptr, "version store query returned an empty row");
        // SDK row-ready time after next(), not a server-side execution timestamp.
        if (timing) {
          if (!timing->record.first_row_ns) timing->mark(timing->record.first_row_ns);
          ++timing->record.rows;
        }
        if (timing) {
          timing->stage("visitor");
          detail::OwnerReadTiming::Duration duration(*timing, timing->record.visitor_ns);
          visitor(row->fields);
        } else visitor(row->fields);
      }
      if (timing) {
        timing->mark(timing->record.consume_end_ns);
        timing->stage("operation_close");
        timing->mark(timing->record.operation_close_start_ns);
        ++timing->record.close_attempts;
      }
      rows->closeOperationHandle();
      if (timing) timing->mark(timing->record.operation_close_end_ns);
      rows.reset();
    } catch (...) {
      if (timing) {
        if (timing->record.execute_start_ns && !timing->record.execute_end_ns)
          timing->mark(timing->record.execute_end_ns);
        if (timing->record.first_row_ns && !timing->record.consume_end_ns)
          timing->mark(timing->record.consume_end_ns);
      }
      if (rows) {
        if (timing) {
          if (!timing->record.operation_close_start_ns) timing->mark(timing->record.operation_close_start_ns);
          ++timing->record.close_attempts;
        }
        try { rows->closeOperationHandle(); } catch (...) {}
        if (timing) timing->mark(timing->record.operation_close_end_ns);
      }
      rows.reset();
      throw;
    }
  }

  std::optional<Bytes> read_cell(const std::string& table, const std::string& predicate,
      std::size_t maximum_size) {
    std::optional<Bytes> result;
    if (table == head_table) {count(&Stats::head_cell_reads);query_profile_count("version_store.head_reads");
      iotdb_profile_counters.version_store_head_reads.fetch_add(1, std::memory_order_relaxed);}
    else {count(&Stats::object_cell_reads);query_profile_count("version_store.object_reads");
      iotdb_profile_counters.version_store_object_reads.fetch_add(1, std::memory_order_relaxed);}
    query("SELECT value FROM " + table + " WHERE time=0 AND " + predicate,
      [&](const auto& fields) {
        require(fields.size() == 1 && fields[0].dataType == TSDataType::BLOB &&
          fields[0].stringV.is_initialized(), "version store cell is not a complete BLOB");
        const auto& value = fields[0].stringV.value();
        require(value.size() <= maximum_size, "version store BLOB exceeds its limit");
        count(&Stats::returned_blob_bytes, value.size());query_profile_count("version_store.blob_bytes",value.size());
        iotdb_profile_counters.version_store_blob_bytes.fetch_add(value.size(), std::memory_order_relaxed);
        result = Bytes(value.begin(), value.end());
      }, 1);
    return result;
  }

  std::string object_predicate(const NamespaceIdentity& identity, const ObjectRef& ref) {
    require(uuid(identity.namespace_id) && uuid(identity.incarnation),
      "invalid persistent namespace identity");
    require(!ref.type.empty() && ref.type.size() <= 128 &&
      !ref.protocol.empty() && ref.protocol.size() <= 128 && lower_hex(ref.id, 64),
      "invalid immutable object reference");
    return "namespace_id=" + quote(identity.namespace_id) + " AND incarnation_id=" +
      quote(identity.incarnation) + " AND object_type=" + quote(ref.type) +
      " AND object_protocol=" + quote(ref.protocol) + " AND content_id=" + quote(ref.id);
  }

  void verify_schema(const std::string& table, const Schema& expected) {
    Schema actual;
    query("DESCRIBE " + table, [&](const auto& fields) {
      require(fields.size() >= 3, "version store DESCRIBE result is incomplete");
      actual.emplace_back(field_text(fields[0]), field_text(fields[1]), field_text(fields[2]));
    }, expected.size() + 1);
    require(actual == expected, "version store table schema does not match protocol");
  }

  Capabilities capabilities() {
    Capabilities result;
    std::size_t nodes = 0;
    query("SHOW DATANODES", [&](const auto& fields) {
      require(fields.size() >= 4, "version store SHOW DATANODES result is incomplete");
      ++nodes;
      result.datanode_id = field_int(fields[0]);
      require(field_text(fields[1]) == "Running", "version store DataNode is not Running");
      const auto address = field_text(fields[2]);
      require((address == "127.0.0.1" || address == "::1" || address == "localhost") &&
        field_int(fields[3]) == config.port, "version store DataNode endpoint differs from configured loopback");
    }, 2);
    require(nodes == 1, "version store requires exactly one DataNode");
    std::map<std::string, std::string> values;
    query("SHOW ALL CONFIGURATION ON " + std::to_string(result.datanode_id),
      [&](const auto& fields) {
        require(fields.size() >= 3, "version store configuration result is incomplete");
        const auto key = field_text(fields[0]);
        if (key != "wal_mode" && key != "enable_partial_insert") return;
        require(values.emplace(key, fields[1].stringV.is_initialized()
          ? field_text(fields[1]) : field_text(fields[2])).second,
          "version store configuration contains a duplicate key");
      }, 8192);
    require(values.size() == 2 && values.at("wal_mode") == "SYNC" &&
      values.at("enable_partial_insert") == "false",
      "version store requires effective WAL=SYNC and enable_partial_insert=false");
    result.wal_mode = "SYNC";
    result.partial_insert = false;
    return result;
  }

  void schemas() {
    verify_schema(head_table, {{"time", "TIMESTAMP", "TIME"},
      {"slot", "STRING", "TAG"}, {"value", "BLOB", "FIELD"}});
    verify_schema(object_table, {{"time", "TIMESTAMP", "TIME"},
      {"namespace_id", "STRING", "TAG"}, {"incarnation_id", "STRING", "TAG"},
      {"object_type", "STRING", "TAG"}, {"object_protocol", "STRING", "TAG"},
      {"content_id", "STRING", "TAG"}, {"value", "BLOB", "FIELD"}});
  }

  void require_writer() {
    require(verified, "version store capabilities have not been verified");
    require(!writer_uncertain, "version store has an unresolved write; owner must stop admission");
    if (!writer) writer = connect();
  }
};

IoTDBVersionStore::IoTDBVersionStore(const IoTDBClientConfig& config, std::string prefix)
  : IoTDBVersionStore(config, std::move(prefix), Options{}) {}
IoTDBVersionStore::IoTDBVersionStore(const IoTDBClientConfig& config, std::string prefix, Options options)
  : impl_(std::make_unique<Impl>(config, prefix, options)) {}
IoTDBVersionStore::~IoTDBVersionStore() = default;

void IoTDBVersionStore::create_schema() {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->capabilities();
  require(!impl_->writer_uncertain, "version store has an unresolved schema write");
  try {
    impl_->count(&Stats::nonquery_attempts);query_profile_count("version_store.sql_nonqueries");
    impl_->writer->executeNonQueryStatement("CREATE TABLE IF NOT EXISTS " + impl_->head_table +
      " (slot STRING TAG,value BLOB FIELD)");
    impl_->count(&Stats::nonquery_attempts);query_profile_count("version_store.sql_nonqueries");
    impl_->writer->executeNonQueryStatement("CREATE TABLE IF NOT EXISTS " + impl_->object_table +
      " (namespace_id STRING TAG,incarnation_id STRING TAG,object_type STRING TAG,"
      "object_protocol STRING TAG,content_id STRING TAG,value BLOB FIELD)");
  } catch (...) {
    impl_->writer_uncertain = true;
    throw std::runtime_error("version store schema write result is indeterminate");
  }
  impl_->schemas();
  for (const auto& table : {impl_->head_table, impl_->object_table}) {
    bool nonempty = false;
    impl_->query("SELECT time FROM " + table + " LIMIT 1",
      [&](const auto&) { nonempty = true; }, 1);
    require(!nonempty, "version store initialization requires an empty namespace");
  }
  impl_->verified = true;
}

IoTDBVersionStore::Capabilities IoTDBVersionStore::verify_capabilities() {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->verified = false;
  auto capabilities = impl_->capabilities();
  impl_->schemas();
  impl_->verified = true;
  return capabilities;
}

IoTDBVersionStore::Stats IoTDBVersionStore::stats() const {
  Stats result;
  { std::lock_guard<std::mutex> lock(impl_->counters_mutex); result = impl_->counters; }
  const auto pool = impl_->object_readers.stats();
  result.reader_pool_size = pool.capacity;
  result.reader_pool_active = pool.active;
  result.reader_pool_peak = pool.peak;
  result.reader_pool_waits = pool.waits;
  result.reader_pool_acquire_timeouts = pool.acquire_timeouts;
  result.reader_pool_created = pool.created;
  result.reader_pool_discarded = pool.discarded;
  return result;
}

std::optional<Bytes> IoTDBVersionStore::read_head() {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  auto value = impl_->read_cell(impl_->head_table, "slot='main'", kHeadLimit);
  return value;
}

void IoTDBVersionStore::accept_reconciled_head() {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->pending_head) {
    auto value = impl_->read_cell(impl_->head_table, "slot='main'", kHeadLimit);
    require(value && *value == *impl_->pending_head, "attempted head is not confirmed");
  }
  impl_->pending_head.reset();
  impl_->writer_uncertain = false;
  Impl::close(impl_->writer);
}

Bytes IoTDBVersionStore::get_object(const NamespaceIdentity& identity, const ObjectRef& ref) {
  const auto predicate = impl_->object_predicate(identity, ref);
  detail::OwnerReadTiming timing(false, 1);
  timing.record.payload_blocks = ref.type == "payload-block";
  using Lease = decltype(impl_->acquire_object_reader(timing));
  std::optional<Lease> lease;
  detail::OwnerReadTiming::Release<std::optional<Lease>> release(timing, lease);
  lease.emplace(impl_->acquire_object_reader(timing));
  std::optional<Bytes> value;
  impl_->count(&Stats::object_cell_reads);
  query_profile_count("version_store.object_reads");
  iotdb_profile_counters.version_store_object_reads.fetch_add(1, std::memory_order_relaxed);
  impl_->query_on(*(*lease)->session, "SELECT value FROM " + impl_->object_table +
    " WHERE time=0 AND " + predicate, [&](const auto& fields) {
      require(fields.size() == 1 && fields[0].dataType == TSDataType::BLOB &&
        fields[0].stringV.is_initialized(), "version store cell is not a complete BLOB");
      const auto& binary = fields[0].stringV.value();
      require(binary.size() <= kObjectLimit, "version store BLOB exceeds its limit");
      impl_->count(&Stats::returned_blob_bytes, binary.size());
      query_profile_count("version_store.blob_bytes", binary.size());
      iotdb_profile_counters.version_store_blob_bytes.fetch_add(binary.size(), std::memory_order_relaxed);
      timing.record.returned_bytes += binary.size();
      detail::OwnerReadTiming::Duration copy(timing, timing.record.copy_ns);
      value = Bytes(binary.begin(), binary.end());
    }, 1, false, true, &timing);
  timing.stage("validation");
  {
    detail::OwnerReadTiming::Duration validation(timing, timing.record.validation_ns);
    require(value.has_value(), "immutable version object is missing");
    detail::OwnerReadTiming::Duration digest(timing, timing.record.digest_ns);
    require(framed_digest(ref, *value) == ref.id, "immutable version object digest mismatch");
  }
  timing.stage("final_checkpoint");
  storage_read_checkpoint();
  lease->reuse();
  timing.record.success = true;
  timing.stage("complete");
  return std::move(*value);
}

std::vector<Bytes> IoTDBVersionStore::get_objects(const NamespaceIdentity& identity,
    const std::vector<ObjectReadRequest>& requests) {
  require(!requests.empty()&&requests.size()<=kObjectBatchEntries,"version object batch count exceeds bounds");
  using Key=std::tuple<std::string,std::string,ContentId>;
  std::map<Key,std::size_t> positions;
  std::uint64_t total=0;
  std::string sql="SELECT object_type,object_protocol,content_id,value FROM "+impl_->object_table+
    " WHERE time=0 AND (";
  for(std::size_t i=0;i<requests.size();++i) {
    const auto& request=requests[i];const auto& ref=request.ref;
    require(request.size>0&&request.size<=kObjectBatchBytes-total,"version object batch byte bound exceeded");
    total+=request.size;
    require(positions.emplace(Key{ref.type,ref.protocol,ref.id},i).second,"duplicate version object batch request");
    if(i)sql+=" OR ";
    sql+='('+impl_->object_predicate(identity,ref)+')';
  }
  sql+=')';
  std::vector<Bytes> result(requests.size());
  std::vector<bool> seen(requests.size(),false);
  detail::OwnerReadTiming timing(true, requests.size());
  timing.record.payload_blocks = std::all_of(requests.begin(), requests.end(),
    [](const ObjectReadRequest& request) { return request.ref.type == "payload-block"; });
  using Lease = decltype(impl_->acquire_object_reader(timing));
  std::optional<Lease> lease;
  detail::OwnerReadTiming::Release<std::optional<Lease>> release(timing, lease);
  lease.emplace(impl_->acquire_object_reader(timing));
  impl_->count(&Stats::batch_object_reads, requests.size());
  impl_->count(&Stats::object_cell_reads, requests.size());
  query_profile_count("version_store.object_reads",requests.size());
  iotdb_profile_counters.version_store_object_reads.fetch_add(requests.size(),std::memory_order_relaxed);
  impl_->query_on(*(*lease)->session,sql,[&](const auto& fields) {
    require(fields.size()==4&&fields[3].dataType==TSDataType::BLOB&&fields[3].stringV.is_initialized(),
      "version object batch row is malformed");
    const auto found=positions.find(Key{field_text(fields[0]),field_text(fields[1]),field_text(fields[2])});
    require(found!=positions.end(),"unexpected version object batch identity");
    const auto index=found->second;
    require(!seen[index],"duplicate version object batch row");
    const auto& value=fields[3].stringV.value();
    impl_->count(&Stats::returned_blob_bytes, value.size());query_profile_count("version_store.blob_bytes",value.size());
    iotdb_profile_counters.version_store_blob_bytes.fetch_add(value.size(),std::memory_order_relaxed);
    require(value.size()==requests[index].size,"version object batch length mismatch");
    timing.record.returned_bytes += value.size();
    {
      detail::OwnerReadTiming::Duration copy(timing, timing.record.copy_ns);
      result[index]=Bytes(value.begin(),value.end());
    }
    {
      detail::OwnerReadTiming::Duration digest(timing, timing.record.digest_ns);
      require(framed_digest(requests[index].ref,result[index])==requests[index].ref.id,
        "version object batch digest mismatch");
    }
    seen[index]=true;
  },requests.size(),true,true,&timing);
  timing.stage("validation");
  {
    detail::OwnerReadTiming::Duration validation(timing, timing.record.validation_ns);
    require(std::all_of(seen.begin(),seen.end(),[](bool value){return value;}),"version object batch is incomplete");
  }
  timing.stage("final_checkpoint");
  storage_read_checkpoint();
  lease->reuse();
  timing.record.success = true;
  timing.stage("complete");
  return result;
}

void IoTDBVersionStore::put_object(const NamespaceIdentity& identity, const ObjectRef& ref,
    const Bytes& bytes) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const auto predicate = impl_->object_predicate(identity, ref);
  require(bytes.size() <= kObjectLimit && framed_digest(ref, bytes) == ref.id,
    "immutable version object size or digest mismatch");
  impl_->require_writer();
  auto existing = impl_->read_cell(impl_->object_table, predicate, kObjectLimit);
  if (existing) {
    require(*existing == bytes, "immutable version object collision");
    return;
  }
  try {
    impl_->count(&Stats::nonquery_attempts);query_profile_count("version_store.sql_nonqueries");
    impl_->writer->executeNonQueryStatement("INSERT INTO " + impl_->object_table +
      " (time,namespace_id,incarnation_id,object_type,object_protocol,content_id,value) VALUES (0," +
      quote(identity.namespace_id) + "," + quote(identity.incarnation) + "," + quote(ref.type) +
      "," + quote(ref.protocol) + "," + quote(ref.id) + ",X'" + hex(bytes) + "')");
  } catch (...) {
    // Object writes have the same unknown transport outcome as head writes.
    // Only observing the exact immutable value permits continuation.
    impl_->writer_uncertain = true;
    existing = impl_->read_cell(impl_->object_table, predicate, kObjectLimit);
    require(existing && *existing == bytes, "immutable object write result is indeterminate");
    impl_->writer_uncertain = false;
    Impl::close(impl_->writer);
  }
  existing = impl_->read_cell(impl_->object_table, predicate, kObjectLimit);
  require(existing && *existing == bytes, "immutable object is not durably readable after write");
}

HeadWriteResult IoTDBVersionStore::write_head(const Bytes& bytes) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  require(!bytes.empty() && bytes.size() <= kHeadLimit, "version head size is invalid");
  impl_->require_writer();
  try {
    impl_->count(&Stats::nonquery_attempts);query_profile_count("version_store.sql_nonqueries");
    impl_->writer->executeNonQueryStatement("INSERT INTO " + impl_->head_table +
      " (time,slot,value) VALUES (0,'main',X'" + hex(bytes) + "')");
    return HeadWriteResult::Acknowledged;
  } catch (...) {
    impl_->writer_uncertain = true;
    impl_->pending_head = bytes;
    return HeadWriteResult::Indeterminate;
  }
}

} // namespace iotdbfs::versioning
