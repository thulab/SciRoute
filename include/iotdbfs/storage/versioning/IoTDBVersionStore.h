#pragma once

#include "iotdbfs/IoTDBClient.h"
#include "iotdbfs/storage/versioning/VersionDirectory.h"

#include <chrono>
#include <memory>
#include <string>

namespace iotdbfs::versioning {

// Persistence only: ownership is supplied by the namespace owner, not by an
// IoTDB compare-and-swap or database credential isolation mechanism.
class IoTDBVersionStore final : public Store {
public:
  struct Stats {
    std::uint64_t query_attempts{0}, nonquery_attempts{0};
    std::uint64_t head_cell_reads{0}, object_cell_reads{0}, returned_blob_bytes{0};
    std::uint64_t batch_query_attempts{0}, batch_object_reads{0};
    std::size_t reader_pool_size{0}, reader_pool_active{0}, reader_pool_peak{0};
    std::uint64_t reader_pool_waits{0}, reader_pool_acquire_timeouts{0};
    std::uint64_t reader_pool_created{0}, reader_pool_discarded{0};
  };
  Stats stats() const;

  struct Capabilities {
    int datanode_id{-1};
    std::string wal_mode;
    bool partial_insert{true};
  };

  struct Options {
    // Optional process-wide diagnostics: IOTDBFS_OWNER_READ_TRACE_JSONL names
    // a regular append-only trace file (at most 4096 read records per process).
    // Set before the first immutable read. Disabled by default; no SQL, keys or
    // values are logged. SDK phase intervals are not DataNode execution clocks.
    // Applies only to immutable object reads. Head/schema/recovery and writes
    // keep their independent serialized control connections.
    std::size_t reader_pool_size{4};
    // Waiting checks the query cancellation/deadline every 10 ms. Opening and
    // executing a session retain the SDK/query timeouts, independently.
    std::chrono::milliseconds reader_acquire_timeout{5000};
  };

  // Uses an existing database. Never creates a database or changes server
  // configuration. The prefix must begin with vd_ and name an owned namespace.
  IoTDBVersionStore(const IoTDBClientConfig& config, std::string table_prefix);
  IoTDBVersionStore(const IoTDBClientConfig& config, std::string table_prefix, Options options);
  ~IoTDBVersionStore() override;
  IoTDBVersionStore(const IoTDBVersionStore&) = delete;
  IoTDBVersionStore& operator=(const IoTDBVersionStore&) = delete;

  // Explicit initialization under the owner's prefix lock. Existing empty,
  // correctly shaped tables can finish an interrupted schema initialization.
  // Any stored row rejects initialization; normal reopen uses verify_capabilities.
  void create_schema();
  Capabilities verify_capabilities();

  std::optional<Bytes> read_head() override;
  Bytes get_object(const NamespaceIdentity&, const ObjectRef&) override;
  std::vector<Bytes> get_objects(const NamespaceIdentity&,
    const std::vector<ObjectReadRequest>&) override;
  void put_object(const NamespaceIdentity&, const ObjectRef&, const Bytes&) override;
  HeadWriteResult write_head(const Bytes&) override;
  void accept_reconciled_head() override;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace iotdbfs::versioning
