#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <chrono>

#include "iotdbfs/IoTDBClientPolicy.h"
#include "SessionDataSet.h"
#include "TableSession.h"

struct IoTDBClientConfig {
  std::string host;
  int port{ 0 };
  std::string user;
  std::string pass;
  std::string db;
  int use_prepared{ 0 };
  bool auto_create_db{ true };
  bool auto_use_db{ true };
  std::chrono::milliseconds acquire_timeout{ 5000 };
};

// An independent IoTDB table client. Queries lease one connection until the
// returned dataset is destroyed, including when it outlives this client.
class IoTDBClient {
public:
  IoTDBClient() = default;
  explicit IoTDBClient(const IoTDBClientConfig& cfg, size_t pool_size_hint);
  IoTDBClient(const IoTDBClient&) = delete;
  IoTDBClient& operator=(const IoTDBClient&) = delete;

  // Publish the pool only after every connection and requested CREATE/USE
  // succeeds. Failure throws and leaves this instance available for retry.
  // Once successful, later calls leave the original pool unchanged.
  void initialize(const IoTDBClientConfig& cfg, size_t pool_size_hint);
  bool isInitialized() const;
  size_t poolSize() const;

  bool executeNonQuery(const std::string& sql);
  std::shared_ptr<SessionDataSet> executeQuery(const std::string& sql);
  bool insertTablet(Tablet& tablet);

private:
  struct SessionPool;
  std::shared_ptr<SessionPool> initializedPool() const;

  std::once_flag init_flag_;
  std::shared_ptr<SessionPool> pool_;
};
