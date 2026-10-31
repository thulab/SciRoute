#pragma once

#include "iotdbfs/IoTDBClient.h"
#include "iotdbfs/Logging.h"

struct IoTDBConfig {
  std::string host;
  int port{ 0 };
  std::string user;
  std::string pass;
  std::string db;
  int use_prepared{ 0 };
};

// The filesystem's default client, sharing the IoTDBClient pool implementation.
class IoTDBConnector {
public:
  static IoTDBConnector& instance();

  void initialize(const IoTDBConfig& cfg, size_t pool_size_hint);
  bool executeNonQuery(const std::string& sql);
  std::shared_ptr<SessionDataSet> executeQuery(const std::string& sql);
  bool insertTablet(Tablet& tablet);
  IoTDBClientConfig configuration() const;

private:
  IoTDBConnector() = default;
  IoTDBConnector(const IoTDBConnector&) = delete;
  IoTDBConnector& operator=(const IoTDBConnector&) = delete;

  IoTDBClient client_;
  mutable std::mutex configuration_mutex_;
  IoTDBClientConfig configuration_;
};
