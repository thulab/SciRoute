#include "iotdbfs/IoTDBConnector.h"

IoTDBConnector& IoTDBConnector::instance() {
  static IoTDBConnector inst;
  return inst;
}

void IoTDBConnector::initialize(const IoTDBConfig& cfg, size_t pool_size_hint) {
  std::lock_guard<std::mutex> lock(configuration_mutex_);
  if (client_.isInitialized()) return;
  IoTDBClientConfig client_cfg;
  client_cfg.host = cfg.host;
  client_cfg.port = cfg.port;
  client_cfg.user = cfg.user;
  client_cfg.pass = cfg.pass;
  client_cfg.db = cfg.db;
  client_cfg.use_prepared = cfg.use_prepared;
  client_.initialize(client_cfg, pool_size_hint);
  configuration_ = std::move(client_cfg);
}

IoTDBClientConfig IoTDBConnector::configuration() const {
  std::lock_guard<std::mutex> lock(configuration_mutex_);
  if (!client_.isInitialized()) throw std::runtime_error("IoTDB connector is not initialized");
  return configuration_;
}

bool IoTDBConnector::executeNonQuery(const std::string& sql) {
  return client_.executeNonQuery(sql);
}

std::shared_ptr<SessionDataSet>
IoTDBConnector::executeQuery(const std::string& sql) {
  return client_.executeQuery(sql);
}

bool IoTDBConnector::insertTablet(Tablet& tablet) {
  return client_.insertTablet(tablet);
}
