// V00 probe of an explicitly authorized existing instance; only a unique test DB is written.
#include "TableSession.h"
#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

static void require(bool value, const char* error) {
  if (!value) throw std::runtime_error(error);
}
static std::string hex(const std::string& raw) {
  constexpr char digits[] = "0123456789abcdef";
  std::string result;
  for (unsigned char c : raw) { result += digits[c >> 4]; result += digits[c & 15]; }
  return result;
}
int main(int argc, char** argv) {
  try {
    require(argc == 2, "loopback scratch RPC port required");
    int port = std::stoi(argv[1]);
    require(port == 34667, "only isolated Table 5 RPC allowed");
    const char* db = std::getenv("IOTDB_V00_PROBE_DATABASE");
    require(db != nullptr, "test database required");
    const std::string database(db);
    require(database.size() == 25 && database.substr(0, 9) == "v00_head_" &&
      std::all_of(database.begin()+9, database.end(), [](char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
      }), "only unique V00 test database permitted");
    const char* cell_env = std::getenv("IOTDB_V00_PROBE_CELL");
    const std::string cell = cell_env ? cell_env : "n";
    require(cell == "n" || (cell.size() == 16 &&
      std::all_of(cell.begin(), cell.end(), [](char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
      })), "invalid test cell tag");
    AbstractSessionBuilder builder;
    builder.host = "127.0.0.1"; builder.rpcPort = port;
    const char* password = std::getenv("IOTDB_V00_PROBE_PASSWORD");
    require(password && *password, "trusted connection password required in environment");
    builder.username = "root"; builder.password = password;
    builder.sqlDialect = "table"; builder.enableAutoFetch = false;
    TableSession session(std::make_shared<Session>(&builder));
    session.open(false);
    std::cout << "READY" << std::endl;
    std::string line;
    while (std::getline(std::cin, line)) {
      try {
        require(line.size() <= 132000, "command exceeds probe bound");
        const auto split = line.find('\t');
        auto op = line.substr(0, split);
        auto payload = split == std::string::npos ? "" : line.substr(split + 1);
        if (op == "init") {
          session.executeNonQueryStatement("CREATE DATABASE " + database);
          session.executeNonQueryStatement("USE " + database);
          session.executeNonQueryStatement("CREATE TABLE IF NOT EXISTS head (ns STRING TAG, value BLOB FIELD)");
          session.executeNonQueryStatement("CREATE TABLE IF NOT EXISTS objects (id STRING TAG, value BLOB FIELD)");
        } else if (op == "use") {
          session.executeNonQueryStatement("USE " + database);
        } else if (op == "write" || op == "object") {
          require(!payload.empty() && payload.size() % 2 == 0 && payload.size() <= (op == "write" ? 8192 : 131072), "bad payload length");
          require(std::all_of(payload.begin(), payload.end(), [](char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); }), "non-hex payload");
          auto table = op == "write" ? "head" : "objects";
          auto tag = op == "write" ? "ns" : "id";
          session.executeNonQueryStatement(std::string("INSERT INTO ") + table + " (time," + tag + ",value) VALUES (0,'" + cell + "',X'" + payload + "')");
        } else if (op == "read" || op == "read_object") {
          auto rows = session.executeQueryStatement(std::string("SELECT value FROM ") + (op == "read" ? "head WHERE ns" : "objects WHERE id") + "='" + cell + "' AND time=0", 10000);
          require(bool(rows), "null result");
          if (!rows->hasNext()) { rows->closeOperationHandle(); std::cout << "EMPTY" << std::endl; continue; }
          auto row = rows->next();
          require(row && row->fields.size() == 1 && row->fields[0].stringV, "head result is not one BLOB");
          auto value = hex(row->fields[0].stringV.value());
          require(!rows->hasNext(), "multiple head rows");
          rows->closeOperationHandle();
          std::cout << "VALUE\t" << value << std::endl; continue;
        } else if (op == "settings") {
          auto rows = session.executeQueryStatement("SHOW ALL CONFIGURATION ON 1", 10000);
          std::string wal, partial;
          while (rows->hasNext()) {
            auto row = rows->next();
            if (!row || row->fields.size() < 3 || !row->fields[0].stringV) continue;
            const auto& key = row->fields[0].stringV.value();
            auto value = row->fields[1].stringV ? row->fields[1].stringV : row->fields[2].stringV;
            if (value && key == "wal_mode") wal = value.value();
            if (value && key == "enable_partial_insert") partial = value.value();
          }
          rows->closeOperationHandle();
          require((wal == "SYNC" || wal == "ASYNC" || wal == "DISABLE") &&
                  (partial == "true" || partial == "false"), "missing effective settings");
          std::cout << "CONFIG\t" << wal << "\t" << partial << std::endl; continue;
        } else if (op == "flush") {
          session.executeNonQueryStatement("FLUSH " + database);
        } else throw std::runtime_error("unsupported operation");
        std::cout << "OK" << std::endl;
      } catch (const std::exception& e) {
        std::cerr << "probe operation failed: " << e.what() << std::endl;
        std::cout << "ERROR" << std::endl;
      }
    }
    session.close();
  } catch (const std::exception& e) {
    std::cerr << "probe setup failed: " << e.what() << std::endl; return 1;
  }
}
