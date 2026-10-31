// Independent read-only plan probe. No schema, fixture, or configuration writes.
#include "TableSession.h"
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace {
std::string env(const char* name) {
  const char* value = std::getenv(name);
  if (!value) throw std::runtime_error(std::string("missing connection environment: ") + name);
  return value;
}
std::string json(const std::string& value) {
  std::string out = "\"";
  constexpr char hex[] = "0123456789abcdef";
  for (unsigned char c : value) {
    if (c == '"' || c == '\\') { out += '\\'; out += c; }
    else if (c < 32) { out += "\\u00"; out += hex[c >> 4]; out += hex[c & 15]; }
    else out += c;
  }
  return out + '"';
}
std::string safe_error(std::string value) {
  for (const char* name : {"IOTDB_PASS", "IOTDB_USER"}) {
    const auto* raw = std::getenv(name);
    const std::string secret = raw ? raw : "";
    if (secret.empty()) continue;
    std::size_t pos = 0;
    while ((pos = value.find(secret, pos)) != std::string::npos) {
      value.replace(pos, secret.size(), "<redacted>");
      pos += 10;
    }
  }
  return value;
}
void field_json(std::ostream& out, const Field& field, bool compact_plan) {
  if (field.stringV) {
    auto text=field.stringV.value();
    if (compact_plan) {
      // EXPLAIN's wide ASCII graph is padded quadratically in branch count.
      // Collapse padding only, retaining all operator/expression tokens.
      std::string compact; bool space=false;
      for (char c:text) {
        if (c==' ' || c=='\t') { if (!space) compact+=' '; space=true; }
        else { compact+=c; space=false; }
      }
      text=std::move(compact);
    }
    out << json(text);
  }
  else if (field.longV) out << field.longV.value();
  else if (field.intV) out << field.intV.value();
  else if (field.boolV) out << (field.boolV.value() ? "true" : "false");
  else if (field.doubleV) out << field.doubleV.value();
  else if (field.floatV) out << field.floatV.value();
  else out << "null";
}
}
int main(int argc, char** argv) {
  try {
    if (argc != 2 || env("IOTDB_DB") != "h09probe1790939889dbfc1f")
      throw std::runtime_error("unexpected probe arguments/database");
    std::ifstream input(argv[1]);
    if (!input) throw std::runtime_error("missing bounded statement input");
    AbstractSessionBuilder builder;
    builder.host = env("IOTDB_HOST");
    builder.rpcPort = std::stoi(env("IOTDB_PORT"));
    builder.username = env("IOTDB_USER");
    builder.password = env("IOTDB_PASS");
    builder.sqlDialect = "table";
    builder.database = "h09probe1790939889dbfc1f";
    builder.enableAutoFetch = false;
    TableSession session(std::make_shared<Session>(&builder));
    session.open(false);
    std::string line;
    unsigned queries = 0;
    while (std::getline(input, line)) {
      if (++queries > 128 || line.size() > 65536)
        throw std::runtime_error("statement input exceeded probe bounds");
      const auto split = line.find('\t');
      if (split == std::string::npos) throw std::runtime_error("malformed statement input");
      const auto label = line.substr(0, split), sql = line.substr(split + 1);
      if (sql.rfind("SELECT ", 0) && sql.rfind("EXPLAIN SELECT ", 0) &&
          sql.rfind("EXPLAIN ANALYZE SELECT ", 0))
        throw std::runtime_error("only read-only select/plan statements admitted");
      const auto start = std::chrono::steady_clock::now();
      std::ostringstream output;
      output << "{\"label\":" << json(label) << ",\"rows\":[";
      bool first_row = true;
      unsigned row_count = 0;
      bool ok = true;
      std::string error;
      try {
        auto rows = session.executeQueryStatement(sql, 30000);
        if (!rows) throw std::runtime_error("null query result");
        while (rows->hasNext()) {
          if (++row_count > 2048 || output.tellp() > 1024 * 1024)
            throw std::runtime_error("query output exceeded probe bounds");
          const auto row = rows->next();
          if (!first_row) output << ',';
          first_row = false;
          output << '[';
          bool first = true;
          for (const auto& field : row->fields) {
            if (!first) output << ',';
            first = false;
            field_json(output, field, sql.rfind("EXPLAIN SELECT ",0)==0);
          }
          output << ']';
        }
        rows->closeOperationHandle();
      } catch (const std::exception& e) { ok = false; error = safe_error(e.what()); }
      const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - start).count();
      output << "],\"ok\":" << (ok ? "true" : "false")
             << ",\"elapsed_ns\":" << ns << ",\"error\":" << json(error) << '}';
      std::cout << output.str() << std::endl;
    }
    session.close();
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "readonly probe setup failed: " << safe_error(e.what()) << std::endl;
    return 1;
  }
}
