#include "iotdbfs/FormatRegistry.h"
#include "iotdbfs/Ingestion.h"
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/NamespaceWriterLock.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

namespace {

template<class T> T checked(arrow::Result<T> value) {
  if (!value.ok()) throw std::runtime_error(value.status().ToString());
  return std::move(value).ValueOrDie();
}
void checked(const arrow::Status& status) {
  if (!status.ok()) throw std::runtime_error(status.ToString());
}

std::string env(const char* name, const char* fallback) {
  if (const char* value = std::getenv(name)) return value;
  return fallback;
}

iotdbfs::StructuredBlobBytes fixture() {
  auto leaves = std::make_shared<arrow::FloatBuilder>();
  auto channel_builder = std::make_shared<arrow::ListBuilder>(
    arrow::default_memory_pool(), leaves);
  arrow::ListBuilder rows(arrow::default_memory_pool(), channel_builder);
  checked(rows.Append());
  checked(channel_builder->Append());
  checked(leaves->AppendValues({1.0F, 2.0F, 3.0F}));
  checked(channel_builder->Append());
  checked(leaves->AppendValues({10.0F, 20.0F, 30.0F}));
  std::shared_ptr<arrow::Array> array;
  checked(rows.Finish(&array));
  auto batch = arrow::RecordBatch::Make(
    arrow::schema({arrow::field("target", array->type())}), 1, {array});
  auto sink = checked(arrow::io::BufferOutputStream::Create());
  auto writer = checked(arrow::ipc::MakeStreamWriter(sink, batch->schema()));
  checked(writer->WriteRecordBatch(*batch));
  checked(writer->Close());
  auto buffer = checked(sink->Finish());
  return {buffer->data(), buffer->data() + buffer->size()};
}
} // namespace

int main(int argc, char** argv) {
  if ((argc != 2 && argc != 3) || argv[1][0] != '/') {
    std::cerr << "usage: a12_legacy_seed /exact/path [small-ipc-stream]\n";
    return 2;
  }
  try {
    IoTDBConfig config;
    config.host = env("IOTDB_HOST", "127.0.0.1");
    config.port = std::stoi(env("IOTDB_PORT", "6667"));
    config.user = env("IOTDB_USER", "root");
    config.pass = env("IOTDB_PASS", "root");
    config.db = env("IOTDB_DB", "dfs");
    std::string error;
    auto lock = iotdbfs::NamespaceWriterLock::acquire(
      iotdbfs::namespace_writer_lock_path(config.host, config.port, config.db), &error);
    if (!lock) throw std::runtime_error(error);
    IoTDBConnector::instance().initialize(config, 2);
    if (!IoTDBConnector::instance().executeNonQuery(
          "CREATE DATABASE IF NOT EXISTS " + config.db))
      throw std::runtime_error("cannot create isolated database");
    auto backend = std::make_shared<iotdbfs::IoTDBStructuredBlobBackend>();
    if (!backend->ensure_schema())
      throw std::runtime_error("cannot create structured blob schema");
    iotdbfs::StructuredBlobStorage storage(backend);
    iotdbfs::FormatRegistry no_formats;
    iotdbfs::IngestionModule ingestion(storage, no_formats, {});
    auto bytes = fixture();
    if (argc == 3) {
      std::ifstream input(argv[2], std::ios::binary | std::ios::ate);
      if (!input || input.tellg() < 0 || input.tellg() > 64 * 1024 * 1024)
        throw std::runtime_error("source file unavailable or exceeds 64 MiB");
      const auto expected = static_cast<std::size_t>(input.tellg());
      input.seekg(0);
      bytes.assign(std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>());
      if (bytes.size() != expected)
        throw std::runtime_error("source file read failed");
    }
    const auto version = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
    iotdbfs::StructuredBlobVersionKey key{argv[1], version};
    iotdbfs::FormatSource source{bytes.size(),
      [&bytes](std::uint64_t offset, std::uint64_t length)
          -> std::optional<iotdbfs::StructuredBlobBytes> {
        if (offset > bytes.size() || length > bytes.size() - offset)
          return std::nullopt;
        auto begin = bytes.begin() + static_cast<std::size_t>(offset);
        return iotdbfs::StructuredBlobBytes(begin,
          begin + static_cast<std::size_t>(length));
      }};
    auto result = ingestion.ingest(key, source,
      [&](const iotdbfs::IngestionOutcome& outcome) {
        Inode inode;
        inode.row_time = version;
        inode.path = argv[1];
        inode.parent = "/";
        inode.name = inode.path.substr(1);
        inode.size = static_cast<long long>(outcome.payload_size);
        inode.mode = 0644;
        inode.uid = inode.gid = 0;
        inode.ctime = inode.mtime = inode.atime = version;
        return backend->upsert_inode(inode, false);
      });
    if (!result.ok()) throw std::runtime_error(result.message());
    std::cout << "{\"path\":\"" << key.path << "\",\"version\":"
              << key.version << ",\"bytes\":" << bytes.size() << "}\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
