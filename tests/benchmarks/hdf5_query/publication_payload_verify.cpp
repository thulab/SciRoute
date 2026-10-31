// Read-only post-publication acceptance. Never contributes to publish timing.
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/StructuredBlobStorage.h"

#include <openssl/evp.h>
#include <algorithm>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>

namespace {
std::string env(const char* name, const char* fallback = "") {
  const char* value = std::getenv(name);
  return value ? value : fallback;
}
void need(bool valid, const std::string& message) {
  if (!valid) throw std::runtime_error(message);
}
}

int main(int argc, char** argv) {
  try {
    need(argc == 5, "usage: publication_payload_verify PATH VERSION SIZE SHA256");
    const iotdbfs::StructuredBlobVersionKey key{argv[1], std::stoll(argv[2])};
    const auto size = std::stoull(argv[3]);
    IoTDBConfig config{env("IOTDB_HOST", "127.0.0.1"),
      std::stoi(env("IOTDB_PORT", "6667")), env("IOTDB_USER"),
      env("IOTDB_PASS"), env("IOTDB_DB"), 0};
    need(!config.db.empty(), "explicit experiment database is required");
    IoTDBConnector::instance().initialize(config, 2);
    iotdbfs::StructuredBlobStorage storage(
      std::make_shared<iotdbfs::IoTDBStructuredBlobBackend>());
    auto reader = storage.open_published_version(key);
    need(reader.ok(), reader.status().message());
    need(reader.value().manifest().payload_size == size, "published size differs");
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> hash(
      EVP_MD_CTX_new(), EVP_MD_CTX_free);
    need(hash && EVP_DigestInit_ex(hash.get(), EVP_sha256(), nullptr) == 1,
      "cannot initialize SHA-256");
    std::size_t reads = 0;
    for (std::uint64_t offset = 0; offset < size;) {
      const auto length = std::min<std::uint64_t>(8 * 1024 * 1024, size - offset);
      auto bytes = reader.value().read_payload_range(offset, length);
      need(bytes.ok(), bytes.status().message());
      need(bytes.value().size() == length, "short payload read");
      need(EVP_DigestUpdate(hash.get(), bytes.value().data(), bytes.value().size()) == 1,
        "cannot update SHA-256");
      offset += length;
      ++reads;
    }
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_size = 0;
    need(EVP_DigestFinal_ex(hash.get(), digest, &digest_size) == 1,
      "cannot finalize SHA-256");
    std::ostringstream hex;
    for (unsigned int i = 0; i < digest_size; ++i)
      hex << std::hex << std::setw(2) << std::setfill('0') << unsigned(digest[i]);
    need(hex.str() == argv[4], "full published payload SHA-256 differs");
    std::cout << "{\"ok\":true,\"path\":" << std::quoted(key.path)
      << ",\"version\":" << key.version << ",\"bytes\":" << size
      << ",\"sha256\":\"" << hex.str() << "\",\"read_calls\":" << reads
      << "}\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
