#include "iotdbfs/migration/MigrationPolicy.h"
#include <openssl/evp.h>
#include <array>
#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace iotdbfs {
std::string migration_json_quote(const std::string& text) {
  std::ostringstream out; out << '"';
  for (unsigned char c : text) {
    switch (c) {
      case '"': out << "\\\""; break;
      case '\\': out << "\\\\"; break;
      case '\n': out << "\\n"; break;
      case '\r': out << "\\r"; break;
      case '\t': out << "\\t"; break;
      default:
        if (c < 32) out << "\\u" << std::hex << std::setw(4)
          << std::setfill('0') << static_cast<unsigned>(c) << std::dec;
        else out << static_cast<char>(c);
    }
  }
  out << '"'; return out.str();
}
std::string migration_sha256(const StructuredBlobBytes& bytes) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{}; unsigned length = 0;
  if (EVP_Digest(bytes.data(), bytes.size(), digest.data(), &length,
      EVP_sha256(), nullptr) != 1) throw std::runtime_error("cannot compute migration SHA-256");
  std::ostringstream out; out << std::hex << std::setfill('0');
  for (unsigned i = 0; i < length; ++i) out << std::setw(2) << static_cast<unsigned>(digest[i]);
  return out.str();
}
bool migration_valid_sha256(const std::string& text) {
  if (text.size() != 64) return false;
  for (char c : text) if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
  return true;
}
const char* migration_kind_name(MigrationKind kind) {
  switch (kind) {
    case MigrationKind::Candidate: return "candidate";
    case MigrationKind::HistoricalVersion: return "historical_version";
    case MigrationKind::AlreadyIndexed: return "already_indexed";
    case MigrationKind::RequiresReingestion: return "requires_reingestion";
    case MigrationKind::Unsupported: return "unsupported";
    case MigrationKind::OtherFormat: return "other_format";
    case MigrationKind::NotFormat: return "not_format";
    case MigrationKind::Error: return "error";
  }
  return "error";
}
} // namespace iotdbfs
