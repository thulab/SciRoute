#include "iotdbfs/query/frontend/TypedQueryFrontend.h"

#include <openssl/rand.h>

#include <array>
#include <charconv>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace iotdbfs {
namespace {

constexpr std::string_view kTypedPath = "/.iotdbfs/query/v1/typed.ipc";
constexpr std::string_view kWindowPath = "/.iotdbfs/query/v1/windows.ipc";
constexpr std::size_t kMaxRouteBytes = 1024;
constexpr std::size_t kIdBytes = 24;

QueryStatus invalid(std::string message) {
  return QueryStatus::failure(QueryError::InvalidQuery, QueryPhase::Validation,
    std::move(message));
}

QueryStatus exhausted(std::string message) {
  return QueryStatus::failure(QueryError::ResourceExhausted,
    QueryPhase::Validation, std::move(message));
}

bool valid_utf8(std::string_view value) {
  for (std::size_t i = 0; i < value.size();) {
    const auto first = static_cast<unsigned char>(value[i]);
    if (first < 0x80) {
      if (first < 0x20 || first == 0x7f) return false;
      ++i;
      continue;
    }
    std::size_t continuation = 0;
    std::uint32_t scalar = 0;
    std::uint32_t minimum = 0;
    if ((first & 0xe0) == 0xc0) {
      continuation = 1; scalar = first & 0x1f; minimum = 0x80;
    } else if ((first & 0xf0) == 0xe0) {
      continuation = 2; scalar = first & 0x0f; minimum = 0x800;
    } else if ((first & 0xf8) == 0xf0) {
      continuation = 3; scalar = first & 0x07; minimum = 0x10000;
    } else return false;
    if (value.size() - i <= continuation) return false;
    for (std::size_t offset = 1; offset <= continuation; ++offset) {
      const auto byte = static_cast<unsigned char>(value[i + offset]);
      if ((byte & 0xc0) != 0x80) return false;
      scalar = (scalar << 6) | (byte & 0x3f);
    }
    if (scalar < minimum || scalar > 0x10ffff ||
        (scalar >= 0xd800 && scalar <= 0xdfff)) return false;
    i += continuation + 1;
  }
  return true;
}

int hex_digit(char byte) {
  if (byte >= '0' && byte <= '9') return byte - '0';
  if (byte >= 'a' && byte <= 'f') return byte - 'a' + 10;
  if (byte >= 'A' && byte <= 'F') return byte - 'A' + 10;
  return -1;
}

QueryResult<std::string> decode_parameter(std::string_view encoded) {
  std::string decoded;
  decoded.reserve(encoded.size());
  for (std::size_t i = 0; i < encoded.size(); ++i) {
    if (encoded[i] != '%') {
      decoded.push_back(encoded[i]);
      continue;
    }
    if (encoded.size() - i < 3) {
      return QueryResult<std::string>::failure(invalid(
        "incomplete percent escape in query route"));
    }
    const int high = hex_digit(encoded[i + 1]);
    const int low = hex_digit(encoded[i + 2]);
    if (high < 0 || low < 0) {
      return QueryResult<std::string>::failure(invalid(
        "invalid percent escape in query route"));
    }
    decoded.push_back(static_cast<char>((high << 4) | low));
    i += 2;
  }
  if (!valid_utf8(decoded)) {
    return QueryResult<std::string>::failure(invalid(
      "query route contains invalid UTF-8 or control characters"));
  }
  return QueryResult<std::string>::success(std::move(decoded));
}

bool lowercase_hex(std::string_view value, std::size_t length) {
  if (value.size() != length) return false;
  for (char byte : value) {
    if (!((byte >= '0' && byte <= '9') ||
          (byte >= 'a' && byte <= 'f'))) return false;
  }
  return true;
}

struct ParsedRoute {
  bool windows{false};
  std::string id;
  std::optional<std::int64_t> version;
  std::optional<std::string> snapshot;
};

QueryResult<ParsedRoute> parse(std::string_view route) {
  if (route.size() > kMaxRouteBytes) {
    return QueryResult<ParsedRoute>::failure(exhausted(
      "query route exceeds maximum path length"));
  }
  const auto query_begin = route.find('?');
  if (query_begin == std::string_view::npos ||
      route.find('#') != std::string_view::npos) {
    return QueryResult<ParsedRoute>::failure(invalid(
      "query route requires a query string and no fragment"));
  }
  const auto path = route.substr(0, query_begin);
  if (path != kTypedPath && path != kWindowPath) {
    return QueryResult<ParsedRoute>::failure(invalid(
      "unknown query route"));
  }
  ParsedRoute parsed;
  parsed.windows = path == kWindowPath;
  std::string_view query = route.substr(query_begin + 1);
  if (query.empty()) {
    return QueryResult<ParsedRoute>::failure(invalid(
      "query route requires an id parameter"));
  }
  bool seen_id = false;
  bool seen_precondition = false;
  while (!query.empty()) {
    const auto separator = query.find('&');
    const auto pair = query.substr(0, separator);
    if (pair.empty()) {
      return QueryResult<ParsedRoute>::failure(invalid(
        "query route contains an empty parameter"));
    }
    const auto equal = pair.find('=');
    if (equal == std::string_view::npos ||
        pair.find('=', equal + 1) != std::string_view::npos) {
      return QueryResult<ParsedRoute>::failure(invalid(
        "query route parameter must have one equals sign"));
    }
    const auto key = decode_parameter(pair.substr(0, equal));
    if (!key.ok()) return QueryResult<ParsedRoute>::failure(key.status());
    const auto value = decode_parameter(pair.substr(equal + 1));
    if (!value.ok()) return QueryResult<ParsedRoute>::failure(value.status());
    if (key.value() == "id") {
      if (seen_id) return QueryResult<ParsedRoute>::failure(invalid(
        "duplicate query route id"));
      seen_id = true;
      if (!lowercase_hex(value.value(), kIdBytes * 2)) {
        return QueryResult<ParsedRoute>::failure(invalid(
          "query route id must be a 192-bit lowercase hex token"));
      }
      parsed.id = value.value();
    } else if (!parsed.windows && key.value() == "version") {
      if (seen_precondition) return QueryResult<ParsedRoute>::failure(invalid(
        "duplicate query route version"));
      seen_precondition = true;
      if (value.value().empty()) return QueryResult<ParsedRoute>::failure(invalid(
        "empty query route version"));
      std::int64_t version;
      const char* begin = value.value().data();
      const char* end = begin + value.value().size();
      auto [next, error] = std::from_chars(begin, end, version);
      if (error != std::errc{} || next != end) {
        return QueryResult<ParsedRoute>::failure(invalid(
          "query route version must be an int64"));
      }
      parsed.version = version;
    } else if (parsed.windows && key.value() == "snapshot") {
      if (seen_precondition) return QueryResult<ParsedRoute>::failure(invalid(
        "duplicate query route snapshot"));
      seen_precondition = true;
      if (!lowercase_hex(value.value(), 64)) {
        return QueryResult<ParsedRoute>::failure(invalid(
          "query route snapshot must be a SHA-256 fingerprint"));
      }
      parsed.snapshot = value.value();
    } else {
      return QueryResult<ParsedRoute>::failure(invalid(
        "unknown or incompatible query route parameter"));
    }
    if (separator == std::string_view::npos) break;
    query.remove_prefix(separator + 1);
    if (query.empty()) return QueryResult<ParsedRoute>::failure(invalid(
      "query route has a trailing separator"));
  }
  if (!seen_id) return QueryResult<ParsedRoute>::failure(invalid(
    "query route requires an id parameter"));
  return QueryResult<ParsedRoute>::success(std::move(parsed));
}

QueryResult<std::string> random_id() {
  std::array<unsigned char, kIdBytes> bytes{};
  if (RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) != 1) {
    return QueryResult<std::string>::failure(QueryStatus::failure(
      QueryError::Internal, QueryPhase::Validation,
      "cryptographic query route id generation failed"));
  }
  constexpr char digits[] = "0123456789abcdef";
  std::string id;
  id.reserve(bytes.size() * 2);
  for (const auto byte : bytes) {
    id.push_back(digits[byte >> 4]);
    id.push_back(digits[byte & 0x0f]);
  }
  return QueryResult<std::string>::success(std::move(id));
}

} // namespace

TypedQueryFrontend::TypedQueryFrontend(std::size_t max_registrations,
    std::chrono::milliseconds ttl, ClockFn clock)
  : max_registrations_(max_registrations), ttl_(ttl),
    clock_(std::move(clock)) {
  if (ttl_.count() <= 0 || !clock_)
    throw std::invalid_argument("invalid typed query route TTL or clock");
}

void TypedQueryFrontend::expire_locked(Clock::time_point now) const {
  for (auto it = registrations_.begin(); it != registrations_.end();) {
    if (it->second.expires_at <= now) it = registrations_.erase(it);
    else ++it;
  }
}

QueryResult<std::string> TypedQueryFrontend::register_entry(
    FrontendExecutionRequest entry, std::string_view route_prefix,
    std::string suffix) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto now = clock_();
  expire_locked(now);
  if (registrations_.size() >= max_registrations_) {
    return QueryResult<std::string>::failure(exhausted(
      "query route registration limit exhausted"));
  }
  for (unsigned attempt = 0; attempt < 3; ++attempt) {
    auto id = random_id();
    if (!id.ok()) return id;
    auto [position, inserted] = registrations_.emplace(id.value(),
      Registration{entry, now + ttl_});
    if (inserted) {
      std::string route(route_prefix);
      route += "?id=";
      route += position->first;
      route += suffix;
      return QueryResult<std::string>::success(std::move(route));
    }
  }
  return QueryResult<std::string>::failure(QueryStatus::failure(
    QueryError::Internal, QueryPhase::Validation,
    "could not allocate a unique query route id"));
}

QueryResult<std::string> TypedQueryFrontend::register_typed(
    QueryRequest request, std::shared_ptr<const QueryModule> module,
    std::shared_ptr<const void> declaration_lifetime) {
  auto status = validate_query_request(request);
  if (!status.ok()) return QueryResult<std::string>::failure(std::move(status));
  std::string suffix;
  if (request.sources.versions.kind == VersionSelectionKind::Exact) {
    suffix = "&version=" + std::to_string(*request.sources.versions.exact);
  }
  std::shared_ptr<const QueryRequest> immutable;
  if (declaration_lifetime) {
    struct RetainedDeclaration {
      // Destroy the request's borrowed immutable components before refunding
      // their original reservation. Aliasing makes even a separately retained
      // request pointer preserve this lifetime, including after route expiry.
      std::shared_ptr<const void> lifetime;
      QueryRequest request;
    };
    auto retained = std::make_shared<const RetainedDeclaration>(
      RetainedDeclaration{std::move(declaration_lifetime), std::move(request)});
    immutable = std::shared_ptr<const QueryRequest>(retained, &retained->request);
  } else immutable = std::make_shared<const QueryRequest>(std::move(request));
  return register_entry(TypedQueryExecutionRequest{std::move(immutable), std::move(module)},
    kTypedPath, std::move(suffix));
}

QueryResult<std::string> TypedQueryFrontend::register_prepared(
    std::shared_ptr<const QueryModule> module,
    std::shared_ptr<const PreparedQuery> prepared) {
  if (!module || !prepared)
    return QueryResult<std::string>::failure(invalid("prepared route requires module and plan"));
  const auto& request = prepared->request();
  if (!request.subset || request.sources.paths.size() != 1 ||
      request.sources.paths.front().kind != PathScopeKind::Exact ||
      request.sources.versions.kind != VersionSelectionKind::Exact ||
      !request.sources.versions.exact || prepared->sources().size() != 1)
    return QueryResult<std::string>::failure(invalid("prepared route requires one exact subset source"));
  auto immutable = std::make_shared<const QueryRequest>(request);
  auto suffix = "&version=" + std::to_string(*request.sources.versions.exact);
  return register_entry(TypedQueryExecutionRequest{std::move(immutable),
      std::move(module), std::move(prepared)}, kTypedPath, std::move(suffix));
}

QueryResult<std::string> TypedQueryFrontend::register_windows(
    std::shared_ptr<const TrainingQueryModule> module,
    WindowRequest request) {
  if (!module) return QueryResult<std::string>::failure(invalid(
    "window route requires a prepared training module"));
  const auto& fingerprint = module->snapshot().fingerprint;
  if (!lowercase_hex(fingerprint, 64)) {
    return QueryResult<std::string>::failure(invalid(
      "prepared training snapshot has no SHA-256 fingerprint"));
  }
  auto immutable = std::make_shared<const WindowRequest>(std::move(request));
  return register_entry(WindowExecutionRequest{std::move(module),
      std::move(immutable)}, kWindowPath, "&snapshot=" + fingerprint);
}

QueryResult<FrontendExecutionRequest> TypedQueryFrontend::resolve(
    std::string_view route) const {
  auto resolved = resolve_with_identity(route);
  if (!resolved.ok())
    return QueryResult<FrontendExecutionRequest>::failure(resolved.status());
  return QueryResult<FrontendExecutionRequest>::success(
    std::move(resolved.value().request));
}

QueryResult<ResolvedFrontendRoute> TypedQueryFrontend::resolve_with_identity(
    std::string_view route) const {
  auto parsed = parse(route);
  if (!parsed.ok()) {
    return QueryResult<ResolvedFrontendRoute>::failure(parsed.status());
  }
  std::lock_guard<std::mutex> lock(mutex_);
  const auto now = clock_();
  expire_locked(now);
  const auto position = registrations_.find(parsed.value().id);
  if (position == registrations_.end()) {
    return QueryResult<ResolvedFrontendRoute>::failure(
      QueryStatus::failure(QueryError::SourceNotFound, QueryPhase::Catalog,
        "query route registration was not found"));
  }
  const auto& entry = position->second.request;
  const bool windows = std::holds_alternative<WindowExecutionRequest>(entry);
  if (windows != parsed.value().windows) {
    return QueryResult<ResolvedFrontendRoute>::failure(invalid(
      "query route kind does not match its registration"));
  }
  if (windows) {
    const auto& registered = std::get<WindowExecutionRequest>(entry);
    if (parsed.value().snapshot && *parsed.value().snapshot !=
        registered.module->snapshot().fingerprint) {
      return QueryResult<ResolvedFrontendRoute>::failure(invalid(
        "window route snapshot precondition does not match registration"));
    }
  } else if (parsed.value().version) {
    const auto& selection = std::get<TypedQueryExecutionRequest>(entry)
      .request->sources.versions;
    if (selection.kind != VersionSelectionKind::Exact ||
        selection.exact != parsed.value().version) {
      return QueryResult<ResolvedFrontendRoute>::failure(invalid(
        "typed route version conflicts with registered selection"));
    }
  }
  position->second.expires_at = now + ttl_;
  return QueryResult<ResolvedFrontendRoute>::success(
    ResolvedFrontendRoute{std::move(parsed.value().id), entry});
}

bool TypedQueryFrontend::erase(std::string_view registration_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  expire_locked(clock_());
  return registrations_.erase(std::string(registration_id)) != 0;
}

std::size_t TypedQueryFrontend::size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  expire_locked(clock_());
  return registrations_.size();
}

} // namespace iotdbfs
