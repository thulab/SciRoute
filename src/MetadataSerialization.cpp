#include "iotdbfs/MetadataSerialization.h"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <iomanip>
#include <limits>
#include <locale>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <variant>

namespace iotdbfs {
namespace {

bool valid_utf8(const std::string& text) {
  for (std::size_t i = 0; i < text.size();) {
    const unsigned char c = static_cast<unsigned char>(text[i]);
    std::size_t length = 0;
    std::uint32_t code = 0;
    if (c <= 0x7f) { ++i; continue; }
    if (c >= 0xc2 && c <= 0xdf) { length = 2; code = c & 0x1f; }
    else if (c >= 0xe0 && c <= 0xef) { length = 3; code = c & 0x0f; }
    else if (c >= 0xf0 && c <= 0xf4) { length = 4; code = c & 0x07; }
    else return false;
    if (i + length > text.size()) return false;
    for (std::size_t j = 1; j < length; ++j) {
      const unsigned char continuation = static_cast<unsigned char>(text[i + j]);
      if ((continuation & 0xc0) != 0x80) return false;
      code = (code << 6) | (continuation & 0x3f);
    }
    if ((length == 3 && code < 0x800) || (length == 4 && code < 0x10000) ||
        code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff)) return false;
    ++i;
    for (std::size_t j = 1; j < length; ++j) ++i;
  }
  return true;
}

std::string json_string(const std::string& text) {
  if (!valid_utf8(text))
    throw std::invalid_argument("metadata contains invalid UTF-8");
  std::ostringstream out;
  out << '"';
  for (const unsigned char c : text) {
    switch (c) {
      case '"': out << "\\\""; break;
      case '\\': out << "\\\\"; break;
      case '\b': out << "\\b"; break;
      case '\f': out << "\\f"; break;
      case '\n': out << "\\n"; break;
      case '\r': out << "\\r"; break;
      case '\t': out << "\\t"; break;
      default:
        if (c < 0x20) {
          out << "\\u" << std::hex << std::setw(4) << std::setfill('0')
              << static_cast<int>(c) << std::dec;
        } else {
          out << static_cast<char>(c);
        }
    }
  }
  out << '"';
  return out.str();
}

class MetadataParser {
public:
  explicit MetadataParser(const std::string& input) : input_(input) {}

  MetadataMap parse() {
    MetadataMap result;
    whitespace();
    expect('{');
    if (!take('}')) {
      do {
        if (result.size() == 128) fail("too many metadata fields");
        auto id = string(64);
        expect(':');
        auto value = field();
        if (!result.emplace(std::move(id), std::move(value)).second)
          fail("duplicate metadata id");
      } while (take(','));
      expect('}');
    }
    whitespace();
    if (position_ != input_.size()) fail("trailing JSON content");
    return result;
  }

private:
  [[noreturn]] void fail(const char* message) const {
    throw std::runtime_error(message);
  }

  void whitespace() {
    while (position_ < input_.size() &&
      (input_[position_] == ' ' || input_[position_] == '\t' ||
       input_[position_] == '\n' || input_[position_] == '\r')) ++position_;
  }

  bool take(char expected) {
    whitespace();
    if (position_ == input_.size() || input_[position_] != expected) return false;
    ++position_;
    return true;
  }

  void expect(char expected) {
    if (!take(expected)) fail("unexpected JSON token");
  }

  std::uint32_t hex_code_unit() {
    std::uint32_t unit = 0;
    for (int digit_index = 0; digit_index < 4; ++digit_index) {
      if (position_ == input_.size()) fail("truncated JSON Unicode escape");
      const unsigned char c = input_[position_++];
      unit <<= 4;
      if (c >= '0' && c <= '9') unit += c - '0';
      else if (c >= 'a' && c <= 'f') unit += c - 'a' + 10;
      else if (c >= 'A' && c <= 'F') unit += c - 'A' + 10;
      else fail("invalid JSON Unicode escape");
    }
    return unit;
  }

  void unicode_escape(std::string& result) {
    auto code = hex_code_unit();
    if (code >= 0xd800 && code <= 0xdbff) {
      if (input_.compare(position_, 2, "\\u") != 0)
        fail("missing JSON low surrogate");
      position_ += 2;
      const auto low = hex_code_unit();
      if (low < 0xdc00 || low > 0xdfff) fail("invalid JSON low surrogate");
      code = 0x10000 + ((code - 0xd800) << 10) + low - 0xdc00;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      fail("unpaired JSON low surrogate");
    }
    if (code <= 0x7f) result.push_back(static_cast<char>(code));
    else if (code <= 0x7ff) {
      result.push_back(static_cast<char>(0xc0 | (code >> 6)));
      result.push_back(static_cast<char>(0x80 | (code & 0x3f)));
    } else if (code <= 0xffff) {
      result.push_back(static_cast<char>(0xe0 | (code >> 12)));
      result.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3f)));
      result.push_back(static_cast<char>(0x80 | (code & 0x3f)));
    } else {
      result.push_back(static_cast<char>(0xf0 | (code >> 18)));
      result.push_back(static_cast<char>(0x80 | ((code >> 12) & 0x3f)));
      result.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3f)));
      result.push_back(static_cast<char>(0x80 | (code & 0x3f)));
    }
  }

  std::string string(std::size_t limit = 4096) {
    expect('"');
    std::string result;
    while (position_ < input_.size()) {
      const unsigned char c = input_[position_++];
      if (c == '"') {
        if (!valid_utf8(result)) fail("invalid UTF-8 string");
        return result;
      }
      if (c < 0x20) fail("invalid JSON string");
      if (c == '\\') {
        if (position_ == input_.size()) fail("truncated JSON escape");
        switch (input_[position_++]) {
        case '"': result.push_back('"'); break;
        case '\\': result.push_back('\\'); break;
        case '/': result.push_back('/'); break;
        case 'b': result.push_back('\b'); break;
        case 'f': result.push_back('\f'); break;
        case 'n': result.push_back('\n'); break;
        case 'r': result.push_back('\r'); break;
        case 't': result.push_back('\t'); break;
        case 'u': unicode_escape(result); break;
        default: fail("invalid JSON escape");
        }
      } else result.push_back(static_cast<char>(c));
      if (result.size() > limit) fail("metadata string exceeds size cap");
    }
    fail("unterminated JSON string");
  }

  MetadataValue field() {
    expect('{');
    std::string type;
    ParsedValue value;
    bool have_type = false;
    bool have_value = false;
    do {
      const auto property = string();
      expect(':');
      if (property == "type" && !have_type) {
        type = string();
        have_type = true;
      } else if (property == "value" && !have_value) {
        value = parsed_value();
        have_value = true;
      } else fail("unknown or duplicate metadata property");
    } while (take(','));
    expect('}');
    if (!have_type || !have_value) fail("missing metadata property");
    if (type == "string" && !value.array &&
        value.scalar.kind == ScalarKind::String)
      return MetadataValue(std::move(value.scalar.text));
    if (type == "bool" && !value.array &&
        value.scalar.kind == ScalarKind::Bool)
      return MetadataValue(value.scalar.text == "true");
    if (type == "int64" && !value.array &&
        value.scalar.kind == ScalarKind::Number) {
      std::int64_t number = 0;
      const auto converted = std::from_chars(value.scalar.text.data(),
        value.scalar.text.data() + value.scalar.text.size(), number);
      if (converted.ec != std::errc() ||
          converted.ptr != value.scalar.text.data() + value.scalar.text.size())
        fail("invalid metadata int64");
      return MetadataValue(number);
    }
    if (type == "double" && !value.array &&
        value.scalar.kind == ScalarKind::Number) {
      double number = 0;
      std::istringstream input(value.scalar.text);
      input.imbue(std::locale::classic());
      input >> std::noskipws >> number;
      if (input.fail() || !input.eof() || !std::isfinite(number))
        fail("invalid metadata double");
      return MetadataValue(number);
    }
    if (type == "string_set" && value.array) {
      MetadataStringSet result;
      for (auto& member : value.members) {
        if (member.kind != ScalarKind::String)
          fail("invalid metadata string set member");
        result.insert(std::move(member.text));
      }
      return MetadataValue(std::move(result));
    }
    if (type == "int64_set" && value.array) {
      MetadataInt64Set result;
      for (const auto& member : value.members) {
        if (member.kind != ScalarKind::Number)
          fail("invalid metadata int64 set member");
        std::int64_t number = 0;
        const auto converted = std::from_chars(member.text.data(),
          member.text.data() + member.text.size(), number);
        if (converted.ec != std::errc() ||
            converted.ptr != member.text.data() + member.text.size())
          fail("invalid metadata int64 set member");
        result.insert(number);
      }
      return MetadataValue(std::move(result));
    }
    if (type == "double_set" && value.array) {
      MetadataDoubleSet result;
      for (const auto& member : value.members) {
        if (member.kind != ScalarKind::Number)
          fail("invalid metadata double set member");
        double number = 0;
        std::istringstream input(member.text);
        input.imbue(std::locale::classic());
        input >> std::noskipws >> number;
        if (input.fail() || !input.eof() || !std::isfinite(number))
          fail("invalid metadata double set member");
        result.insert(number);
      }
      return MetadataValue(std::move(result));
    }
    if (type == "bool_set" && value.array) {
      MetadataBoolSet result;
      for (const auto& member : value.members) {
        if (member.kind != ScalarKind::Bool)
          fail("invalid metadata bool set member");
        result.insert(member.text == "true");
      }
      return MetadataValue(std::move(result));
    }
    fail("invalid metadata type or value");
  }

  enum class ScalarKind { String, Number, Bool };
  struct Scalar {
    ScalarKind kind{ ScalarKind::String };
    std::string text;
  };

  struct ParsedValue {
    bool array{ false };
    Scalar scalar;
    std::vector<Scalar> members;
  };

  ParsedValue parsed_value() {
    whitespace();
    if (position_ == input_.size()) fail("missing metadata value");
    if (input_[position_] != '[') return {false, scalar(), {}};
    ++position_;
    ParsedValue result;
    result.array = true;
    if (take(']')) return result;
    do {
      if (result.members.size() == 4096)
        fail("too many metadata set members");
      result.members.push_back(scalar());
    } while (take(','));
    expect(']');
    return result;
  }

  bool digit() const {
    return position_ < input_.size() && input_[position_] >= '0' &&
      input_[position_] <= '9';
  }

  Scalar scalar() {
    whitespace();
    if (position_ == input_.size()) fail("missing metadata value");
    if (input_[position_] == '"') return { ScalarKind::String, string() };
    for (const std::string token : { "true", "false" }) {
      if (input_.compare(position_, token.size(), token) == 0) {
        position_ += token.size();
        return { ScalarKind::Bool, token };
      }
    }
    const auto begin = position_;
    if (input_[position_] == '-') ++position_;
    if (!digit()) fail("invalid JSON scalar");
    if (input_[position_] == '0') ++position_;
    else while (digit()) ++position_;
    if (position_ < input_.size() && input_[position_] == '.') {
      ++position_;
      if (!digit()) fail("invalid JSON fraction");
      while (digit()) ++position_;
    }
    if (position_ < input_.size() &&
      (input_[position_] == 'e' || input_[position_] == 'E')) {
      ++position_;
      if (position_ < input_.size() &&
        (input_[position_] == '+' || input_[position_] == '-')) ++position_;
      if (!digit()) fail("invalid JSON exponent");
      while (digit()) ++position_;
    }
    return { ScalarKind::Number, input_.substr(begin, position_ - begin) };
  }

  const std::string& input_;
  std::size_t position_{ 0 };
};

} // namespace

MetadataResult<MetadataMap> deserialize_metadata(const std::string& json,
  std::size_t max_bytes) {
  if (json.size() > std::min<std::size_t>(65536, max_bytes))
    return MetadataResult<MetadataMap>::failure("metadata exceeds size cap");
  if (json.empty()) return MetadataResult<MetadataMap>::success(MetadataMap{});
  try {
    return MetadataResult<MetadataMap>::success(MetadataParser(json).parse());
  } catch (const std::exception& error) {
    return MetadataResult<MetadataMap>::failure(error.what());
  }
}

bool valid_metadata_utf8(const std::string& text) {
  return valid_utf8(text);
}

std::string metadata_value_text(const MetadataValue& value) {
  std::ostringstream output;
  output.imbue(std::locale::classic());
  const auto& raw = value.value();
  if (std::holds_alternative<std::string>(raw)) {
    const auto& text = std::get<std::string>(raw);
    if (!valid_utf8(text)) throw std::invalid_argument("metadata contains invalid UTF-8");
    return text;
  }
  if (std::holds_alternative<std::int64_t>(raw)) {
    output << std::get<std::int64_t>(raw);
  } else if (std::holds_alternative<double>(raw)) {
    const auto number = std::get<double>(raw);
    if (!std::isfinite(number)) throw std::invalid_argument("metadata contains non-finite number");
    output << std::setprecision(std::numeric_limits<double>::max_digits10) << number;
  } else if (std::holds_alternative<bool>(raw)) {
    output << (std::get<bool>(raw) ? "true" : "false");
  } else if (std::holds_alternative<MetadataStringSet>(raw)) {
    output << '[';
    bool first = true;
    for (const auto& member : std::get<MetadataStringSet>(raw)) {
      if (!first) output << ',';
      first = false;
      output << json_string(member);
    }
    output << ']';
  } else if (std::holds_alternative<MetadataInt64Set>(raw)) {
    output << '[';
    bool first = true;
    for (const auto member : std::get<MetadataInt64Set>(raw)) {
      if (!first) output << ',';
      first = false;
      output << member;
    }
    output << ']';
  } else if (std::holds_alternative<MetadataDoubleSet>(raw)) {
    output << '[';
    bool first = true;
    for (const auto member : std::get<MetadataDoubleSet>(raw)) {
      if (!std::isfinite(member))
        throw std::invalid_argument("metadata contains non-finite number");
      if (!first) output << ',';
      first = false;
      output << std::setprecision(std::numeric_limits<double>::max_digits10)
             << member;
    }
    output << ']';
  } else {
    output << '[';
    bool first = true;
    for (const auto member : std::get<MetadataBoolSet>(raw)) {
      if (!first) output << ',';
      first = false;
      output << (member ? "true" : "false");
    }
    output << ']';
  }
  return output.str();
}

} // namespace iotdbfs
