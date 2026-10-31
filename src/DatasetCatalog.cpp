#include "iotdbfs/DatasetCatalog.h"
#include <algorithm>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <limits>
#include <locale>
#include <set>
#include <sstream>
#include <stdexcept>

namespace iotdbfs {
namespace {
struct Writer {
  std::vector<std::uint8_t> out;
  void number(std::uint64_t x) {
    for (int i = 0; i < 8; ++i)
      out.push_back((x >> (i * 8)) & 255);
  }
  void text(const std::string &s) {
    number(s.size());
    out.insert(out.end(), s.begin(), s.end());
  }
};
struct Reader {
  const std::vector<std::uint8_t> &in;
  std::size_t pos{0};
  std::uint64_t number() {
    if (in.size() - pos < 8)
      throw std::runtime_error("truncated dataset catalog");
    std::uint64_t x = 0;
    for (int i = 0; i < 8; ++i)
      x |= std::uint64_t(in[pos++]) << (8 * i);
    return x;
  }
  std::string text(std::size_t max = 4096) {
    auto n = number();
    if (n > max || n > in.size() - pos)
      throw std::runtime_error("invalid dataset catalog text length");
    std::string s(in.begin() + pos, in.begin() + pos + n);
    pos += n;
    return s;
  }
  bool flag() {
    auto x = number();
    if (x > 1)
      throw std::runtime_error("invalid dataset catalog flag");
    return x;
  }
  std::size_t count(std::size_t max) {
    auto n = number();
    if (n > max)
      throw std::runtime_error("dataset catalog count exceeds bound");
    return static_cast<std::size_t>(n);
  }
};
bool text_ok(const std::string &s, std::size_t max = 4096) {
  if (s.empty() || s.size() > max || s.find('\0') != std::string::npos)
    return false;
  // Reuse the strict UTF-8 validator of the existing metadata codec.
  return serialize_metadata({{"text", MetadataValue(s)}}, max * 6 + 64).ok();
}
} // namespace
std::string dataset_attribute_type(const DatasetAttributeValue &v) {
  static const char *labels[] = {"string", "int64", "uint64", "float64",
                                 "bool"};
  return labels[v.index()];
}
std::string dataset_attribute_text(const DatasetAttributeValue &v) {
  return std::visit(
      [](const auto &x) -> std::string {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_same_v<T, std::string>)
          return x;
        else if constexpr (std::is_same_v<T, bool>)
          return x ? "true" : "false";
        else {
          std::ostringstream s;
          s.imbue(std::locale::classic());
          if constexpr (std::is_same_v<T, std::uint64_t>)
            s << std::setw(20) << std::setfill('0');
          if constexpr (std::is_same_v<T, double>)
            s << std::setprecision(17);
          s << x;
          return s.str();
        }
      },
      v);
}
MetadataResult<bool> validate_dataset_catalog(const DatasetCatalogArtifact &a) {
  const auto bad = [](std::string s) {
    return MetadataResult<bool>::failure(std::move(s));
  };
  if (a.protocol != "iotdbfs-dataset-catalog/1" || !text_ok(a.format_id, 128) ||
      !text_ok(a.layout_protocol, 128) || a.layout_digest.size() != 64 ||
      a.layout_digest.find_first_not_of("0123456789abcdef") !=
          std::string::npos)
    return bad("invalid dataset catalog identity");
  if (a.records.size() > 65536)
    return bad("dataset catalog record limit exceeded");
  std::set<std::string> ids, paths;
  std::size_t attrs = 0, bytes = 256;
  for (const auto &r : a.records) {
    if (!text_ok(r.object_id, 256) || !text_ok(r.canonical_path) ||
        r.canonical_path.front() != '/' || !ids.insert(r.object_id).second ||
        !paths.insert(r.canonical_path).second)
      return bad("invalid or duplicate dataset catalog object");
    bytes += r.object_id.size() + r.canonical_path.size() + 64;
    if (r.attributes.size() > 256 || (attrs += r.attributes.size()) > 16384)
      return bad("dataset catalog attribute limit exceeded");
    std::set<std::string> names;
    for (const auto &at : r.attributes) {
      if (!text_ok(at.name) || !names.insert(at.name).second ||
          at.shape.size() > 32 || at.reason.size() > 4096 ||
          int(at.state) < 0 ||
          int(at.state) > int(DatasetAttributeState::Invalid))
        return bad("invalid dataset catalog attribute");
      if ((at.state == DatasetAttributeState::Complete) != at.value.has_value())
        return bad("dataset catalog scalar state/value mismatch");
      if (at.value) {
        if (auto v = std::get_if<double>(&*at.value); v && !std::isfinite(*v))
          return bad("dataset catalog non-finite scalar");
        if (auto v = std::get_if<std::string>(&*at.value);
            v &&
            (v->size() > 4096 || v->find('\0') != std::string::npos ||
             !serialize_metadata({{"text", MetadataValue(*v)}}, 32768).ok()))
          return bad("dataset catalog invalid string scalar");
        std::uint64_t n = 1;
        for (auto d : at.shape) {
          if (!d || d > 1 || n > 1)
            return bad("dataset catalog complete attribute is not scalar");
          n *= d;
        }
        bytes += dataset_attribute_text(*at.value).size();
      }
      bytes += at.name.size() + at.reason.size() + at.shape.size() * 8 + 96;
      if (bytes > kMaxDatasetCatalogBytes)
        return bad("dataset catalog byte limit exceeded");
    }
  }
  return MetadataResult<bool>::success(true);
}
MetadataResult<std::vector<std::uint8_t>>
serialize_dataset_catalog(const DatasetCatalogArtifact &a) {
  auto v = validate_dataset_catalog(a);
  if (!v.ok())
    return MetadataResult<std::vector<std::uint8_t>>::failure(v.error());
  Writer w;
  w.text(a.protocol);
  w.text(a.format_id);
  w.text(a.layout_protocol);
  w.text(a.layout_digest);
  w.number(a.directory_complete);
  w.number(a.records.size());
  for (const auto &r : a.records) {
    w.text(r.object_id);
    w.text(r.canonical_path);
    w.number(r.attributes_complete);
    w.number(r.attributes.size());
    for (const auto &at : r.attributes) {
      w.text(at.name);
      w.number(int(at.state));
      w.text(at.reason);
      w.number(at.shape.size());
      for (auto x : at.shape)
        w.number(x);
      w.number(at.value ? at.value->index() + 1 : 0);
      if (at.value)
        std::visit(
            [&](const auto &x) {
              using T = std::decay_t<decltype(x)>;
              if constexpr (std::is_same_v<T, std::string>)
                w.text(x);
              else if constexpr (std::is_same_v<T, double>) {
                std::uint64_t b;
                std::memcpy(&b, &x, 8);
                w.number(b);
              } else
                w.number(static_cast<std::uint64_t>(x));
            },
            *at.value);
    }
  }
  if (w.out.size() > kMaxDatasetCatalogBytes)
    return MetadataResult<std::vector<std::uint8_t>>::failure(
        "dataset catalog byte limit exceeded");
  return MetadataResult<std::vector<std::uint8_t>>::success(std::move(w.out));
}
MetadataResult<DatasetCatalogArtifact>
deserialize_dataset_catalog(const std::vector<std::uint8_t> &b) {
  try {
    if (b.size() > kMaxDatasetCatalogBytes)
      throw std::runtime_error("dataset catalog byte limit exceeded");
    Reader r{b};
    DatasetCatalogArtifact a;
    a.protocol = r.text(128);
    a.format_id = r.text(128);
    a.layout_protocol = r.text(128);
    a.layout_digest = r.text(64);
    a.directory_complete = r.flag();
    auto n = r.count(65536);
    a.records.reserve(n);
    std::size_t attribute_count = 0;
    for (std::size_t i = 0; i < n; ++i) {
      DatasetCatalogRecord d;
      d.object_id = r.text(256);
      d.canonical_path = r.text();
      d.attributes_complete = r.flag();
      auto m = r.count(256);
      if (m > 16384 - attribute_count)
        throw std::runtime_error("dataset catalog attribute limit exceeded");
      attribute_count += m;
      for (std::size_t j = 0; j < m; ++j) {
        DatasetCatalogAttribute at;
        at.name = r.text();
        auto state = r.number();
        if (state > 3)
          throw std::runtime_error("unknown dataset attribute state");
        at.state = DatasetAttributeState(state);
        at.reason = r.text();
        auto shape = r.count(32);
        for (std::size_t k = 0; k < shape; ++k)
          at.shape.push_back(r.number());
        auto t = r.number();
        if (t == 1)
          at.value = r.text();
        else if (t == 2) {
          auto v = r.number();
          std::int64_t x;
          std::memcpy(&x, &v, 8);
          at.value = x;
        } else if (t == 3)
          at.value = r.number();
        else if (t == 4) {
          auto v = r.number();
          double x;
          std::memcpy(&x, &v, 8);
          at.value = x;
        } else if (t == 5)
          at.value = r.flag();
        else if (t != 0)
          throw std::runtime_error("unknown dataset attribute scalar type");
        d.attributes.push_back(std::move(at));
      }
      a.records.push_back(std::move(d));
    }
    if (r.pos != b.size())
      throw std::runtime_error("trailing dataset catalog bytes");
    auto checked = validate_dataset_catalog(a);
    if (!checked.ok())
      throw std::runtime_error(checked.error());
    return MetadataResult<DatasetCatalogArtifact>::success(std::move(a));
  } catch (const std::exception &e) {
    return MetadataResult<DatasetCatalogArtifact>::failure(e.what());
  }
}
} // namespace iotdbfs
