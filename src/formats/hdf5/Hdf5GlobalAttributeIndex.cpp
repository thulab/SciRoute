#include "iotdbfs/formats/hdf5/Hdf5GlobalAttributeIndex.h"
#include "iotdbfs/query/core/ImmutableEqualityIndex.h"
#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <string_view>
#include <tuple>

namespace iotdbfs {
namespace {
constexpr const char* names[] = {"shortName", "typeOfLevel", "level", "step", "validityDate", "validityTime"};
constexpr std::array<std::string_view, 6> fields = {"hdf5.attr:shortName", "hdf5.attr:typeOfLevel",
  "hdf5.attr:level", "hdf5.attr:step", "hdf5.attr:validityDate", "hdf5.attr:validityTime"};
using Tuple = EqualityCompositeKey<2, 4>;
using Index = OrderedEqualityIndex<Tuple, Hdf5GlobalAttributeIndex::Match>;
using Row = Index::Entry;
std::optional<Tuple> extract(const QueryExpressionPtr& expression) {
  return compile_complete_equality_key<2, 4>(expression, fields);
}
bool bind(const DatasetCatalogRecord& record, Tuple& tuple) {
  if (!record.attributes_complete) return false;
  unsigned seen = 0;
  for (const auto& attribute : record.attributes) {
    std::size_t slot = 0;
    for (; slot < 6; ++slot) if (attribute.name == names[slot]) break;
    if (slot == 6) continue;
    if ((seen & (1U << slot)) || attribute.state != DatasetAttributeState::Complete || !attribute.value ||
        attribute.shape.size() > 32 || std::any_of(attribute.shape.begin(), attribute.shape.end(), [](auto n) {return n != 1;})) return false;
    seen |= 1U << slot;
    if (slot < 2) {
      const auto* value = std::get_if<std::string>(&*attribute.value);
      if (!value || value->find('\0') != std::string::npos || value->size() > 4096) return false;
      tuple.text[slot] = *value;
    } else {
      const auto* value = std::get_if<std::int64_t>(&*attribute.value);
      if (!value) return false;
      tuple.number[slot - 2] = *value;
    }
  }
  return seen == 63;
}
struct Charge {
  std::uint64_t limit, used{0};
  bool add(std::uint64_t bytes) {
    if (bytes > limit - used) return false;
    used += bytes; return true;
  }
  bool array(std::size_t count, std::size_t size) {
    return count <= (limit - used) / size && add(static_cast<std::uint64_t>(count) * size);
  }
  bool text(const std::string& value) { return add(value.capacity()) && add(32); }
};
bool catalog_charge(Charge& charge, const DatasetCatalogArtifact& artifact) {
  if (!charge.add(sizeof(artifact) + 256) || !charge.text(artifact.protocol) || !charge.text(artifact.format_id) ||
      !charge.text(artifact.layout_protocol) || !charge.text(artifact.layout_digest) ||
      !charge.array(artifact.records.capacity(), sizeof(DatasetCatalogRecord))) return false;
  for (const auto& record : artifact.records) {
    if (!charge.add(128) || !charge.text(record.object_id) || !charge.text(record.canonical_path) ||
        !charge.array(record.attributes.capacity(), sizeof(DatasetCatalogAttribute))) return false;
    for (const auto& attribute : record.attributes) {
      if (!charge.add(128) || !charge.text(attribute.name) || !charge.text(attribute.reason) ||
          !charge.array(attribute.shape.capacity(), sizeof(std::uint64_t))) return false;
      if (attribute.value) if (const auto* text = std::get_if<std::string>(&*attribute.value))
        if (!charge.text(*text)) return false;
    }
  }
  return true;
}
} // namespace
struct Hdf5GlobalAttributeIndex::Impl {
  std::vector<std::shared_ptr<const DatasetCatalogArtifact>> catalogs;
  std::vector<std::size_t> counts;
  Index index;
  std::uint64_t bytes{0};
};
std::shared_ptr<const Hdf5GlobalAttributeIndex> Hdf5GlobalAttributeIndex::build(
    const std::vector<std::shared_ptr<const DatasetCatalogArtifact>>& catalogs, std::uint64_t maximum,
    std::optional<std::string> dataset_path) {
  try {
    Charge initial{maximum};
    if (!initial.add(sizeof(Hdf5GlobalAttributeIndex) + sizeof(Impl) + 4096) ||
        !initial.array(catalogs.size(), sizeof(catalogs.front()) + sizeof(std::size_t))) return {};
    std::size_t total = 0;
    for (const auto& catalog : catalogs) {
      if (!catalog || catalog->format_id != "hdf5" || !catalog->directory_complete ||
          catalog->protocol != "iotdbfs-dataset-catalog/1" || !catalog_charge(initial, *catalog)) return {};
      for (const auto& row : catalog->records) if (!dataset_path || row.canonical_path == *dataset_path) {
        if (total == std::numeric_limits<std::size_t>::max() || !initial.add(sizeof(Row))) return {};
        ++total;
      }
    }
    auto impl = std::make_shared<Impl>(); impl->catalogs = catalogs; impl->counts.resize(catalogs.size());
    std::vector<Row> rows; rows.reserve(total);
    for (std::size_t source = 0; source < catalogs.size(); ++source) {
      const auto& records = catalogs[source]->records;
      for (std::size_t record = 0; record < records.size(); ++record) {
        if (dataset_path && records[record].canonical_path != *dataset_path) continue;
        Row row; row.locator = {source, record};
        if (!bind(records[record], row.key)) return {};
        rows.push_back(std::move(row)); ++impl->counts[source];
      }
    }
    impl->index = Index(std::move(rows), [&](const auto& a, const auto& b) {
      if (a.source_ordinal != b.source_ordinal) return a.source_ordinal < b.source_ordinal;
      const auto& records = catalogs[a.source_ordinal]->records;
      return std::tie(records[a.record_ordinal].object_id, a.record_ordinal) <
             std::tie(records[b.record_ordinal].object_id, b.record_ordinal);
    });
    Charge actual{maximum};
    if (!actual.add(sizeof(Hdf5GlobalAttributeIndex) + sizeof(Impl) + 4096) ||
        !actual.array(impl->catalogs.capacity(), sizeof(impl->catalogs.front())) ||
        !actual.array(impl->counts.capacity(), sizeof(std::size_t)) || !actual.array(impl->index.capacity(), sizeof(Row))) return {};
    for (const auto& catalog : catalogs) if (!catalog_charge(actual, *catalog)) return {};
    impl->bytes = actual.used;
    return std::shared_ptr<const Hdf5GlobalAttributeIndex>(new Hdf5GlobalAttributeIndex(std::move(impl)));
  } catch (const std::bad_alloc&) { return {}; }
}
bool Hdf5GlobalAttributeIndex::supports(const QueryExpressionPtr& predicate) { return bool(extract(predicate)); }
std::optional<std::vector<Hdf5GlobalAttributeIndex::Match>> Hdf5GlobalAttributeIndex::lookup(const QueryExpressionPtr& predicate) const {
  const auto key = extract(predicate); if (!key) return std::nullopt;
  auto [begin, end] = impl_->index.equal_range(*key);
  std::vector<Match> result; result.reserve(static_cast<std::size_t>(end - begin));
  for (; begin != end; ++begin) result.push_back(begin->locator);
  return result;
}
std::uint64_t Hdf5GlobalAttributeIndex::estimated_bytes() const { return impl_->bytes; }
const std::vector<std::shared_ptr<const DatasetCatalogArtifact>>& Hdf5GlobalAttributeIndex::catalogs() const { return impl_->catalogs; }
const std::vector<std::size_t>& Hdf5GlobalAttributeIndex::candidate_counts() const { return impl_->counts; }
} // namespace iotdbfs
