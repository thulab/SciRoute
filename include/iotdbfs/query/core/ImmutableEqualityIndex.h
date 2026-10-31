#pragma once

#include "iotdbfs/query/core/QueryModel.h"
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace iotdbfs {

// A borrowed, allocation-free composite equality key. Its owner (an immutable
// format artifact for build keys, or the query AST for lookup keys) must outlive
// all uses of its text. Format adapters retain and account those owners.
template <std::size_t TextSlots, std::size_t IntegerSlots>
struct EqualityCompositeKey {
  std::array<std::string_view, TextSlots> text{};
  std::array<std::int64_t, IntegerSlots> number{};

  friend bool operator<(const EqualityCompositeKey& a,
                        const EqualityCompositeKey& b) {
    return std::tie(a.text, a.number) < std::tie(b.text, b.number);
  }
};

// Recognize a complete conjunction of unique, non-null typed equalities. Field
// names and their meaning belong to the adapter: text fields precede int64
// fields. Unsupported syntax is a decline, never a successful empty match.
// This helper does not evaluate records, validate format metadata, select
// versions, charge a request, or authorize sources.
template <std::size_t TextSlots, std::size_t IntegerSlots>
std::optional<EqualityCompositeKey<TextSlots, IntegerSlots>>
compile_complete_equality_key(const QueryExpressionPtr& expression,
    const std::array<std::string_view, TextSlots + IntegerSlots>& field_ids) {
  static_assert(TextSlots + IntegerSlots > 0, "an equality key needs fields");
  EqualityCompositeKey<TextSlots, IntegerSlots> result;
  std::array<bool, TextSlots + IntegerSlots> seen{};
  std::size_t nodes = 0;
  const auto visit = [&](const auto& recurse, const QueryExpressionPtr& e,
                         std::size_t depth) -> bool {
    if (!e || ++nodes > 128 || depth > 32) return false;
    return std::visit([&](const auto& node) -> bool {
      using T = std::decay_t<decltype(node)>;
      if constexpr (std::is_same_v<T, LogicalExpression>) {
        if (node.operation != LogicalOperator::And || node.operands.empty())
          return false;
        for (const auto& child : node.operands)
          if (!recurse(recurse, child, depth + 1)) return false;
        return true;
      } else if constexpr (std::is_same_v<T, ComparisonExpression>) {
        if (node.operation != ComparisonOperator::Equal || node.value.is_null())
          return false;
        std::size_t slot = 0;
        for (; slot < field_ids.size(); ++slot)
          if (std::string_view(node.field.field_id) == field_ids[slot]) break;
        if (slot == field_ids.size() || seen[slot]) return false;
        seen[slot] = true;
        if (slot < TextSlots) {
          if (node.value.type() != LogicalTypeId::Utf8) return false;
          result.text[slot] = std::get<std::string>(node.value.value());
        } else {
          if (node.value.type() != LogicalTypeId::Int64) return false;
          result.number[slot - TextSlots] =
            std::get<std::int64_t>(node.value.value());
        }
        return true;
      } else return false;
    }, e->node);
  };
  if (!visit(visit, expression, 0) ||
      std::any_of(seen.begin(), seen.end(), [](bool value) {return !value;}))
    return std::nullopt;
  return result;
}

// Immutable ordered key-to-locator storage. Adapters supply their observable
// tie order (e.g. source/object order or source/message order). Locators and
// keys are deliberately independent of any file format or catalog type.
template <typename Key, typename Locator>
class OrderedEqualityIndex {
public:
  struct Entry {
    Key key;
    Locator locator;
  };
  using const_iterator = typename std::vector<Entry>::const_iterator;

  OrderedEqualityIndex() = default;
  template <typename LocatorLess>
  OrderedEqualityIndex(std::vector<Entry> entries, LocatorLess locator_less)
      : entries_(std::move(entries)) {
    std::sort(entries_.begin(), entries_.end(), [&](const Entry& a, const Entry& b) {
      if (a.key < b.key) return true;
      if (b.key < a.key) return false;
      return locator_less(a.locator, b.locator);
    });
  }

  std::pair<const_iterator, const_iterator> equal_range(const Key& key) const {
    const auto begin = std::lower_bound(entries_.begin(), entries_.end(), key,
      [](const Entry& row, const Key& value) {return row.key < value;});
    const auto end = std::upper_bound(begin, entries_.end(), key,
      [](const Key& value, const Entry& row) {return value < row.key;});
    return {begin, end};
  }

  const std::vector<Entry>& entries() const {return entries_;}
  std::size_t size() const {return entries_.size();}
  std::size_t capacity() const {return entries_.capacity();}
  bool empty() const {return entries_.empty();}

private:
  std::vector<Entry> entries_;
};
} // namespace iotdbfs
