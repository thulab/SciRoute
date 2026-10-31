#include "iotdbfs/query/core/DatasetCatalogQuery.h"
#include <algorithm>
#include <array>
#include <cmath>
#include <map>
#include <set>
#include <sstream>
#include <type_traits>

namespace iotdbfs {
namespace {
QueryStatus error(QueryError c, std::string text) {
  return QueryStatus::failure(c, QueryPhase::Catalog, std::move(text));
}
using Hints = std::map<std::string, std::optional<LogicalTypeId>>;
QueryStatus collect(const QueryExpressionPtr &e, Hints &hints,
                    std::size_t &nodes, std::size_t depth,
                    const std::string &prefix) {
  if (!e)
    return error(QueryError::InvalidQuery, "null dataset predicate operand");
  if (++nodes > 128 || depth > 32)
    return error(QueryError::ResourceExhausted,
                 "dataset predicate complexity limit exceeded");
  return std::visit(
      [&](const auto &n) -> QueryStatus {
        using T = std::decay_t<decltype(n)>;
        if constexpr (std::is_same_v<T, LogicalExpression>) {
          if (n.operands.empty() ||
              (n.operation == LogicalOperator::Not && n.operands.size() != 1))
            return error(QueryError::InvalidQuery,
                         "invalid dataset logical operand count");
          for (const auto &p : n.operands) {
            auto s = collect(p, hints, nodes, depth + 1, prefix);
            if (!s.ok())
              return s;
          }
          return QueryStatus::success();
        } else {
          auto name = dataset_attribute_name(n.field, prefix);
          if (!name.ok())
            return name.status();
          std::optional<LogicalTypeId> hint;
          if constexpr (std::is_same_v<T, ComparisonExpression>)
            hint = n.value.type();
          if constexpr (std::is_same_v<T, InExpression>) {
            if (n.values.empty() || n.values.size() > 1024)
              return error(QueryError::InvalidQuery,
                           "dataset IN requires 1..1024 literals");
            hint = n.values.front().type();
          }
          auto &prior = hints[n.field.field_id];
          if (hint) {
            if (prior && *prior != *hint)
              return error(QueryError::TypeMismatch,
                           "inconsistent dataset attribute literal types");
            prior = hint;
          }
          if (hints.size() > 32)
            return error(QueryError::ResourceExhausted,
                         "dataset predicate attribute limit exceeded");
          return QueryStatus::success();
        }
      },
      e->node);
}
LogicalValue logical(const DatasetAttributeValue &v) {
  return std::visit(
      [](const auto &x) -> LogicalValue {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_same_v<T, std::string>)
          return LogicalValue::utf8(x);
        else if constexpr (std::is_same_v<T, std::int64_t>)
          return LogicalValue::int64(x);
        else if constexpr (std::is_same_v<T, std::uint64_t>)
          return LogicalValue::uint64(x);
        else if constexpr (std::is_same_v<T, double>)
          return LogicalValue::float64(x);
        else
          return LogicalValue::boolean(x);
      },
      v);
}
QueryResult<std::map<std::string, LogicalValue>>
values(const DatasetCatalogRecord &r, const Hints &hints,
       const std::string &prefix) {
  std::map<std::string, LogicalValue> out;
  for (const auto &h : hints) {
    auto name = dataset_attribute_name({h.first}, prefix);
    auto at =
        std::find_if(r.attributes.begin(), r.attributes.end(),
                     [&](const auto &a) { return a.name == name.value(); });
    if (at == r.attributes.end()) {
      if (!r.attributes_complete)
        return QueryResult<std::map<std::string, LogicalValue>>::failure(
            error(QueryError::LayoutUnavailable,
                  "dataset attribute directory incomplete: " + name.value()));
      out.emplace(h.first,
                  LogicalValue::null(h.second.value_or(LogicalTypeId::Binary)));
    } else {
      if (at->state != DatasetAttributeState::Complete || !at->value)
        return QueryResult<std::map<std::string, LogicalValue>>::failure(error(
            QueryError::UnsupportedCapability,
            "dataset attribute is not an available scalar: " + name.value()));
      out.emplace(h.first, logical(*at->value));
    }
  }
  return QueryResult<std::map<std::string, LogicalValue>>::success(
      std::move(out));
}
enum class Truth { False, True, Unknown };
int compare(const LogicalValue &a, const LogicalValue &b) {
  return std::visit(
      [&](const auto &x) -> int {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_same_v<T, std::monostate> ||
                      std::is_same_v<T, TimestampNs>)
          return 0;
        else {
          const auto &y = std::get<T>(b.value());
          return x < y ? -1 : (x > y ? 1 : 0);
        }
      },
      a.value());
}
Truth eval(const QueryExpressionPtr &e,
           const std::map<std::string, LogicalValue> &vs) {
  return std::visit(
      [&](const auto &n) -> Truth {
        using T = std::decay_t<decltype(n)>;
        if constexpr (std::is_same_v<T, LogicalExpression>) {
          if (n.operation == LogicalOperator::Not) {
            auto t = eval(n.operands[0], vs);
            return t == Truth::Unknown
                       ? t
                       : (t == Truth::True ? Truth::False : Truth::True);
          }
          bool unknown = false;
          for (const auto &p : n.operands) {
            auto t = eval(p, vs);
            unknown |= t == Truth::Unknown;
            if (n.operation == LogicalOperator::And && t == Truth::False)
              return Truth::False;
            if (n.operation == LogicalOperator::Or && t == Truth::True)
              return Truth::True;
          }
          return unknown ? Truth::Unknown
                         : (n.operation == LogicalOperator::And ? Truth::True
                                                                : Truth::False);
        } else {
          const auto &v = vs.at(n.field.field_id);
          if constexpr (std::is_same_v<T, NullExpression>)
            return (v.is_null() != n.negated) ? Truth::True : Truth::False;
          else {
            if (v.is_null())
              return Truth::Unknown;
            if constexpr (std::is_same_v<T, ComparisonExpression>) {
              int c = compare(v, n.value);
              bool yes = false;
              switch (n.operation) {
              case ComparisonOperator::Equal:
                yes = c == 0;
                break;
              case ComparisonOperator::NotEqual:
                yes = c != 0;
                break;
              case ComparisonOperator::Less:
                yes = c < 0;
                break;
              case ComparisonOperator::LessEqual:
                yes = c <= 0;
                break;
              case ComparisonOperator::Greater:
                yes = c > 0;
                break;
              case ComparisonOperator::GreaterEqual:
                yes = c >= 0;
                break;
              }
              return yes ? Truth::True : Truth::False;
            } else {
              bool unknown = false, found = false;
              for (const auto &x : n.values) {
                if (x.is_null())
                  unknown = true;
                else if (compare(v, x) == 0)
                  found = true;
              }
              if (!found && unknown)
                return Truth::Unknown;
              return (found != n.negated) ? Truth::True : Truth::False;
            }
          }
        }
      },
      e->node);
}
std::string quote(const std::string &s) {
  std::string r = "'";
  for (char c : s) {
    r += c;
    if (c == '\'')
      r += '\'';
  }
  return r + "'";
}
std::optional<DatasetAttributeValue> scalar(const LogicalValue &v) {
  if (v.is_null())
    return std::nullopt;
  return std::visit(
      [](const auto &x) -> std::optional<DatasetAttributeValue> {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::int64_t> ||
                      std::is_same_v<T, std::uint64_t> ||
                      std::is_same_v<T, double> || std::is_same_v<T, bool>)
          return DatasetAttributeValue(x);
        else
          return std::nullopt;
      },
      v.value());
}
} // namespace
QueryResult<std::string> dataset_attribute_name(const FieldRef &f,
                                                const std::string &prefix) {
  if (f.field_id.compare(0, prefix.size(), prefix) != 0)
    return QueryResult<std::string>::failure(
        error(QueryError::UnknownField,
              "dataset predicate requires attribute field: " + prefix));
  auto hex = [](char c) -> int {
    if (c >= '0' && c <= '9')
      return c - '0';
    if (c >= 'A' && c <= 'F')
      return c - 'A' + 10;
    return -1;
  };
  std::string name;
  for (std::size_t i = prefix.size(); i < f.field_id.size(); ++i) {
    unsigned char c = f.field_id[i];
    if (c == '%') {
      if (i + 2 >= f.field_id.size() || hex(f.field_id[i + 1]) < 0 ||
          hex(f.field_id[i + 2]) < 0)
        return QueryResult<std::string>::failure(
            error(QueryError::InvalidQuery, "invalid attribute field escape"));
      c = (hex(f.field_id[i + 1]) << 4) | hex(f.field_id[i + 2]);
      i += 2;
      if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
          (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' ||
          c == '~')
        return QueryResult<std::string>::failure(error(
            QueryError::InvalidQuery, "noncanonical attribute field escape"));
    } else if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                 (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' ||
                 c == '~'))
      return QueryResult<std::string>::failure(
          error(QueryError::InvalidQuery,
                "attribute field requires canonical percent encoding"));
    name += char(c);
  }
  if (name.empty() || name.size() > 4096 ||
      name.find('\0') != std::string::npos ||
      !serialize_metadata({{"a", MetadataValue(name)}}, 32768).ok())
    return QueryResult<std::string>::failure(
        error(QueryError::InvalidQuery, "invalid attribute name"));
  return QueryResult<std::string>::success(std::move(name));
}
QueryResult<LogicalSchema>
build_dataset_attribute_schema(const DatasetCatalogRecord &r,
                               const QueryExpressionPtr &e,
                               const std::string &prefix) {
  Hints hints;
  std::size_t nodes = 0;
  if (e) {
    auto s = collect(e, hints, nodes, 0, prefix);
    if (!s.ok())
      return QueryResult<LogicalSchema>::failure(s);
  }
  auto vs = values(r, hints, prefix);
  if (!vs.ok())
    return QueryResult<LogicalSchema>::failure(vs.status());
  std::vector<LogicalField> fields;
  for (const auto &v : vs.value())
    fields.push_back({v.first,
                      v.first,
                      {v.second.type(), true, std::nullopt},
                      FieldRole::Attribute,
                      std::nullopt});
  auto schema = LogicalSchema::create(std::move(fields));
  if (!schema.ok())
    return schema;
  if (e) {
    auto s = validate_expression(e, schema.value());
    if (!s.ok())
      return QueryResult<LogicalSchema>::failure(s);
  }
  return schema;
}
QueryResult<bool>
evaluate_dataset_attribute_predicate(const DatasetCatalogRecord &r,
                                     const QueryExpressionPtr &e,
                                     const std::string &prefix) {
  if (!e)
    return QueryResult<bool>::success(true);
  auto schema = build_dataset_attribute_schema(r, e, prefix);
  if (!schema.ok())
    return QueryResult<bool>::failure(schema.status());
  Hints hints;
  std::size_t nodes = 0;
  auto s = collect(e, hints, nodes, 0, prefix);
  if (!s.ok())
    return QueryResult<bool>::failure(s);
  auto vs = values(r, hints, prefix);
  if (!vs.ok())
    return QueryResult<bool>::failure(vs.status());
  return QueryResult<bool>::success(eval(e, vs.value()) == Truth::True);
}
struct DatasetAttributePredicate::Impl {
  struct Field {
    std::string id, name;
    std::optional<LogicalTypeId> type;
  };
  struct Node {
    QueryExpressionPtr expression;
    std::size_t slot{0};
    std::vector<std::size_t> children;
  };
  std::vector<Field> fields;
  std::vector<Node> nodes;
  std::vector<std::size_t> type_checks;

  std::size_t append(const QueryExpressionPtr& expression) {
    const auto index = nodes.size();
    nodes.push_back({expression, 0, {}});
    std::visit([&](const auto& node) {
      using T = std::decay_t<decltype(node)>;
      if constexpr (std::is_same_v<T, LogicalExpression>) {
        for (const auto& child : node.operands) {
          const auto child_index = append(child);
          nodes[index].children.push_back(child_index);
        }
      } else {
        const auto slot = static_cast<std::size_t>(std::find_if(fields.begin(), fields.end(),
            [&](const auto& field) { return field.id == node.field.field_id; }) - fields.begin());
        nodes[index].slot = slot;
        if constexpr (!std::is_same_v<T, NullExpression>) {
          if (std::find(type_checks.begin(), type_checks.end(), slot) == type_checks.end())
            type_checks.push_back(slot);
        }
      }
    }, expression->node);
    return index;
  }

  using Bound = std::array<const DatasetAttributeValue*, 32>;
  Truth evaluate_node(std::size_t index, const Bound& values) const {
    const auto& program = nodes[index];
    return std::visit([&](const auto& node) -> Truth {
      using T = std::decay_t<decltype(node)>;
      if constexpr (std::is_same_v<T, LogicalExpression>) {
        if (node.operation == LogicalOperator::Not) {
          const auto value = evaluate_node(program.children.front(), values);
          return value == Truth::Unknown ? value :
              (value == Truth::True ? Truth::False : Truth::True);
        }
        bool unknown = false;
        for (const auto child : program.children) {
          const auto value = evaluate_node(child, values);
          unknown |= value == Truth::Unknown;
          if (node.operation == LogicalOperator::And && value == Truth::False) return Truth::False;
          if (node.operation == LogicalOperator::Or && value == Truth::True) return Truth::True;
        }
        return unknown ? Truth::Unknown :
            (node.operation == LogicalOperator::And ? Truth::True : Truth::False);
      } else {
        const auto* value = values[program.slot];
        if constexpr (std::is_same_v<T, NullExpression>) {
          return ((!value) != node.negated) ? Truth::True : Truth::False;
        } else {
          if (!value) return Truth::Unknown;
          const auto compare_literal = [&](const LogicalValue& literal) {
            return std::visit([&](const auto& left) {
              using V = std::decay_t<decltype(left)>;
              const auto& right = std::get<V>(literal.value());
              return left < right ? -1 : (left > right ? 1 : 0);
            }, *value);
          };
          bool matched = false;
          if constexpr (std::is_same_v<T, ComparisonExpression>) {
            const int compared = compare_literal(node.value);
            switch (node.operation) {
            case ComparisonOperator::Equal: matched = compared == 0; break;
            case ComparisonOperator::NotEqual: matched = compared != 0; break;
            case ComparisonOperator::Less: matched = compared < 0; break;
            case ComparisonOperator::LessEqual: matched = compared <= 0; break;
            case ComparisonOperator::Greater: matched = compared > 0; break;
            case ComparisonOperator::GreaterEqual: matched = compared >= 0; break;
            }
          } else {
            for (const auto& literal : node.values)
              if (compare_literal(literal) == 0) { matched = true; break; }
            matched = matched != node.negated;
          }
          return matched ? Truth::True : Truth::False;
        }
      }
    }, program.expression->node);
  }
};

QueryResult<DatasetAttributePredicate> DatasetAttributePredicate::compile(
    const QueryExpressionPtr& expression, const std::string& prefix) {
  const auto fail = [](QueryStatus status) {
    return QueryResult<DatasetAttributePredicate>::failure(std::move(status));
  };
  Hints hints;
  std::size_t nodes = 0;
  if (expression) {
    auto checked = collect(expression, hints, nodes, 0, prefix);
    if (!checked.ok()) return fail(checked);
  }
  auto program = std::make_shared<Impl>();
  std::vector<LogicalField> fields;
  for (const auto& hint : hints) {
    auto name = dataset_attribute_name({hint.first}, prefix);
    if (!name.ok()) return fail(name.status());
    program->fields.push_back({hint.first, std::move(name.value()), hint.second});
    fields.push_back({hint.first, hint.first,
        {hint.second.value_or(LogicalTypeId::Binary), true, std::nullopt},
        FieldRole::Attribute, std::nullopt});
  }
  auto schema = LogicalSchema::create(std::move(fields));
  if (!schema.ok()) return fail(schema.status());
  if (expression) {
    auto checked = validate_expression(expression, schema.value());
    if (!checked.ok()) return fail(checked);
    program->nodes.reserve(nodes);
    program->append(expression);
  }
  return QueryResult<DatasetAttributePredicate>::success(
      DatasetAttributePredicate(std::move(program)));
}

QueryResult<bool> DatasetAttributePredicate::evaluate(const DatasetCatalogRecord& record) const {
  Impl::Bound bound{};
  for (std::size_t slot = 0; slot < impl_->fields.size(); ++slot) {
    const auto& field = impl_->fields[slot];
    const auto found = std::find_if(record.attributes.begin(), record.attributes.end(),
        [&](const auto& attribute) { return attribute.name == field.name; });
    if (found == record.attributes.end()) {
      if (!record.attributes_complete)
        return QueryResult<bool>::failure(error(QueryError::LayoutUnavailable,
            "dataset attribute directory incomplete: " + field.name));
    } else {
      if (found->state != DatasetAttributeState::Complete || !found->value)
        return QueryResult<bool>::failure(error(QueryError::UnsupportedCapability,
            "dataset attribute is not an available scalar: " + field.name));
      bound[slot] = &*found->value;
    }
  }
  // Match the reference evaluator: unavailable attributes are diagnosed before
  // type validation, and type errors follow expression traversal order.
  constexpr LogicalTypeId types[] = {LogicalTypeId::Utf8, LogicalTypeId::Int64,
      LogicalTypeId::UInt64, LogicalTypeId::Float64, LogicalTypeId::Boolean};
  for (const auto slot : impl_->type_checks) {
    if (bound[slot] && types[bound[slot]->index()] != *impl_->fields[slot].type) {
      QueryErrorDetail detail;
      detail.field_id = impl_->fields[slot].id;
      return QueryResult<bool>::failure(QueryStatus::failure(QueryError::TypeMismatch,
          QueryPhase::Validation, "query literal type does not match field type", std::move(detail)));
    }
  }
  return QueryResult<bool>::success(impl_->nodes.empty() ||
      impl_->evaluate_node(0, bound) == Truth::True);
}

QueryStatus validate_dataset_catalog_request(const DatasetCatalogRequest &r) {
  if (r.format_id != "hdf5" || r.max_sources == 0 || r.max_sources > 4096 ||
      r.max_datasets == 0 || r.max_datasets > 65536 ||
      r.sources.paths.size() > 4096 || r.max_catalog_bytes == 0 ||
      r.max_catalog_bytes > 256ULL * 1024 * 1024)
    return error(QueryError::InvalidQuery,
                 "invalid dataset discovery format or limits");
  if (r.sources.file_predicate)
    return error(QueryError::UnsupportedCapability,
                 "dataset discovery file predicates are not supported");
  if (r.time_range && r.time_range->lower >= r.time_range->upper)
    return error(QueryError::InvalidDomainSlice,
                 "dataset UTC time range must be nonempty and half-open");
  if (r.time_range && !r.dataset_path)
    return error(QueryError::InvalidQuery,
                 "verified UTC time coverage requires an exact dataset path");
  if (r.dataset_path &&
      (r.dataset_path->empty() || r.dataset_path->front() != '/' ||
       r.dataset_path->size() > 4096 ||
       r.dataset_path->find('\0') != std::string::npos))
    return error(QueryError::InvalidQuery, "invalid dataset discovery path");
  auto source_check = make_in_memory_published_version_catalog({})->scan(
      r.sources, r.max_sources);
  if (!source_check.ok())
    return source_check.status();
  DatasetCatalogRecord absent;
  auto s = build_dataset_attribute_schema(absent, r.predicate);
  if (!s.ok())
    return s.status();
  return QueryStatus::success();
}
QueryResult<bool> matches_verified_utc_time(
    const StructuredBlobManifest &m,
    const std::string &dataset_path,
    const DatasetCatalogRequest::UtcTimeRange &range) {
  constexpr const char *protocol_key = "__iotdbfs.time_coverage.protocol";
  constexpr const char *second_key = "__iotdbfs.time_coverage.utc_second";
  constexpr const char *digest_key = "__iotdbfs.time_coverage.layout_digest";
  constexpr const char *path_key = "__iotdbfs.time_coverage.dataset_path";
  constexpr const char *catalog_digest_key =
      "__iotdbfs.dataset_catalog.layout_digest";
  auto protocol = m.metadata.find(protocol_key);
  auto second = m.metadata.find(second_key);
  auto digest = m.metadata.find(digest_key);
  auto path = m.metadata.find(path_key);
  auto catalog_digest = m.metadata.find(catalog_digest_key);
  const auto valid_string = [](const auto &it, const auto &end,
                               const std::string &expected) {
    return it != end && it->second.type() == MetadataValueType::String &&
           std::get<std::string>(it->second.value()) == expected;
  };
  const auto digest_ok = [&](const auto &it) {
    if (it == m.metadata.end() ||
        it->second.type() != MetadataValueType::String) return false;
    const auto &text = std::get<std::string>(it->second.value());
    return text.size() == 64 && std::all_of(text.begin(), text.end(),
        [](char c) { return (c >= '0' && c <= '9') ||
                           (c >= 'a' && c <= 'f'); });
  };
  if (!m.published || m.format_id != std::optional<std::string>("hdf5") ||
      m.phase != StructuredBlobPhase::Published || !m.layout_size ||
      !valid_string(protocol, m.metadata.end(), "utc-point/1") ||
      !valid_string(path, m.metadata.end(), dataset_path) ||
      second == m.metadata.end() ||
      second->second.type() != MetadataValueType::Int64 ||
      !digest_ok(digest) || !digest_ok(catalog_digest) ||
      std::get<std::string>(digest->second.value()) !=
          std::get<std::string>(catalog_digest->second.value()))
    return QueryResult<bool>::failure(error(
        QueryError::LayoutUnavailable,
        "source lacks verified UTC time coverage for its exact layout: " +
            m.key.path + "@" + std::to_string(m.key.version)));
  const auto point = std::get<std::int64_t>(second->second.value());
  return QueryResult<bool>::success(range.lower <= point && point < range.upper);
}
QueryResult<DatasetCatalogResult>
discover_datasets_reference(const PublishedVersionCatalog &cat,
                            const DatasetCatalogRequest &r,
                            const DatasetCatalogLoader &loader,
                            const DatasetCatalogAuthorizer &authorize) {
  auto valid = validate_dataset_catalog_request(r);
  if (!valid.ok())
    return QueryResult<DatasetCatalogResult>::failure(valid);
  auto selector = r.sources;
  const bool permitted_format =
      selector.formats.adapters == AdapterSelectionKind::All ||
      std::find(selector.formats.adapter_ids.begin(),
                selector.formats.adapter_ids.end(),
                r.format_id) != selector.formats.adapter_ids.end();
  selector.formats = {AdapterSelectionKind::Listed,
                      permitted_format ? std::vector<std::string>{r.format_id}
                                       : std::vector<std::string>{},
                      false};
  auto selected = cat.scan(selector, r.max_sources);
  if (!selected.ok())
    return QueryResult<DatasetCatalogResult>::failure(selected.status());
  std::vector<QuerySourceReference> refs;
  for (const auto &m : selected.value().versions())
    refs.push_back({m.key.path, m.key.version, m.format_id.value_or("")});
  if (!authorize)
    return QueryResult<DatasetCatalogResult>::failure(
        error(QueryError::PermissionDenied,
              "dataset catalog requires authorization"));
  auto allowed = authorize(refs, selected.value());
  if (!allowed.ok())
    return QueryResult<DatasetCatalogResult>::failure(allowed);
  DatasetCatalogResult out;
  out.selected_sources = refs.size();
  for (const auto &m : selected.value().versions()) {
    if (r.time_range) {
      auto matched = matches_verified_utc_time(m, *r.dataset_path,
                                               *r.time_range);
      if (!matched.ok())
        return QueryResult<DatasetCatalogResult>::failure(matched.status());
      if (!matched.value())
        continue;
    }
    ++out.time_candidate_sources;
    auto a = loader(m);
    if (!a.ok())
      return QueryResult<DatasetCatalogResult>::failure(a.status());
    auto encoded = serialize_dataset_catalog(a.value());
    if (!encoded.ok())
      return QueryResult<DatasetCatalogResult>::failure(
          error(QueryError::CorruptLayout, encoded.error()));
    if (encoded.value().size() > r.max_catalog_bytes - out.catalog_read_bytes)
      return QueryResult<DatasetCatalogResult>::failure(
          error(QueryError::ResourceExhausted,
                "dataset catalog byte budget exhausted"));
    out.catalog_read_bytes += encoded.value().size();
    auto checked = validate_dataset_catalog(a.value());
    if (!checked.ok() || !a.value().directory_complete ||
        a.value().format_id != r.format_id)
      return QueryResult<DatasetCatalogResult>::failure(
          error(QueryError::LayoutUnavailable,
                "dataset catalog missing, incomplete or invalid"));
    for (const auto &d : a.value().records) {
      if (r.dataset_path && *r.dataset_path != d.canonical_path)
        continue;
      ++out.candidate_datasets;
      auto matched = evaluate_dataset_attribute_predicate(d, r.predicate);
      if (!matched.ok())
        return QueryResult<DatasetCatalogResult>::failure(matched.status());
      if (matched.value()) {
        if (out.datasets.size() == r.max_datasets)
          return QueryResult<DatasetCatalogResult>::failure(
              error(QueryError::ResourceExhausted,
                    "dataset discovery result limit exceeded"));
        out.datasets.push_back({{m.key.path, m.key.version, r.format_id},
                                m.revision,
                                d.object_id,
                                d.canonical_path,
                                a.value().layout_digest,
                                a.value().layout_protocol});
      }
    }
  }
  return QueryResult<DatasetCatalogResult>::success(std::move(out));
}
QueryResult<DatasetPredicateSql>
compile_dataset_predicate_sql(const QueryExpressionPtr &e) {
  DatasetCatalogRecord absent;
  auto schema = build_dataset_attribute_schema(absent, e);
  if (!schema.ok())
    return QueryResult<DatasetPredicateSql>::failure(schema.status());
  Hints hints;
  std::size_t nodes = 0;
  if (e) {
    auto s = collect(e, hints, nodes, 0, "hdf5.attr:");
    if (!s.ok())
      return QueryResult<DatasetPredicateSql>::failure(s);
  }
  DatasetPredicateSql out;
  std::map<std::string, std::string> aliases;
  for (const auto &h : hints) {
    aliases[h.first] = "a" + std::to_string(out.attributes.size());
    out.attributes.push_back(dataset_attribute_name({h.first}).value());
  }
  bool exact = true;
  std::function<std::string(const QueryExpressionPtr &)> emit =
      [&](const auto &p) -> std::string {
    return std::visit(
        [&](const auto &n) -> std::string {
          using T = std::decay_t<decltype(n)>;
          if constexpr (std::is_same_v<T, LogicalExpression>) {
            if (n.operation == LogicalOperator::Not)
              return "(NOT " + emit(n.operands[0]) + ")";
            std::string s = "(";
            for (std::size_t i = 0; i < n.operands.size(); ++i) {
              if (i)
                s += n.operation == LogicalOperator::And ? " AND " : " OR ";
              s += emit(n.operands[i]);
            }
            return s + ")";
          } else {
            auto a = aliases.at(n.field.field_id);
            if constexpr (std::is_same_v<T, NullExpression>)
              return a + ".value_type IS " + (n.negated ? "NOT " : "") + "NULL";
            else {
              auto lit = [&](const LogicalValue &v)
                  -> std::pair<std::string, std::string> {
                if (v.is_null())
                  return {"", "NULL"};
                auto x = scalar(v);
                if (!x) {
                  exact = false;
                  return {"", "NULL"};
                }
                auto type = dataset_attribute_type(*x);
                std::string col = type == "int64"     ? "ivalue"
                                  : type == "float64" ? "fvalue"
                                  : type == "bool"    ? "bvalue"
                                                      : "svalue";
                auto text = dataset_attribute_text(*x);
                return {a + "." + col, (type == "string" || type == "uint64")
                                           ? quote(text)
                                           : text};
              };
              if constexpr (std::is_same_v<T, ComparisonExpression>) {
                auto v = lit(n.value);
                if (n.value.type() == LogicalTypeId::Utf8 &&
                    n.operation != ComparisonOperator::Equal &&
                    n.operation != ComparisonOperator::NotEqual)
                  exact = false;
                static const char *ops[] = {"=", "!=", "<", "<=", ">", ">="};
                return v.first + ops[int(n.operation)] + v.second;
              } else {
                std::string col, items;
                for (const auto &v : n.values) {
                  auto x = lit(v);
                  if (!x.first.empty())
                    col = x.first;
                  if (!items.empty())
                    items += ',';
                  items += x.second;
                }
                if (col.empty()) {
                  exact = false;
                  return "TRUE";
                }
                return col + (n.negated ? " NOT IN (" : " IN (") + items + ")";
              }
            }
          }
        },
        p->node);
  };
  out.predicate = e ? emit(e) : "TRUE";
  if (!exact) {
    out.predicate = "TRUE";
    // The caller evaluates the entire predicate against the validated catalog.
    // No SQL alias remains referenced after this conservative fallback.
    out.attributes.clear();
  }
  return QueryResult<DatasetPredicateSql>::success(std::move(out));
}
} // namespace iotdbfs
