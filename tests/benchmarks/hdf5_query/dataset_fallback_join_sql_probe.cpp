// Read-only SQL generator linked against each measured core. No DB connection.
#include "iotdbfs/IoTDBPublishedCatalogSql.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

int main(int argc, char **argv) {
  using namespace iotdbfs;
  try {
    if (argc != 3) throw std::runtime_error("keys TSV and expression kind required");
    std::ifstream input(argv[1]);
    std::vector<DatasetCatalogSqlSource> sources;
    std::string line;
    while (std::getline(input, line)) {
      std::istringstream row(line);
      DatasetCatalogSqlSource source;
      std::string version, records, attributes;
      if (!std::getline(row, source.key.path, '\t') || !std::getline(row, version, '\t') ||
          !std::getline(row, records, '\t') || !std::getline(row, attributes))
        throw std::runtime_error("malformed exact identity input");
      source.key.version = std::stoll(version);
      source.expected_records = std::stoull(records);
      source.expected_attributes = std::stoull(attributes);
      sources.push_back(std::move(source));
      if (sources.size() > 128) throw std::runtime_error("bounded cohort exceeded");
    }
    if (sources.empty()) throw std::runtime_error("empty identity input");
    const auto ordering = make_comparison({"hdf5.attr:standard_name"},
        ComparisonOperator::Less, LogicalValue::utf8("b"));
    QueryExpressionPtr expression = ordering;
    if (std::string(argv[2]) == "and")
      expression = make_logical(LogicalOperator::And, {ordering,
          make_comparison({"hdf5.attr:units"}, ComparisonOperator::Equal,
                          LogicalValue::utf8("K"))});
    else if (std::string(argv[2]) == "equality")
      expression = make_comparison({"hdf5.attr:standard_name"},
          ComparisonOperator::Equal, LogicalValue::utf8("air_temperature"));
    else if (std::string(argv[2]) != "lt") throw std::runtime_error("unexpected expression kind");
    const auto predicate = compile_dataset_predicate_sql(expression);
    if (!predicate.ok()) throw std::runtime_error(predicate.status().message());
    const auto batches = compile_dataset_catalog_index_sql_batches(
        sources, predicate.value(), std::string("/T"));
    if (!batches.ok() || batches.value().size() != 1)
      throw std::runtime_error("unexpected bounded SQL compiler result");
    const auto &batch = batches.value().front();
    std::cout << "attributes\t" << predicate.value().attributes.size() << '\n'
              << "entry_count\t" << batch.entry_count_sql << '\n'
              << "attribute_count\t" << batch.attribute_count_sql << '\n'
              << "filter\t" << batch.filter_sql << '\n';
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "SQL generator failed: " << e.what() << '\n';
    return 1;
  }
}
