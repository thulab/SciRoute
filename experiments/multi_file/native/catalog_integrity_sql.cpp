// Diagnostic only: compile real production count SQL, without connecting to DB.
#include "iotdbfs/IoTDBPublishedCatalogSql.h"
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
int main(int argc, char** argv) {
  using namespace iotdbfs;
  try {
    if (argc != 2) throw std::runtime_error("keys TSV required");
    std::ifstream input(argv[1]);
    std::vector<DatasetCatalogSqlSource> sources;
    std::string line;
    while (std::getline(input,line)) {
      std::istringstream row(line);
      DatasetCatalogSqlSource source;
      std::string version,records,attributes;
      if (!std::getline(row,source.key.path,'\t') || !std::getline(row,version,'\t') ||
          !std::getline(row,records,'\t') || !std::getline(row,attributes))
        throw std::runtime_error("invalid keys TSV");
      source.key.version=std::stoll(version);
      source.expected_records=std::stoull(records);
      source.expected_attributes=std::stoull(attributes);
      sources.push_back(source);
      if (sources.size()>128) throw std::runtime_error("bounded cohort exceeded");
    }
    if (sources.empty()) throw std::runtime_error("empty cohort");
    auto predicate=compile_dataset_predicate_sql({});
    if (!predicate.ok()) throw std::runtime_error(predicate.status().message());
    for (int i=0;i<21;++i) {
      const auto start=std::chrono::steady_clock::now();
      auto batches=compile_dataset_catalog_index_sql_batches(sources,predicate.value());
      const auto ns=std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now()-start).count();
      if (!batches.ok() || batches.value().size()!=1) throw std::runtime_error("unexpected batches");
      std::cout<<"compile_ns\t"<<ns<<'\n';
      if (i==0) {
        std::cout<<"entry_count\t"<<batches.value()[0].entry_count_sql<<'\n';
        std::cout<<"attribute_count\t"<<batches.value()[0].attribute_count_sql<<'\n';
      }
    }
  } catch (const std::exception& e) {
    std::cerr<<"count SQL compilation failed: "<<e.what()<<'\n'; return 1;
  }
}
