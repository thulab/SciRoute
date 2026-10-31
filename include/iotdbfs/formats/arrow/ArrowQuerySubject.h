#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace iotdbfs {

// Arrow request coordinates only. Keep this header independent of planning,
// layout readers and decoders: QueryFragment stores these coordinates by value.
// Coordinates in a stored, nested array. They deliberately carry no dataset
// meaning: a child index is not necessarily a channel, and a leaf position is
// not necessarily a timestamp. The source File Version is selected separately
// by the QueryRequest's exact SourceSelector.
struct ArrayValuePath {
  // One index for each List level before the innermost List. An empty path
  // addresses a row of list<float32>; {2} addresses child 2 of a row of
  // list<list<float32>>.
  std::vector<std::uint64_t> child_indices;
};

struct PhysicalArraySubject {
  std::string field_id;
  std::uint64_t record_batch_index{0};
  std::uint64_t row_index{0};
  std::vector<ArrayValuePath> values;
};

} // namespace iotdbfs
