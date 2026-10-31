# HDF5 scientific-value conversion review (2026-10-07)

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

The old conversion loop chose its native type and physical/scientific view per
element, scanned the CF missing-value list per element, and used a `volatile`
temporary to prevent fused scale-and-offset arithmetic. The refactor selects
the native-type/view kernel once per batch. The HDF5 kernel source alone is
compiled with `-ffp-contract=off` in both production and unit builds; a
Float64 regression uses operands for which FMA and two separately rounded
operations return different results.

For scientific Int16, a query-local table has one value and one state byte for
each of 65,536 raw values (576 KiB). The state distinguishes masked, valid and
transform-overflow values. A table entry that would overflow does not reject
the query unless that raw value is actually selected. The table is built once
per cursor and reused across batches. Its size is deducted from the query's
HDF5 buffer budget. The direct kernel remains in use below 524,288 selected
values or when that budget cannot accommodate the table.

This threshold is based on an initial Release-build measurement on the public
GEOS experiment server. All times below are one diagnostic run, measured by
the same batch profile timer; they indicate a crossover, not a stable system
latency gain:

| Selected Int16 values | Table build | Lookup conversion | Direct conversion | Build + lookup |
| ---: | ---: | ---: | ---: | ---: |
| 131,072 | 0.538 ms | 0.183 ms | 0.387 ms | 0.721 ms |
| 524,288 | 0.582 ms | 0.683 ms | 1.483 ms | 1.265 ms |

The 524,288-value lookup and direct paths produced identical Float64 values
and masks. Other tests cover small packed data, Float32 and Float64 native
types, no-FMA rounding, and selected versus unselected Int16 overflow. The
raw diagnostic test output is here（服务器归档：`docs/hdf5-science-conversion-unit.xml`）.
The unchanged GEOS physical-value probe also matched all 24 previous
row-count, mask and value digests (new records（服务器归档：`docs/hdf5-science-conversion-geos.jsonl`）,
[reference](../hdf5-copy-path-results/D-copy-after.jsonl)). The full unit suite
passed all 1,336 tests.

Moving type/view decisions and `volatile` out of the element loop removes
avoidable scalar work. It does **not** establish that the remaining loop is
SIMD-vectorized: in-place backward expansion, per-value validity updates and
arbitrary CF missing-value predicates can still inhibit vectorization. A
separate output buffer or dedicated SIMD mask pass would need its own memory
budget and benchmark before claiming such a gain.
