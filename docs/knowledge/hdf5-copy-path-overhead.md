# HDF5 copy-path overhead check (2026-10-06)

The review identified three real per-run costs. Chunk lookup constructed a
`vector<uint64_t>` and searched an ordered map even though `Demand::key` was
already the linear chunk-grid ordinal. Each native copy charged the shared
decoded-fragment cache under its global mutex. Detailed profiles also created
a timer for every copied intersection, converted row and coordinate row.

The provider now indexes chunk records by linear key, reuses the coordinate
scratch for contiguous runs, and accumulates native copy bytes until the slab
finishes. Cancellation, an error, a metrics observation and destruction flush
any outstanding count. The cursor times native copying, scientific conversion
and coordinate projection once each per output batch. `hdf5.assemble.copy`
**now includes fragment acquisition and native-value copy**, whereas the old
measurement covered only `memcpy`; its before/after values must not be compared
as equivalent stages. `hdf5.assemble` still measures the full batch assembly.

The public GEOS `/T` direct-path probe used the same six-pass request, source,
runtime configuration and output digest before and after. The old executable
was preserved before the edit. Warm-pass medians (passes 1–5, milliseconds):

| Case | Total before | Total after | Assembly before | Assembly after |
| --- | ---: | ---: | ---: | ---: |
| point | 4.232 | 3.627 | 0.097 | 0.098 |
| roi | 4.792 | 3.579 | 0.131 | 0.151 |
| multichunk | 4.736 | 3.954 | 0.166 | 0.099 |
| levels | 4.308 | 3.765 | 0.184 | 0.121 |

All 24 paired query results matched in row count, mask count and FNV digest.
Every warm query had zero backend payload/layout calls, so this measured the
in-process warm path. The sample is small and runs were sequential; total
latency differences include unrelated preparation variation. The result
supports removal of per-run overhead but does not establish a reliable
end-to-end speedup. The narrow point case had unchanged assembly time at the
reported precision; multichunk and levels improved in this run.

Raw records: [before](../hdf5-copy-path-results/D-copy-before.jsonl),
[after](../hdf5-copy-path-results/D-copy-after.jsonl). The full remote unit suite
passed: 1,333 tests.
