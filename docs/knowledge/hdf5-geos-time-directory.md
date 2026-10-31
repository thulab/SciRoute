# GEOS UTC time coverage directory and FS control

GEOS-5.7.2 `inst3_3d_asm_Np` files contain one instantaneous time coordinate.
The [NASA file specification](https://gmao.gsfc.nasa.gov/media/gmao_products/IqPeIBhIWLGrweILfVPj9Vmdoc/GEOS-5.7.2_File_Specification.pdf)
declares the product's three-hourly sample times in UTC. This is a specific
product rule, not a general CF calendar conversion: typed HDF5 queries still
retain the native `minutes since ...` coordinate and never join files.

At ingestion, the HDF5 adapter derives a `utc-point/1` coverage only after the
GEOS root profile, `/T` temperature attributes, four forward dimension-scale
links, and coordinate indexes pass the production semantic binder. `/time` must
have one finite integral-minute value, `standard` calendar, and a canonical
modern `minutes since YYYY-MM-DD HH:MM:SS` reference. The resulting UTC second,
protocol and SHA-256 layout digest are published as immutable manifest metadata
for that exact source path/version. Unsupported or ambiguous evidence publishes
**no** coverage marker.

The `dataset_catalog` registration accepts an optional half-open UTC range
with the exact `/T` dataset path. The marker is scoped to `/T`; it does not
claim that every variable in the file has been time-validated:

```json
{"kind":"dataset_catalog","request":{"sources":{"paths":[{"kind":"subtree","path":"/geos"}]},"dataset_path":"/T","time_range":{"lower_unix_s":1309597200,"upper_unix_s":1309597201},"predicate":{"kind":"compare","field":"hdf5.attr:standard_name","operator":"eq","value":{"type":"utf8","value":"air_temperature"}}}}
```

All selected sources are authorized first. The backend then checks that each
manifest is still the same published revision, that its coverage protocol and
dataset path are known, and that its coverage digest equals the dataset
catalog's layout digest.
Only matching versions load a dataset catalog and execute the attribute filter.
The response exposes both `selected_sources` and `time_candidate_sources`.
Missing coverage or a digest mismatch fails with `LayoutUnavailable`; it never
silently excludes a source. Existing GEOS `/5` versions published before this
change need **new-version reingestion** to acquire the marker. Layout `/5` is
unchanged and published versions are not edited in place.

The ordinary-FS control is
[`geos_time_directory.py`](../../tests/benchmarks/hdf5_query/geos_time_directory.py).
`build` reads complete original files, checks their frozen size and SHA-256,
revalidates the native GEOS profile and dimension scales, and writes the same
`utc-point/1` records into a prebuilt JSON directory. Each record retains raw
path, content hash, file identity, exact IoTDB path/version for pairing, native
time units and calendar. `compare` applies the same half-open predicate and
checks the candidate identities against the IoTDB-FS control socket. FS lookup
checks size and mtime against the frozen entry; content hashes are charged to
index construction, not to every lookup. Report index build time and size
separately from query latency on both sides.

This first directory reduces **catalog reads** for time misses, but the current
IoTDB path still scans the published manifest snapshot and validates each
selected manifest before pruning. It is not yet a global SQL time-range index
with `O(log N + K)` discovery. The FS control is a prebuilt directory rather
than per-query `H5Dopen`; both directory construction costs must be reported.
The 16-file experiment can measure this boundary without crediting an
unimplemented database index.

The two complete 09:00 and 12:00 originals in the existing pilot yield UTC
seconds `1309597200` and `1309608000`, respectively, when the FS directory is
built. The production-adapter
[`geos_time_ingest_probe`](../../tests/benchmarks/hdf5_query/geos_time_ingest_probe.cpp)
independently produced the same seconds and `/T` scope from both originals;
its layout digests were `74a91c7a…c1bfa` and `2f3363c9…34686e4`, matching
the exact published `/5` versions in the prior GEOS pilot. FS lookup returned
candidate counts 1, 1, 0 and 2 for the two point intervals, their gap and
their union. The server's isolated source build passed all 1,346 unit tests.

The first 16-source scale acceptance is complete in the isolated
`geostime20261007` database, without expanding DataNodes. Every source is a
distinct, complete public GEOS three-hourly original, reingested as an exact
version with a coverage marker. Three mount campaigns measured N=1/4/8/16
with five hit and five miss repetitions per N; candidate identities matched
the FS directory in every case. At N=16, the warm time-hit median was
165.520 ms for IoTDB-FS, 46.593 ms for native per-file FS scanning, and
0.066 ms for the FS prebuilt directory. The time predicate cut IoTDB-FS
catalog reads from 16 to one, but source authorization and manifest checks
still scaled with N. The [full methods, measurements and raw records](../reports/hdf5-geos-time-scale-results.md)
distinguish this internal pruning gain from
an end-to-end FS advantage, which was not observed.
