# HDF5 scientific query: matched discovery and array-read benchmark

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

This benchmark measures the new layout `/4` dataset-attribute catalog and CF
rectilinear scientific-value query on unchanged NOAA OISST v2.1 netCDF-4/HDF5
files. It is distinct from the older [H11 synthetic/GWOSC matrix](hdf5-query-benchmark.md).
The complete raw campaign（服务器归档：`docs/results/hdf5-sc-perf-20261006/raw-evidence.tar.gz`）,
independent summary（服务器归档：`docs/results/hdf5-sc-perf-20261006/summary.json`）,
source/version manifest（服务器归档：`docs/results/hdf5-sc-perf-20261006/campaign.json`）, and
broad catalog control（服务器归档：`docs/results/hdf5-sc-perf-20261006/catalog-broad.json`） are
retained for review.
Build and artifact fingerprints（服务器归档：`docs/results/hdf5-sc-perf-20261006/verification.json`）
record the measured binaries and benchmark sources.

## Workload and equivalence contract

Seven consecutive NOAA AVHRR OISST daily files (2024-01-01 through 2024-01-07)
form seven independent exact source versions. Each has a rank-4, packed Int16
`/sst` dataset with CF `scale_factor`, missing and validity attributes. The
attribute predicate is `hdf5.attr:long_name == "Daily sea surface temperature"`.
Discovery produces exact `(path, version, /sst)` identities; there is no
cross-file array concatenation. An offline local catalog is constructed once
from the same seven native HDF5 files. Its build time, native per-request
attribute scan, local catalog filter, and IoTDB dataset-catalog request are
recorded separately.

For each selected source, the four queries are:

| Case | Latitude bound | Longitude bound | Expected selection |
|---|---|---|---|
| `point` | `[0, 0.25)` | `[180, 180.25)` | one ocean cell |
| `roi` | `[-1, 1)` | `[180, 182)` | 8 × 8 ocean cells |
| `full` | none | none | 720 × 1,440 cells |
| `negative` | `[91, 92)` | none | no rows, no payload read |

All five paths return an owned Float64 value vector and one byte of validity
per row, in C order. The raw-domain fill/missing/validity mask is applied
before `raw * scale_factor + add_offset`; invalid values are represented as
zero only for the cross-path digest. Every query's row count, mask count and
canonical value/mask FNV-1a digest must match native HDF5 exactly. Hashing and
verification are outside the timed query interval.

| Path | Discovery | Value reader | Timed read boundary |
|---|---|---|---|
| A | native HDF5 attribute scan or prebuilt local catalog | local native `H5Dread` | coordinate bound resolution, hyperslab read, Float64/mask materialization |
| B | same local catalog | production HDF5 adapter on local `pread` | prepare, execute, cursor consumption, owned copy |
| C | same local catalog | native `H5Dread` on ordinary IoTDB-FS FUSE file | same as A |
| D | IoTDB dataset catalog | production HDF5 adapter on IoTDB QuerySource | same as B |
| E | IoTDB dataset catalog | typed socket registration and immutable IPC FUSE route | registration plus mmap, IPC decode and owned NumPy value/mask copy |

A/C keep the file/dataset handle and coordinate vectors open across the four
query cases within one source. B/D keep one shared 64 MiB decoded-cache runtime
across all sources and cases. E uses the daemon's 64 MiB decoded-cache budget.
The native 64 MiB chunk cache is per open dataset; the B/D/E budget is global.
The seven `/sst` decoded chunks fit both limits, but metadata caches and total
process RSS are not equalized. A/C resolve coordinate bounds inside each timed
query. Native file open, coordinate-vector loading, B's local layout build,
IoTDB publication, and the local catalog build are separate setup metrics.
The native C path opens a logical FUSE file frozen for the whole campaign;
B/D/E use exact published versions.

Each campaign starts a new private mount and a fresh A/C/B/D process/runtime.
Within each path, pass 0 is the application's first trace; passes 1–3 are
retained-cache repeats. Three campaigns alternate path order `ABCDE`, `EDCBA`,
`ABCDE`. Campaigns are the independent timing observations, not the 28 cases
or per-case repeats. OS page cache and IoTDB server cache are uncontrolled, so
"first" does not mean cold physical media. Detailed profiling is enabled in
B/D/E and may perturb very short requests; no profile-off correction is made.
All three nonempty spatial cases touch one large compressed `/sst` chunk in
each file. Their byte selectivity is therefore low even though the requested
point/ROI is small; this is an important negative control for a message-like
layout advantage.

## Metrics and evidence

Every raw request records wall time. Native A/C additionally record `H5Dread`,
coordinate resolution, transform, CPU, page faults, file bytes, compressed
dataset bytes and chunk shape. B/D record prepare, execute, cursor, production
profile stages, logical output, decoded-cache hits/misses/decodes, backend
payload/layout calls, ranges and bytes. E records client registration, IPC
conversion, validation/cleanup, encoded result bytes, server stage times and
IoTDB SQL, metadata, layout and payload counters. Stage durations can nest;
they must not be summed blindly. Native HDF5 byte counters refer to logical
raw elements and compressed dataset storage size, not physical disk bytes read.

`sc_perf_campaign.py` preserves source URLs and SHA-256 hashes, exact deployed
versions, catalog responses, every native and direct JSON record, IPC route
results, and server JSONL profiles. `summarize_sc_perf.py` independently checks
the complete A–E matrix, raw-to-observation mapping, source/version bindings,
route/profile cardinality and every value/mask digest before computing
descriptive statistics and paired campaign ratios. Source publication, setup
and catalog construction are visible, but excluded from steady query latency;
any end-to-end conclusion must add the appropriate online discovery time.

The experiment does not assert that a seven-file catalog demonstrates index
scaling or that seven days represent annual training. It also does not claim
native `H5Dread` and the production decoder are the same reader: B/A and D/B
must be interpreted separately. Other HDF5 dataset chunk geometries, more
selective attribute predicates and larger archives require later experiments.

## Measured result, 2026-10-06

The seven original files total 10,722,366 B. Their seven `/sst` datasets each
contain one approximately 663 KB compressed chunk that decodes to 2,073,600 B.
The three 4-pass campaigns produced **1,680/1,680 validated A–E observations**.
The raw archive was transferred back from the server, extracted locally, and
re-summarized; its new summary SHA-256 exactly matched the server-generated
summary. The table is the median **per request**, in milliseconds, over seven
files and three campaigns; warm includes passes 1–3. The campaign-level paired
ratios and every raw observation remain in the linked summary.

| Case | A local native first / warm | B local adapter first / warm | C FUSE native first / warm | D IoTDB adapter first / warm | E typed IPC first / warm |
|---|---:|---:|---:|---:|---:|
| one ocean point | 10.482 / 0.023 | 11.423 / 0.396 | 52.420 / 0.029 | 38.863 / 18.401 | 143.219 / 121.671 |
| 8 × 8 ocean ROI | 0.027 / 0.013 | 0.512 / 0.455 | 0.044 / 0.014 | 18.733 / 18.834 | 123.957 / 123.410 |
| full 720 × 1,440 | 26.349 / 18.851 | 28.538 / 27.127 | 26.826 / 18.842 | 50.006 / 49.274 | 201.761 / 201.807 |
| no matching latitude | 0.005 / 0.004 | 0.444 / 0.432 | 0.005 / 0.004 | 15.449 / 15.360 | 120.000 / 119.829 |

The warm point paired campaign ratios are B/A **16.50×**, C/A **1.22×**,
D/B **45.13×**, and E/D **6.75×**. The warm full-field paired ratios are
**1.36×**, **1.01×**, **1.85×**, and **4.05×**, respectively. These are means
of within-campaign paired workload ratios, rather than ratios of the displayed
medians. They are descriptive observations from three campaigns.

The first point request in each source loads one compressed chunk: B and D
each make one payload call/range and decode one 2,073,600 B chunk, averaging
663,481 encoded bytes across the seven daily files. The following ROI and
full-field requests reuse that chunk. On all warm point/ROI/full requests,
B and D make **zero payload calls and zero new decodes**. They still load one
manifest and read the layout on every query: four layout calls and 33,281 B
for nonempty cases, three calls and 33,221 B for the empty case. Warm point
prepare averages 0.347 ms on local B and **14.829 ms** on IoTDB D; warm D
point wall time is 18.401 ms. This identifies repeated metadata/layout access
as the principal warm point cost, not chunk payload retrieval. Native A/C
retain an open HDF5 handle and its coordinate/chunk caches across cases,
which is a strong but different reader state; B versus D isolates the same
production adapter over different backends.

For a warm point, E averages **62.457 ms** in socket registration and
**62.915 ms** from mmap through owned NumPy conversion. The server-side
`typed_prepare` stage averages 23.554 ms, whereas cursor execution is 0.076 ms
and IPC encoding 0.187 ms. A point result carries 9 B of owned value/mask data
but occupies a 9,080 B IPC file because schema and scientific provenance are
fixed costs. The full-field IPC file averages 8,462,544 B for 9,331,200 B of
owned value/mask data; warm E registration and IPC conversion then average
124.110 ms and 77.967 ms. The stage durations are nested within larger
server/client intervals and cannot be added as independent components.

The per-campaign exact-`/sst` catalog request takes a median **297.229 ms**
at the client, versus **6.721 ms** for repeated native attribute scans of the
seven `/sst` datasets and **0.015 ms** for filtering an already constructed
local catalog. The latter's build cost is separate. In the broader control,
native construction of a 56-dataset catalog takes **14.687 ms**; both positive
and no-match queries examine all 56 entries. Three-campaign medians are:

| Broad attribute predicate | Matched datasets | Native scan | FS prebuilt filter | IoTDB catalog client |
|---|---:|---:|---:|---:|
| `long_name = Daily sea surface temperature` | 7 | 10.346 ms | 0.015 ms | 305.596 ms |
| `long_name = __absent_sc_benchmark__` | 0 | 10.309 ms | 0.014 ms | 296.646 ms |

Both IoTDB broad queries use 43 SQL calls and read 33,026 B of catalog
artifacts in each campaign; the no-match predicate avoids subsequent payload
queries but does not prune these metadata reads. The local adapter rebuilds
the seven source layouts in an average **104.979 ms** of separately reported
setup time; one-time IoTDB publication of the seven original files took
**3.010 s**. These setup numbers have different work scopes and are not
included in the online latency table.

This workload does **not** show an IoTDB-FS query-speed benefit. It shows
correct cross-file property discovery and CF value semantics, while locating
the current query penalty in repeat layout/manifest access, catalog traversal,
and typed route/client fixed costs. OISST's one large SST chunk means a point
query cannot avoid decoding the same chunk on either backend; a future
message-like or suitably derived chunk layout must be tested with the same
A–E controls before a scientific-layout advantage is claimed.
