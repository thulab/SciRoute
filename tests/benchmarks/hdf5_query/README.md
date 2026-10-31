# HDF5 H00 local-file validation

This is a bounded correctness spike, not the production HDF5 adapter. It uses
libhdf5 to inventory metadata, selects rank 1–3 IEEE Float32/Float64 arrays,
reads their actual file ranges, reverses supported filters, and compares the
result bitwise with `H5Dread`, h5py, and independently generated formula values.
Dynamic filter plugins are disabled. Unsupported profiles and corrupt data are
reported separately; there is no hidden native-read fallback.

The public baseline is the original GWOSC GW150914 H1/L1 32-second, 4096 Hz V2
HDF5 pair (2,048,012 bytes total), licensed CC BY 4.0. URLs, SHA256, shape,
chunking, selection coordinates and expected value hashes are in
[public-data.lock.json](public-data.lock.json). Attribution and citation:
[GWOSC acknowledgement](https://gwosc.org/acknowledgement/) and
[original tutorial](https://gwosc.org/s/events/GW150914/LOSC_Event_tutorial_GW150914.html).
The files are downloaded unchanged, outside the source tree.

From the repository root on Linux (CMake >= 3.19; C/C++17 compiler; zlib headers):

```bash
python3 ci/install_dependencies.py --only hdf5 \
  --prefix /tmp/h00-hdf5-prefix --cc gcc --cxx g++ --jobs 4
python3 ci/install_dependencies.py --only hdf5 \
  --prefix /tmp/h00-hdf5-prefix --verify-only
python3 -m venv /tmp/h00-venv
/tmp/h00-venv/bin/python -m pip install \
  -r tests/benchmarks/hdf5_query/requirements-h00.txt
cmake -S tests/benchmarks/hdf5_query -B /tmp/h00-probe \
  -DCMAKE_BUILD_TYPE=Release \
  -DHDF5_DIR=/tmp/h00-hdf5-prefix/lib/cmake/hdf5
cmake --build /tmp/h00-probe -j 4
/tmp/h00-venv/bin/python tests/benchmarks/hdf5_query/run_h00.py \
  --probe /tmp/h00-probe/h00_hdf5_probe \
  --output /tmp/h00-results --public-dir /tmp/h00-public --download
/tmp/h00-venv/bin/python -m unittest discover \
  -s tests/benchmarks/hdf5_query -p 'test_h00_*.py' -v
```

The installer accepts `--source-archive` for an offline copy of the locked
archive. A failed/incomplete prefix must be preserved for diagnosis or replaced
with a fresh directory; it is not silently reused. The Linux-only installer is
not a macOS installer. macOS standalone verification used the same source and
CMake options with Apple Clang, as recorded in the spike report.

The existing root project also exposes an opt-in target:
`-DIOTDBFS_BUILD_HDF5_H00_PROBE=ON -DHDF5_DIR=...`, then
`cmake --build <build> --target h00_hdf5_probe`. Its binary is under `bin/`.
Root builds retain `LANGUAGES CXX`; HDF5's exact CONFIG imported C-library target
does not require enabling C in that project. Ordinary production builds and
release targets do not include this probe.

Outputs contain a fixture manifest, one JSON trace per case and `summary.json`.
Only an empty `failures` list and `passed: true` establish success. A nonzero
exit is expected for checksum-negative probe cases, and the runner validates
that failure explicitly. Fixture hashes are reproducible within the recorded
generator/library toolchain, not promised identical across HDF5 versions.

For a single request:

```bash
/tmp/h00-probe/h00_hdf5_probe \
  --file /tmp/h00-public/H-H1_LOSC_4_V2-1126259446-32.hdf5 \
  --dataset /strain/Strain --start 65536 --count 8192 \
  --output /tmp/h00-event.json --values-output /tmp/h00-event.f64le
```

Omitting `--dataset/--start/--count` performs inventory only. The trace contains
**raw value payload reads only**. libhdf5 metadata reads, its native oracle reads,
process startup and filesystem cache effects are not attributed as system I/O
performance. Recorded subprocess wall time is diagnostic, not an FS versus
IoTDB-FS result. Run with `HDF5_DRIVER`, `HDF5_VOL_CONNECTOR` and `HDF5_PLUGIN_PATH` unset.
The probe uses the default FAPL and opens trusted local files; H02 must provide the
restricted source-backed VFD and full metadata/read budgeting.

See [H00 results and limitations](../../../docs/reports/hdf5-query-spikes.md).

For the warm native-library concurrency diagnostic (same total reads, 3 trials
at each of 1/2/4 threads), the standalone project also builds:

```bash
/tmp/h00-probe/h00_hdf5_concurrency \
  --file /tmp/h00-public/H-H1_LOSC_4_V2-1126259446-32.hdf5 \
  --reads 4096 --output /tmp/h00-concurrency.json
```

This measures H5Dread plus equality checks and worker close/join after warmup;
it is not a storage-system concurrency or cold-cache benchmark.

## H02 source-backed diagnostic

The same standalone CMake project also builds `h02_source_probe`. It passes a
bounded `pread` callback into the production read-only source driver; the driver
receives no local pathname. It uses explicit native VOL and the source VFD,
keeps dynamic HDF5 plugin loading disabled for the process lifetime, and rejects
nonempty `HDF5_DRIVER` / `HDF5_VOL_CONNECTOR` before initializing the library.

```bash
cmake --build /tmp/h00-probe --target h02_source_probe -j 4
/tmp/h00-venv/bin/python tests/benchmarks/hdf5_query/run_h02.py \
  --probe /tmp/h00-probe/h02_source_probe \
  --output /tmp/h02-results --public-dir /tmp/h00-public
```

The H02 runner validates 144 supported selections, 29 unsupported-profile
rejections, three corrupt-checksum rejections, and two fresh-process environment
checks. It retains canonical value bytes and traces, checks source callbacks
against VFD accounting, and verifies values against the H00 formula/frozen hashes
and h5py. Native reads include metadata and any library read-ahead. `raw_data` is
the VFD `H5FD_MEM_DRAW` category: HDF5 also uses it for global heaps, including VDS
mapping metadata, so it is not a semantic assertion that every byte is an array
value. Rejected VDS files must not open any external value source.

This is the explicit native diagnostic, not the H04/H05 raw-chunk optimized
query or a filesystem performance comparison. Application buffers and source
I/O have limits; native filter/metadata working memory is not a hard RSS sandbox.
See [the H02 contract and validation](../../../docs/contracts/hdf5-source-driver.md).

## H03 ingestion and layout validation

```bash
cmake --build /tmp/h00-probe --target h03_ingestion_probe -j 4
/tmp/h00-venv/bin/python tests/benchmarks/hdf5_query/run_h03.py \
  --probe /tmp/h00-probe/h03_ingestion_probe \
  --output /tmp/h03-results --public-dir /tmp/h00-public
```

The runner checks 59 files: all 55 H00 fixtures, two graph/paging fixtures,
and the unchanged GWOSC public pair. The probe runs the production ingestion
and Structured Blob lifecycle against a recording in-memory backend, then opens
the published QuerySource and checks the paged layout. It retains original B,
encoded L, the graph/chunk report, and source read ranges. h5py independently
checks shape, native allocated count, physical addresses, lengths and filter
masks. This is metadata-only validation: corrupt value checksums are retained
and will fail at later decoding, rather than being scanned during ingestion.

`summary.json` must report `passed: true` and 59 successful files. This does not
claim live IoTDB/FUSE validation, typed query execution, or performance results.
See [H03 layout and ingestion contract](../../../docs/contracts/hdf5-layout-ingestion.md).

## H04 unfiltered typed queries

```bash
cmake --build /tmp/h00-probe --target h04_query_probe -j 4
/tmp/h00-venv/bin/python tests/benchmarks/hdf5_query/run_h04.py \
  --probe /tmp/h00-probe/h04_query_probe \
  --output /tmp/h04-results --public-dir /tmp/h00-public
```

This executes actual ingestion → published QuerySource → QueryModule → HDF5
adapter against a recording in-memory backend. After H05, this runner keeps
185 uncompressed regression cases (183 value comparisons and two unsupported
profiles); the former compressed/fill rejection cases moved to H05. Rank 1–3
Float32/Float64, both byte orders, contiguous/chunked, aliases, userblocks,
edge slices, small batches, value-only and axis projections are covered.
Contiguous read checks allow exactly touched 4096-element tiles clipped to the
dataset, including a fixture where only two of five tiles are touched.

Each successful query records canonical LE values, schema, source references,
slab domains, axes and phase-separated payload/layout reads. `summary.json`
must report `passed: true`; this is not a live mount or performance comparison.
See [H04 execution and validation](../../../docs/contracts/hdf5-query-execution.md).

## H05 filters, fill and reusable decoding

```bash
cmake --build /tmp/h00-probe --target h04_query_probe -j 4
/tmp/h00-venv/bin/python tests/benchmarks/hdf5_query/run_h05.py \
  --probe /tmp/h00-probe/h04_query_probe \
  --output /tmp/h05-results --public-dir /tmp/h00-public
```

With both public GWOSC files, expect 416 cases: 384 value comparisons, 25
unsupported-profile rejections and seven corrupt-payload rejections. This covers
canonical shuffle/DEFLATE/Fletcher32 subsets, GWOSC's Fletcher32→DEFLATE order,
per-chunk masks, partial-edge filter bypass, deterministic sparse/contiguous
fill and special IEEE values. Output, domains, range traces and useful/payload/
decoded/inflated byte metrics are independently checked. Malformed native reads
run in subprocesses because native HDF5 may tolerate malformed encoding or crash;
production strictly rejects those cases. The current layout is `/2`; `/1` needs
re-ingestion. See [H05 contract](../../../docs/contracts/hdf5-filter-decoding.md).

## H06 shared chunks, batching and concurrent transport

```bash
cmake --build /tmp/h00-probe --target h06_query_probe -j 4
/tmp/h00-venv/bin/python tests/benchmarks/hdf5_query/run_h06.py \
  --probe /tmp/h00-probe/h06_query_probe --output /tmp/h06-results
```

The self-contained runner creates three h5py fixtures: 64 selected nonadjacent
chunks, 2,048 small selected chunks, and one 28 MiB native chunk. Thirteen
configurations exercise scalar versus true multi-range transport and 1/2/4
concurrent calls. Each runs cold, repeated, hard-link alias, overlapping, and
four-client concurrent queries, checking all value bytes against h5py. Resident
warm/alias/overlap reads must perform no payload I/O or decode; concurrent clients
must decode each selected chunk once. Physical read unions must match exactly,
including the four <=8 MiB reads that transport the large chunk.

The probe uses a recording memory backend with **synthetic per-call sleep** for
observable concurrency. These timings validate execution and profiling; they
are not live IoTDB/FUSE measurements. Defaults here are a 256 MiB runtime budget,
8,192 entries, a 192 MiB cursor buffer cap and 30 s admission timeout. Read
parallelism is per producer; runtime workers and backend session capacity are
separate limits. The zero-latency small-chunk case checks reuse beyond 1,024
entries without claiming a latency improvement.

The report separates execution wall time, summed source-call wall time, summed
producer-load wall time, codec-only wall time and sampled codec thread CPU time.
Concurrent durations overlap: do not add them as a wall-time decomposition.
`decode_cpu_samples` indicates how many successful decodes had a valid thread CPU
clock reading. I/O calls, physical ranges/bytes, decodes, copies, observed active
call peak, memory peak and evictions are retained per scenario. Expect
`passed: true` with 13 configurations (65 scenario runs).

Rerun H04 and H05 against the rebuilt probe as regressions. Their range oracle
compares the exact allocated-chunk/tile union rather than assuming one call per
chunk: adjacent merging and bounded splitting must not add gap bytes or omit
selected data.


## H07 metadata and GPS semantics validation

Build `h07_query_probe`, then run:

```sh
python run_h07.py --probe <build>/h07_query_probe --output <results> \
  --public-data <h00>/raw
```

The standalone binary is in the build root; the full project puts it in `bin/`.
The oracle uses h5py independently for values and coordinates. It covers fixed
and variable attributes, explicit declarations, dyadic precision, half-open and
nextafter boundaries, physical/semantic cache sharing, and both locked GWOSC
public samples. Layout is `/3`; old `/1` and `/2` require re-ingestion. Numeric
attributes are validated, and unread variable unit strings are explicitly
declared by the opted-in GWOSC profile, never silently decoded or inferred.
No live mount or RPC performance claim is made. See
[H07 contract](../../../docs/contracts/hdf5-scientific-binding.md).

## H08 live IPC, registration and FUSE validation

Use an isolated database, mountpoint, namespace writer lock and control socket.
Do not run these scripts against an existing experiment or production mount.
The daemon must be built with H08 and expose `IOTDBFS_QUERY_PROFILE_JSONL`.
`query.hdf5.read_parallelism` accepts 1, 2 or 4; the acceptance run uses 2.

```sh
# This process needs h5py and NumPy, but does not need PyArrow.
python h08_mount_smoke.py prepare --directory <prepared> --public-raw <h00>/raw
# This process and its independent children need PyArrow and NumPy, not h5py.
python h08_mount_smoke.py run --prepared <prepared> --mount <mount> \
  --socket <socket> --output <results> --profile <profile.jsonl>
```

The harness verifies original bytes, layout identity, exact source versions,
compressed multidimensional slab reconstruction, a hard-link alias, value-only
projection, schema-only empty selection and public GWOSC GPS seconds against
native h5py oracles. It exercises stat/read/seek/EOF/mmap and concurrent opens.
Two sequential independent processes prove warm chunk reuse. Two further
processes cross a start barrier on a fresh source; server time intervals must
overlap and cumulative counters must show exactly nine chunk decodes. Shared
per-request profile deltas are never summed across overlapping intervals.

For actual supplementary-group authorization and immediate revocation, start
this **isolated** daemon as root with `-o allow_other,attr_timeout=0,entry_timeout=0`
and `IOTDBFS_QUERY_SOCKET_MODE=0666`, using an explicit socket path in a searchable
directory. Then run as root:

```sh
python h08_permission_smoke.py --prepared <prepared> --mount <mount> \
  --socket <socket> --output <permissions.json>
```

Children drop to numeric uid/gid 65534, with or without supplementary group
40077; no host account is created. The test compares ordinary reads, warm typed
mmap and new registrations, revokes/restores source permissions, then checks
that an already-open result retains working fstat/read after source unlink,
while a new open is denied. Both scripts clean only their unique source
directories. Pinned immutable attributes remain visible until the last close;
with `default_permissions`, `access(R_OK)` is advisory and may return true while
a new open still fails authorization. The report records both outcomes.
Unit tests cover bounded encoding, publication pressure, expiry,
malformed frames, disconnect cleanup and waiter timeout without cancelling an
independent producer. See [H08 contract](../../../docs/contracts/hdf5-ipc-runtime.md).

## H09 offline re-ingestion and recovery

H09 uses the full IoTDB/FUSE project, not the standalone H00 probe build. From
an already configured full build with the locked HDF5 dependency, build the
daemon and the explicitly excluded fixture target:

```sh
cmake --build <full-build> --target iotdb_fs h09_migration_fixture -j 4
```

Prepare the native oracle once with h5py/NumPy if H08's prepared directory is
not already available. The live runner needs PyArrow/NumPy and access to a
local IoTDB service; on Linux it reads the specified existing process's
connection configuration without printing credentials:

```sh
/path/to/h5py-python tests/benchmarks/hdf5_query/h08_mount_smoke.py prepare \
  --directory /absolute/path/h09-prepared \
  --public-raw /absolute/path/h00-public
/path/to/pyarrow-python tests/benchmarks/hdf5_query/h09_migration_smoke.py \
  --source-pid EXISTING_DAEMON_PID \
  --build /absolute/path/full-build \
  --reports-parent /absolute/path/isolated-h09-reports \
  --prepared /absolute/path/h09-prepared
```

The runner creates its own database, report directory, writer lock, short Unix
socket path and temporary mount. It preserves database/report evidence and
unmounts its own mount on exit. It does not migrate the existing daemon's
namespace. The fixture tool's deliberate seed/pause operations are test-only;
do not use it against a production namespace.

The test covers latest pass-through candidates, retained historical versions,
ordinary non-HDF5 bytes, another registered identity, current indexed files,
and legacy `/1`/`/2` markers. Legacy markers are synthetic classification
fixtures, not genuine historical layout encodings. Only latest pass-through
candidates are migrated. Both original and new exact versions must preserve
full byte hashes; the new version must have `/3` layout and correct typed
queries, including the public GWOSC sample.

Recovery checks include repeated resume, latest-version conflicts, an actual
SIGKILL of a fixture child paused after a durable `started` event and before
DB ingestion, and a separate simulation that removes final events after
publication. The report distinguishes real interruption from simulation; it
does not claim arbitrary crash-point or power-loss coverage. The live mount
also checks namespace lock exclusion, ordinary reads and query results.

Production commands remain:

```sh
bin/iotdb_fs hdf5-migrate inventory --report /absolute/path/fresh-report \
  --path /science/observation.bin
bin/iotdb_fs hdf5-migrate run --report /absolute/path/fresh-report
bin/iotdb_fs hdf5-migrate resume --report /absolute/path/fresh-report
```

Stop all writers to that production namespace first. The common report has a
mandatory `report.manifest` binding endpoint/database, format policy, target
layout and inventory SHA-256. Old A12 reports are rejected; use a fresh inventory
directory. Existing HDF5 `/1` and `/2` identities are reported as
`requires_reingestion` and are not automatically upgraded. See the complete
[H09 operating guide](../../../docs/knowledge/hdf5-data-migration.md) and
[Arrow migration update](../../../docs/knowledge/arrow-data-migration.md).

The 2026-10-02 isolated H09 run passed: Linux CTest 1,254/1,254 and
[the live summary](../../../docs/results/hdf5-h09-20261002/live-summary.json)
reports `ok=true`. Eight inventoried versions produced exactly two migrated
latest pass-through candidates: the 4,473-byte compressed fixture and the
1,040,592-byte original GWOSC H1 file. Both preserve source/destination hashes.
The summary retains interruption, repeat-resume, conflict, lock and FUSE
checks; this is not a bulk migration or performance result.

## H10 fixed traces and cost profiles

H10 measures where time and work go; it is not the H11 A–E performance study.
Use the same H08 prepared raw files and independent native oracles. Prepare
requires h5py/NumPy; run and summarize need PyArrow/NumPy but do not import h5py.
A fresh trace directory contains `small/` and `gwosc/`, each with four fixed
requests, stable content-derived sample IDs, and canonical value oracles.

```sh
python tests/benchmarks/hdf5_query/run_h10.py prepare \
  --prepared /absolute/h08-prepared --output /absolute/h10-traces
python tests/benchmarks/hdf5_query/run_h10.py recording \
  --probe /absolute/build/bin/h10_profile_probe \
  --trace /absolute/h10-traces/gwosc/manifest.json \
  --output /absolute/h10-recording-p2 --parallelism 2 \
  --cache-bytes 67108864 --max-call-ranges 32 --expect-resident
python tests/benchmarks/hdf5_query/run_h10.py summarize \
  --trace /absolute/h10-traces/gwosc/manifest.json \
  --run /absolute/h10-recording-p2 --output /absolute/h10-rechecked.json
python -m unittest discover -s tests/benchmarks/hdf5_query -p test_h10_runner.py -v
```

The native probe accepts positional `FILE OUTPUT TRACE.tsv PARALLELISM
CACHE_BYTES MAX_CALL_RANGES`. It ingests into a recording **memory** B/L backend;
its timing is not native FS throughput. Parallelism accepts 1/2/4.
`--max-call-ranges 1` disables multi-range source transport; `32` enables the
production bounded multi-range interface. `--evict-before-request` flushes
reclaimable decoded entries before every request; within-request caching remains
active. This is not cache-off. Omit `--expect-resident` for capacity-limited runs;
warm redecodes are then reported instead of incorrectly failing the run.

For live measurement, use an isolated freshly started H10 daemon and an exact
indexed source version whose bytes match the trace. Root's H09 fixture or normal
mount ingestion can supply it. Configure `IOTDBFS_QUERY_PROFILE_JSONL` before
startup and retain that JSONL and daemon configuration. No other client may use
that runtime during the sequential trace:

```sh
python tests/benchmarks/hdf5_query/run_h10.py live \
  --trace /absolute/h10-traces/gwosc/manifest.json \
  --output /absolute/h10-live-gwosc --mount /absolute/isolated-mount \
  --socket /absolute/private/query.sock --profile /absolute/profile.jsonl \
  --source-path /h09-public.hdf5 --version EXACT_VERSION \
  --fresh-runtime --expect-resident
```

Use a new runtime for each source/configuration requiring a first-request cache
condition. `--fresh-runtime` is an explicit environment declaration, not an OS
cache-drop operation. Source SHA verification reads ordinary mounted bytes
before the trace, so neither first requests nor mapping faults imply cold disk.
The runner separates mmap establishment, page touching and fault counts, Arrow
reconstruction, NumPy copy, validation, and a later already-touched ordinary
read. Recording uses a local IPC artifact; live uses the actual FUSE route.

`summary.json` is accepted only with `passed: true` after exact ordered trace
coverage, hash, artifact, source/version and metrics checks. It preserves nested
and concurrent stages rather than adding them as disjoint wall time. Raw files
remain necessary evidence. Missing/duplicate/wrong samples and changed output
bytes fail validation. Encoded-only cache and true cache-off are not configured.
See [H10 measurement contract](../../../docs/knowledge/hdf5-query-profile.md).

## H11 controlled A–E benchmark

[Protocol and interpretation](../../../docs/reports/hdf5-query-benchmark.md) define the
HDF5-specific A–E mapping, owned-array timing, repeated campaigns, cache budgets,
and limitations. Build `h11_query_probe` from the root test-enabled build. Python
uses the existing h5py/NumPy environment plus PyArrow for E.

```bash
python run_h11.py prepare --public-file /data/H-H1_LOSC_4_V2-1126259446-32.hdf5 \
  --output /data/h11-prepared
python run_h11.py run --prepared /data/h11-prepared/manifest.json \
  --deployment /data/h11/deployment.json --probe /build/bin/h11_query_probe \
  --output /data/h11/base-c0/run --campaign-offset 0 --campaigns 1 \
  --paths ABCDE --clients 1 --passes 4 --cache-bytes 67108864 \
  --parallelism 1 --max-call-ranges 32 --target-batch-rows 4096 --fresh-runtime
python run_h11.py summarize --run /data/h11/base-c0/run \
  --output /data/h11/base-c0/run/revalidated-summary.json
python run_h11.py aggregate --campaign /data/h11/campaign.json \
  --output /data/h11/aggregate.json
python -m unittest discover -s . -p 'test_h11_runner.py' -v
```

Run these commands from this directory. `prepare` and `run` require fresh output
directories. The public source is verified against the locked original SHA256;
synthetic source hashes and every selection's independent h5py oracle are frozen
in the prepared manifest.

A deployment JSON has protocol `iotdbfs-hdf5-h11-deployment/1`, `config`, `mount`,
`socket`, `profile`, and a `sources` array of `{id,logical_path,version,fuse_path}`.
The parent `h11_campaign.py --help` describes seeding, byte verification and fresh
daemon setup. Use `--fresh-runtime` only when a new mount runtime actually exists;
E supports one independent campaign per invocation. Never point a destructive
campaign lifecycle at an unrelated existing mount.

`--scenario pressure --paths BD --cache-bytes 4194304` runs full datasets exceeding
the decoded working budget. `--scenario catalog` checks production numeric metadata
candidate selection against a prebuilt native-file catalog. B/D also support
`--cache-policy evict_before_request|encoded`; both remain bounded diagnostic
policies, not a true decoded-cache bypass. Each concurrent wave repeats the same
selection across clients; disjoint-client concurrency is not covered.

The native CLI accepts `h11_query_probe MANIFEST.json OUTPUT.json`. A/C use separate
processes with divided total cache budgets; B/D share a runtime. Keep the generated
worker reports and E `server-profiles.json`: offline summary validates the raw
evidence and rejects incomplete or altered results. Campaign aggregation requires
all `expected_runs` and treats campaign warm means as independent observations.

## Scientific catalog and OISST A–E timing

The [matched workload contract](../../../docs/reports/hdf5-scientific-query-benchmark.md)
uses seven unchanged NOAA OISST daily netCDF-4/HDF5 files. Build the opt-in
`sc_perf_native` and `sc_perf_direct` targets in a test-enabled root build. An
existing isolated `h09probe...` database and its writer lock are required; the
runner publishes only new unique paths and uses private mounts.

```bash
cmake --build /build --target sc_perf_native sc_perf_direct -j 4
python sc_perf_campaign.py --build /build --output /data/sc-perf \
  --database h09probe_EXACT --source-pid EXISTING_DAEMON_PID \
  --writer-lock /data/isolated-h09/namespace.lock \
  --campaigns 3 --passes 4 --raw /data/oisst/oisst-avhrr-v02r01.202401*.nc
python summarize_sc_perf.py /data/sc-perf
python sc_catalog_compare.py --campaign /data/sc-perf --build /build \
  --output /data/sc-perf-catalog
```

The Python environment needs h5py, NumPy and PyArrow. No credentials enter the
reports: the runner inherits connection settings from the explicitly selected
daemon's environment. `sc_perf_campaign.py` writes every raw observation, exact
published version, source SHA-256, catalog response and typed server profile;
`summarize_sc_perf.py` rejects incomplete or mismatched A–E matrices. The
separate broad catalog control drops the exact `/sst` path prefilter, scans all
datasets in the native and prebuilt-FS baselines, and tests both a positive and
a no-match attribute predicate. Source publication, index creation and
catalog construction are separate setup costs.

## GEOS pressure-coordinate production acceptance

The [two-time GEOS report](../../../docs/reports/hdf5-geos-production-multitime.md)
uses `geos_pilot.py --passes 0` to publish two public `inst3_3d_asm_Np`
files through the production mount and record their exact source versions.
`geos_multitime_semantic.py --pilot-dir <published-pilot> --daemon <iotdb_fs>
--writer-lock <lock> --output <new-output> --repeats 3` then discovers `/T`
across both files, executes physical Float32 values with validated time,
pressure, latitude and longitude slices, and compares every IPC row with
h5py's dimension-scale oracle. It saves the raw report and server profiles.
