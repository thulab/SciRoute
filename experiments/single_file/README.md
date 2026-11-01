# Single-file FS / IoTDB-FS preparation

Generated JSON/JSONL, logs, manifests and compressed result rows under `results/` are local/server-only as of 2026-10-24. Handwritten result summaries remain versioned. The original 66 generated files are preserved in `/data_01/zzy/iotdb-fs-exp/document-archives/20261024-pr7-content-curation-6e63794/files.tar.gz`, with a per-file SHA-256 manifest alongside; historical analysis requires restoring those inputs at their original paths.

This experiment measures the basic cost of writing one HDF5 file and reading
local subsets. The FS baseline uses native HDF5 without an external index or
sidecar. The objective is to measure IoTDB-FS's additional work, not to prove
an indexing advantage on a single file.

Environment scripts, controlled data, logs and results belong under this
directory on both the workstation and experiment server. Large data/builds
stay in ignored `work/`; small accepted manifests and reports are copied to
`results/`. Nothing is written to `docs/results/`. Existing mounts and dependency
prefixes are preserved.

GRIB readiness is checked separately by `grib_readiness.py`, using an existing
published multi-message source and a native ecCodes oracle. The accepted
2026-10-12 pilot (local archive: `experiments/single_file/results/grib-readiness-20261012/RESULTS.md`) verifies message
pruning, exact encoded bytes, numerical crops and fixed-query reuse. It does
not run the HDF5 timing reader or claim a formal GRIB FS/IoTDB-FS comparison.

## Frozen input

`workload.json` describes **controlled synthetic data**, not a NASA GEOS
product. All tiers contain one `/T` dataset with little-endian Float32 values,
four axes, chunks `[1,1,91,144]`, shuffle, gzip level 1, and no Fletcher32.
Spatial shape is `[728,1152]`; the single channel axis has extent 1. Only the
time extent changes: small 2, medium 160, large 1024. Every chunk is allocated
and contains deterministic finite Float32 bit patterns. This avoids changing
chunk edge geometry, codec or compressibility between tiers.

Actual file-size gates, checked after writing:

- small: at most 10 MiB;
- medium: 0.3–0.7 GiB;
- large: at least 2 GiB.

File padding, sparse holes, unallocated fill chunks and all-zero compression
are not substitutes for data. Gates fail explicitly; the script does not tune
sizes after seeing results. The small test workload is marked non-production
and cannot be accepted by the campaign runner.

All tiers use the same point, small ROI and cross-chunk ROI coordinates and
return identical prefix values. The manifest therefore records full source
shape as well as C-order value/mask SHA-256. Readers must verify both; a correct
small slice hash alone cannot identify the selected size tier.

Generation, source fsync, oracle construction and whole-source SHA are offline
preparation. Source hashing reads the whole file and warms OS pages. A first
query is a fresh reader/runtime observation, **not cold disk I/O**.

## HDF5 1.14.4 environment

The production CMake/CI lock now requires runtime **1.14.4 EXACT**. Its official
maintenance source archive is `hdf5-1.14.4-3.tar.gz`; the `-3` archive suffix is
not a different runtime patch version.

`environment.py` builds an isolated shared, thread-safe, serial, DEFLATE-enabled,
core-only library. The daemon, bounded scale worker and timed native FS probe
must resolve the same library path and SHA. Dynamic plugins remain disabled.

The offline generator/oracle uses the h5py 3.12.1 wheel, whose bundled HDF5 must
also report 1.14.4. It is not the timed FS reader. Its bundled library paths and
hashes are recorded separately; they are not claimed byte-identical to the
thread-safe measured core. NumPy is pinned to 2.1.3 and PyArrow to 21.0.0.
This avoids enabling the upstream unsupported high-level/thread-safe build
combination merely to prepare input data.

Launch Python with `LD_LIBRARY_PATH` and `PYTHONPATH` unset. C++ Arrow is an
IPC-focused build and must not shadow the PyArrow wheel's libraries. The
environment verifier checks these roles independently; the runner passes the
C++ library path only to the daemon/native child processes.

The source archive SHA is
`019ac451d9e1cf89c0482ba2a06f07a46166caf23f60fea5ef3c37724a318e03`.
See the [official release](https://github.com/HDFGroup/hdf5/releases/tag/hdf5_1.14.4.3).
Old 1.14.6 artifacts and historical performance reports are not overwritten.

## Server preparation

Use `/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/single_file` as the server
experiment root. The main server repository copy is an older source deployment;
provide a fresh complete source snapshot, including the current uncommitted
dependency changes and this experiment, as `work/<run>/source`.

```bash
env -u LD_LIBRARY_PATH -u PYTHONPATH python3 environment.py build \
  --source-dir "$PWD/work/<run>/source" --work "$PWD/work/<run>" \
  --arrow-prefix /data_01/zzy/iotdb-fs-exp/public-arrow-baseline/deps/arrow-prefix \
  --sdk-root /data_01/zzy/iotdb-fs-exp/sdk/iotdb-session-cpp --jobs 6

env -u LD_LIBRARY_PATH -u PYTHONPATH work/<run>/venv/bin/python prepare_data.py --output "$PWD/work/<run>/data"
env -u LD_LIBRARY_PATH -u PYTHONPATH work/<run>/venv/bin/python -m unittest -v test_prepare_data.py test_runner.py
```

`environment.py verify` uses the same `--work`, `--arrow-prefix` and `--sdk-root`
arguments. It rechecks the frozen source, binary/library hashes and runtime
versions. Build/dependency logs and temporary files remain inside `work/`.

## Measurement runner

```bash
env -u LD_LIBRARY_PATH -u PYTHONPATH work/<run>/venv/bin/python runner.py \
  --manifest "$PWD/work/<run>/data/manifest.json" \
  --environment "$PWD/work/<run>/env.json" \
  --source-pid 1001546 --database geostime20261007 \
  --writer-lock /data_01/zzy/iotdb-fs-exp/public-arrow-baseline/geos-time-20261007/writer.lock \
  --output "$PWD/work/<run>/smoke" --smoke
```

The trusted source PID must be a currently running owned `iotdb_fs` daemon.
Connection settings are read from its environment/config in memory, never
written into the evidence. Each campaign uses a new logical prefix in the
existing isolated database. No GRIB baseline or old published source is removed.
The local writer lock does not replace coordinating writers on other hosts.

Without `--smoke`, the runner requires all tiers and Q=1/10/100, with three
campaigns by default. It reverses FS/IoTDB write and query wave order between
campaigns. `--smoke` runs only small, Q=1/2, one campaign; it proves preparation
and result correctness, not formal scale performance.

For the formal campaign, use the same arguments with a fresh output such as
`--output "$PWD/work/q100-<date>"` and omit `--smoke`. `summarize.py --campaign
<campaign.json-or-json.gz> --output <results-directory>` validates the three
campaigns' actual Q totals and writes the aggregate JSON and Markdown report.
Gzip archives retain every raw observation while keeping accepted evidence
small; file copies and private logs remain in ignored `work/`.

Both write arms use the same bounded 1 MiB copy loop, explicit **file fsync**, and
close. The FS result is a kernel fsync acknowledgement; IoTDB-FS fsync commits
and awaits backend publication acknowledgement. Directory setup is outside
the write timer. Database WAL/sync policy is not changed, so these acknowledgements
are not claimed to prove equivalent power-loss durability.

The FS query timer includes H5Fopen/H5Dopen, physical selection, H5Dread, owned
copy, and close on every query. The probe's process startup/library initialization,
hashing and JSON output are excluded and its process wall time is reported
separately. No external index or preloaded coordinate table is used.

Every IoTDB-FS query performs a new control registration/materialization and then
opens, decodes and copies the complete IPC result. It includes request JSON,
socket, authorization, catalog/manifest checks, planning, payload/decode,
encoding, publication and FUSE consumption. Re-reading a previously published
IPC file is not counted as another query. Exact version binding after publication
is timed separately and included once in lifecycle totals.

For each tier/case, the runner starts a fresh daemon/runtime, then retains it for
the repeated requests. Native file handles close on each request; OS caches and
the IoTDB server cache are not reset. Raw-file prefetch/cache is capped at 1 B;
typed decoded cache is 256 MiB, p=1, permission/attribute TTL=1 s. These policies
are recorded rather than hidden.

Results contain the first request, subsequent within-case p50/p95, actual
prefix sums for Q=1/10/100 and `write + one binding + Q complete queries`.
Nested server stages are diagnostic and are never summed into total wall time.
Each case is an independent scenario; adding its lifecycle total to another
case would count the same write twice. A median ratio of at least 10 is recorded
as a failed performance target, not discarded or treated as a correctness failure.

## Bind-once paired experiment

`session_runner.py` repeats the reopen/re-register baseline and adds a fixed
query session. The same publication is shared by both modes within a campaign.
Native FS keeps H5F/H5D handles and its native chunk cache open; IoTDB-FS binds
one exact source/version and one PreparedQuery. Each read still executes a
cursor, encodes IPC, publishes a route and consumes/copies the result through
FUSE. A content-addressed result ID may repeat for identical bytes; new route
identities and execution/encoding counters verify actual fresh execution.

The experimental socket operations are `experiment_typed_bind`,
`experiment_typed_read` and `experiment_typed_release`. They are disabled on
ordinary mounts. The runner sets `IOTDBFS_EXPERIMENT_FROZEN_TYPED=1` only for
bound daemons; the server additionally requires root/private 0600 access, one
exact version, a fixed subset, one active binding, a five-minute absolute TTL,
and at most 1,024 results. During the experiment, source bytes, visibility and
permissions must stay unchanged. This is not a production permission cache or
a varying-coordinate source session.

For each Q=1/10/100, bound mode uses an independent native session and fresh
daemon. It measures its own bind + exactly Q reads + close/release, including
Q=1. Release retires all of that session's result routes. Both mode and arm
orders alternate across three campaigns. Each arm contains 2,700 baseline
reads and 2,997 bound reads. The default command is:

```bash
env -u LD_LIBRARY_PATH -u PYTHONPATH work/<run>/venv/bin/python session_runner.py \
  --manifest "$PWD/work/prep-20261012/data/manifest.json" \
  --environment "$PWD/work/<run>/env.json" \
  --source-pid 1001546 --database geostime20261007 \
  --writer-lock /data_01/zzy/iotdb-fs-exp/public-arrow-baseline/geos-time-20261007/writer.lock \
  --output "$PWD/work/session-q100-<date>"
```

Add `--smoke` for the small tier/Q=1,2, one campaign. `session_summary.py` accepts
the complete formal JSON or gzip archive and produces paired read/session and
publication-to-query lifecycle tables. It verifies actual Q counts, binding
and release accounting, and zero catalog/planning in bound reads. First reads
may perform payload SQL/decode; warm bound reads must perform neither. Profile
collection waits for asynchronous JSONL completion outside request timing.

Value hashes, masks, full and selected shapes, dtype and exact source/version
are verified outside query timing. Full FS and exact-version IoTDB-FS payload
SHA are checked after timing. All mounts/processes created by the runner are
closed in `finally`; failed runs retain an incomplete campaign, not a success.

## Preparation results

Accepted source identities, environment and smoke reports are placed in
`results/` after server validation. Raw logs, private mount logs and large
payloads remain under the server `work/` tree. Reports should distinguish
dependency/build acceptance, preparation smoke, and a formal Q=1/10/100 run.

- Preparation acceptance, 2026-10-12 (local archive: `experiments/single_file/results/preparation-20261012/RESULTS.md`)
- Three paired Q=1/10/100 campaigns, 2026-10-12 (local archive: `experiments/single_file/results/q100-20261012/RESULTS.md`)
- Reopen versus bind-once paired campaigns, 2026-10-12 (local archive: `experiments/single_file/results/session-q100-20261012/RESULTS.md`)

The formal campaign passed all correctness checks with 2,700 queries per arm;
none of the nine cases met the <10x paired median query latency target. The
report keeps query, publication and complete lifecycle costs separate.

The preparation and formal single-file campaign do not introduce version
compression, distributed filesystem tests or new query optimizations.

## Native GRIB reader

`native/grib_native_probe.cpp` provides an independent ecCodes filesystem
reader. It needs ecCodes >=2.34 and OpenSSL; its standalone build does not need
HDF5, Arrow, FUSE or IoTDB. The original native build also includes this target
at `grib/grib_native_probe`, while its existing HDF5 `native_probe` is unchanged.

```bash
cmake -S experiments/single_file/native/grib \
  -B experiments/single_file/work/grib-native/build -DCMAKE_BUILD_TYPE=Release
cmake --build experiments/single_file/work/grib-native/build -j 4
experiments/single_file/work/grib-native/build/grib_native_probe --versions
experiments/single_file/work/grib-native/build/grib_native_probe \
  --file /path/to/raw.grib2 --view values \
  --short-name clwmr --type-of-level isobaricInhPa --level 500 --step 0 \
  --lat-range 0,0.75 --lon-range 0,0.75 --repeat 100
```

Use `--view binary` without coordinate ranges to return encoded-message
digests. Omitted filter fields impose no restriction; supplied equality terms
are combined with AND. All matching messages are retained in source order.
Numeric conditions run before expensive ecCodes concept/string resolution.
No external index or sidecar is created: ordinary queries reopen and scan
native headers each time, and only selected messages undergo explicit payload
reads. The library's metadata I/O bytes are not measured; that field is `null`.

Add `--bound` for the agreed frozen-file experiment: bind scans metadata once
and retains the file descriptor and selected physical ranges. The first query
loads encoded bytes and, for values, complete decoded Float64 fields. Later
queries reuse those buffers but freshly crop, convert to Float32 and copy each
owned output. No final window/result is cached. All selected data must fit the
working budget; failure is explicit rather than silently evicting or bypassing
the budget. This experiment cache is implemented in the FS reader and declared
as such, rather than attributed to native ecCodes automatically. In particular,
this was a stronger binary baseline than the pre-optimization typed GRIB
BinaryView in the original paired campaign. Current BinaryView reuses bounded
encoded fragments; the cache ablation below records that change separately.

One JSONL record is emitted per actual query. It separates file open,
discovery, payload read, decode, selection/crop, owned copy, close and residual
time; their sum equals `total_ns`. In bound mode `session_bind_ns` and its
open/discovery/residual sub-stages are charged only on the first record, and
`session_close_ns` only on the last. A session costs
`sum(session_bind_ns) + sum(total_ns) + sum(session_close_ns)`. Q=1/10/100 must
be separately executed sessions when comparing complete session costs.
Hashes, diagnostic JSON and process/library setup stay outside query timers;
the acceptance harness records process wall time separately.

Results include candidate/selected-message counts, offsets/lengths, explicit
payload-read and decode counts, cache hits/misses and resident bytes, plus
per-message and concatenated value/coordinate/mask SHA-256. Values use Float32,
coordinates Float64 and native row-major order. Physical validity is all ones,
matching the present typed GRIB contract: ecCodes bitmap sentinels are retained
as values, not reinterpreted as NULL.

Limits are configurable with `--max-result-bytes` (default 64 MiB),
`--max-working-bytes` (256 MiB) and `--max-candidates` (100,000). The working
budget covers reader buffers/cache and estimated native message copies; it is
**not** an allocation cap on ecCodes internals, definitions or temporary
unpacking workspace. Use trusted experiment inputs. Source framing is checked
through native offsets/lengths and file bounds; native headers-only behavior
is preserved rather than re-reading every footer. Values are limited to
single-field regular_ll grids with ordinary row scanning and increasing
longitude; wrapping/normalization, reduced grids, projection transforms and
nonstandard scan orders are explicitly unsupported. Binary mode returns whole
physical messages.

`grib_native_validation.py` verifies the reader against a separately generated
native message directory and raw-source/numerical oracles:

```bash
python3 experiments/single_file/grib_native_validation.py \
  --probe /path/to/grib_native_probe --raw /path/to/small-GFS.grib2 \
  --bound --output experiments/single_file/work/grib-native-acceptance
```

The validation environment needs NumPy and PyArrow (the existing offline/IPC
venv suffices). macOS may need `DYLD_FALLBACK_LIBRARY_PATH` set to its ecCodes
library directory for the ctypes oracle. Accepted evidence and the exact
server library identity are in GRIB native reader acceptance (local archive: `experiments/single_file/results/grib-native-20261012/RESULTS.md`).
This acceptance does not constitute the paired GRIB FS/IoTDB-FS benchmark.

On Linux, `grib_native_environment.py --probe <binary> --daemon-environment
<existing-env.json> --acceptance <acceptance-report.json> --output <lock.json>`
verifies the probe against the daemon's exact ecCodes library bytes and records
the compiler, actual flags, binary identity and source hashes.

## Paired GRIB campaigns

`grib_paired_runner.py` uses two existing GFS GRIB2 inputs: 2,446,828 bytes /
22 messages and 549,606,683 bytes /4,598 messages. Both select one
`clwmr/isobaricInhPa/level=500/step=0` message from a 1440×721 grid with
16-bit complex spatial-differencing packing. These are real product files,
not a synthetic byte-identical size gradient: their selected encoded lengths
and values are independently validated per tier.

The three cases are encoded-message retrieval, a numerical point, and a
numerical 4×4 ROI. Each runs ordinary reopen/re-register mode and the agreed
fixed-query bound mode, with independent Q=1/10/100 sessions in three paired
campaigns. This produces **3,996 reads per arm**. Campaign order is FS/B,
B/FS, FS/B; mode order is similarly reversed in the middle campaign. Each
case/mode/Q gets a fresh daemon and native process; OS/database caches remain
warm. No source/permission mutation is allowed during a fixed session.

```bash
python3 experiments/single_file/grib_paired_runner.py \
  --environment /path/to/verified-daemon-env.json \
  --native-environment /path/to/grib-native-environment.json \
  --raw-dir /path/to/GFS/raw \
  --writer-lock /path/to/isolated-namespace.lock \
  --output experiments/single_file/work/grib-paired-campaign
python3 experiments/single_file/grib_paired_summary.py \
  --campaign experiments/single_file/work/grib-paired-campaign/campaign.json \
  --output experiments/single_file/results/grib-paired-campaign
```

The runner uses existing isolated database `h09probe1790939889dbfc1f` and
unique source prefixes, preserving GRIB baseline data. `--smoke` runs one
small-file campaign with Q=1/2 and is rejected as formal evidence. Both arms
publish through the same 1 MiB copy/fsync/close operation. B pays GRIB layout
ingestion and one minimal metadata-only exact-version locator. No full
message-attribute IPC is transferred just to identify a source version. Source
bytes on FS and the exact IoTDB-FS version are SHA-checked after query timings.

The FS reader has no external index. Its bound working budget is 64 MiB;
the adapter's decoded-field cache is also 64 MiB, but its encoded-message and
index caches are separate services and total RSS is not equated. This workload
selects one field, avoiding capacity-driven eviction on either side. Warm B
numerical and current BinaryView reads should perform no payload SQL. Encoded
fragments share the mount's QueryRuntime budget; every logical request still
charges its selected message ranges on cache hits. Historical paired results
retain their original uncached BinaryView policy.

The client uses bulk Arrow-to-NumPy conversion and independent owned arrays;
result mapping, conversion and close are included in B query time. Value,
coordinate, mask and selected shape checks run afterward. A new result route
and positive cursor/IPC encoding counters are required for every read.
Message pruning is established by requested/returned payload bytes and exact
message hash, not by the public runtime's GRIB-excluding decode counter or by
claims about database physical disk bytes. `grib_paired_summary.py` validates
every actual Q, session closure, oracle and lifecycle total before producing
aggregate reports. Partial campaigns stay in ignored `work/`.

The accepted three-campaign GRIB results (local archive: `experiments/single_file/results/grib-paired-q100-20261012/RESULTS.md`)
and interpretation (local archive: `experiments/single_file/results/grib-paired-q100-20261012/INTERPRETATION.md`) preserve
all 3,996 reads per arm. They distinguish repeated message-discovery benefits
from faster bound FS reads and the persistent-index publication cost.

The subsequent BinaryView cache acceptance (local archive: `experiments/single_file/results/grib-binary-cache-20261012/RESULTS.md`)
pairs the original daemon with a rebuilt daemon on the same existing exact
versions, without publishing sources again. Three campaigns cover both size
tiers and ordinary/bound modes, 100 reads per session: 2,400 total observations.
Warm payload SQL becomes zero in both modes; the original uncached results
remain unchanged. `grib_binary_cache_ablation.py` records setup/release separately
and checks each fresh cursor/IPC result, hash, actual I/O and cache hit/miss.
The shared service covers encoded buffers; decoded GRIB field retention stays
separate. Full Linux unit acceptance contains 1,459 passing tests, including
multi-source logical-budget enforcement even for encoded cache hits.

## Cross-format timing audit

HDF5 / GRIB comparison matrices (local archive: `experiments/single_file/results/cross-format-audit-20261012/RESULTS.md`)
recompute all accepted raw records, including historical HDF5, without rerunning
queries. The primary HDF5 bind-once and GRIB paired campaigns use the same daemon
binary. Warm medians are standardized to pooled post-first Q=100 observations;
Q totals, once-only binding and publication-to-query totals remain separate.
The audit verifies labels, phase partitions, enclosing timer bounds, source
artifact hashes and exact reproduction of all three stored summaries.

```bash
python3 experiments/single_file/cross_format_comparison.py \
  --output experiments/single_file/results/cross-format-audit-20261012/audit.json
```

The matrices explicitly preserve HDF5 unbound Q-prefix versus independent-Q
session sampling, unequal physical/output work and cache policies. The HDF5
unbound `native_probe_process_wall_ns` diagnostic includes harness JSON parsing
and validation after subprocess completion; its formal query totals instead use
the native C++ timers and are unaffected. No historical timings are overwritten.
