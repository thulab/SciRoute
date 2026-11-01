# Controlled multi-file data manager

> 正式性能采集统一使用 [FM1 固定矩阵](../fixed_matrix/README.md)。历史数据保留追溯，不作为新基线；后续优化复用已封印 corpus/case，新增实验组先确认并登记。

> 当前正式代码仅提供 [`native-hdf5` / `iotdb-indexed`](../../docs/contracts/hdf5-query-modes.md)。本目录历史 runner、arm 和计时结果保留当时口径；含旧 catalog 设置或 frozen-typed 请求的 runner 必须配合其冻结二进制运行，不能直接用于当前二进制。归档说明见 legacy (local archive: `experiments/legacy/README.md`)。

`manager.py` prepares and verifies raw experimental corpora independently of
IoTDB ingestion and online query timing. HDF5 and GRIB use the same explicit
N/K/D/depth contract. The initial feasibility audit (local archive: `experiments/multi_file/feasibility-20261013/README.md`)
records the existing public sources and why regrouping alone cannot supply
all independent gradients.

## Strict experiment identities

New maintained-runner work uses the [experiment identity contract](../../docs/contracts/experiment-identity.md).
A corpus records the **complete independently published population**; a case
records its query subset, version window, predicate, ROI and output. In
particular, four complete buckets selecting 128 paths from the route3 N512
corpus are labelled **N512/B4/C128**, never an independent N128 corpus.

From the repository root, register an existing manifest and explicit accepted
publication bindings before constructing cases:

```sh
python3 -m experiments.identity import-corpus --registry REGISTRY \
  --format multi-file --manifest EXISTING_MANIFEST \
  --expected-sha256 ACCEPTED_BYTE_SHA256 --publication PUBLICATION_EVIDENCE
python3 -m experiments.identity case --registry REGISTRY \
  --corpus-id CORPUS_ID --request REQUEST_JSON
python3 -m experiments.identity run --registry REGISTRY --document RUN_JSON
python3 -m experiments.identity compare --registry REGISTRY \
  --left BASELINE_RUN_ID --right CANDIDATE_RUN_ID
```

These commands only manage evidence files. Raw ordinals are not database
versions, and raw files alone do not prove publication. Missing binding evidence
is retained as unbound. Missing native mask hashes are not synthesized; an old
HDF5 value oracle therefore cannot certify a value-plus-mask case without
recorded mask evidence.

The maintained `small_runner.py` strict gate and frozen historical replay
scripts have distinct roles. Future execution must pass the maintained gate
before publication/startup/timing. Supply `--identity-registry REGISTRY` and
`--identity-cases CASE_BUNDLE` together with `--reuse-preparation` and
`--reuse-work`. The bundle has exactly `grib` and `hdf5` entries, each mapping
`discover` and `discover-values` to registered case IDs. The runner accepts full
independent stored-V1/Latest corpora, and its values phase requires both value
and mask oracles. An old HDF5 corpus lacking native mask hashes cannot fill
that bundle slot; it fails before connecting or mounting. `--prepare-only`
keeps the existing data-generation and database-publication workflow and does
not collect query measurements; it is not an offline-only command.

A reused publication may be queried with a different, independently verified
daemon binary; both preparation and query binary hashes are recorded. The
native worker must remain identical while its existing FS-index is reused.

Historical recipes and timing descriptions
below retain their original scope and frozen binaries; legacy output does not
become eligible for strict comparison automatically. `map-history` can add an
explicit hash-bound external interpretation while preserving the original
artifact. Registry identity checks do not replace the runner's oracle, timing
or original-service-restoration checks.

## Contract

| Parameter | Meaning |
|---|---|
| N | Complete independent logical-file population, one experimental version per path |
| K | Files with a match; each matching file has exactly one matching unit |
| D | Data datasets per controlled HDF5 file, or messages per GRIB file |
| depth | Directory components below `raw/`, excluding the filename |

The scalar equality query is part of each frozen spec. Controlled generation
uses a fixed compound predicate: `shortName=t`, `typeOfLevel=isobaricInhPa`,
`level=500`, `step=0`, `validityDate=20261013`, `validityTime=0`. Background units
have partial matches but never satisfy the complete predicate. The first K
logical files contain one target unit each; opaque physical leaf names and
hash-bucket directories do not expose that assignment. This first tree policy
is an explicitly controlled arrangement, not evidence about semantic filenames
in public archives. Width, actual directory count, leaf occupancy and path
length are also recorded, because changing depth changes more than path length.

For public HDF5 inventory, `--dataset` explicitly identifies the data units.
Coordinates and metadata objects excluded from that list do not contribute to
D; `total_dataset_count` still records every dataset. Inventory can describe
irregular files or files with multiple matches; a generated/regrouped corpus
must meet the regular N/K/D contract. GRIB inventory covers every complete
message. Duplicate content IDs are reported through `unique_unit_count` and
are not additional independent scientific observations.

## Commands

| Command | Effect |
|---|---|
| `plan` | Validate one spec and show logical payload estimate and paths |
| `generate` | Build dense controlled HDF5 arrays or native ecCodes messages |
| `inventory` | Freeze external sources, metadata, hashes and native value oracles |
| `regroup` | Split/merge controlled HDF5 datasets or copy raw GRIB message bytes |
| `reorganize` | Copy complete files byte-for-byte into another external tree |
| `verify` | Verify a corpus or complete collection: re-read sources and check hashes, native values, dimensions and candidate tree |
| `gradient` | Freeze a one-axis N/K/D/depth plan and all its specs |
| `materialize` | Build every planned variant and publish a collection index |

`generate` allocates every HDF5 chunk. The shape, chunks, Float32 type,
shuffle and gzip level are fixed by the spec. Values use a stable per-unit
PCG64 seed and finite Float32 bit patterns. No synthetic GEOS/CF coordinate
binding is implied. HDF5 units have bounded fixed-length strings and numeric
attributes which the existing dataset-attribute query interface can discover.

GRIB generation uses a regular latitude/longitude grid, native ecCodes
`grid_simple` packing, explicit bit depth, and nonconstant values. Native
re-reading verifies actual geometry and packing. Encoded message hashes,
decoded Float32 values, validity masks and geographic coordinates are frozen.
The generation oracle reflects actual quantized values, not pre-packing inputs.

Regrouping requires `payload: {"policy": "preserve"}`. It never repacks GRIB
messages or changes HDF5 dataset filters, dtype, shape or scalar attributes.
It selects K matching units and N×D−K distractors without replacement. It fails
when the pool is insufficient, and records both unused units and the full
source-to-output mapping. A different seed does not create new observations
when regrouping. Public HDF5 dataset regrouping is rejected; whole-file
`reorganize` preserves references and dimension scales through exact byte copies.

## Runtime and examples

Use HDF5 **1.14.4**, h5py **3.12.1**, NumPy **2.1.3** and native ecCodes.
The server already has the pinned Python runtime:

```sh
PY=/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/single_file/work/session-20261012/venv/bin/python
cd /data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/multi_file
```

Run the following commands with `LD_LIBRARY_PATH` and `PYTHONPATH` unset and
`HDF5_PLUGIN_PRELOAD=::`; dynamic HDF5 plugins are not needed. This offline tool
does not change the mount process or its permanent HDF5 plugin policy.

```sh
env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" manager.py plan \
  --spec specs/hdf5-smoke.json

env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" manager.py generate \
  --spec specs/hdf5-smoke.json --output work/hdf5-base

env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" manager.py regroup \
  --input work/hdf5-base --spec specs/hdf5-regroup-smoke.json --output work/hdf5-merged

env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" manager.py reorganize \
  --input work/hdf5-merged --depth 8 --output work/hdf5-depth8

env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" manager.py verify \
  --input work/hdf5-depth8
```

The examples generate `(N,K,D,depth)=(8,2,4,2)`, regroup all 32 units into
`(4,2,8,4)`, then copy the complete files to depth 8. The equivalent GRIB specs
are `grib-smoke.json` and `grib-regroup-smoke.json`.

For an independent N gradient, prepare a base spec with enough raw-byte budget
for its largest N. `K≤N` and N×D bounds are checked for every point before the
plan is published. Do not assume the smoke spec's 16 MiB budget is suitable for
N=1,024.

```sh
env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" manager.py gradient \
  --base-spec specs/hdf5-smoke.json --axis N --values 4 8 16 --output work/n-plan

env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" manager.py materialize \
  --plan work/n-plan --mode generate --max-total-bytes 67108864 --output work/n-data
```

For a depth gradient, use `--mode reorganize --input work/hdf5-base` so files
remain byte-identical. K or D regrouping plans use `--mode regroup` and require
a sufficiently large pool. Plans do not silently duplicate scarce units.

For real GRIB, run `inventory --format grib --source ORIGINAL` (repeat `--source`
for additional files), with a query JSON appropriate to the source dates and
variables. Use the same query in the preserve-layout regroup spec. Native GRIB
inventory decodes all units to build numeric oracles and can take substantial
time on the full ERA5 month; it is an offline step. Public HDF5 inventory also
streams the physical values of explicitly selected datasets. Neither command
builds or imports an IoTDB index.

```sh
env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" manager.py inventory \
  --format hdf5 --source ORIGINAL.nc4 --dataset /T --query-file query.json \
  --max-source-bytes 1073741824 --output work/public-inventory
```

## Frozen artifacts and operating limits

Each corpus has `manifest.json` under `iotdbfs-multifile-data/1`. It stores the
normalized spec, actual file/unit match counts, physical schema and filters,
source SHA-256, unit IDs, value oracles, tree statistics, native versions,
lineage and offline phase timings. The manifest checksum covers all fields,
including preparation timings, and therefore identifies a particular frozen
preparation record. Repeatable scientific content is checked using per-unit
IDs and native value/coordinate/mask hashes. Preparation timing is not part of
a unit's identity.

Gradient plans use `iotdbfs-multifile-gradient/1`; materialized collections use
`iotdbfs-multifile-collection/1`. `collection.json` maps axis values/spec IDs
to each corpus manifest and actual dimensions, with a frozen `plan.json` copied
into the collection. `verify --input COLLECTION` checks the plan, every member
and aggregate sizes. Raw bytes are stored only under
`work/`, which is ignored by Git. Specs, code and small acceptance evidence can
be versioned separately.

Limits are explicit: N≤4,096, D≤512, N×D≤262,144, depth≤8, at most 32 gradient
points, and bounded per-unit/row working data. Specs carry `max_output_bytes`
for **raw file bytes**; batch materialization also has a total raw-byte budget.
Manifest bytes and directory metadata are outside those raw-byte budgets;
manifests are capped at 256 MiB. Estimates exclude format metadata, and actual
sizes are checked during preparation. Unknown fields/unsupported protocols,
invalid scalar types, symlink escapes, extra raw candidates and changed source
bytes fail explicitly. Unsupported extended GRIB1 lengths and very large GRIB
decoding arrays are rejected by the native reader's bounds.

Writers use an exclusive local lock and a staging directory; finished artifacts
are renamed into place and never intentionally overwritten. A failed operation
removes only its own staging files. Regrouping and tree-copy input files remain
untouched. These locks coordinate this manager, not external writers or IoTDB
writers. Public sources must be kept stable while their inventory is in use.

Generation, inventory, grouping, input revalidation, output verification,
whole-file hashing and directory synchronization are offline work. Phase
durations in manifests exclude final manifest writing and artifact directory
publication. CLI acceptance records complete process wall time separately;
neither duration is an online query measurement. Source hashing warms OS pages
and must be accounted for by later cold/warm experiments.

N/K/D controls do not make IoTDB candidate pushdown automatically scale to N→K.
Production authorization, catalog budgets, index construction, ingestion and
FS persistent-index controls remain separate measured stages. Query templates
freeze predicates but contain no invented published version: bind each upload
to an exact real version before running an A–B query campaign.

## Verification

```sh
env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" -m unittest discover \
  -s . -p test_manager.py -v

env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" smoke.py \
  --output results/NEW-acceptance --work work/NEW-acceptance
```

The regression suite exercises corruption, predicate multiplicity, insufficient
pools, atomic budget failure, reproducible value identities, native HDF5/GRIB
regrouping, public HDF5 restrictions and one-axis collections. `smoke.py` also
supports `--raw-grib` for the existing small GFS file and `--raw-hdf5` for the
existing GW150914 H1 file. These optional public-source probes have explicit
product-specific queries; they are acceptance checks, not a general dataset
selection policy or performance runner.

Final server acceptance on 2026-10-13 passed **24 tests without skips** and
**24 CLI operations**, including real GFS message regrouping, GWOSC typed
attribute selection/whole-file copying, and depth=1/4/8 collection validation.
See [acceptance results](results/manager-acceptance-20261013/RESULTS.md).

## Historical small paired query runner

This section preserves the original pilot protocol and command recipe. Current
maintained-runner measurements additionally require the strict identity and
existing-publication arguments described above; the old recipe alone is not
an executable bypass of that gate.

`small_runner.py` runs the approved `(N,K,D,depth)=(8,2,4,2)` pilot with three
arms: native FS scan, an equivalent persistent SQLite attribute index, and the
production IoTDB-FS registration/query path. It records metadata discovery and
discovery followed by complete matching-unit reads separately. Each matching
unit has 4,096 Float32 values. There are three campaigns and five observations
per arm/phase/campaign; each campaign has a fresh native worker and private
daemon, while caches persist between its requests. Arm order rotates between
campaigns. Source pages are warm from preparation; no cold-disk claim is made.

The C++ reader enumerates the candidate tree and evaluates all six attributes;
GRIB scan uses ecCodes' header-only reader, then reads/decodes matching original
message ranges. The persistent SQLite index stores all units with a composite
predicate index, source size/mtime and root identity. Queries check selected
sources before reading. Neither FS arm retains decoded arrays. HDF5 native
reading links the same thread-safe HDF5 1.14.4 library as the daemon, with
dynamic plugins permanently disabled. The Python IPC reader owns its copied
Float32 arrays and validity masks. Source identities, full result hashes and
mask hashes are checked outside query clocks against the frozen manifest.

HDF5 typed reads require a single exact source/version, so the complete pilot
query consists of **one catalog request plus two typed requests**. GRIB can
read the selected sources together, requiring **one discovery plus one typed
request**. The report records these counts; they are not equal-RPC experiments.
Production freshness/authorization checks remain enabled. No frozen prepared
session or process-local result ID is reused for the online query arm.

Native FS clocks end at owned buffers; hashing, JSON and worker transport are
outside those clocks. The separately reported worker round-trip includes that
proof transport. IoTDB-FS clocks include registration, materialization and
FUSE/mmap/IPC reads into owned arrays, but exclude hashes and local profile-log
collection. Server stage timers are inclusive and must not be added together.
Preparation, explicit file-fsync/close publication, index construction, locator
binding and verification have separate measurements. Directory fsync is not
part of the file-publication timing. Publication is measured once per format.

Build and execute on the server using its pinned environment:

```sh
BASE=/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C
cmake -S "$BASE/experiments/multi_file/native" -B "$BASE/experiments/multi_file/work/runner-20261013/native-build" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH="$BASE/experiments/single_file/work/session-20261012/hdf5-prefix"
cmake --build "$BASE/experiments/multi_file/work/runner-20261013/native-build" -j2

env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" small_runner.py \
  --native work/runner-20261013/native-build/multifile_native \
  --environment work/runner-20261013/environment.json \
  --writer-lock /data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock \
  --output results/NEW-query --work work/NEW-query --campaigns 3 --repeats 5
```

The environment is the existing single-file build descriptor. Connection
credentials are read transiently from the authorized baseline daemon and are
not saved. The runner is restricted to the idle `h09probe1790939889dbfc1f`
namespace and a new opaque subtree; it does not create/drop databases or modify
the public/ERA5/GRIB baseline mounts. Complete preparation may be reused using
`--reuse-preparation PRIOR/report.json --reuse-work PRIOR_WORK`; the original
publication/index costs are explicitly carried forward, not reported as a new
measurement. Source SHA and published versions must still match.

`analyze_small.py REPORT --output NEW_ANALYSIS` checks campaign balance and
extracts SQL/stage/copy metrics. `verify_small.py` performs a separate read-only
post-audit of raw, FS and IoTDB bytes and every published version. Run all local
regression checks with `-m unittest discover -s . -p 'test_*.py' -v`.

The completed [2026-10-13 paired pilot](results/small-query-20261013-paired/RESULTS.md)
contains 180 validated observations and the construction/publication costs.

## Fixed-output scale experiment

`scale_runner.py` keeps K=2, depth=2, seed, tree width, native libraries,
chunk/grid geometry and filters fixed. It runs N=8/32/128 at D=4, and
D=4/16/64 at N=8. The two matching units retain the same numerical oracles.
The runner publishes every new corpus and builds every persistent FS index
before starting any query campaign, then remeasures the original N=8/D=4
corpus under that final database state. It does not import during query timing.
Each of the five points retains the pilot's three campaigns × five repeats
and rotated arm order, giving 900 main observations.

```sh
env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" scale_runner.py \
  --native work/runner-20261013/native-build/multifile_native \
  --environment work/runner-20261013/environment.json \
  --writer-lock /data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock \
  --baseline-report results/small-query-20261013-paired/report.json \
  --baseline-work work/small-query-20261013 \
  --output results/NEW-scale --work work/NEW-scale

env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: "$PY" analyze_scale.py \
  results/NEW-scale --output results/NEW-scale/analysis.json
```

`small_runner.py` accepts explicit paired `--hdf5-spec` / `--grib-spec` and
`--prepare-only` for this staging sequence; existing small-spec defaults remain
unchanged. All runs keep production freshness and permission checks. Main
point order is N8/D4, N32/D4, N128/D4, N8/D16, N8/D64; arm order rotates inside
each point, while point order is not randomized. The 2026-10-13 experiment adds
a separate N8/D4 bookend remeasurement to inspect temporal drift. These are
warm descriptive observations, not significance tests.

See [scale results](results/scale-query-20261013/RESULTS.md). The report compares
both absolute gaps and ratios to FS scan, retains the FS persistent-index
control, and records SQL/layout calls. That unmodified baseline used a separate
**16-version** GRIB parsed-index limit, unaffected by the shared fragment cache's
65,536-entry configuration. It caused repeated layout reads on N32/N128. The
subsequent [cache repair](results/grib-index-cache-20261013/RESULTS.md) changes
the default parsed-index safety cap to 65,536, retains the 64 MiB byte budget,
and exposes `query.grib.index_cache.*` configuration.

`grib_index_cache_ab.py` compares the retained old binary, the fixed binary's
defaults, and the fixed binary explicitly configured back to 16 versions.
It reuses published N8/32/128, K2/D4/depth2 corpora and persistent FS indexes;
it does not publish new versions. Each variant has three private daemons ×
five repeats × two phases. FS scan/index controls run alongside fixed-default
requests, giving 420 observations. The capacity-16 control validates the actual
mount configuration path, while cache counters distinguish count from byte
evictions. Profile collection waits boundedly for a complete log line after
the socket response, outside query clocks. `--resume-report` only carries
complete variants with identical binary/corpus identities; an uncommitted
variant is rerun in a fresh daemon, never appended as partial warm observations.

`hdf5_cache_diagnosis.py` replays N32/N128 catalog discovery with the retained
scale binary. It compares the six-attribute query with a no-predicate diagnostic
and checks all warm cache hits/misses/evictions and actual object/layout reads.
The no-predicate case returns all datasets, so it isolates stages rather than
serving as an equal-result performance arm. The [2026-10-13 diagnosis](results/hdf5-cache-diagnosis-20261013/RESULTS.md)
records 60 successful observations and confirms that COUNT/GROUP BY and attribute
SQL cost, rather than cache thrashing, dominate the HDF5 N32→N128 growth.

`catalog_integrity_plan.py` independently measures the real HDF5 secondary
index COUNT SQL at N32/N128, captures EXPLAIN/ANALYZE, retains paired exact
versions, and compares a redundant global time envelope and bounded 32-key
batches. It reads the frozen scale reports and existing experiment namespace;
it creates no database fixtures. Build its probes against the original core
using `--source`, `--build`, `--scale-root`, `--environment`, and a new `--output`.
`--reuse-output` revalidates saved successful SQL observations without
resubmitting them. This tool does not change production integrity semantics.
The [2026-10-13 plan and version-reuse assessment](results/catalog-integrity-plan-20261013/RESULTS.md)
separates scan/filter cost, SQL construction, and the correctness boundary of
reusing a successful version-bound count check.

## Frozen N=128 historical-best acceptance (2026-10-14)

This acceptance replays the existing workload; it does not generate another
multi-file experiment. A is the immutable historical client warm median for
`discover-values`: **GRIB 480.361781 ms; HDF5 480.385748 ms**. Both come from
`results/scale-query-20261013/n128-d4/report.json`. A newly measured control
must never replace those acceptance thresholds. B executes the current clean
checkout against the same published exact versions and FS files/index.

Freeze `small_runner.py` at SHA-256
`36d5517d587bd19265804484c7fa96af88dd9f07f464acca366a381a40fbaa99`,
the exact script recorded by the historical report. The preserved copy lives
on the experiment server under
`work/fixed-historical-reproduction-20261014/source/experiments/multi_file/`.
The current repository copy has an additional post-clock profile wait and is
not substituted into this replay. Imported helpers, specifications, daemon,
native reader and source bytes are recorded in `frozen-contract.json`.

Keep N=128, K=2, D=4, depth=2, seed=20261013, tree_width=4, three campaigns
and five repeats. Rotate the existing FS-scan / FS-index / IoTDB arm order
exactly as the original runner does. Exclude repeat 0 of each campaign; use
the median of the remaining 12 client observations per format. Include
discovery, complete selected-unit reads, registration, IPC and owned array
copy; retain the original exclusion of oracle/hash/profile collection.
Do not replace the metric with server-only or discovery-only time.

Reuse preparation with no generation, publication or FS index build. The
execution preparation locator changes only `daemon_sha256` to satisfy the
existing runner's binary identity guard; preserve the original report and
record both hashes. Historical preparation costs are inherited evidence,
not newly measured costs.

If B exceeds its historical A, mark that format **failed** and stop tuning.
Do not change runner, limits, cache policy, query geometry or baseline to
turn the failure into a pass. Preserve all observations, including slow ones.
Raw outputs and the locked comparison are retained under
`results/fixed-historical-reproduction-20261014/` on the experiment server.

The subsequent [commit and environment audit](results/fixed-historical-reproduction-20261014/COMMIT-AUDIT.md)
checks source/parameter identities and preserves complete auxiliary replays.
These diagnostics never replace historical A or the first B. Do not build
experimental daemons from an unverified server working copy; use the isolated
source/build whose full production source hashes match the local checkout.

The [server code/database and network-outage check](results/fixed-historical-reproduction-20261014/database-state/RESULTS.md)
binds observations to the actual 33667 DataNode, inventories old client sockets
and background jobs, and records read-only SHOW/EXPLAIN diagnostics. It does
not restart services, mutate database contents, or replace benchmark results.

The HDF5/GRIB cost-structure audit (local archive: `docs/knowledge/hdf5-grib-query-cost-structure-audit-20261014.md`)
uses those existing reports and plans without another timing run. It separates
SQL allocation, secondary-index integrity/filter execution, value-request
grouping, and physical assembly, including limits of cross-format decode counters.

### Sealed HDF5 substructure-index N=128 validation (2026-10-15)

The measured report (local archive: `docs/reports/hdf5-substructure-index-n128-20261015.md`)
uses the same frozen runner, native reader, raw corpus and N/K/D/depth controls.
New sealed source versions were published because legacy unsealed versions cannot
use the new mode. Strict and substructure-index query the same new exact versions;
the historical A values remain unchanged. A deployment launcher sets the existing
catalog-mode property before exec, outside query timing; it does not replace the
runner or alter query grouping. Publication and relocated FS index construction
are reported separately. Complete raw reports and failed preparation records live
under `work/hdf5-substructure-n128-20261015/` on the experiment server.

The subsequent predicate-precompilation ablation (local archive: `docs/reports/hdf5-predicate-precompile-ablation-20261015.md`)
fixes the best-A threshold at 121.067539 ms and reuses those same sealed source
versions and FS indexes. An initial incomplete attempt exposed the frozen
runner's asynchronous profile-collection race. The user approved only the
existing bounded wait after the end timestamp; both old/new binaries were then
fully rerun with that collector fix. AST/dependency checks and the original
63-record failed attempt are preserved. The repaired runner hash is recorded
separately; online timing, workload, limits and arm rotation are unchanged.
