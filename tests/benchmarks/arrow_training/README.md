# PB00 public Arrow data and oracle

> Evidence marked as a server archive is excluded from the retained Git documents. See the [archive location](../../../docs/README.md#服务器归档).

This directory contains a standalone converter and a NumPy/PyArrow correctness oracle. It does not import TimeBench or TimerLathe. Large source and converted files remain in the experiment work directory; the six small Arrow fixtures and their SHA-256 manifest are versioned here.

## Cache capacity and parallel read ablation

`run_cache_parallel_ablation.py` runs Electricity with the normal HF random sampler,
50 steps, batch size 2, seed 20260929 and both public models. It compares 1,024
entries/one read lane against 16,384 entries with one, two and four lanes. All
arms hold the 1 GiB budget, four workers and 256 in-flight fragments constant.
Each cold run starts a new daemon; the following warm run repeats the same trace.
OS and IoTDB caches are not dropped. Two replications reverse configuration order
in the second replication. Each B run is checked against that replication's A
run for trace and loss equality, and its 51 train requests must all join the
server profile without overlap. Worker-duration sums are not wall time.

The script restarts an existing dedicated experiment mount, so do not run it
against a mount serving other users. It inherits daemon credentials in memory,
writes separate mode-0600 configuration variants and restores the original
configuration with profiling disabled in `finally` (with a fallback binary).

```bash
"$BASE/venv/bin/python" "$BASE/run_cache_parallel_ablation.py" \
  --work-dir "$BASE" --mount "$BASE/runtime/mount" \
  --socket "$BASE/runtime/public-arrow-ab.query.sock" \
  --binary "$BASE/a05-decoded-cache-20260927/build/bin/iotdb_fs" \
  --fallback "$BASE/tmp/iotdb_fs.before-cache-parallel-20260930" \
  --daemon-pid "$DAEMON_PID" --source-version 1790670177863 \
  --output "$BASE/ablation-cache-parallel-20260930" --repetitions 2
```

Mount properties are `query.cache.max_entries` (default 16384),
`query.cache.max_inflight` (256), `query.cache.worker_threads` (4),
`query.cache.memory_budget_bytes` (1073741824), and
`query.training.read_parallelism` (1; supported 1–4). The daemon requires at
least 64 in-flight fragments and one worker per lane. The socket client cannot
override server parallelism. Library `QueryRuntimeConfig` defaults remain
unchanged; these larger defaults belong to the mount runtime. The entry limit
remains a metadata safety bound, with resident/temporary memory separately
limited by bytes.

## Preparation profiling

Preparation profiling is available through `profile_preparation_baseline.py`.
It accepts the same mount/binary/fallback/source arguments as the cache ablation,
plus `--controls` pointing to saved `r1-{model}-local.json` correctness controls.
It fixes the expanded cache and four read lanes, measures both models cold/warm,
and restores the mount in `finally`. New nested server timers distinguish
fingerprints, catalog/source access, layout read/parse, and coverage construction;
client timings distinguish JSON encoding from the socket round trip. These
nested details are not added to `prepare_total`.
See preparation measurements (server archive: `docs/public-training-preparation-profile-20260930.md`).

## Pinned sources

| Dataset | Source | SHA-256 | Layout |
| --- | --- | --- | --- |
| ETTm1 | [ETDataset ETTm1.csv](https://github.com/zhouhaoyi/ETDataset/blob/main/ETT-small/ETTm1.csv) | `6ce1759b1a18e3328421d5d75fadcb316c449fcd7cec32820c8dafda71986c9e` | 69,680 steps × HUFL, HULL, MUFL, MULL, LUFL, LULL, OT |
| UCI Electricity ZIP | [UCI original](https://archive.ics.uci.edu/dataset/321/electricity), [public SageMaker mirror](https://sagemaker-example-files-prod-us-east-1.s3.amazonaws.com/datasets/timeseries/uci_electricity/LD2011_2014.txt.zip) | `f6c4d0e0df12ecdb9ea008dd6eef3518adb52c559d04a9bac2e1b81dcfc8d4e1` | ZIP containing `LD2011_2014.txt` |
| UCI Electricity TXT | `LD2011_2014.txt` inside the ZIP | `d51565f2cb5a6b768d06ba1bbd3c084c6e2f3aab07f00c6f2dcb80e90175124b` | 140,256 steps × MT_001…MT_370 |

ETTm1 is CC BY-ND 4.0 and UCI Electricity is CC BY 4.0. The source bytes and converted data are not included here. The UCI source has a quoted header, `;` separator, and decimal comma. Its zero readings remain valid zero values. The local time labels are preserved as text and are **not** converted to invented UTC timestamps.

## Conversion on the prepared server

```bash
BASE=/data_01/zzy/iotdb-fs-exp/public-arrow-baseline
PY="$BASE/venv/bin/python"

# ETTm1.csv must first be placed in $BASE/raw and verified by the converter.
"$PY" "$BASE/prepare_public_data.py" convert \
  --dataset ETTm1 --source "$BASE/raw/ETTm1.csv" \
  --source-sha256 6ce1759b1a18e3328421d5d75fadcb316c449fcd7cec32820c8dafda71986c9e \
  --work-dir "$BASE"

curl -fL -o "$BASE/raw/LD2011_2014.txt.zip" \
  https://sagemaker-example-files-prod-us-east-1.s3.amazonaws.com/datasets/timeseries/uci_electricity/LD2011_2014.txt.zip
"$PY" "$BASE/prepare_public_data.py" extract-electricity \
  --archive "$BASE/raw/LD2011_2014.txt.zip" \
  --archive-sha256 f6c4d0e0df12ecdb9ea008dd6eef3518adb52c559d04a9bac2e1b81dcfc8d4e1 \
  --out "$BASE/raw/LD2011_2014.txt"
"$PY" "$BASE/prepare_public_data.py" convert \
  --dataset UCI_Electricity_370 --source "$BASE/raw/LD2011_2014.txt" \
  --source-sha256 d51565f2cb5a6b768d06ba1bbd3c084c6e2f3aab07f00c6f2dcb80e90175124b \
  --work-dir "$BASE"

"$PY" "$BASE/prepare_public_data.py" fixtures --out "$BASE/arrow/fixtures"
"$PY" "$BASE/verify_public_data.py" \
  --fixture-manifest "$BASE/arrow/fixtures/fixture-manifest.json" \
  --dataset-manifest "$BASE/manifests/ETTm1-pb00.json" \
  --dataset-manifest "$BASE/manifests/UCI_Electricity_370-pb00.json" \
  --work-dir "$BASE"
```

The real-data stream uses `dataset_id`, `entity_id`, `series_id`, `segment_index`, `start_step`, `end_step`, and `target: list<list<float32>>`. Each row represents one `[channel][step]` segment of the same named series. The manifest explicitly records continuation and global step coverage; row adjacency alone is not proof of continuity. Default segment length is 8192 steps and each record batch has up to 2 rows. The canonical NumPy arrays are time-major `[step, channel]` and carry a separate validity mask. The stream is uncompressed by default.

Synthetic fixtures cover 5 independent rows over 2 batches, ragged and empty channels, an outer null row, an inner null channel, and null float leaves. They are generated in uncompressed, LZ4, and ZSTD streams, plus a target-only uncompressed stream convenient for physical buffer tracing. Two target-first streams carry a dictionary-encoded `role` field whose dictionary changes in the second batch: one prefix delta, one replacement. Their manifest records SHA-256 and the PyArrow version used to write them. `verify_public_data.py` checks every possible row/channel/step interval in each small fixture against NumPy/source-derived expectations; it samples real-data windows throughout all segments and checks a continuation boundary. Run `python3 -m unittest discover -s tests/benchmarks/arrow_training -p test_prepare_public_data.py -v` for parser and fixture regression checks.

This is PB00 data preparation and correctness only. It does not claim a query-service result, measured range I/O, HF Trainer epoch, or performance advantage.

## PB00/PB01 data acceptance

After preparing both public datasets with the commands above, run the exhaustive
source, stream, mask and segment-boundary check:

```bash
"$PY" "$BASE/accept_public_data.py" \
  --work-dir "$BASE" \
  --output "$BASE/manifests/pb00-pb01-full-data-acceptance.json"
```

This validates pinned source and converted-file hashes, all Arrow leaf values
and validity bits against the canonical NumPy arrays, explicit segment metadata,
every adjacent segment boundary, and all six nested fixtures. The boundary
windows cross segments **within one IPC stream**, not separate files. The
2026-09-29 run is recorded in
the acceptance report (local archive: `docs/reports/public-training-pb00-pb01-acceptance-20260929.md`).
This command does not itself produce the split, training-only normalization or
window manifest. The first two artifacts are now produced separately below;
public cross-file shards are outside the initial A/B workload.

## PB01 windows and first local-versus-IoTDB-FS Trainer connection

The 2026-09-29 isolated experiment uses one physical IPC stream per dataset.
It does not create artificial cross-file windows. With the server's dedicated
mount running at `$BASE/runtime/mount` and registration socket at
`$BASE/runtime/public-arrow-ab.query.sock`, prepare the frozen split and
train-only statistics, ingest the exact streams, compare full-channel windows,
then run a single-GPU two-step Trainer pair:

```bash
for dataset in ETTm1 UCI_Electricity_370; do
  "$PY" "$BASE/prepare_public_windows.py" --work-dir "$BASE" --dataset "$dataset"
  "$PY" "$BASE/ingest_public_arrow.py" --work-dir "$BASE" \
    --mount "$BASE/runtime/mount" --dataset "$dataset" \
    --output "$BASE/manifests/$dataset-iotdbfs-ingest.json"
  "$PY" "$BASE/verify_public_ab.py" --work-dir "$BASE" \
    --mount "$BASE/runtime/mount" \
    --socket "$BASE/runtime/public-arrow-ab.query.sock" \
    --dataset "$dataset" --all-channels \
    --output "$BASE/manifests/$dataset-ab-all-channels.json"
done

"$PY" "$BASE/run_public_ab_smoke.py" --work-dir "$BASE" \
  --mount "$BASE/runtime/mount" \
  --socket "$BASE/runtime/public-arrow-ab.query.sock" \
  --dataset ETTm1 --model patchtst --gpu 0
```

Run the last command for `patchtsmixer` and for `UCI_Electricity_370` as well.
The runner queries the exact source version if it is not supplied, then pins it
for the pair. It forces `CUDA_VISIBLE_DEVICES` to the chosen GPU; `train_hf.py`
rejects a mismatch between the expected and actual visible GPU count. Both
backends consume the same compact anchor manifest through a map-style Dataset
and the installed Trainer's normal random-train/sequential-evaluation samplers.
The local reader indexes IPC messages once and maintains a bounded decoded
batch cache. The IoTDB-FS reader submits each Trainer batch as one typed window
request and reads the result through Arrow IPC on the mounted route.

These commands require the query-enabled `iotdb_fs` daemon and its isolated
database, socket and namespace lock; the mount's private config and IoTDB
credentials are supplied by the experiment operator and are not stored in this
repository. The existing run's exact bytes, versions, correctness checks and
two-step results are in the A/B wiring record (server archive: `docs/public-training-ab-wiring-20260929.md`).
Two steps are a connection check, not a full training or steady-state benchmark.

To isolate the socket registration path after the 50-step smoke, run
`profile_public_registration.py` against the same mounted experiment. The
reported seconds include eager server-side window execution, IPC encoding,
and publication before the socket responds. `--anchors` selects exact window
coordinates, `--channel-counts` selects prefix views, and `--valid-only`
omits the rejected-series control:

```bash
"$PY" "$BASE/profile_public_registration.py" --work-dir "$BASE" \
  --mount "$BASE/runtime/mount" \
  --socket "$BASE/runtime/public-arrow-ab.query.sock" \
  --dataset UCI_Electricity_370 --anchors 28748 67181 \
  --channel-counts 341 342 370 --valid-only --repeats 3 \
  --output "$BASE/manifests/UCI_Electricity_370-registration-profile.json"
```

The 2026-09-29 diagnosis (server archive: `docs/public-training-registration-diagnosis-20260929.md`)
records the resulting 1,024-entry decoded-cache threshold and the important
distinction between registration preparation and materialization.

For server stage timing, set `IOTDBFS_QUERY_PROFILE_JSONL` to a writable JSONL
file when starting the experiment daemon. It is disabled by default and does
not change the socket response. Each completed registration logs its immutable
result path, synchronous stage durations, and shared-runtime counter deltas.
`server_total_ns` starts after the request frame is received and ends before
response serialization/send and profile-log output. Parent stages include
their children; `payload_read_nanoseconds` is contained in
`fragment_load_nanoseconds`, which measures worker loader wall time, not CPU
time. Payload calls/bytes exclude layout and manifest reads and are not
physical disk I/O measurements.

Each actual payload call also contributes to `payload_read_shape`, a 5×5
range-count/byte-count histogram with calls, nanoseconds, bytes and ranges.
Range buckets are `<=1, <=4, <=16, <=32, >32`; byte buckets are `<=16 KiB,
<=64 KiB, <=256 KiB, <=1 MiB, >1 MiB`. The summarizer checks shape totals
against the original counters. For controlled byte-versus-range measurements,
build `arrow_read_shape_probe` explicitly and run it on one exact published
source version, then summarize its CSV with `summarize_read_shape_probe.py`.
The Electricity read-shape report (local archive: `docs/reports/public-training-read-shape-profile-20260929.md`)
records the 50-cycle live run and its limits.

New training reports retain per-request paths and client timings. Join them
to a complete server log after the run:

```bash
"$PY" "$BASE/summarize_query_profile.py" \
  --server-log "$BASE/runtime/logs/query-profile-20260929.jsonl" \
  --client-report "$BASE/manifests/UCI_Electricity_370-patchtst-stages50-20260929-iotdbfs.json" \
  --split train --output "$BASE/manifests/electricity-train-stages-summary.json"
```

Use `--probe-report` instead of `--client-report` for fixed-window probes.
Run one client on an otherwise idle experiment mount: cache counters are
shared by the daemon. The summarizer excludes logged overlapping requests;
unlogged activity cannot be ruled out by the log itself. Counter deltas may
be summed, but memory gauges and nested wall times must not be added together.

## A00 physical I/O and native API spikes

`a00_range_spike.py` records every pread and compares the selected row/channel/step with PyArrow. It indexes stream message metadata once, reads offsets/validity/value tiles on demand for uncompressed data, and caches whole decoded IPC buffers for LZ4/ZSTD. The generated traces in `manifests/a00-range-*.json` include every message offset, buffer read, window value/mask, and a sequential PyArrow reader byte count. For example:

```bash
"$PY" "$BASE/a00_range_spike.py" "$BASE/arrow/ETTm1-none-8192.arrows" \
  --batch 0 --row 0 --channel 3 --start 128 --end 160 \
  --output "$BASE/manifests/a00-range-ett.json"

g++ -std=c++17 -O2 -I"$BASE/deps/arrow-prefix/include" \
  "$BASE/a00_cpp_stream_spike.cpp" -L"$BASE/deps/arrow-prefix/lib" \
  -Wl,-rpath,"$BASE/deps/arrow-prefix/lib" -larrow \
  -o "$BASE/deps/a00_cpp_stream_spike"
"$PY" "$BASE/verify_cpp_stream_spike.py" \
  --binary "$BASE/deps/a00_cpp_stream_spike" --data-root "$BASE/arrow" \
  --output "$BASE/manifests/a00-cpp-stream-oracle.json"
```

The C++ spike uses Arrow's normal batch reader to check native API, codec, and library compatibility; the Python spike supplies the selective physical-read trace. Neither is the production QuerySource adapter. Measurements and limits are recorded in A00 results (local archive: `docs/reports/arrow-query-spikes.md`).

## A03 native ingestion validation

`a03_ingestion_probe.cpp` drives the production Arrow ingestion adapter with 11-byte chunks and compares its layout with an independent native Arrow reader after ingestion. It checks actual List/LargeList offsets, parent/List validity and leaf counts, and verifies that index construction never rereads float values beyond optional compression-size prefixes. It accepts all input paths as arguments and emits JSON lines. Build after the normal `iotdb_fs`/`unit_tests` build:

```bash
A03_SOURCE="$BASE/a03-arrow-layout-20260927/IoTDB-FS-C"
A03_BUILD="$BASE/a03-arrow-layout-20260927/build"
A03_ARROW="$BASE/deps/arrow-prefix"
g++ -std=c++17 -O2 -I"$A03_SOURCE/include" -I"$A03_ARROW/include" \
  "$A03_SOURCE/tests/benchmarks/arrow_training/a03_ingestion_probe.cpp" \
  "$A03_BUILD/libiotdb_fs_core.a" -L"$A03_ARROW/lib" \
  -Wl,-rpath,"$A03_ARROW/lib" -larrow -pthread \
  -o "$A03_BUILD/bin/a03_ingestion_probe"
"$A03_BUILD/bin/a03_ingestion_probe" "$BASE"/arrow/fixtures/*.arrows \
  "$BASE/arrow/ETTm1-none-8192.arrows" \
  "$BASE/arrow/UCI_Electricity_370-none-8192.arrows"
```

Recorded A03 results (server archive: `docs/arrow-a03-validation-20260927.json`) cover eight inputs. The `source_reread_bytes` counter includes detection and index reads; `index_read_bytes` excludes detection. Sequential ingestion still reads/copies the entire B payload once. These counters do not measure Arrow query or training throughput. Format/limits are specified in [Arrow layout v1](../../../docs/contracts/arrow-layout-format.md).


## Prepared registration ablation

`train_hf.py --backend iotdbfs` defaults to `--registration-mode prepared_compact`:
a reader lazily sends one `window_prepare` with shared channel tables, then only
`prepared_id` and coordinates. Initial preparation stays inside the first Trainer
fetch. Each decoded result route is explicitly retired after its mapping closes;
reader close releases its prepared ID. `full` and `prepared` select the two
control arms without silent fallback.

`run_prepared_ablation.py` runs full / prepared / prepared_compact with fixed
16,384 cache entries, 1 GiB, four read lanes, two models and cold/warm traces,
two repetitions with reversed arm order. Arguments match
`run_cache_parallel_ablation.py`. The runner restores the original mount in
`finally`; use it only on the dedicated experiment mount. Request-byte totals
include the one-time preparation and per-result release messages. See
[protocol](../../../docs/contracts/arrow-prepared-training-registration.md) and
results (local archive: `docs/reports/public-training-prepared-registration-20260930.md`).


## Client array conversion ablation

The previous `--conversion-mode batch` path resolves value-column
positions once per table, converts each input/target column across all samples,
then copies role slices into owned, writable, C-contiguous NumPy arrays. The
`sample` option is the unchanged previous loop retained as an explicit ablation
control, not an automatic fallback. IPC validation and server behavior are shared.

`run_client_conversion_ablation.py` fixes registration to `prepared_compact`,
16,384 cache entries and four read lanes; it compares both conversion modes for
PatchTST/PatchTSMixer, cold/warm cache, two repetitions with reversed arm order.
It accepts the same daemon/work/mount/socket/source-version arguments as
`run_prepared_ablation.py` and restores the original mount in `finally`.
`ipc_validation_seconds` and `ipc_array_seconds` are nested within
`ipc_decode_seconds`; do not add the three timers together.

Run `python -m unittest test_window_conversion test_public_prepared_client test_summarize_query_profile` in the baseline environment to verify array,
mask, memory ownership, schema order and validation contracts.

Client conversion results (server archive: `docs/public-training-client-conversion-20260930.md`).


## Prepared execution and buffer conversion ablation

The default client mode is now `--conversion-mode buffers`. It reads Float32
buffers and bit-packed validity directly, respects Arrow offsets, combines only
multi-chunk columns, and writes independent dense arrays. Nulls become zero with
mask=false; valid NaNs remain valid. The `batch` and `sample` modes remain explicit
experimental controls. Schema, role, sample, position, descriptor and artifact
validation is unchanged.

Prepared modules now reuse their own immutable snapshot/view validation and
compile ordered normalization statistics once. Public mutable-input factories
still validate fully; request/continuation limits, source/layout checks and
permissions remain enforced. The server properties
`query.training.reuse_prepared_validation` and `query.training.compiled_transform`
accept only `true`/`false`, default true, and are recorded in query profiles.
Clients cannot override them through registration declarations.

`run_execution_cost_ablation.py` compares five arms: both server switches off +
`batch` conversion, validation only, transform only, buffers only, and all three.
It uses the same arguments as the prior runners, fixed 1 GiB/16,384 entries/four
read lanes, two models, cold/warm cache and two repetitions (reversed arm order).
First preparation remains timed. The dedicated mount is restored in `finally`.
Cold means decoded cache only; OS/database caches are not dropped.

Prepared execution and buffer conversion results (local archive: `docs/reports/public-training-execution-cost-20260930.md`).
