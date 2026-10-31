# ERA5 regional sample and D1 training benchmark

`sample_contract.py` is the shared F1/Q2 coordinate contract. It fixes the 20
channels, UTC 6-hour cadence, the 16 overlapping 128×128 patches in each
region, input `[t-6h, t]`, and one target at `t+lead`. V0/V1/V2 use a 6-hour
lead; V3 uses 24 hours. D1 uses half-open train/validation/test boundaries of
January 1/22/27/February 1, 2018. A window is admitted only when every input
and target timestamp belongs to one split. Sample IDs hash source content
identity, the view declaration, the initiation time, and the patch ID; they
do not depend on a process or storage path.

`prepare_era5.py` verifies both source SHA-256 hashes and freezes a manifest
with every sample ID and coordinate. `local_reader.py` uses the validated GRIB
message offsets to decode with Python ecCodes. `iotdb_reader.py` submits typed
queries for *exact* IoTDB-FS source versions, using `validity_date`,
`validity_time`, and `param_id` predicates and lat/lon slices. It checks each
returned message against the frozen catalog before assembling channel-major
Float32 tensors. `compare_sample.py` compares X, Y, and masks byte-for-byte
and writes a JSON report. The bindings file contains one `sources` object with
`single-levels` and `pressure-levels` entries, each specifying `path`,
`version`, and source `sha256`.

For the two validated D0 manifests, run `prepare_era5.py` first, then
`compare_sample.py --view V0 --sample-index 0` with the manifest, contract,
bindings, socket, and output paths shown by `--help`. D0 has six legal
initiation times and 96 samples per full-channel 6-hour view. This slice
checks numerical identity; it does not implement the full GRIB training
window service or measure throughput.

The D1 benchmark uses `train.py` and `run_matrix.py`. `model.py` defines the
deterministic RegionalCNN-v1; `dataset.py` freezes a 50-step, batch-two trace
and applies train-only channel statistics. `zarr_backend.py` converts the same
validated GRIB messages to one reusable `[time,channel,y,x]` Zarr store. F1
reads local GRIB with a private decoded-field cache, Z1 reads that derived
store, and Q2 uses exact-version GRIB typed queries through the mount's
registration socket. `run_matrix.py` starts one process per view and samples
process RSS/PSS, `/proc` I/O and GPU utilization every second. Its output
contains `matrix-summary.json`, `resources.jsonl`, and per-view `summary.json`
and `steps.jsonl`; it rejects an existing output directory.

Prepare both D1 manifests and the D1 contract before fitting statistics or
freezing traces. Fit East Asia and North America separately from the training
time range, then freeze one trace per view with `train.py --mode freeze-trace`.
Use the same contract, stats artifacts and traces for all three backends.
The tested matrix is V0 alone, V0/V1, and V0/V1/V2/V3, each 50 steps with
batch size two. F1 divides the common 4 GiB decoded-cache budget among view
processes; Q2 uses one 4 GiB daemon cache. `--mode replay` omits GPU training
while executing the same sample trace. Check each view's `sample_ids`, loss
series and initial/final weight hashes across the training matrix.

Timing fields are wall-clock nanoseconds unless named otherwise. In Q2,
`registration_ns` measures socket submission until a ready route is returned:
the current FUSE bridge eagerly performs authorization, typed query, decode,
IPC encoding and publication before replying. It is **not** the cost of
registration alone. `ipc_read_ns` then measures memory mapping and Arrow IPC
reading, and `array_convert_ns` covers client validation and NumPy assembly.
`query_ns` contains these phases; do not sum parent and child times. F1's
`decode_ns` and `source_read_ns` are inside `sample_ns`. `setup_ns` includes
process imports and reader setup, while `first_batch_ready_ns` is measured
from process start. `resources.jsonl` uses periodic samples; PSS and GPU
utilization peaks can occur between samples.
