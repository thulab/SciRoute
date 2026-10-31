# Add Arrow typed queries, reusable training windows, and offline migration

Arrow files could previously be stored as opaque bytes, but could not participate in typed array queries or reproducible training-window queries. This change adds an end-to-end Arrow IPC stream path: indexed ingestion, exact-version queries, shared decoding, on-demand training windows, bounded IPC results, FUSE access, and migration of existing pass-through data.

Scope: **14 commits**, from `ca32e41d8beba3175a7cd21c1c894e7cfeb8782e` through `9078917b70ff25cd3a859006675c6078fbd1e510`, inclusive (`ca32e41^..9078917`).

## Query and storage foundations

- Add content-based Arrow stream detection and a versioned physical layout containing schema, message/batch boundaries, nested offsets, validity, and buffer locations. Preserve the original payload bytes and register Arrow format identity through the existing ingestion lifecycle.
- Supply ingestion adapters with checked, stable source access for incremental indexing. A header probe beyond EOF rejects that format candidate without preventing short or empty files from being stored; actual read failures remain fatal.
- Bind physical array queries to an exact source/version, batch, row, field, and child path. Support one- and two-level `List`/`LargeList` arrays with Float32 leaves without inferring channel or time semantics from their shape.
- Return scalar position/value columns with projection, residual predicates, explicit null provenance, and domain positions. Reject ragged selected lists. Share parsed layout state within planning while retaining execution-time source/layout checks and request-specific limits.

## Shared decoding and training windows

- Introduce a format-neutral `QueryRuntime` and decoded-fragment cache with single-flight loading, immutable leases, memory accounting, bounded backpressure, and independent consumer cancellation/deadlines. Eviction prefers unleased entries, releases only the capacity needed, and permits new requests to replace abandoned loads.
- Prepare `DatasetSnapshot` and coverage indexes from explicit semantic declarations and exact source versions. Separate stable content fingerprints used by views/sample IDs from process-local storage identity. Validate per-channel coverage and sidecar completeness; optional sidecar write failures do not fail preparation.
- Assemble overlapping input/target windows on demand, including explicitly declared cross-file continuity and variable partitions. Preserve masks, provenance, split boundaries, missing-data policies, deterministic sharding, pagination, and cancellation.
- Support independent experiment views, fixed training-statistic standardization, and fair scheduling with per-experiment admission/output budgets. Different views share decoded base fragments without modifying cached values.
- Batch missing-tile demands across bounded window groups: merge contiguous ranges, issue multi-range reads for disjoint ranges, and allow bounded parallel read waves. Keep fine-grained cache identities; this batching belongs to the training path rather than changing ordinary Arrow/GRIB query semantics.

## Runtime access and migration

- Encode typed and window results as bounded, self-describing Arrow IPC streams, including transform metadata. Align encoding limits with result-store capacity and provide registration expiry, result release, and memory-pressure eviction.
- Wire a persistent runtime into FUSE. Authenticated Unix-socket registration returns immutable, content-addressed paths under `/.iotdbfs/query/v1/`, allowing ordinary reads and `mmap`. Handle stale sockets, writable runtime-directory selection, configurable response timeouts, supplementary groups, bounded bridge waits, and authorization caching. Socket startup failure leaves ordinary filesystem access available.
- Register reusable prepared snapshot/view objects and normalized channel tables, so subsequent training requests send IDs and coordinates instead of repeating full declarations.
- Add `iotdb_fs arrow-migrate inventory|run|resume`. Reingest eligible latest pass-through versions into new Arrow versions, preserve source versions, verify payload hashes, and journal recovery. Mount and migration share a local writer lock; writers on other hosts must be stopped separately.

## Module boundaries and execution optimizations

Separate query core, training, encoding/frontend, Arrow, GRIB, and migration modules; update public include paths and callers, and enforce dependency boundaries in CI. Old include-path forwarding headers are not retained. Pin Arrow dependencies and add public benchmark tooling.

Prepared modules own deep copies of snapshot/index/view data and reuse validation through private factories. They retain request/source checks and reject use after `fork`. Compile normalization statistics once into channel-ordered numeric arrays, eliminating per-sample maps and per-value lookups while preserving double-precision arithmetic and error behavior. Convert client IPC columns directly from Float32 buffers and validity bitmaps into independently owned NumPy arrays, preserving offsets, nulls, and valid NaNs. Explicit reference modes and profile fields support ablation.

## Validation and measured results

Recorded validation for the final implementation includes:

- **978 C++ tests** and **18 Python conversion/client/profile tests** passing, including mutable-input isolation, fork rejection, paging, cancellation, cache accounting, transform equivalence, and Arrow buffer ownership.
- Native Arrow/PyArrow oracle checks, public ETTm1/Electricity data validation, live FUSE read/mmap checks, and isolated migration/resume smoke tests. Earlier implementation stages also include targeted sanitizer runs; these are not a claim of full sanitizer coverage for the final revision.
- Public HF Trainer baselines using PatchTST and PatchTSMixer, with identical source data, sampling, splits, normalization, and training settings across local Arrow and IoTDB-FS readers.
- A final Electricity ablation with **40 IoTDB-FS runs and 4 local controls**: two models, two repetitions, 50 training steps, cold/warm decoded caches, and five optimization configurations. All sample-trace and loss comparisons passed; **2,040 training request profiles** matched.

Mean training wall time across models and repetitions:

| Configuration | Cold decoded cache | Warm decoded cache |
|---|---:|---:|
| Reference execution paths | 7.436 s | 6.759 s |
| Validation reuse only | 7.212 s | 6.373 s |
| Compiled normalization only | 7.013 s | 6.177 s |
| Buffer conversion only | 7.012 s | 6.065 s |
| All three optimizations | **6.071 s** | **5.287 s** |

The combined change reduces wall time by **18.4% cold / 21.8% warm**, with unchanged sampling, payload-read counts/bytes, and IPC size. First preparation remains timed. These are short-run paired measurements, not full-epoch throughput results; “cold” does not mean cold OS or database caches. The local control averages **2.195 s**, so this PR does **not** establish performance parity with local mmap or an end-to-end multi-experiment advantage.

See the [execution-cost report](docs/reports/public-training-execution-cost-20260930.md), [public baseline plan](docs/deliverables/public-training-baseline-plan.md), and [migration runbook](docs/knowledge/arrow-data-migration.md) for protocols, reproduction steps, and evidence locations.

## Scope and operational limits

- Supports Arrow IPC streams and the declared nested Float32 shapes; does not add IPC file/Feather support, arbitrary nested types, automatic dataset-semantic inference, or a TsFile adapter.
- Decoded-cache sharing is limited to the same runtime/backend namespace within a daemon. Compressed Arrow buffers remain whole-buffer decode units; smaller tiles do not enable partial decompression.
- Exact-version snapshots do not provide a storage deletion lease. Referenced versions must remain available throughout training.
- Migration is offline and limited to inventoried latest eligible versions. Bulk production migration and cross-host writer coordination have not been validated; the local lock is not a distributed lock.
- Cross-file functionality is tested independently. The public training A/B experiments use standard sampling and do not manufacture cross-file windows to claim a performance benefit.
