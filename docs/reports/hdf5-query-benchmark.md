# HDF5 H11: controlled query benchmark

H11 separates reader/layout costs from backend and service costs. It queries the original HDF5 files and their ingestion-produced layout indexes; it does not introduce a new derived data layout. All paths must return an owned, contiguous array with the same shape, floating-point type and canonical little-endian value hash as an independent h5py oracle. A successful run does not imply a performance advantage.

## Paths and attribution

| Path | Source and reader | Output and comparison |
|---|---|---|
| A | Local ordinary filesystem, native libhdf5 hyperslab | Owned array; strongest native-library baseline |
| B | Local filesystem `pread`, production HDF5 layout/query kernel/decoder/cache | Owned array; holds the reader constant for B versus D |
| C | Ordinary IoTDB-FS FUSE file, native libhdf5 hyperslab | Owned array; same native reader as A |
| D | Direct IoTDB QuerySource, production HDF5 kernel/decoder/cache | Owned array; avoids result IPC and the typed FUSE route |
| E | Socket registration, production typed query, immutable FUSE IPC, PyArrow | Owned NumPy array; complete user-visible query path |

**HDF5 uses C for native reads over ordinary FUSE and D for the direct query kernel. These letters differ from the earlier Arrow experiment.** Compare names as well as letters when interpreting cross-format results.

B/A measures a reader/index/cache implementation difference, not an IoTDB backend advantage. D/B is the most direct backend comparison. E/D includes registration, authorization, result encoding/publication, FUSE transport and client reconstruction; it is not an isolated IPC encoder measurement. C/A measures ordinary mounted file access with the same native reader. B/D/E have detailed H10 profiling enabled, whereas A/C have only harness timing. No profile-off perturbation experiment is included, so a microsecond-scale B/A difference cannot all be attributed to intrinsic kernel work.

## Alignment with the GRIB experiments

The shared experiment categories are structured local selection, metadata candidate filtering, compressed versus uncompressed storage, and overlapping concurrent readers. GRIB's message selection maps here to dataset/chunk selection and an ordinal hyperslab; HDF5's explicit chunk directory is already available to its native library. The reader-level control therefore retains native file/dataset handles and a bounded raw chunk cache. A favorable chunk shape benefits both native FS and IoTDB-FS and cannot alone establish a system advantage.

As documented in the earlier [cross-format methodology](../deliverables/arrow-query-paths-experiment.md#与-grib-论文的关系), the historical GRIB harness can rescan a source and can materialize some FUSE results before its timed read. H11 includes typed materialization in E and reports actual backend ranges/bytes separately from useful output. It also uses three independent campaign means rather than treating repeated warm requests as independent samples. These changes strengthen attribution but prevent directly combining H11 absolute latency or speedup ratios with the historical GRIB table. No new GRIB run was performed in H11.

## Frozen data and selections

`run_h11.py prepare` creates four deterministic 512 × 256 Float64 `/signal` datasets: chunks 64 × 8 or 8 × 64, each either uncompressed or gzip level 1 with shuffle and Fletcher32. Values are identical across these four layouts. The fifth source is the unchanged, hash-locked public GWOSC H1 file from [public-data.lock.json](../../tests/benchmarks/hdf5_query/public-data.lock.json), querying `/strain/Strain`. Its original metadata and encoding are retained.

The normal trace has six selections per source: small, chunk-aligned, misaligned, crossing many chunks, overlapping a prior selection, and a separate region. The synthetic separate region is `[256:512,192:256]`; it does not intersect the preceding narrow selections at columns 127–128. A seventh selection scans the full dataset and is used only for cache-pressure diagnostics. The manifest records exact coordinates, source hashes, dtype, useful bytes, physical chunk offsets and lengths, and independently computed value hashes. Sample IDs hash the source bytes, dataset, coordinates and case label; they do not depend on process IDs or storage-instance identities.

All clients in a concurrent case wave request **the same selection**. This tests overlap, singleflight and shared-cache reuse across clients. It does not test disjoint concurrent client streams. A sequence containing a separate-region case is not evidence of disjoint-client concurrency.

The catalog scenario uses the real supported metadata predicate `sigma.hdf5.datasets > 1`. A prebuilt native-file catalog and the production IoTDB catalog must select exactly the same deployed paths and versions. Catalog construction belongs to setup; the local filter time and the production catalog service time are reported separately from subsequent value reads. The query is candidate selection, not a cross-file join, and this experiment does not establish database index pruning.

## Fairness and timing

A/C reuse open HDF5 file and dataset handles. Native `H5Dread` writes directly into the final owned array; no artificial second full-array copy is added. For 2/4 clients, A/C use separate processes to avoid benchmarking only the HDF5 library's process-wide serialization. B/D share one runtime across 1/2/4 worker threads. E has concurrent independent registrations and shares the mount's runtime.

The default total data-cache allowance is 64 MiB. A/C divide it across both processes and opened datasets. B/D use a shared budget. This equalizes the declared data-cache allowance, not total RSS: HDF5 metadata caches, library scratch, output arrays, daemon metadata and transport buffers remain distinct costs. C's controlled mount configuration disables extra whole-file caching and full-file prefetch, while retaining the actual per-handle buffers; the campaign stores the precise configuration. This makes C a backend-attribution control rather than a claim about every possible FUSE cache configuration.

Each case records an owned-read monotonic start/end interval. Each pass's query wall time is the sum of the case-wave envelopes: latest client end minus earliest client start. A/C synchronize independent processes with atomically published future monotonic timestamps; B/D synchronize worker waves; E uses a bounded client pool. Thread dispatch/join time is also retained separately. Hashing, oracle checks, profile collection, source setup, layout construction and process launch occur outside these owned-read intervals. Their costs are retained in setup/evidence where measured, rather than attributed to loading values.

E records registration separately from the combined mmap → IPC parsing → Arrow reconstruction → owned-array phase. This H11 client does not subdivide that latter phase; the finer client decomposition belongs to H10. PyArrow's basic conversion is exercised once before timing to avoid labeling a one-time lazy initialization as array-copy cost. mmap construction alone does not prove the file's pages were read; the final owned copy forces materialization. OS lifetime peak RSS is a diagnostic gauge, not the declared cache budget.

A pass has one request per selection per client, so increasing client count also increases useful output work. Compare throughput as requests or useful bytes divided by the case-wave wall time; a lower or higher multi-client pass time alone is not a fixed-work speedup.

Each matrix cell has three independent campaigns with alternating path order. Each campaign has one first trace pass and three retained-cache warm passes. The warm statistic is first averaged within a campaign; the three campaign means are the independent observations. The individual requests and warm passes do not increase the independent sample count. Mean, median, standard deviation and paired campaign ratios are descriptive; no significance or population-confidence claim is made.

A fresh process/runtime makes the **application cache** initially empty. Later cases in its first trace can already hit earlier cases' data. The OS page cache, IoTDB caches and storage caches are uncontrolled, and earlier paths can warm them. Neither “cold disk” nor a cold physical database is claimed.

## Diagnostics and acceptance

The base A–E trace and overlapping 2/4-client variants are supplemented by bounded B/D diagnostics: read parallelism 2/4, single-range transport, eviction before each case wave, encoded-range caching, and a 4 MiB decoded-cache pressure trace. Native B keeps real scalar `pread`; it is not artificially wrapped in IoTDB's transport behavior. A configured parallelism value is not proof of observed concurrent reads; use the recorded active-read peak.

`evict_before_request` evicts decoded fragments before a wave but retains within-wave decoding and singleflight. It is not a true cache bypass. `encoded` is a benchmark-owned exact `(source,offset,length)` encoded-range cache, splitting the total budget between encoded storage and the production decoded runtime; decoded entries are evicted before each wave. It diagnoses repeated encoded reads versus repeated decoding and is not a new production cache feature. True decoded-cache bypass is unavailable in this harness.

Pressure acceptance requires measured distinct decoded bytes to exceed the actual decoded-cache budget, a complete unique-fragment trace, evictions, and later redecodes. The encoded variant must show encoded hits, warm decoding, and fewer backend bytes than runtime requested payload bytes. Eviction-before-request must actually redecode. A diagnostic with no observed effect is rejected rather than counted as evidence for its intended condition.

The offline summarizer rejects missing, duplicate, unexpected, reordered or incorrect samples; shape/type/value-byte/hash mismatches; changed configuration; missing native worker reports; and mismatched native versus normalized requests or pass timing. It independently recomputes every owned-read wave. E also requires one successful raw server profile for each route, verifies observed budget/parallelism and interval containment, and recomputes the saved client waves. Aggregate validation uses the campaign's complete expected cell list, not merely whichever runs happened to finish. Failure invalidates a prior summary before validation starts.

## Running and evidence

See the [benchmark README](../../tests/benchmarks/hdf5_query/README.md#h11-controlled-ae-benchmark) for commands. `h11_campaign.py` owns deployment and fresh mount lifecycles; `run_h11.py` only prepares local fixtures or queries an existing deployment. The campaign inherits connection secrets through its environment. Generated query configuration files contain no credentials; result JSON does not copy connection secrets.

Each run preserves a frozen experiment manifest, original native worker manifests/reports/logs, raw requests and pass counters, E server profiles, and a separately regenerated summary. The campaign manifest binds the intended matrix and independent campaign identities. Publication conclusions must use the final verified matrix, not the earlier functionality smoke trace.

## Final results: 2026-10-06

The final matrix completed **36/36 cells**, comprising 12 variants × 3 independent campaigns, 108 path runs and 18,120 owned-array queries, and passed the independent aggregate validator. Linux CTest passed **1,265/1,265**; the Python runner validation suite passed **24 tests**. Earlier smoke traces are excluded. The machine-readable [aggregate](../results/hdf5-h11-20261006/final/aggregate.json) preserves every campaign mean and paired ratio; original reports and profiles are retained beside it. Values below are milliseconds per full trace pass, not milliseconds per individual request.

The base trace has 30 selections, one client, 64 MiB declared cache, read parallelism 1, and up to 32 ranges per IoTDB call. “First” denotes the application's first trace pass; warm values are mean ± standard deviation of the three campaign warm means.

| Path | First-pass mean (ms) | Warm mean ± SD (ms) |
|---|---:|---:|
| A — native local FS | 12.833 | 0.938 ± 0.017 |
| B — kernel local FS | 32.939 | 13.967 ± 0.204 |
| C — native ordinary FUSE | 573.928 | 1.341 ± 0.383 |
| D — kernel IoTDB | 809.812 | 583.548 ± 138.742 |
| E — complete typed service | 2,036.501 | 1,749.565 ± 268.257 |

These results do **not** demonstrate a query-speed advantage over a native filesystem reader for these small HDF5 sources. Native libhdf5 already has a chunk index, selective decoding and effective repeated-read caching. The warm paired campaign ratios are B/A **14.90×**, C/A **1.43×**, D/B **41.86×**, and E/D **3.03×**. They are the mean of paired ratios, not ratios formed from the displayed mean times. C approaching A when warm shows that the native application's cache can remove most repeated FUSE access. B remaining slower than A isolates a reader/planning/materialization gap before adding IoTDB transport. This is evidence for this implementation and bounded workload, not a theorem that scientific data in a database cannot be faster.

For every base warm pass, B and D performed **zero payload reads and zero new chunk decodes**, but still made **120 layout reads and 30 manifest accesses** for the 30 selections. Retaining decoded values therefore does not remove repeated source/layout validation. One preserved example, base campaign 0 warm pass 1, splits D's 754 ms into about 594 ms prepare, 148 ms execute setup and 12 ms cursor consumption. The nested layout/source read timers account for most of the first two stages. They must not be added again to their parent stages. This identifies repeated metadata/layout access as the main warm backend cost; compression or tile payload read tuning cannot explain that warm delay.

The two chunk directions also provide a format-controlled geometry contrast. The synthetic `[0:512,127:129]` selection returns 8 KiB but intersects 16 chunks in the 64 × 8 layout and 128 chunks in the 8 × 64 layout. Each chunk decodes to 4 KiB, yielding 64 KiB versus 512 KiB of necessary full-chunk decoding before cache effects. Both the native reader and the query kernel can exploit the favorable layout; this geometric reduction is not exclusive to IoTDB-FS. The synthetic compressed files are about 966 KB versus 1,066 KB uncompressed, so this matrix represents mild compression and does not establish behavior for highly compressible scientific arrays.

### Overlapping clients

| Clients / requests per pass | A warm (ms) | B warm (ms) | C warm (ms) | D warm (ms) | E warm (ms) |
|---|---:|---:|---:|---:|---:|
| 1 / 30 | 0.938 | 13.967 | 1.341 | 583.548 | 1,749.565 |
| 2 / 60 | 3.992 | 30.834 | 4.326 | 715.289 | 1,960.205 |
| 4 / 120 | 4.528 | 51.371 | 4.624 | 920.925 | 2,951.126 |

D and E amortize some costs across overlapping clients, but remain substantially slower than both native controls. More clients also produce more arrays. This table cannot be read as a fixed-work scaling curve, and the small per-case native intervals are sensitive to process scheduling. No disjoint-client workload or saturation limit was measured.

### Range, parallelism and cache diagnostics

| B/D variant | B warm (ms) | D first pass (ms) | D warm (ms) |
|---|---:|---:|---:|
| Retain, p1 / r32 | 13.967 | 809.812 | 583.548 |
| Retain, p2 / r32 | 13.822 | 700.422 | 482.846 |
| Retain, p4 / r32 | 14.842 | 706.622 | 486.407 |
| Retain, p1 / r1 | 16.350 | 1,307.778 | 500.669 |
| Retain, p2 / r1 | 15.860 | 1,018.339 | 492.321 |
| Retain, p4 / r1 | 13.144 | 854.146 | 477.913 |
| Evict before each wave | 31.449 | 775.473 | 767.703 |
| Encoded-range cache | 32.396 | 707.895 | 538.360 |
| Pressure: 4 MiB, five full arrays | 85.324 | 588.445 | 589.516 |

The normal first trace reads 2,012,629 encoded payload bytes. B performs 243 real scalar reads; D groups the same 243 ranges into 47 calls with r32, versus 243 calls with r1. With r32, observed D payload-read concurrency remains **1** even when configured p2/p4. With r1, observed peaks reach **2/4**, establishing that the parallel path was actually exercised. The single-range first-pass means decrease from 1,308 to 1,018 to 854 ms as configured parallelism increases. The batching and concurrency counters support reduced transport work or overlap; the timing differences remain descriptive because OS/DB cache state and deterministic alternating variant order are not independently randomized. In particular, the retained warm passes have no payload I/O, so their differences are **not evidence of payload-read parallelism gains**.

The encoded diagnostic has zero warm backend payload bytes while still requesting 2,732,061 payload bytes through the runtime and decoding 616 fragments per pass. Its exact-range cache serves those bytes, separating avoided transport from remaining decode work. Evict-before-wave also decodes 616 fragments per warm pass but must re-read payload. This supports the diagnostic distinction without establishing that the benchmark-owned encoded cache should replace the production decoded cache.

Pressure is genuine: the distinct decoded working set is **5,242,880 bytes**, exceeding the 4 MiB declared budget. Each later pass observes **1,088 redecodes and 1,088 evictions** in both B and D, with 5,010,015 encoded payload bytes fetched. This is a bounded eviction validation, not a performance comparison against an A/C cache-pressure baseline, which was not included in that variant.

### Metadata filtering and interpretation limits

The numeric catalog predicate selects the public GWOSC source (15 datasets) from five candidate sources; each synthetic file has one dataset. The subsequent six-selection trace has warm means A/B/C/D/E of **0.196 / 1.992 / 0.196 / 96.236 / 320.181 ms**. Catalog selection is measured separately; these values are only the selected-source value reads. The runtime checks the actual catalog IPC source/version set against the prebuilt local catalog before proceeding. The ordinary summarizer validates the downstream trace but does not reread `catalog-selection.json`; the separate [catalog evidence audit](../results/hdf5-h11-20261006/catalog-independent-audit.json) passed all three campaigns. It checks the saved selection against the frozen native catalog, the successful metadata-only profile (11 metadata queries and one output row), and the selected source binding in downstream A–E traces. The original catalog IPC bytes were checked at execution time but not retained, so this audit cannot independently reconstruct their source/version values. The prebuilt FS filter took 5.5–8.2 μs; the full IoTDB catalog service took 59.6–61.6 ms. These have different operational scope and neither includes native catalog construction. No index-pruning or large-catalog scaling conclusion follows from five candidates.

The repeat count is three, the sources each expose only 1 MiB of selected Float64 data, compression is mild, and concurrent clients deliberately overlap. Deterministic alternating rather than randomized variant order, shared OS/DB state, detailed profiling overhead and native scheduling remain limitations. The experiment establishes cross-path value correctness and locates current costs; it neither establishes a general HDF5 speedup nor rules out workloads with larger arrays, selective multi-file catalogs, different chunk geometry, or stronger reuse opportunities. H11 adds no new derived layout and makes no H13 workload claim.

Source/build/test fingerprints, the measured binary and per-cell configuration hashes, and local-versus-server verification are preserved in [verification.json](../results/hdf5-h11-20261006/verification.json) and [provenance.json](../results/hdf5-h11-20261006/provenance.json). A compressed source snapshot accompanies the evidence because the measured build includes uncommitted H10/H11 changes. The [environment audit](../results/hdf5-h11-20261006/environment-audit.json) records the failed fresh-database provisioning caused by IoTDB direct-memory exhaustion; the final matrix used new unique source paths in the existing isolated H09 namespace. Existing Arrow/GRIB mounts were preserved, and private H11 mounts and sockets were removed.
