# IoTDB-FS-C: Arrow & TsFile on the typed query model — implementation plan

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

Sep 26, 2026 · @alan cheung  
Code review revision: Sep 27, 2026

Execution order update: Arrow goes first, including its training-window path and migration. See [Arrow code implementation plan](arrow-query-implementation-plan.md) for A00–A13, milestones, file ownership and validation gates. TsFile implementation follows the stable fragment/window contracts; its dependency spike is not a prerequisite for Arrow delivery.

Public evaluation update: TimeBench/TimerLathe are unpublished and remain optional internal regression inputs. Required reproducible baselines use HF Trainer, public datasets and open models; see [Public training baseline plan](public-training-baseline-plan.md) for verified sources, licenses, adapters and PB00–PB04. The public converters emit the same channel-major nested IPC stream target; the native Arrow scope is unchanged.

**Status: reviewed implementation proposal. The training workload and optimization goals in section 4A are user requirements confirmed on Sep 27; Arrow IPC stream input, both `list<float32>` and `list<list<float32>>`, and a physical data model independent of layer semantics are also confirmed requirements. Remaining interface and policy recommendations D1–D19 remain proposals pending confirmation.** This revision corrects assumptions against `feat/arrow` at `41751b2267137133b2001c1cde24b49ff45aec72`. A02 has started with the non-semantic physical model and pre-capability subject entry; no native Arrow adapter or acceptance result is claimed. Follow [AGENTS.md](../../AGENTS.md): uncertain code design requires Alan's confirmation; do not introduce compatibility code unless requested; Git writes remain with Alan.

## 1. Goal and scope

Add Arrow IPC and TsFile as the second and third Format Identities, each with an ingestion `FormatAdapter` and a typed `FormatQueryAdapter`, reachable through file paths. Reuse the typed model, but do **not** promise that all public seams remain unchanged: ingestion source access is a signature change, and sequence/table selection before capability resolution, catalog membership, budgets, shared decode access and training-window output need explicit decisions first.

**Primary workload: multivariate time-window training across multiple experiments.** Training sample boundaries do not generally match stored files, Arrow record batches/buffers, or TsFile pages/chunks. The GRIB counterpart is reading selected messages from a much larger file. File-format support is the foundation; the core training goal is to generate requested windows on demand, reuse immutable decoded fragments across overlapping windows and experiments, and assemble only the necessary portions of multiple files. Experiments share base data while keeping their variable selections, window lengths/strides, labels and transformations in separate views. Pre-materializing every overlapping window as another stored training dataset is not the default design.

The intended evidence addresses F12 and W10 in [PAPER_CODE_CONSISTENCY_AUDIT.md](../../../VLDB-2027-IoTDB-FS/PAPER_CODE_CONSISTENCY_AUDIT.md). These audit items close only after implementation and live acceptance, not when this plan or in-memory fixtures are complete.

For each format, completion requires:

- Content detection, persisted Format Identity, and queries that survive suffix changes and rename.
- Byte-identical conventional reads of payload `B`; versioned Layout Artifact `L` and bounded typed metadata `Sigma`; publication remains `B -> L -> Sigma/identity -> publish`.
- Projection, typed predicates and Domain Slices planned into bounded byte ranges, followed by exact row semantics and typed `QueryBatch` output with provenance.
- Cross-file selection, latest/exact/all/between version selection, and stable Prepared Queries. Mixed-format **file selection** and homogeneous **data concatenation** are separate capabilities.
- A typed FUSE path read by a standard Arrow IPC stream reader, with defined size, random-read, error, and handle-lifetime behavior.
- Measured range-read reduction on the live IoTDB + FUSE stack, with client bytes separated from server/storage I/O.
- Lazy multivariate windows with declared entity/time alignment, input and target intervals, stride and missing-data policy; local assembly across file boundaries.
- Decode reuse for overlapping windows and for multiple experiments/views over the same versioned fragments, with bounded memory, concurrent-load deduplication and view isolation.
- Training acceptance TW-01–TW-10 and E5–E8, including decode counts and sample throughput. Passing only the single-query AQ/TQ rows is no longer sufficient for the training goal.

Confirmed Arrow v1 target is a persisted IPC **stream** with a selected `list<float32>` or `list<list<float32>>` field. The physical contract fixes source/version, batch/row, native field, anonymous List child path and leaf positions; it does not assign series/channel/time meaning. The TimeBench multivariate case can be interpreted as channel-major `[C][T]` only after its layer semantics are validated. Nested slicing is core, not deferred as an unsupported logical type. Preserve the original stream in B; build its random-access directory in L. IPC file/Feather input and generic recursive logical-array support are optional follow-ups, not prerequisites. Scalar side fields can supply explicit semantic bindings; single-level List is read as itself, never silently reshaped. TsFile remains proposed unencrypted v4 table-model scope; tree/v3, schema union, natural-language queries and web/Python frontend migration remain separate work. The environment baseline in arrow-experiment-environment-baseline.md（服务器归档：`docs/arrow-experiment-environment-baseline.md`） is sampling evidence, not a claim that all TimeBench payloads have been inspected.

## 2. Verified base system and blocking gaps

Only `GribFormatAdapter` is registered in `main.cpp`; the typed `QueryModule` is exercised by tests but not wired into production FUSE dispatch. A01 now passes the existing `FormatSource` (`size`, `read(offset,length)`) into `FormatIngestionSession` for synchronous, checked range reads. `DomainKind::Rows` and `TimeSeries` already exist. `QueryModule` already provides residual evaluation, final projection, execution I/O budgets, cancellation and immutable version selection, subject to the limitations below.

Code pointers describe the reviewed commit; prefer symbols over line numbers after implementation starts.

| Gap | Verified code | Migration consequence |
| --- | --- | --- |
| G1: production dispatch is legacy | `main.cpp`: `op_getattr`, `op_open`, `op_read`; `FormatAdapter::parse_query/open_virtual` | Adding query adapters alone exposes no typed path. `op_access`, handle dispatch, release and size reporting must migrate together. |
| G2: missing Sigma becomes `UnknownField` | `QueryModule.cpp`: `manifest_predicate_schema`, `manifest_values`, `QueryModule::prepare` | Validation is per manifest. Introduce a stable scalar metadata schema and distinguish absent values from misspelled fields. |
| G3: strict result schema equality | `QueryModule.cpp`: `same_field`, `same_schema`, `projection_schema` | Equality applies to the **final projected schema**, including id, display name, type/nullability/timezone, role and unit in projection order. Extra unprojected columns alone do not cause failure. |
| G4: no nested logical arrays | `QueryModel.h`: `LogicalTypeId`, `LogicalValue`, `LogicalArrayData` | No recursive List type exists. Decode the required native `[C][T]` structure privately into a bound scalar step view / WindowBatch; recursive LogicalArray support is not mandatory. Other unsupported native types require deliberate errors. |
| G5: indexed source access unavailable to ingestion session (resolved by A01) | `FormatAdapter.h`: `begin_ingestion(const FormatSource&)`, `finish()`; `Ingestion.cpp` | The session receives the original stable source through an exact, bounded reader. A detector's past-end request means that candidate does not match; real short/failed reads abort instead of silently selecting pass-through. A selected session's past-end request also aborts. The callback is invalidated when synchronous ingestion returns. Arrow-specific message/FlatBuffer indexing remains A03. |
| G6: metadata config and peripheral routes assume GRIB | `MetadataConfig.cpp`, `resources/metadata.properties`, `CrossFileClient.cpp`, `SemanticGrepClient.cpp` | Per-format configuration needs parser changes and a migration policy. Old entry points do not become generic automatically. |
| G7: catalog loads all manifests | `IoTDBStructuredBlobBackend.cpp`: `make_iotdb_published_version_catalog` | In-memory scalar filtering is a valid v1 implementation, but catalog scan cost must be reported. `max_sources` currently limits candidates before residual filtering. |
| G8: collection predicates are absent from typed catalog expressions | `QueryModule.cpp`: scalar-only Sigma schema; `QueryModel.h`: `InExpression` | `IN` means scalar-in-literal-list, not set membership. Storing StringSet in Sigma/`blob_metadata_set` does not make `contains(column)` queryable through the typed API. |
| G9: residual-only capability declaration is rejected | `QueryPlanning.cpp`: `validate_query_capabilities` | Nonempty predicate operators with `PushdownLevel::None`, and `sliceable=true` with `None`, are rejected. The proposed no-statistics path needs a validation contract change. |
| G10: budgets do not cover the proposed frontend/decoder contract | `QueryPlanning.h`: `QueryLimits`, `FormatQueryAdapter::prepare`; `QueryModule.cpp`: `ExecutionBudget` | `prepare(source,fragment)` receives no limits. Planning checks declared layout size, not all actual reads. `target_batch_bytes` is not a hard memory or encoded-result cap. |
| G11: contextual sequence/table selection is absent | `QueryPlanning.h`: `QueryFragment`; `FormatQueryAdapter::capabilities(source)` | Capabilities are resolved before fragment planning. A sequence row/channel binding can change schema and step count; resolve it before capabilities, including ordinary typed queries. Frontend-only `series=...` or `table=...` cannot fix this seam. |

The GRIB implementation already offers a useful precedent: `GribFormatQueryAdapter.cpp` contains `GribDecodedFieldCache`, keyed by immutable version and message range, with shared futures for concurrent loads; `decode_cached_grid` separates native decoding from view rendering. This is adapter-scoped GRIB behavior, not an Arrow/TsFile training-window implementation. `QueryFragment` has no window specification, `CombinedQueryCursor` concatenates sources, and `LogicalArray` owns its values rather than representing shared slices.

### Core invariants to retain

1. Conventional file reads reconstruct `B`; typed queries never overwrite `B` with normalized Arrow/TsFile data.
2. Adapters use `QuerySource`, not IoTDB SQL, local-path reopening, or storage/backend handles. Execute must use the **execution-time** budgeted source, not a source captured during prepare.
3. A Prepared Query pins versions. Adapter plans/cache keys include source identity, layout/schema version, relevant options, and normalized query semantics. A cursor owns decoded buffers needed by outstanding batches and releases them on cancel/destruction.
4. Candidate pruning must have no false negatives. Missing statistics, missing optional summaries, NaN/unsupported comparisons and incomplete sets cannot prove absence.
5. Exact pushdown means exact output rows, not merely intersecting batch/page statistics. Unsupported operations fail explicitly; they are not silently ignored or implicitly cast.
6. Window/view identifiers must not fragment the canonical decode cache. Immutable source identity and physical decode dependencies determine base-fragment identity; experiment transformations never mutate cached base data.
7. One training snapshot fixes the source-version map across minibatches/epochs. Re-running prepare(latest) independently for every window does not provide this guarantee.

## 3. Arrow IPC stream and channel-major sequence design

`format_id = "arrow-ipc"`; L records the container variant. **User-confirmed physical target:** a top-level row contains either `list<float32>` or `list<list<float32>>`. The first supported numeric leaf is float32, checked against the real schema before ingestion. Batch/row and List child indices are physical coordinates only. Record-batch rows are not training timesteps, and native columns are not necessarily variables; those meanings require separate discovery and validation.

| Stage | Proposed implementation contract |
| --- | --- |
| Detect | Validate stream message framing and an initial Schema message using bounded reads. A stream has no `ARROW1` footer requirement; a continuation marker alone is insufficient detection. D14 distinguishes unmatched bytes, recognized corruption and unsupported variants. Optional file support uses a separate file-specific path. |
| Ingest | Use C4 to traverse schema/dictionary/record-batch metadata and advance by validated message/body lengths. Preserve B exactly. Index required offsets/validity and declared series bindings; skip unrelated value bodies except approved statistics or irreducible compressed dependencies. Validate lengths, bounds and record counts before publication. |
| Sigma | Bounded `arrow.num_rows` means top-level series rows, never time positions; record batches and schema fingerprint plus optional complete field/channel inventories. Channel count and per-channel lengths can differ by series and belong in L; do not invent a global time-row count or timestamp bounds. |
| Layout | Versioned `IOTDBFS-ARROW-LAYOUT/1`: stream framing/schema, batch ordinals and top-level row ranges, metadata/body spans, recursive field nodes/buffers, compression descriptors, dictionary dependency epochs and end condition. Include lossless native schema, selected sequence path, offset widths, series-to-channel and channel-to-child indexing, validity dependencies and versioned semantic bindings. |
| Logical schema | `PhysicalArraySubject` selects exact source/version, field, batch/row and anonymous child paths **before** capabilities; both List depths use one physical resolver. A separate semantic binding may map child paths to variables and leaf positions to step/time after validation. WindowBatch carries sample/variable axes separately. Keep raw nested schema private; no fabricated Binary type and no mandatory recursive LogicalArray migration. |
| Capabilities | Bound channels are projectable; exact step/time slices require proper child trimming. Value predicates use decoded scalar views with C7 residual support. Batch statistics are optional candidate evidence, not a prerequisite. Unbound/ambiguous series selection is an explicit error. |
| Prepare | Resolve requested channels and internal step intervals through outer/inner offsets, include all bitmap/offset/time and predicate dependencies, deduplicate stable child tiles or physical compressed-buffer keys, and produce bounded sorted ranges. Include metadata/index reads and coalescing overhead in budgets. |
| Execute | Read only through budgeted QuerySource. Decode indexed dependencies once through the shared service; assemble exact scalar step views or WindowBatch outputs with source row/channel/child-span provenance. Validate/rebase bitmaps, retain fragment leases, and copy only bounded final results. Arrow C++ decoding is private and must not silently load the full stream or batch. |

### 3.1 One- and two-level offsets and validity

For `list<float32>`, an empty child path selects row `r`; a leaf range `[u,v)` resolves directly to `[I[r]+u,I[r]+v)`, provided `0 <= u <= v <= I[r+1]-I[r]`. For `list<list<float32>>`, the following extra offset level selects a child ordinal `c`. Neither shape alone assigns `c` a variable name or `u` a time meaning.

For a selected batch, let `O` be outer list offsets, `I` inner list offsets, and `V` the float32 child values. The formulas below use validated serialized child coordinates; any Arrow API array slice/base offsets must be normalized before applying them.

```text
r = selected top-level row (one series)
C_r = O[r+1] - O[r]
j = O[r] + c                       # 0 <= c < C_r; inner-list/channel index
T_rc = I[j+1] - I[j]                # this channel's own length
requested local steps = [u, v)     # 0 <= u <= v <= T_rc
child interval = [I[j]+u, I[j]+v)
uncompressed values byte range = values_base + 4*(I[j]+u), length 4*(v-u)
valid(r,c,t) = row_valid[r] AND channel_valid[j] AND value_valid[I[j]+t]
```

Validate offset counts, nonnegative monotonic offsets, child bounds and checked arithmetic at every present level. Offsets are structural, not proof of validity. An absent bitmap is all-valid only under the corresponding Arrow null-count/layout contract. Null row List, null child List, empty List and null float remain distinct; ignore undefined values under null parents. Rebase non-byte-aligned bitmap slices. The v1 exact physical types are ordinary List/Float32 and List/List/Float32 with 32-bit offsets; LargeList, FixedSizeList, mixed offset widths and other leaves need explicit additional support, not reinterpretation.

Example: `O=[0,2,4]`, `I=[0,8,16,22,28]`. Row 0 has two channels of length 8, row 1 two channels of length 6. Row 0/channel 1 steps `[2,5)` are `V[10:13]`; row 1/channel 0 steps `[1,4)` are `V[17:20]`. This illustrates why neither batch row slicing nor top-level column projection implements the requested windows. Unequal channel lengths are legal structurally; an aligned training view must reject or apply an explicitly chosen mask policy, never silently pad, truncate or transpose.

### 3.2 Stream indexing and bounded access

Persisted stream bytes can be randomly read through FormatSource/QuerySource; the missing stream footer is supplied by an external L message directory. Initial ingestion can scan the message sequence once; queries must use the directory and never replay all earlier batches for every window. Recognize schema-only/zero-batch streams, zero-length batches, explicit EOS and clean EOF at a complete-message boundary according to the pinned reader contract. Partial prefixes, truncated metadata/body, invalid padding/offsets and bytes after EOS receive explicit errors/policy; do not accept a second concatenated stream or trailing garbage silently.

Dictionary use is not expected in the float32 leaf, but side fields may depend on dictionaries. Index each dictionary id's replacements/deltas by stream epoch and the exact state needed at each record batch. Support restoration through bounded indexed dependencies, or explicitly reject a requested unsupported dictionary field; do not use the final dictionary for an earlier batch or fetch every dictionary for an unrelated channel query.

List offsets are body buffers, not merely record-batch metadata. L must persist or page-index the required row/channel offsets, validity and series bindings with byte/count/decompression limits, and support bounded lookup rather than deserializing the whole per-series index at query time. Report ingestion reads and L size; do not claim metadata-only ingestion if offsets or binding fields are read. If a compressed offset buffer is irreducible, budget its complete decompression or fail explicitly. No full value scan is required solely to locate a float32 sequence window.

### 3.3 Physical reuse and performance boundary

For uncompressed children, use stable aligned tiles of the canonical child values buffer plus reusable bitmap/offset dependencies; tile identity is based on immutable physical child coordinates, never exact window endpoints. Fetching a small interval may include measured tile/coalescing overhead. Several channels/rows can share one child buffer, so keys include the physical buffer/tile rather than an arbitrary view channel name.

For compressed buffers, the reusable decode unit is the **whole physical compressed buffer**, with its encoded prefix and all dependencies. It may contain values for many channels or series rows. Cache/deduplicate its decompression once and then create channel/step slices; a per-channel cache key would duplicate work. Treat a buffer marked stored-uncompressed within compression framing according to its actual descriptor. Cold-read savings are not necessarily proportional to selected channel/time fraction; huge buffers can make first-window cost high. Enforce advertised decoded-size and in-flight memory limits before allocation; no partial-decompression promise. Distinguish encoded-byte hits, decoded-buffer hits, output slicing/copy work and re-decodes after eviction.

Cache keys contain immutable source/version, layout/schema/decode version, physical buffer and optional stable tile coordinates, codec/dictionary epoch and canonical decode options. They exclude experiment/view id, window anchor/length/stride and projected channel sets. Mapped names, reorderings and transforms are applied after base decoding.

### 3.4 Binding and result semantics

Use actual channel labels only when supplied by schema/data or a versioned dataset binding. Otherwise expose explicit positional channel ids and preserve their order; do not infer names or claim positional ids align across unrelated files. A series can be selected by validated identity or explicit `(batch,row)`; absent identities remain source-local. Dataset manifests/shard order (and any row-selection index) must be resolved explicitly, never treated as time continuity.

The default time axis is an integer step index. Absolute timestamps require supplied timestamps or declared `start + step * cadence`, with units, precision, overflow and timezone checks. No epoch/absolute time is fabricated from row number or sample frequency. Bind all selected channels to one consistent axis; unequal lengths, missing timestamps and per-channel axes follow D15. Cross-file continuation requires the same declared logical series, channel mapping and global step/time coverage (section 4A.5).

Supported scalar side fields can use the prior bool/int32/64/uint32/64/float32/64/Utf8/Binary/timestamp mapping if implemented and tested. Keep unsupported native fields inventoried privately and return deliberate unsupported errors when selected. Date/Decimal/Struct/general nesting and dictionary value decoding remain separate support decisions; the required two-list sequence path is not deferred by this policy.

Current LogicalArray can own the scalar channels of a bound step view. A contiguous positional result can use `dimensions=[step]`, `shape=[steps]`, `origin=[first_step]`; a TimeSeries domain requires a real bound time dimension. The DomainDescriptor shape product must equal row count, and its unsigned origin cannot carry signed timestamps. Sparse predicate output must retain explicit step/time values and not claim contiguous coverage. A dedicated WindowBatch or versioned lossless flat encoding represents sample/input/target axes; ordinary residual filtering must not collapse cadence inside training windows.

## 4. TsFile design

`format_id = "tsfile"`. Proposed initial scope: unencrypted v4 table-model files with an explicit supported type × encoding × compression matrix established by S2. File version, table/tree model, and decoder support are separate checks; the version byte alone does not prove the table schema is supported.

| Stage | Proposed implementation contract |
| --- | --- |
| Detect | Recognize `TsFile` and version byte, then validate footer/trailer and metadata bounds. v3, tree-model and encrypted files need explicit unsupported behavior under D6/D14. |
| Ingest | Use C4 to parse footer, table schemas, metadata index, device/tag identities, timeseries/chunk metadata and required chunk/page headers. Bound traversal depth/count/bytes and validate offsets. Record raw timestamp precision and normalization policy in `L` and Sigma under D7. |
| Sigma | `tsfile.version`, optional complete table/device/measurement sets, normalized time bounds with a documented unit. Full inventories remain in `L`. Never truncate a set and then use it as exact negative evidence. |
| Layout | Header `IOTDBFS-TSFILE-LAYOUT/1`; table schemas and roles; device/tag mapping; raw types/time precision; chunk header/body offsets and lengths, encodings/codecs, statistics with availability flags; page header/body spans, compressed/uncompressed sizes, row counts and time/value associations. Single-page chunks may omit page statistics; use chunk statistics or retain the candidate. |
| Logical schema | Time is a Dimension; tags are Utf8 Attributes; fields are Measures. Native schema nullability and aligned missing values must map consistently. One relation per semantic query requires D11, since source-wide capabilities currently have no relation selector. |
| Capabilities | Time-range statistics are Candidate pruning; claim Exact slice handling only with row-level trimming. Tag Equal/In can be Exact only when complete device/tag identities prove the result and null semantics agree. Value comparisons remain Candidate with residual evaluation unless execute applies them exactly. |
| Prepare | Select devices/chunks/pages conservatively. Required value fields include predicate-only fields. Retain all time-page/value-page/null-bitmap dependencies; deduplicate shared time reads. Bind plan to source, table, schema/layout version and precision. |
| Execute | Read through the budgeted QuerySource; decompress and decode pages; associate aligned values with time positions and null bitmaps. Produce rows by device identity plus timestamp/position, not timestamp alone across devices. Apply exact boundaries and predicates, then emit bounded batches with provenance. |

**Aligned semantics are an S2 gate.** Decoding one standalone value page is insufficient. S2 must reconstruct time plus at least two aligned value columns across page boundaries, including missing/all-null values, and match the pinned reader. Do not zip independently pruned value pages or inner-join away null rows. The exact row-presence rule, duplicate timestamps, ordering across devices and overlapping chunks must be recorded from the supported writer/reader contract. If the supported subset excludes a case, detect it explicitly. A global time-order guarantee requires a merge; do not claim it merely because each device is ordered.

**Decoder choice (D8).** Prefer a pinned Apache TsFile dependency and adapter-private shim around proven decoder/decompressor entry points. Public `TsFileReader::open` is path-based in the inspected API; a temp copy is an oracle only, not the query execution path. Whether vendoring, linking, or a small upstream extension is sufficient is the S2 result. Writing our own decoder subset is a scope change requiring review, not an automatic fallback.

**Types and time.** BOOLEAN/INT32/INT64/FLOAT/DOUBLE map naturally. TEXT/STRING require a documented text validity policy; BLOB is Binary; TIMESTAMP and the primary time dimension need the persisted precision policy. Do not silently assume ms from a filename or current mount configuration. D7 must identify where ingestion obtains precision and what happens when it is absent or inconsistent.

**DATE correction.** TsFile DATE is encoded as an integer calendar date `YYYYMMDD` in the inspected C++ converter; it is **not** epoch days. If D5 chooses Int32 days or Date32, perform validated calendar conversion with an explicit epoch. Arrow Date32/Date64 need their own mapping. Neither can be handled by relabeling the raw TsFile integer with `unit=days`.

## 4A. Training windows and shared experiment views — core design

### 4A.1 Coverage assessment of the previous plan

| Training requirement | What the previous plan contained | What is still needed |
| --- | --- | --- |
| Multivariate time windows | Projection and one-shot row/time slices | Series/channel/step-time binding, cadence/alignment, window anchors, lookback/stride/horizon and target variables. |
| Overlapping windows on demand | Bounded QueryBatch output | Lazy window enumeration and bounded union of fragment demands; no full expanded-window dataset. |
| Decode reuse between windows | Version/cache invariants and warm/cold measurements | Named canonical decoded fragments, cache ownership/lifetime, keys, leases, concurrent-load deduplication and counters. |
| Cross-file partial windows | File pruning and source concatenation | Series/step/time coverage directory, boundary-fragment fetch and deterministic chronological/alignment assembly. |
| Multiple experiments, different views | Generic projection and predicate semantics | Shared base-fragment pool plus immutable experiment view definitions, snapshot identity, transformation isolation and process-sharing scope. |
| Evidence of training benefit | E1–E4 measure individual queries/ingestion | Overlap sweeps, partition-boundary workloads, multiple experiments, bounded-cache tests and decompression/decode counts. |

Therefore the previous plan contained useful prerequisites but did **not** yet design or verify these optimizations. They are core requirements in this revision, not a post-v1 cache improvement.

### 4A.2 Separate stored data, decoded fragments, windows and views

Proposed flow (component names are design vocabulary, not existing APIs):

```text
DatasetSnapshot + TrainingViewSpec + requested sample ids
                   |
       WindowPlanner / coverage lookup
                   |
     bounded union of required fragment keys
                   |
     Arrow/TsFile fragment decoder + shared cache
                   |
  WindowAssembler: entity/time alignment across files
                   |
     experiment view transforms / input-target masks
                   |
       WindowBatch -> loader or bounded IPC result
```

- **Stored base:** existing immutable `B + L + Sigma`. Do not repack files per experiment or per window. `L` records reusable decode boundaries and time/entity coverage independently of a particular window length.
- **Decoded base fragment:** immutable native/logical scalar values, optional bound timestamps and validity for a stable physical unit. No experiment normalization, label construction or random augmentation is baked in. Reuse this expensive decode output even when final windows must be copied into a tensor/IPC buffer.
- **Window descriptor:** a requested sample's entity, anchor, input/target spans and references to pieces of decoded fragments. It can straddle several physical units and files without storing another full sample.
- **Experiment view:** selected/ordered input and target variables, window specification, alignment/missing policy, deterministic transforms and any train-fitted artifacts. It is cheap metadata over the same snapshot/base fragments; final transformed results are distinct per view.

The first shared deployment target is multiple experiments/workers served by **one long-lived query service or FUSE mount process**. An adapter instance inside each unrelated training process does not share an in-memory cache. D17 must fix this topology; shared-memory/distributed caching is a separate extension. Sequential experiments benefit only while useful fragments remain resident. The cache is an optimization, never a durability requirement.

**2026-09-29 Arrow training specialization:** the decoded tile and the physical storage call are separate units. Ordinary Arrow typed queries retain their present single-fragment `ArrowFragmentProvider::get()` path, matching the current GRIB typed-query style; the training adapter alone batches missing Arrow tiles for a bounded window group. The cache key remains a single canonical tile (or whole compressed buffer), independent of that I/O batch. This specialization is now implemented; the concrete contract and Electricity-only A/B evidence are in [Arrow training batched range design](../knowledge/arrow-training-batched-range-design.md) and [the 50-step result](../reports/public-training-electricity-tilebatch-20260929.md).

### 4A.3 Snapshot, schema binding and window contract

`DatasetSnapshot` proposes an immutable selected `(storage namespace, path, version)` map, schema/time/entity bindings, partition coverage, split policy and a reproducible fingerprint. Resolve latest once when preparing the training view; retain/reopen these exact versions for subsequent sample requests. Different views may share any identical source fragments even if their snapshot fingerprints differ elsewhere. Normal user access checks still apply before cache hits are served.

`TrainingViewSpec` needs these explicit fields before code is written:

| Contract | Required meaning |
| --- | --- |
| Entity identity | Device/tag tuple or explicitly selected Arrow top-level series row/identity. Row identity is separate from inner step position; a window never crosses independent rows merely because they share a batch/file. |
| Time | Arrow internal step index by default; absolute time only from supplied timestamps or explicit start/cadence mapping. Preserve precision/timezone where present. A top-level row ordinal is not a timestamp. |
| Input / target | Ordered variable lists; input length L, target length H, optional gap G; explicit sample-count versus duration units. Target variables may differ from input variables. |
| Anchors / stride | Anchor set or lazy range plus stride S; stable window id from snapshot/view/entity/anchor. Define boundaries as half-open intervals and enumerate deterministically. |
| Alignment | First Arrow path uses declared common step positions for selected channels; timestamps require an explicit binding. Check per-channel lengths and apply a confirmed reject/mask policy for gaps. No silent padding, transpose, resampling or tolerance join. |
| Missing / duplicate data | Explicit reject-window, mask/pad, or approved fill behavior; distinguish absent timestamp from null value. Duplicate `(entity,time,variable)` values across files must follow a declared conflict rule or fail; file enumeration order is not a rule. |
| Temporal split | Split is part of view identity. Specify permitted input and target ranges; train-derived statistics use only the designated training data. Overlapping train/validation windows must follow the declared policy rather than silently sharing labels. |
| Transforms | Record operator/version, units, parameters, fitted-artifact identity and seed where applicable. Do not infer normalization from the currently requested minibatch. |
| Ordering / workers | Sample enumeration, shuffle seed/epoch and worker shard determine delivered order independently of physical fetch order. No dropped/duplicated samples when internal I/O is reordered. |

For regular sample positions with the anchor `a` defined as the first target position, a proposed convention is input `[a-G-L, a-G)` and target `[a, a+H)`. The next anchor is `a+S`. This is a convention to confirm under D15, not a formula applied to arbitrary irregular timestamps. An input-only view sets H=0. The planner includes dependencies needed by targets and supported transforms as well as inputs.

Views over different file schemas bind a common **selected** variable schema before assembly, including types, units and time semantics. This is explicit training-dataset mapping, not implicit schema union or a promise to join arbitrary GRIB/Arrow/TsFile datasets. Arrow and TsFile each must support the training path; cross-format alignment within one sample is a separate extension unless explicitly required.

Predicate placement is part of the view contract. Distinguish entity/source selection, value-validity masks, whole-window eligibility and deliberate row filtering. Removing rows before constructing a sample-count window can collapse time gaps and change the intended sample. For cadence-based training, preserve time positions/masks or reject the window according to policy; do not reuse ordinary residual row filtering without this distinction.

**Arrow indexing addition:** bind series row, sequence field, positional/named channels and inner step/time axis at ingestion or through a versioned dataset index. Persist/page-index outer and inner offsets plus validity, channel lengths and declared continuation coverage. Step-indexed queries need no invented timestamp column. Explicit timestamp mappings need ordering validation/indexes for binary search; never rescan all payloads for each window. Resolve this contextual binding before ordinary typed-query capabilities as well as before window planning. Dataset shard order or adjacent top-level rows alone do not establish continuation; check row-selection indices and reject mismatched shards.

**TsFile indexing addition:** preserve table/device identity and time-page/value-page relationships across files. Coverage lookup finds the relevant device/time intervals; no global timestamp merge across unrelated devices. File-version selection and duplicate partition overlap are distinct concerns.

### 4A.4 Fragment planning and decode reuse

Stable fragment keys correspond to real decoding dependencies:

| Format | Proposed reusable unit | Dependencies |
| --- | --- | --- |
| Arrow IPC stream | Stable uncompressed child-value tiles; whole physical compressed buffers even if spanning channels/rows | Shared outer/inner offset and three-level validity dependencies, schema, declared time mapping and exact dictionary epoch. Channel/window views slice the same base handles. |
| TsFile | Decoded time page and measurement value page(s), with stable row-position association | Chunk/page metadata, codec, null bitmap and corresponding time page. If a codec actually needs a larger unit, expose that verified unit rather than invent independent pages. |
| Existing GRIB precedent | Native decoded message field, before rendering | Immutable file version plus message offset/length. GRIB training-window integration is not implied by reusing this design idea. |

A proposed cache key contains storage namespace, exact file identity/version, adapter/decode version, schema/layout identity, physical unit/field id and canonical decode options (including relevant time normalization). It excludes experiment id, window anchor/length/stride and unrelated projected fields. A bound channel projection `{x,y}` followed by `{y,z}` reuses common child tiles/dependencies. If all channels share one compressed values buffer, the second projection reuses that entire decode; the first may already have decoded z physically. Query predicates and slice boundaries are applied **after** canonical base decoding unless provably independent of the cached representation.

Requested windows are planned in bounded minibatches/lookahead groups. For each field/entity, union their temporal demands, map that union to physical fragments, then deduplicate dependencies. Do not replace disjoint requests with the single enclosing interval and decode a large unused gap. Demand from another experiment can attach to the same pending fragment. Range coalescing is still a physical optimization with measured over-read.

For Arrow training, first claim only true missing keys in the shared cache, then group their byte dependencies by exact source/version. Deduplicate overlapping ranges and merge directly contiguous ranges without over-read; place remaining noncontiguous ranges in one bounded multi-range storage request. Decode and publish each tile under its original key. The training adapter performs this grouping and preserves sample order; common source/storage and cache services supply multi-range transport and per-key single-flight. Do not change ordinary Arrow typed or GRIB query fetching in this step. When the current lookahead size exceeds its bound, split work into budgeted waves rather than silently issuing one SQL per tile. Backend support must be shown to execute multiple ranges in one real request: an API that loops over scalar reads is insufficient. See [the detailed Arrow training contract](../knowledge/arrow-training-batched-range-design.md).

Example: equally spaced input-only windows W0=`[0,96)` and W1=`[24,120)` require 120 unique time positions for a common variable set, although they emit 192 positions. If two files are explicitly declared as continuations of the same series with matching channel mapping and split at global step 80, W0 uses the tail/head pieces `[0,80)` and `[80,96)`; W1 uses `[24,80)` and `[80,120)`. Decode the union of required physical units once while resident, then assemble the two windows. Physical buffers/pages may force more than 120 positions to be decoded; compare with the unique-unit lower bound, not a false exact-row I/O claim.

Reuse guarantees are scoped: with sufficient budget and no eviction, concurrent/repeated demand decodes each identical key once; after eviction, re-decoding is expected and counted. This is not an unbounded decode-once-for-all-epochs promise.

### 4A.5 Cross-file local assembly

1. Prepare a snapshot's small file/partition coverage metadata; bound/index the catalog cost. For Arrow, validate explicit logical-series identity, channel mapping and global step/time coverage. Independent dataset rows or shards are not adjacent time partitions. Loading coverage is distinct from scanning payloads.
2. For each requested entity/window, find only declared continuation partitions overlapping its input/target dependencies; use L to locate Arrow row/channel child intervals or TsFile pages. Match channel schema, cadence/time mapping and partition boundaries. Selected variables may live in explicitly mapped variable partitions.
3. Acquire decoded fragments lazily. Open payload cursors only for demanded sources; current `QueryModule::execute` creates all selected source cursors and is not the desired window scheduler.
4. Assemble by declared entity/time keys and field mapping. Preserve null masks, verify cadence and ordering, and apply duplicate/missing policies. Concatenation in path order is insufficient; value-page row positions must remain associated with their time pages.
5. Record provenance for all contributing file versions and fragment spans per window. Do not claim a provenance list without sample/span mapping is enough to reconstruct a boundary window.

For declared sorted, nonoverlapping time partitions this can be a bounded boundary merge. Irregular timestamps, overlapping partitions and variable-partition joins need explicit bounded alignment state. Choose a first supported subset under D15/D16 and detect excluded cases; do not silently fall back to whole-dataset materialization.

Time-partition stitching and variable-partition joining are separate capabilities. A file containing x and another containing y cannot each satisfy projection `{x,y}` through today's strict per-source QueryModule path. Supporting this case requires field-specific subplans and a training assembler that validates the combined mapped schema. It is not fixed by allowing nullable schema union alone; the entity/time join must be explicit.

### 4A.6 Shared cache, isolation and backpressure

Prefer immutable shared handles/leases for decoded fragments, and a cache service injected into long-lived format adapters/execution contexts. Reuse the GRIB native-decode/render separation as a precedent, but audit its accounting before generalizing it: resident-cache eviction does not bound external references or in-flight allocations.

- Deduplicate concurrent cache misses using one producer and shared waiters per key. Publish only complete validated fragments; remove failed loads so a later request can retry. Error/cancellation tests must cover producer and waiter lifetimes.
- Canceling one experiment detaches its demand. It must not cancel a shared producer still needed by other experiments or leave callbacks referencing a destroyed query source. A shared load owns a version-pinned authorized source capability through completion.
- Bound resident decoded bytes, pinned leases, in-flight encoded/decoded buffers, assembly/transformation/output buffers and prefetch independently under a process-wide ceiling. Reserve before decode; an oversized irreducible fragment receives a defined error or approved bounded alternative. Avoid evict-and-redecode loops when every fragment is pinned; backpressure consumers/producers.
- Eviction may remove only reusable cache ownership; active leases remain valid and counted. Release window/minibatch leases promptly. Do not retain an entire epoch of descriptors with pinned fragment data.
- Admit only currently demanded or bounded-lookahead fragments; share time columns and overlapping variables across views. Fair queues/per-experiment quotas prevent one scan or experiment from displacing all useful data or monopolizing decode workers.
- Separate physical-read/decode accounting (charged once to the shared operation/service) from each consumer's logical demand, delivered samples and output memory. A hit can consume zero payload bytes while still consuming substantial memory and output budget. Retaining a first request's `BudgetedQuerySource` as a global producer is not a complete cross-query budget contract; D17 must define service and per-request limits together.
- Apply each view's normalization, variable ordering, target construction and augmentation to private output or an explicitly keyed derived cache. Optional derived-view caches include the transform/fitted-artifact/seed identity and never replace the canonical base cache.

Cross-experiment sharing means shared computation/storage residency for identical versioned fragments. It does not mean that numerically equal bytes under different source/version identities are deduplicated, or that different native formats share a decode entry.

### 4A.7 Interfaces and delivery

Proposed additional contracts: `DatasetSnapshot`, `TrainingViewSpec`, `WindowRequest` (sample ids/anchor range and bounded batch size), `PreparedWindowQuery`, immutable `DecodedFragmentHandle`, `WindowDescriptor`, `WindowBatch` and `WindowCursor`. Exact class names and ownership belong to D18; these contracts are absent today.

The planner also needs a format-neutral way to resolve window demand to fragment identities and acquire decoded handles. Today's `FormatQueryAdapter` only exposes capabilities/prepare/execute over QueryBatch cursors; it has no fragment-access method. D18 must choose a separate fragment-provider contract implemented alongside the adapters, or an explicit adapter-seam extension. Keep native Arrow/TsFile decoder objects private and share their implementation between ordinary queries and training access so the two paths use the same canonical cache.

The Arrow training batch interface is an extension of that separate fragment-provider seam, not a new `FormatQueryAdapter` capability. Its batch admission is keyed per tile in the same runtime; physical read batching is restricted to the training adapter. A source-level multi-range capability may be shared infrastructure, but the existing typed cursors do not call it. A training-only batch must still use the pinned `QuerySource` identity and the source's ordinary authorization boundary.

A window batch needs sample identity, series/entity, input/target step positions and optional explicitly bound timestamps, ordered variables, values/masks, shape and per-window provenance. The current `DomainDescriptor` describes flat batch rows, not input and target tensors plus a sample axis. Do not overload it to invent implicit training dimensions. Either introduce a dedicated window result model or an explicit lossless flat encoding with sample/step/role identifiers and versioned schema metadata.

Current `LogicalArray` stores owning vectors. A conservative first implementation can reuse native decoded fragments internally and copy only during bounded window/QueryBatch assembly; that already saves repeated I/O and decompression/decoding. Shared array spans or zero-copy tensor delivery need an explicit ownership/API change and copy-byte measurements. Do not claim zero-copy merely because decoding was cached.

Prioritize the in-process training cursor and shared fragment service before FUSE packaging. Bounded materialization for an individual virtual file remains useful, but a virtual file containing every overlapping window in a full epoch would defeat lazy generation. The FUSE training route should represent a bounded window/sample group or a separately approved streaming handle, all served by the same shared base cache. A loader-facing API can follow once the native contract is tested; no framework-specific tensor dependency is required to prove core reuse.

## 5. Core changes and migration contracts

| Change | Required work | Gate / seam impact |
| --- | --- | --- |
| C1: scalar Sigma schema and missing values | Validate scalar file predicates against known metadata definitions plus dynamic fields under an explicit policy; absent known values become typed NULL. Unknown names remain errors, conflicting types fail deterministically, and empty catalogs have a defined validation result. | G2. Missing comparison is Unknown; `IS NULL` is True; `NOT`/`OR` obey three-valued logic. Merely unioning fields from surviving manifests is insufficient when all lack the field. |
| C2: schema policy | Keep strict **projected** schema equality for v1; document all compared properties. Test harmless extra columns and real nullability/type/role/unit mismatches. | G3. No policy stub or union option is needed if strict is approved. Mixed-format file selection needs no schema union. |
| C3: supported type contract | Implement private List/List/Float32 traversal and bound scalar channel/step views; decide remaining D5 types and checked time normalization. | G4. Dedicated WindowBatch avoids making recursive LogicalArray a prerequisite. Any general new logical types still require full validation/comparison/encoding changes. |
| C4: ingestion random access | Proposed signature `begin_ingestion(const FormatSource&)`; session stores a copy of size/read, valid only during synchronous ingestion. Update GRIB and all test adapters. | G5. Read callback must remain stable through `finish`; no reading unpublished storage as a shortcut. The source may be reread, but `B` is persisted exactly once. |
| C5: typed frontend | Path parser → QueryRequest → prepared result → Arrow IPC stream encoder → FUSE handle. Full lifecycle in section 6. | G1. Depends on D1/D2/D4/D12, Arrow contextual binding C10 and TsFile relation decision D11. |
| C6: metadata, registration and builds | Register ingestion and typed adapters together; construct one QueryModule with refreshing published catalog and existing storage; add format-scoped config and real dependency installers/package rules. | G6. Do this incrementally with each adapter, not only at final acceptance. |
| C7: residual-only capabilities | Separate operator support from pushdown level in capability validation and fragment validation. Make supported comparisons/slices with None legal; adapter plans preserve residual expressions and hidden fields. | G9. Regression tests cover no statistics, `OR`/`NOT`/`IN` and mixed Exact/Candidate/None terms. |
| C8: resource accounting | Define and enforce planning, execution, decoded working memory and encoded-result limits. Distinguish adapter plan estimates from actual reads. | G10/D12. Either budget planning sources in QueryModule without changing adapter signatures, or explicitly pass planning limits; record which approach is approved. |
| C9: typed collection membership | Decide whether v1 adds a typed catalog membership operation and residual evaluator, or defers membership/E3's variable-set claim. Scalar IN is not reused as an undocumented contains operator. | G8/D13. In-memory correctness comes before optional `blob_metadata_set` SQL pushdown. |
| C10: contextual sequence/relation selection | ArrowSequenceBinding resolves selected native sequence row/field, channels and axis before contextual capabilities; exact API signature remains to confirm. TsFile single/multi-table remains D11. | G11. Mandatory for Arrow ordinary typed views and training, not optional TsFile-only work. Frontend-only selectors cannot satisfy source-wide capabilities. |

Additional work required by the training goal:

| Change | Required work | Gate / seam impact |
| --- | --- | --- |
| C11: training series/step/time coverage | Versioned Arrow row/channel/axis binding, bounded two-level offset lookup and explicit cross-file continuation coverage. | Locality proof follows actual units: unrelated buffers/tiles are skipped, but a compressed buffer can include unselected rows/channels. |
| C12: shared decoded fragments | Stable Arrow child-tile/compressed-buffer keys and TsFile page keys, immutable leases, concurrent-load deduplication, service lifetime, budgets and metrics. | Repeated/overlapping requests and different projections share actual decode work, not just encoded bytes. |
| C13: window planning/assembly | Snapshot-scoped lazy anchors, bounded demand union, time/entity alignment, cross-file pieces and missing/duplicate policies. | Exact samples against an independent full-data oracle without full payload scan per window. |
| C14: experiment views | Immutable view specs, input/target mappings, transform identities, split/worker rules and isolated outputs. | Multiple experiments share base fragments while producing correct distinct windows/views. |
| C15: training delivery | Window result/cursor contract, bounded output encoding and sample-level provenance. | No implicit tensor shape in DomainDescriptor, no whole-epoch materialization; copy costs reported. |

### Metadata and catalog details

- Stored keys such as `arrow.num_rows` become typed scalar predicate ids such as `sigma.arrow.num_rows`; manifest fields use `manifest.*`, and file-selection output uses `source.*`. Document these distinct namespaces in the frontend.
- Current metadata serialization caps include 128 keys, 64-byte keys, 4096-byte string values/members and 64 KiB serialized metadata (`MetadataConfig.cpp`). Full wide schemas and device inventories belong in `L`; validate limits before publication.
- `MetadataConfig` currently accepts scalar configured types, constrains id/source spelling (including dots), and validates queryable xattrs with GRIB assumptions. `metadata.<format>.field.*` is a proposal requiring parser, validation, allowlist, collision and required-field scoping changes. Built-in adapter summaries and configurable custom-key promotion need separate ownership. Do not add old/new syntax aliases without approval.
- For oversized optional sets, omit the pruning summary and mark it unavailable/incomplete, or fail ingestion according to a documented policy. “Incomplete/unknown” differs from “known empty”. A negative membership result from a capped set is unsafe.
- Missing optional pruning evidence is also different from a semantically NULL field under C1. An unavailable summary must keep the candidate and trigger authoritative evaluation (or an explicit unsupported error); it must not become SQL Unknown and silently remove the file from a membership query. Define this distinction in C9 before omitting summaries.
- Shared time/variable metadata requires an explicit mapping across formats. GRIB coordinates or identifiers do not automatically have Arrow/TsFile meaning. Define units and categorical mappings before E3; do not label data concatenation cross-format merely because names match.
- Specify whether `max_sources` bounds scanned candidates or post-filter matches. Current residual filtering happens after the limit; large mixed catalogs can exhaust the limit before pruning. Do not promise scalable indexed selection until implemented and measured.

### Existing data migration

Published Arrow/TsFile bytes currently ingested as byte pass-through have no identity or `L`. Arrow migration indexes the exact original stream; it does not convert stream to IPC file, flatten sequence rows or expand windows. Installing adapters must not reinterpret those manifests in place. Re-ingest into a **new** File Version through the normal lifecycle; preserve old versions and byte hashes. Queries on an old pass-through version remain `AdapterUnavailable` for semantic subsets. Registration is not a backfill.

GRIB receives the C4 signature update and core semantic regression coverage. The new typed frontend is a separate explicit route pending D1/D2; replacement/removal of legacy query syntax is not part of this documentation task. Do not add translation aliases or silently change bytes returned by existing GRIB virtual paths.

## 6. Typed frontend and FUSE lifecycle

This section specifies bounded file-query results. It does not replace the lazy training cursor in section 4A; C11–C15 now precede full FUSE training delivery on the critical path.

The original `file?select=a,b&where=...&slice=time:[t0,t1)` is illustrative syntax, not a grammar. Freeze a grammar and fixture corpus before wiring callbacks. It must distinguish the typed route from current GRIB range/materialized syntax, cross-file paths, semantic paths and ordinary filenames.

Choose the cross-file entry point explicitly. Current file-query dispatch requires a regular-file base inode and rejects directories; existing `CrossFileClient` does not call QueryModule. Subtree/file-selection queries therefore need a defined virtual directory/query route or a separate programmatic/CLI entry. If exposed through FUSE, source selection must enforce the caller's access to every selected file and ancestor; QueryModule currently receives no user credentials. Reusing a catalog scan must not bypass existing filesystem access rules.

Required parser contract:

- Exact/subtree source scope; explicit format filters with byte pass-through excluded from semantic subsets; latest/exact/all/between versions with half-open between bounds.
- Separate file/Sigma predicate from in-file predicate; projection is mandatory when `subset` is present. File-selection-only queries use the existing fixed source schema.
- Quoted/escaped field identifiers and literals; URL escaping, reserved delimiters, duplicate/unknown parameters, malformed UTF-8 and path length limits. Numeric literals are resolved against schema; no implicit data-type/unit casts.
- Explicit Arrow sequence row/identity and channel selectors resolve before schema/capabilities. Half-open inner step/time bounds are distinct from top-level row selection. Timestamp rules require a real binding; table selection, if approved, is explicit. Do not concatenate user query text into IoTDB SQL.
- Define conflicts between version-bearing paths and explicit version parameters. Either map GRIB BinaryView/type overrides/interpolation options explicitly or narrow the first frontend acceptance gate to the grammar actually implemented.

**D1 recommendation:** Arrow IPC **stream** output, read with `pyarrow.ipc.open_stream` (then `.read_all()`/`.to_pandas()`). Do not imply `pandas.read_feather` reads a stream. Carry logical field id, role, unit and timezone; choose a documented representation for per-batch domain and source provenance. A schema-only stream and EOS must be valid for zero rows. A zero-source semantic selection currently has no projected schema, so choose how the frontend reports/encodes this case instead of inventing one.

**D2 recommendation:** bounded immutable result materialization for v1, with a concrete pre-open size policy. Materializing only in `open` does **not** solve `st_size`: current FUSE2 `getattr` is path-only and may run first, and no `fgetattr` callback is wired.

Before implementation, choose and document one complete lifecycle (for example a bounded prepared-result cache shared by path lookup/open, plus handle-aware attributes), including cache identity, invalidation, snapshot freshness and limits. It must satisfy:

| Callback / case | Required behavior |
| --- | --- |
| Path dispatch / `access` | Recognize the typed route before legacy dispatch; current `op_access` rejects query paths. Typed views are read-only and unsupported flags return deliberate errno values. |
| `getattr` before `open` | Return a size consistent with the defined lookup snapshot. Account for materialization cost, repeated stat calls, latest-version changes and attribute-cache TTL. An unknown-size/direct_io design is a separate approved alternative. |
| `open` | Resolve and pin versions, fully validate/materialize within explicit encoded-output and working-memory limits, then install a distinct typed handle. On failure, expose no successful truncated stream or leaked partial cache entry. |
| `read` | Dispatch by handle **before** reparsing/resolving a live path; serve arbitrary offsets, repeated/backward reads, short reads and EOF from the pinned result. Current legacy `op_read` resolves paths first and must change. |
| Handle attributes / `release` | Handle size refers to the same result; close releases cursor/result references, cancels unfinished work and cleans temporary storage if used. Define flush/fsync behavior for read-only handles. |
| Concurrent publication / rename / unlink | New opens follow the agreed freshness policy; already materialized handles keep their immutable result. Test rename/unlink explicitly rather than assuming pathname-based re-resolution is safe. |
| Errors | Map QueryError/phase to errno with source/field diagnostics in logs. A cursor can emit batches before failing; frontend materialization must fail atomically, while programmatic users must receive the terminal error. |

FUSE, encoder and adapter tests must verify metadata/provenance as well as values. If a standard dataframe reader drops custom metadata, the IPC stream should still preserve it for consumers that inspect it.

The 2026-09-25 GRIB live record remains evidence for its legacy FUSE/CLI routes, not for typed FUSE. Reproduce the relevant semantics through the new route. For BinaryView, compare the decoded IPC Binary column with the encoded GRIB message; the whole IPC stream is not byte-identical to that message.

## 7. Resource, layout and build gates

A query may read bytes during capabilities, prepare, and execute. Instrument all three phases. Current execute budgets count actual `QuerySource` calls, including repeated reads, across sources; planning currently only sums declared layout sizes. Do not let an Arrow reader bypass accounting through prefetch, retained source pointers or local-path access.

D12 must name the scope of each limit: layout bytes, payload bytes, range-call count, source count, output rows, decoded working memory, encoded output bytes, and cache/temp-storage total. Coalesced gaps and metadata/dictionary overhead count. Enforce overflow-safe `offset + length` and size conversions, short-read errors, decompression bounds and cancellation between reads/decoding steps. Adapter batches should honor both row and byte targets where possible; a single oversized value needs defined behavior.

Layouts must have versioned parsing, sufficient native schema for capabilities, source-size/identity consistency checks, bounded record counts, statistics-availability flags and validated span arithmetic. Schema fingerprints are deterministic over the complete relevant schema (including order, ids, type parameters, timezone, nullability and agreed semantic metadata); a hash is not a substitute for schema validation. Test corrupt or unsupported layouts independently from malformed payloads. Re-ingestion creates new layouts; no compatibility readers are implied.

Dependencies must be pinned by release/commit, archive checksum, compiler/C++ standard, codec options and license/NOTICE. The local TsFile checkout is investigation evidence only, not an approved vendored dependency. Verify symbol/codec conflicts, static/shared linking and install/runtime paths.

`ci/dependencies.lock.json` alone is insufficient: `ci/install_dependencies.py` currently builds/verifies its named ecbuild/ecCodes/gtest components. Extend its installer and tests. Kylin's `packaging/kylin/build_release.sh` has a direct compiler source list rather than simply following CMake; update or deliberately consolidate it. S3 must run the actual release/package path on each claimed platform. If Arrow stream output is mandatory, Arrow is a frontend dependency even with Arrow ingestion disabled. A TsFile-disabled build must preserve ordinary reads of existing TsFile blobs and return `AdapterUnavailable` for typed queries.

## 8. Phased work plan

The previous format-only estimate does not cover C11–C15. Re-estimate after P0 and the training-contract spike. The new ordering proves native training behavior before completing all FUSE presentation details; file paths remain a delivery goal.

| Phase | Work | Exit gate |
| --- | --- | --- |
| P0: decisions and spikes | Record confirmed stream/channel-major scope; confirm only unresolved D1–D19 contracts. S1 Arrow I/O; S2 TsFile aligned decoding; S3 real dependency/package builds; S4 training-window/fragment contract. | Written pins, ranges, decoder dependency units, ownership/budget policy and window semantics. |
| P1: core correctness | C1/C3/C4/C7/C8 and Arrow C10; C9 and TsFile relation scope as approved; C2 strict-policy tests. Define C11–C15 interfaces and snapshot/view identity. | Existing suite plus schema, no-statistics, ingestion, budget and training-contract tests. |
| P2: Arrow stream foundation | Message directory, bounded List/List/Float32 offset indexing, sequence binding before capabilities, channel/internal-step query path, registration and shared decode ownership. | AQ stream/nested baseline plus exact child ranges, physical-unit keys and decode counters; IPC file input is not a blocker. |
| P3: Arrow training vertical slice | C11–C15 for Arrow, native cursor first. | Overlapping multivariate windows, boundary windows and two experiment views share measured decode work under bounded memory. |
| P4: TsFile training path | Metadata/page parser and shim; reuse C11–C15 with device/time/page bindings. | TQ and training acceptance on aligned sparse data and local cross-file windows. |
| P5: FUSE delivery, migration and live acceptance | Arrow C5/C6 and bounded sample groups proceed after P3 without waiting for TsFile P4; re-ingest old Arrow versions and run live AQ/TW plus applicable E1–E8. Add TsFile TQ/TW acceptance after P4 later. | Arrow completion is independently reviewable; only formats with supporting evidence are claimed complete. |

S4 uses deterministic samples: overlapping windows in one file, a boundary window across two files, and two views with overlapping variable sets but different L/S/H. First establish expected values/provenance; then record unique fragment keys, decode calls, leases and output copies with cache on/off. This design spike should resolve contracts before optimizing prefetch or adding framework adapters.

S1 uses stream List/List/Float32 fixtures with multiple series rows/batches, variable counts/lengths, offset/bitmap boundaries, compressed/uncompressed child buffers, contextual channels and dictionary epochs in supported side fields. Compare bounded child tiles versus irreducible compressed units, coalescing and cold/warm reads. Record every underlying range call; `skip_io_` in Arrow source alone is not proof of end-to-end I/O behavior.

S2 uses the pinned writer/reader with time plus multiple value pages, tags, nulls, single-page statistics omission and boundary windows. Report precisely which datatype/encoding/codec combinations work. S3 covers Ubuntu 22.04, macOS and the targeted Kylin architecture/toolchain; unavailable platforms remain unverified.

Ownership follows `AGENTS.md`: query/core owner for C1/C7–C15, `iotdb_specialist` for catalog/storage integration, `fuse_specialist` for C5, separate format owners for P3/P4, and verification/packaging ownership for S3/P5. Keep write scopes disjoint when parallel work starts.

## 9. Acceptance matrix

Use `docs/arrow-tsfile-query-acceptance.md` for execution records with commit, dependency versions, fixture hashes/generator commands, live environment, result schema/provenance, error phase, range traces and counters. AQ/TQ labels below are planned rows, not passed tests. Oracle: pinned `pyarrow.ipc.open_stream` plus independent nested-row/channel/step slicing for Arrow; pinned upstream TsFile reader plus independently generated expected rows for TsFile.

| ID | Capability | Required assertion |
| --- | --- | --- |
| 01 | Content dispatch | Wrong suffix and rename preserve identity; malformed stream schema/framing/EOS (TsFile header/footer), unsupported version/model and ordinary pass-through follow D14. |
| 02 | Exact bytes and publication | Source/read hashes equal; injected stream-index/footer/read/layout/metadata/publication failure leaves no partially published version. |
| 03 | Projection | Values/nulls/order equal oracle; record planned versus observed bytes, including hidden predicate columns, time pages and metadata. |
| 04 | Domain slice | Empty/full/boundary intervals, inclusive/exclusive ends and out-of-domain slices are exact; excluded units are absent from logical plan, and physical coalescing over-read is separately reported. |
| 05 | Candidate pruning | Statistics present/absent, NaN, all-null and adversarial min/max cases; no false negatives under AND/OR/NOT/IN. Exact semantics verified after decoding. |
| 06 | Residuals | Predicate/slice fields omitted from projection are still decoded; three-valued null semantics and exact typed literals; unsupported casts return TypeMismatch. |
| 07 | Layout validation | Missing/bad protocol line, unsupported version, invalid counts/offsets/overflows and schema mismatch produce deliberate errors before unsafe reads/allocations. |
| 08 | Payload and codecs | Truncated metadata/body/page, invalid offsets and malformed compressed lengths; unsupported codec/type differs from corruption; short reads propagate. |
| 09 | Snapshot / stale plan | New versions do not retarget prepared sources; a plan used with a different source is rejected; renamed/deleted paths and caches obey documented handle behavior. |
| 10 | Limits and cancellation | Planning and execution I/O limits, total rows, decode memory and encoded-output cap; cumulative multisource accounting; no successful truncated virtual file; cleanup verified. |
| 11 | File selection and versions | Mixed formats plus byte pass-through; known-absent Sigma versus unknown fields, IS NULL, type conflicts and latest/exact/all/between boundary cases. Membership tested only after C9. |
| 12 | FUSE frontend | Access, stat-before-open, handle attrs, random/short/backward reads, EOF, release and concurrency; IPC values/schema/domain/provenance match in-process results. |
| 13 | Cross-source schema | Extra unprojected fields succeed; projected type/nullability/timezone/role/unit mismatch fails under strict policy; file-selection-only mixed catalogs succeed. |
| 14 | Type boundaries | Timestamp overflow/precision/timezone; UInt64 bounds, empty strings/binary, null bitmap slice offsets; DATE calendar normalization if enabled; unsupported native-field policy. |
| 15 | Metadata limits | Wide schema/device sets exceed caps without false-negative pruning; unknown/incomplete differs from empty; optional custom metadata collision/length rules. |
| 16 | Migration | Old pass-through exact version keeps old semantics; re-ingested new version has identity/L/Sigma and identical B; remount/new publication preserves behavior. |
| 17 | Empty results | Schema-only stream, clean boundary EOF/EOS, empty batches/series/channels, zero matching steps and zero selected sources follow approved schema policy; empty bytes are not a valid stream. |
| 18 | Build/feature matrix | Actual package install and smoke queries on each claimed platform; disabled adapter has explicit failure behavior and ordinary reads work. |

Arrow-specific fixtures require stream List/List/Float32, multiple record batches and independent sequence rows; outer/inner offsets with nonzero origins, empty/ragged/null series/channels/values, unequal channel lengths, overflow/corrupt offsets, non-byte-aligned validity, missing labels/time, bound step slicing, channel reorder, EOS/clean EOF/truncation, and compressed buffers spanning multiple channels/rows. Include dictionary replacement/delta epochs where supported; verify unrelated dictionary fields can be skipped. Native schema variants outside the matrix fail explicitly. Retain side-field duplicate/reserved names and unsupported-field tests. TsFile-specific fixtures add multiple devices with equal timestamps, multiple aligned value columns, null/absent values, missing page stats, page/chunk boundaries, selected-table behavior, tags and supported codec combinations. Small deterministic fixtures may be checked in; generators and writer versions are mandatory.

Training acceptance (run on both Arrow and TsFile):

| ID | Scenario | Required assertion |
| --- | --- | --- |
| TW-01 | Multivariate windows | For Arrow, select the exact series row and inner channel-major steps; verify both offset levels and three validity levels. L/S/H/G, ordered variables, declared step/time axis, masks and sample identities equal the oracle. |
| TW-02 | Overlap reuse | With sufficient cache and no eviction, N overlapping windows decode each required canonical physical fragment once; outputs still contain all expected repeated rows. |
| TW-03 | Cross-file locality | Only declared continuations of one logical series/channel mapping can produce a boundary window. Independent rows/shards must not stitch. Read intersecting physical units plus dependencies; account for compressed-buffer amplification and explicit variable-partition joins. |
| TW-04 | Shared experiments | Two views with overlapping variables and different window lengths/strides/targets reuse common fragments; transformations do not modify base values or each other's outputs. |
| TW-05 | Concurrent demand | Simultaneous misses produce one decode per key; cancel/failure of one consumer does not invalidate another; failures are retryable without hanging waiters. |
| TW-06 | Memory/backpressure | Small budgets, pinned leases, in-flight decompression, eviction, oversized units and slow consumers stay within the defined accounting/limits. Re-decode after eviction is counted. |
| TW-07 | Data boundaries | Missing timestamp/null, duplicate overlap, entities with the same timestamp, out-of-order input and train/validation boundary follow explicit policies; no accidental cross-entity window. |
| TW-08 | Snapshot reproducibility | New file versions or files published mid-epoch do not alter the prepared view; identical specs reproduce sample ids; worker shards/shuffle preserve the declared sample set and order. |
| TW-09 | Lazy delivery | Requesting a small sample group does not enumerate/materialize every window or open all payloads; partial last group, empty result, cancellation and provenance work. |
| TW-10 | Cache layers / process scope | Distinguish encoded-byte, native-decoded and derived-view hits; demonstrate multiple clients on the same service and document isolation across services; measure output copies separately. |

## 10. Evaluation and paper claims

| Experiment | Workload and baseline | Metrics / qualification |
| --- | --- | --- |
| E1: Arrow channel and internal-window selection | IPC stream List/List/Float32; k of C channels × W of T steps in selected series rows, varied batches/buffer sizes and compression. Use public R1 mmap/message-index/offset slicing with bounded decoded caching on local storage/plain FUSE; retain R0 in-memory and R2 contiguous memmap alternatives. The unpublished loader is optional internal evidence. Feather file column projection is not an equivalent same-format baseline. | Latency, child intervals, offset/bitmap/index bytes, read/decode/copy amplification and peak memory. Approximately `(k/C)*(W/T)` applies only to controlled equal-length uncompressed values with all overhead stated. A shared compressed buffer can force full cold reads despite narrow logical selection. |
| E2: TsFile time/measurement selection | Time windows 0.1–100% × measurements; local upstream reader and IoTDB `load tsfile` + equivalent SQL. | Page counts, bytes and latency; report import/setup cost separately, align null/row semantics and timestamp precision. Retain byte-identical versions as the reason for blob storage rather than treating native IoTDB loading as equivalent file semantics. |
| E3: cross-format file pruning | Mixed collection with defined common scalar time bounds; variable membership only if C9/shared mapping implemented. Compare file-by-file scan and per-format scripts. | Scanned manifests, matched files, files/payloads opened, catalog/planning/execution times. Do not require heterogeneous data union to demonstrate file selection. |
| E4: ingestion | Each format at multiple sizes, metadata-only indexing versus optional statistics, with ext4 copy and GRIB ingestion comparisons. | Throughput, reread bytes, peak memory, layout/payload ratio, Sigma bytes and publication latency. |

Training experiments are required to substantiate the new core goal:

| Experiment | Controlled comparison | Metrics / qualification |
| --- | --- | --- |
| E5: overlapping windows | Sweep L, S and variable count; compare independent window queries, encoded-byte cache only, and shared decoded fragments with identical sample outputs/cache budgets. | Samples/s, first-window and p50/p95 batch latency, unique versus actual decodes, decoded bytes/time, payload bytes, peak memory and output-copy bytes. For regular equal-length windows, overlap depends on S/L; report physical-unit effects. |
| E6: cross-file local assembly | Fix the logical data/sample set while varying file and batch/page boundaries, including deliberately misaligned partitions; compare whole-file load/concat and native range readers with the same bounded cache opportunity. | Boundary-window latency, opened payload sources, read/decode amplification, coverage lookup cost and exact provenance. Include warm sequential and shuffled requests. |
| E7: shared experimental views | Run 1/2/4 experiments with same snapshot, partially overlapping variables and different L/S/H/normalization; compare private decoded caches against one shared decoded cache under equal total memory. | Aggregate/per-experiment throughput and latency, duplicate decodes avoided, shared/pinned bytes, fairness and output equivalence. Different transforms must produce the expected distinct results. |
| E8: memory and scheduling | Sweep cache/lookahead budgets and sequential/shuffled/multiworker order; compare demand-only reuse and bounded prefetch/union scheduling. | Resident + pinned + in-flight peak bytes, evictions, re-decodes, wasted prefetch, cancellation cleanup and sensitivity to working-set size. Preserve the same seeded sample order. |

Also report preprocessing + repeated-epoch costs for a pre-materialized-window dataset and a well-implemented native reader with decoded caching. Separate one-time indexing/snapshot/normalization costs from epoch execution, and report total materialized storage. Decode reuse is not unique merely because it appears inside a filesystem; the evidence must isolate the benefit of shared versioned fragments, local multi-file assembly and experiment views over those baselines.

### 10.1 Public model/data matrix and Trainer protocol

The required first implementation is HF `Trainer` with **PatchTSTForPrediction** and **PatchTSMixerForPrediction** (`mode="mix_channel"`), trained from scratch on **ETTm1** and **UCI ElectricityLoadDiagrams20112014 raw 370-channel data**. Both models have public HF Trainer recipes ([PatchTST](https://huggingface.co/blog/patchtst), [PatchTSMixer](https://huggingface.co/blog/patchtsmixer)); PatchTST's default channel-independent operation must not be described as explicit channel interaction. Use **PEMS-SF** for wide-channel independent-row cases, **LargeST SD/CA** for a separately verified larger-scale extension, and **IBM TTM-r2** for optional public-checkpoint fine-tuning. These are researched candidates, not completed training runs. Sources, shape audits, licenses and precise interfaces are recorded in the [baseline plan](public-training-baseline-plan.md).

Public CSV/TXT/H5 sources are converted by standalone scripts into `list<list<float32>>` IPC streams with explicit entity/segment/continuation metadata; they are not claimed to ship natively in that format. Preserve one canonical value/mask representation across Arrow, NumPy and later TsFile. Use identical window IDs, target splits, channel ordering, train-fitted transforms and seeded sample order. PEMS-SF days are independent rows, not chronological continuation; raw UCI Electricity is not the common 321-channel hourly ECL derivative. Small datasets alone cannot substantiate out-of-core claims; even LargeST requires reporting actual working set versus available RAM.

Trainer consumes a thin public PyTorch Dataset/collator: form `[B,L,C]` inputs and `[B,H,C]` targets from channel-major storage, set `label_names=["future_values"]`, map model-specific mask names, and verify target-loss validity. Keep model/optimizer/precision/batch settings constant across backends; separately count tensor packing, normalization, IPC, H2D and training compute. Do not pre-materialize all windows as a prerequisite. Report loader-only and end-to-end training measurements; a faster loader does not imply an equal training speedup.

The required public reader comparisons are R0 official in-memory dataframe/window loader, R1 indexed native Arrow with bounded decompression caching, R2 base-sequence NumPy/memmap, R3 materialized-window setup/storage tradeoff, R4 ordinary IoTDB-FS FUSE access, and Q0/Q1/Q2 typed-window cache ablations. E7 must account for OS-shared mmap pages and equal total private/shared cache budgets, adding a public local shared-cache comparison or explicitly limiting the claim. TsFile later reuses this Trainer/window protocol with an upstream native reader adapter; native HF support for TsFile is not assumed. A00/PB00 freezes dependencies after executable smoke checks; A13/PB04 publishes configs, manifests, commands and raw results without private-framework imports.

Report cold/warm cache runs and request/batch/page sizes. Distinguish (a) adapter-requested spans, (b) actual client QuerySource/READ_OBJECT returned bytes, and (c) server/network/disk bytes when observable. `IoTDBStructuredBlobBackend::read_payload_range` issues READ_OBJECT; a small return does not itself prove equal disk I/O reduction. Include planning/stream-index/footer reads, materialization and IPC encoding in end-to-end latency; expose their components as well. Preserve raw data and scripts before updating paper figures.

## 11. Decisions needed before implementation

The training optimization goals and stream/channel-major native input are already requested by Alan and do not need reconfirmation. The remaining interface representation, alignment policies and cache-service choices below remain proposals; this document review does not require pausing for them. Confirm the relevant choices before dependent code, as required by [AGENTS.md](../../AGENTS.md).

| ID | Decision | Recommendation / consequence |
| --- | --- | --- |
| D1 | Typed virtual-file encoding | Arrow IPC stream; explicit metadata/provenance convention. Makes Arrow a frontend dependency. No automatic replacement of legacy GRIB bytes. |
| D2 | FUSE result lifetime/size | Bounded materialized immutable result with a specified pre-open getattr cache/snapshot policy and handle attrs. Open-only materialization is incomplete. Alternative unknown-size direct_io needs separate client acceptance. |
| D3 | Arrow input coverage — user confirmed | IPC stream plus List/Float32 and List/List/Float32; exact B preserved and external message/offset index in L. IPC file input is optional later. Do not convert between List depths implicitly. |
| D4 | Sequence/channel ids and path grammar | Explicit source-local row/series selector, actual labels or positional channel ids, internal step/time bounds and reserved fields. Resolve binding before capabilities; matching names/positions alone do not align files. Exact API/grammar remains to confirm. |
| D5 | Nested binding / other types / dictionary | Required native List/List/Float32 maps to scalar sequence views and WindowBatch; general recursive logical arrays remain optional. Other unsupported native types stay privately inventoried with precise errors. DATE and dictionary-to-value support are separate; index dictionary epochs for supported dependencies. |
| D6 | TsFile format coverage | Unencrypted v4 table model initially; supported writer/codec matrix proven by S2. Version alone does not identify all supported semantics. |
| D7 | TsFile time precision | Explicit ingestion policy persisted in L/Sigma, checked normalization to TimestampNs. Specify ingress API/config source; do not silently default unknown precision to ms. Raw Int64+unit remains an alternative. |
| D8 | TsFile dependency boundary | Pinned upstream library/private shim after S2; source pin, packaging and licensing included. Hand-written codecs need a new scope decision. |
| D9 | Cross-source schemas | Strict projected equality in v1; defer union and avoid an unused schema-policy option. E3 file selection does not require union. |
| D10 | Build shape | Arrow mandatory if D1 chosen; TsFile optional until packaging proven, with explicit disabled-build behavior. |
| D11 | Multi-table TsFile | Smallest v1: require exactly one table per queryable file. If multi-table files are required now, add explicit relation selection before capabilities; revise public seams and estimate. |
| D12 | Resource limits and empty schema | Agree planning-source accounting, decoder memory and encoded/cache byte limits, plus zero-source typed-result behavior. These limits are absent from current QueryLimits. |
| D13 | Collection membership | Implement an explicit typed catalog membership contract if variable/device-set pruning is part of v1; otherwise scope E3 to scalar predicates and mark membership unsupported. |
| D14 | Detection/error policy | Distinguish unrecognized data, recognized malformed payload and recognized unsupported format. Current bool matches/first-match registry cannot carry structured detection errors; choose session validation or an explicit detection-result seam change. |

Training-specific decisions (the goals are fixed; these resolve semantics and interfaces):

| ID | Decision | Recommendation / consequence |
| --- | --- | --- |
| D15 | Window/alignment/split semantics | Confirm default bound series/channel step-axis, sample-count L/S/H/G and ragged/null reject-versus-mask policy. Absolute time requires supplied timestamps or declared start/cadence; split and alignment policy explicit. No silent fill or fabricated times. |
| D16 | Dataset coverage and duplicates | Versioned snapshot + source-local series rows and explicitly declared continuation coverage/channel mapping. Shard order and distinct rows never imply continuity. Account for dataset selection indices; reject mismatched shards and ambiguous duplicates. Variable partitions require separate join mapping. |
| D17 | Shared execution topology and budgets | One long-lived service/mount owns bounded per-format canonical decoded caches; define single-producer lifetime, leases, global/per-consumer accounting and fairness. Multi-process workers must use this owner to share. |
| D18 | Window and shared-fragment API | Dedicated training planner/assembler over reusable format decoders; immutable fragment handles and an explicit WindowBatch model. Keep bounded final copies initially; zero-copy changes need their own ownership contract. |
| D19 | View transformations and delivery | Start with projections/window parameters/input-target construction and explicitly supplied deterministic transforms or train-fitted normalization artifacts. Native cursor first, bounded window-group FUSE encoding next; seeded augmentation/derived caching only under declared semantics. |

## 12. File-level work list and references

**New modules:** `ArrowLayout`, `ArrowFormatAdapter`, `ArrowFormatQueryAdapter`; `TsFileLayout`, `TsFileMetadataParser`, `TsFileDecodeShim`, `TsFileFormatAdapter`, `TsFileFormatQueryAdapter`; `TypedQueryFrontend` and an IPC result encoder/result-handle component as agreed in P0. Add training snapshot/coverage, shared decoded-fragment cache, window planner/assembler, experiment-view and window-cursor modules under the approved C11–C15 contracts. Keep physical format types private.

**Core edits:** `FormatAdapter.h`, `Ingestion.cpp`, `GribFormatAdapter.cpp` and all ingestion test adapters (C4); `QueryModel.h/.cpp` only for approved type/expression changes; `QueryPlanning.h/.cpp` for sequence-context capability resolution and approved relation/budget contracts; `QueryModule.cpp` for scalar Sigma schema, membership if approved, budgeting and empty-result policy; backend/catalog files only for the chosen catalog implementation.

**Integration edits:** `main.cpp` including access/getattr/open/read/release and handle state; metadata config/parser/resources; CMake production/unit/integration targets; `ci/dependencies.lock.json`, `ci/install_dependencies.py`, `ci/tests/test_dependencies.py`, platform package scripts and installation documentation. Add format/layout/ingestion/frontend tests, deterministic fixture generators and the AQ/TQ/TW execution matrix, including decode instrumentation and E5–E8 benchmark drivers.

**Documentation:** update `CONTEXT.md` when contracts are approved. Update user manual with real grammar and loader examples; audit and paper chapters `04-structured-blob.tex` / `06-access-query.tex` only after supporting runs. Do not edit repository policy files merely to make this plan easier to implement.

Review evidence:

- Local IoTDB-FS-C commit above: [query contracts](../../include/iotdbfs/query/core/QueryPlanning.h), [query implementation](../../src/query/core/QueryModule.cpp), [ingestion source](../../include/iotdbfs/FormatAdapter.h), [FUSE implementation](../../src/main.cpp), [metadata validation](../../src/MetadataConfig.cpp).
- [Arrow IPC stream specification](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) and [List layout](https://arrow.apache.org/docs/format/Columnar.html#list-layout): reference formats for stream messages, nested offsets/validity and compression. This plan's external L index and cache/binding contracts are project designs. [Arrow C++ reader implementation](https://github.com/apache/arrow/blob/main/cpp/src/arrow/ipc/reader.cc): selective read paths are evidence to investigate, not a version-pinned S1 result.
- [TsFile C++ interface](https://tsfile.apache.org/UserGuide/latest/QuickStart/InterfaceDefinition/InterfaceDefinition-CPP.html): path-based reader and table-query API. Local `/Users/alan671/Documents/tsfile` at HEAD `fe756406c017435766f8c706eafcfa2de7d7e798` was also inspected, specifically `cpp/src/reader/tsfile_reader.h`, `cpp/src/common/datatype/date_converter.h` and aligned/chunk readers. This local worktree may contain development changes; it is not a release support guarantee. P0 must pin and recheck the actual dependency.
