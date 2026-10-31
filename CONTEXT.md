# IoTDB-FS-C Domain Context

IoTDB-FS-C presents scientific files through POSIX paths while storing each
file version as a structured blob in IoTDB. The storage vocabulary is
format-neutral; GRIB is the first adapter, not the filesystem data model.

## Core concepts

- **Logical File**: the POSIX-visible object identified by a path.
- **File Version**: one immutable stored state of a logical file. A version is
  visible only after publication completes. Once published, its `(path,
  version)` identity is permanently retired after deletion and is never reused
  for different bytes.
- **Structured Blob**: a file version decomposed into the byte payload `B`, an
  optional format-specific layout artifact `L`, and an optional typed metadata
  document `Sigma`.
- **Version Manifest**: the small namespace and lifecycle record for a file
  version. It contains POSIX metadata, format identity, typed metadata, and the
  publication state; it does not contain payload or layout bytes.
- **Payload (`B`)**: the authoritative file byte sequence. For an unrecognized
  format, byte pass-through stores and reads this sequence without an adapter.
- **Layout Artifact (`L`)**: an optional, potentially large, format-specific
  access structure. Its representation is private to the selected format
  adapter and is stored separately from the manifest.
- **Typed Metadata (`Sigma`)**: a small, schema-described document containing
  format-derived file metadata. Generic storage treats it as a document and
  does not introduce format-specific manifest columns. Repeated categorical
  values are represented as typed, de-duplicated sets; ordered numeric domains
  may additionally expose scalar bounds. Exact set membership used for
  cross-file pruning is materialized in the format-neutral
  `blob_metadata_set` storage index, while collection values remain outside the
  scalar logical-expression contract.
- **Dataset Substructure Index**: a publication-sealed immutable directory of
  dataset object identities, typed attributes and layout references. HDF5 can
  uses this directory for memory-resident predicate evaluation in both official
  query modes, analogous to the GRIB message metadata index. The seal binds exact
  source identity and canonical directory content; it does not rewrite payload
  or turn an HDF5 chunk into an independent file. See
  [`docs/contracts/hdf5-dataset-substructure-index.md`](docs/contracts/hdf5-dataset-substructure-index.md).
- **Compiled Dataset Predicate**: an immutable request-local attribute program.
  Sealed-index discovery compiles fields and expression slots once, binds typed
  attributes without per-record schemas/maps, and checks all candidates before
  returning matches or enforcing the output limit. Missing, unreadable and
  mistyped attributes retain their existing semantics. Unsupported predicates
  use the common bounded evaluator.
- **HDF5 Query Mode**: `native-hdf5` uses database cross-file predicates and
  version/location selection, then the native HDF5 in-file index over an
  explicitly bound immutable filesystem file (bounded local pread VFD,
  never database payload or self-FUSE); `iotdb-indexed` uses the
  IoTDB-FS chunk/layout index and decoder. Both share sealed cross-file discovery,
  typed scientific output, authorization, owner admission and request budgets.
  The default is `iotdb-indexed`; mode is fixed per adapter and prepared plan.
  See [`docs/contracts/hdf5-query-modes.md`](docs/contracts/hdf5-query-modes.md).
- **HDF5 Global Attribute Index**: the HDF5 Adapter module's immutable
  cross-file index over the six supported typed dataset attributes. Both HDF5
  query engines share it; it is derived from sealed catalogs and is distinct
  from libhdf5's native in-file index. The owner backend supplies authorized
  frozen domains and retains responsibility for admission and budget charging.
- **Frozen Candidate Domain**: owner-issued, full-head-bound selection and
  permission evidence for all candidate versions, including semantic misses.
  GRIB and HDF5 share one 64 MiB hot pool (16 domains, 64 principal proofs).
  New requests at a new head select again; cached proofs still require actual
  owner admission on execute, publication and new open.
- **Immutable Equality Index**: a format-neutral sorted composite-key array
  with stable record locators and binary range lookup. Format adapters own
  field meaning, borrowed-string lifetimes, completeness certification and
  record ordering. GRIB certifies six-field metadata discovery; HDF5 certifies
  six dataset attributes for both official query engines.
- **Domain Derived State**: optional immutable adapter index and full-domain
  metadata fee evidence, charged to the candidate domain's existing reservation.
  `QueryDomainCache` merges builds and retains accounting through retirement.
  It holds no request ledger, QuerySource, ReadContext or prepared result;
  fresh matching-source plans are constructed for each request. Unsupported
  requests or uncertifiable domains retain ordinary bounded preparation.
- **Format Unit Catalog**: explicit `format_catalog` control operation returning
  exact-version adapter unit identities and encoded byte locations directly as
  bounded JSON metadata. GRIB supports offset-only discovery and request-local
  opaque posting hints; format interpretation stays in its Adapter. Full-domain
  authorization, execution leases, budget charging and final owner admission
  remain mandatory. Value delivery continues through the regular typed path.
- **Format Identity**: the persisted adapter identifier used to interpret
  `Sigma` and `L`. It is empty for baseline byte pass-through.
- **Ingestion**: the synchronous conversion of a stable byte source into one
  Structured Blob File Version. It copies `B` exactly once, optionally derives
  `L` and `Sigma` through a Format Adapter, and completes publication.
- **Ingestion Outcome**: the format-neutral description prepared for
  publication: optional Format Identity, typed `Sigma`, and payload/layout
  sizes. Layout bytes remain outside the outcome and the Version Manifest.
- **Publication**: the final lifecycle transition that makes a fully durable
  version visible to readers. Publication occurs only after payload, layout,
  and typed metadata are durable.
- **Logical Schema**: the format-neutral description of query-visible fields,
  including stable field identity, logical type, nullability, semantic role,
  unit, and timestamp timezone where applicable. It does not describe a
  format's physical layout.
- **Typed Expression**: a query predicate over Logical Schema fields and typed
  literals. It has explicit operators and null semantics and performs no
  implicit type or unit conversion.
- **Domain Slice**: a typed interval over one named scientific-data dimension.
  GRIB latitude/longitude, Arrow row position, and TsFile time are different
  dimensions expressed through the same workload semantic; a Domain Slice does
  not impose a universal two-dimensional data model.
- **Query Batch**: a bounded, schema-described group of result values with an
  optional domain shape and contributing File Version provenance. Its logical
  columns do not expose decoder handles, Arrow buffers, TsFile chunks/pages,
  or private Layout Artifact records; an adapter-defined Binary view remains a
  typed Binary value.
- **Query Status**: the structured query failure model, identifying an error
  class, query phase, optional field/source/operation context, and whether a
  failure is retriable.
- **Published Version Catalog**: the read-only query catalog that selects only
  published File Versions by path scope, version policy, Format Identity, and
  typed `Sigma` predicate. A catalog reports predicates it could not push down
  instead of treating candidate pruning as exact filtering. A live catalog
  observes the current publication state on each scan; an owner may reuse a
  frozen snapshot at the same complete head. New `prepare(latest)` observes
  versions published before its selection, while old prepared queries remain
  fixed to their selected versions.
- **Query Source**: a read-only view of one exact, published File Version. It
  exposes its immutable Version Manifest and bounded reads over `B` and `L`,
  without exposing an IoTDB connector or storage schema to a format adapter.
- **Query Request**: the format-neutral query intent. It separates inter-file
  source/version selection from an optional format-specific in-file semantic
  subset consisting of projection, Typed Expression, Domain Slices, and typed
  output semantics. Output semantics distinguish logical values from a typed,
  adapter-defined Binary view and may request explicit output types or
  named-dimension grid interpolation.
- **Prepared Query**: the immutable result of query planning. Every selected
  source is fixed to an exact File Version and, for semantic subset queries,
  paired with the matching Format Query Adapter and its opaque execution plan.
  Later catalog changes do not retarget an existing Prepared Query.

## Module boundaries

- **Structured Blob Storage Module** owns version lifecycle, durability order,
  publication, and access to manifests, payloads, and layout artifacts.
- **Ingestion Module** selects a Format Adapter by content, copies the source
  into `B`, collects `L` and `Sigma`, coordinates namespace/POSIX preparation,
  and aborts an unpublished version on failure. It orchestrates the lifecycle;
  Structured Blob Storage still enforces legal transitions and visibility.
- **Format Adapter** recognizes a format and derives its typed metadata and
  layout artifact. It does not write `B`, modify an inode, own namespace
  persistence, or publish a version.
- **Query Module** owns the format-neutral Logical Schema, Typed Expression,
  Domain Slice, Query Batch, and Query Status contracts. `prepare` resolves
  catalog candidates to exact published File Versions, applies residual
  inter-file predicates, selects the adapter recorded by Format Identity, and
  reconciles the adapters' result schemas. `execute` delegates private
  `Sigma`/`L`/`B` interpretation, then owns residual in-file semantics, final
  projection, resource budgets, cross-source streaming, and cancellation.
- **Format Query Adapter** describes a format's query capabilities, plans an
  in-file semantic subset against one Query Source, and streams Query Batches
  from an opaque execution plan. Its Interface never exposes message, buffer,
  chunk, page, or private Layout Artifact representations.
- **Format Query Model** lives alongside its adapter:
  `formats/arrow/ArrowQueryModel` and `formats/grib/GribQueryModel` own their
  schemas and capabilities. Arrow also owns its physical List resolver there;
  the former core `ArrayQueryModel` was not a general N-dimensional model.
  `QueryPlanning` retains an explicit dependency on the lightweight
  `ArrowQuerySubject` coordinates and their validation for the existing
  `physical_array` request. No format decoder or layout implementation belongs
  in the core, and this request seam is not full dependency inversion.
- **GRIB Format Query Adapter** is the GRIB Implementation of that Seam. It
  performs exact message selection against the versioned private layout,
  compiles a source-bound opaque plan, delegates grid decoding and transforms
  to the GRIB Query Kernel, and emits row-bounded typed batches with File
  Version provenance. It never parses the legacy FUSE query-string grammar.
- **GRIB Query Kernel** is the adapter-private execution Module for the first
  GRIB query contract. It translates typed output semantics to the legacy
  ecCodes operations and returns typed grid coordinates, values, shape, and
  Query Status. It does not parse user query strings or expose decoded byte
  buffers as the query contract.
- **GRIB Layout Protocol** starts each Layout Artifact with the explicit
  `IOTDBFS-GRIB-LAYOUT/1` protocol line. Typed GRIB readers reject missing,
  malformed, or unsupported versions before interpreting private records.
- **Arrow Training Module** owns the current Dataset Snapshot, coverage index,
  and on-demand window assembly. Its training result model is shared, but its
  coverage and fragment implementation is Arrow-specific; a future TsFile
  training path must not expose TsFile pages through the Query Module.
- **Byte Pass-through** is the baseline storage path for unrecognized content;
  it is behavior of the Ingestion Module, not a file-format adapter or session.

The module source/header layout and dependency direction are recorded in
[`docs/knowledge/query-module-layout.md`](docs/knowledge/query-module-layout.md).

The required commit order is `B -> L -> Sigma/format identity -> publish`.
Readers must not observe a version before the last step succeeds.
