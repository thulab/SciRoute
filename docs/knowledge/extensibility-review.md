# IoTDB-FS-C extensibility review

The extension boundary is now split into eight independently testable layers:

- `StructuredBlobStorage` is the deep lifecycle Module. It owns the durable
  order `B -> seal(B) -> L -> Sigma/format identity -> publish`, published-only
  reads, range validation, abort, and removal. Its Backend seam exposes coarse
  artifact operations and an explicit manifest CAS, not SQL, GRIB records, or
  format-specific layout records. Artifact writes may safely precede their
  manifest frontier because IoTDB has no cross-table transaction.
- `IoTDBStructuredBlobBackend` owns the clean physical layout: a small `inode`
  manifest, `blob_payload` OBJECT data, `blob_layout` OBJECT data, and durable
  `blob_version_identity` tombstones. Only the inode `published` marker controls
  visibility. A published `(path, version)` is retired before its visible row
  is removed and can never identify different bytes. Typed file metadata
  remains a bounded manifest document and never shares the large layout object.

- `MetadataConfig` and `MetadataConfigStore` validate and publish immutable
  typed metadata snapshots. A failed reload leaves the previous generation
  active.
- `MetadataProjector` is the single projection path for typed JSON, xattrs,
  and Web fields. Invalid documents and values are
  omitted rather than fabricated.
- `MetadataQueryEvaluator` parses bounded predicates against queryable fields
  and aliases. It evaluates decoded metadata without changing the inode table
  schema; an indexed backend can be attached later at this seam.
- `IngestionModule` is the deep ingestion Module. Its single operation selects
  an adapter by content, copies the authoritative payload, collects typed
  metadata and opaque layout bytes, prepares the POSIX manifest, publishes
  last, and aborts on failure. FUSE callers no longer know format-specific
  record or flush protocols.
- `FormatRegistry` resolves adapters by content for ingestion and by persisted
  `format_id` for reads, queries, and xattrs; renaming a file therefore cannot
  change its interpretation. The `FormatAdapter` ingestion Interface only
  consumes source chunks and returns `Sigma` plus `L`; it cannot persist
  payload bytes or modify an inode.
- `GribFormatAdapter` contains the existing GRIB scanning, metadata, virtual
  query, and xattr behavior. Content that no adapter recognizes follows the
  Ingestion Module's byte pass-through branch without a format adapter or
  session. The GRIB two-dimensional data model remains unchanged; this phase
  only separates format implementations behind the ingestion seam.

The metadata decoder is protected by a thread-safe LRU cache with entry and
memory budgets. Parsing occurs outside the cache lock, malformed documents are
not retained, and callers can invalidate all revisions for an inode row.

The live IoTDB Published Version Catalog loads a new immutable manifest
snapshot for every scan. Consequently, each new `QueryModule::prepare(latest)`
can observe newly published versions, while an existing Prepared Query remains
fixed to the exact versions selected by its earlier snapshot. Format caches
remain version-keyed and do not require global invalidation when a newer
timestamp is published.

The runtime uses a clean schema cutover. It contains no `inode.data`, no
`inode.index`, no ALTER fallback, and no format-specific inode columns. Typed
collection summaries are stored in the manifest metadata document and their
members are projected into the format-neutral `blob_metadata_set` table for
exact server-side candidate pruning. The scalar logical-expression contract
does not encode collections as binary or delimited strings. Existing data must
be re-ingested when moving to this schema.

Manifest CAS is serialized inside the IoTDB backend. This is correct for the
supported single-filesystem-writer deployment; multiple independent writers
against the same `(path, version)` would require a server-side conditional
update primitive.

IoTDB does not provide a cross-table or cross-key transaction. Version publish
is crash-safe because only the final inode marker grants visibility. Rename
uses target-first publication followed by source removal, favoring no data loss;
a crash between those operations can temporarily leave both names visible.
Crash-atomic rename would require a separate namespace transaction protocol and
is outside this refactoring phase.

This phase intentionally keeps typed file metadata in bounded typed JSON while
moving the potentially large layout to its own OBJECT table. It leaves the
public query evaluator seam for a future indexed implementation without
mistaking GRIB's current two-dimensional model for the workload semantics.

## Verification

Focused tests cover lifecycle ordering, published-only visibility, artifact
write/CAS failure recovery, typed metadata, range validation, abort/remove,
registry identity selection, unmatched byte pass-through, ingestion rollback,
GRIB content detection and derived artifacts, query-view mapping, and xattr
ownership.

Native Windows configuration still fails explicitly and directs users to WSL2.
macOS uses the existing AppleClang/macFUSE/ecCodes CMake branches and installs
`mac/schema.xml` as a Finder metadata resource.
