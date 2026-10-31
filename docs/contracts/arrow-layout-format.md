# Arrow stream layout v1

A03 adds `ArrowFormatAdapter` and `ArrowLayoutDocument`. The persisted format identity is `arrow`; ingestion identifies a complete native Schema message from content. `main.cpp` registers Arrow before GRIB, independently of the GRIB feature switch. Ordinary reads continue to use the unchanged Structured Blob payload. Rename retains its persisted identity and layout.

## Supported ingestion boundary

- Modern IPC stream framing, little-endian schema, metadata V5. A valid legacy Schema is recognized, then ingestion reports an unsupported framing error. IPC file/footer ingestion is outside this adapter.
- One initial Schema, followed by DictionaryBatch and RecordBatch messages. Complete-message EOF and explicit EOS are accepted, including schema-only streams. Partial prefixes, metadata, bodies, EOS, trailing garbage and concatenated streams fail after recognition.
- Uncompressed, LZ4_FRAME and ZSTD buffers. Each compression prefix and decoded index size is bounded. A03 decodes offsets/validity only; compressed float values retain their native buffer span and are checked when decoded in A04.
- Top-level `List<Float32>` / `List<List<Float32>>`, including LargeList offset widths, receive physical indexes with their actual depth. Other native fields remain in the schema and recursive message tree, with an explicit unsupported physical-query reason. Field names must be nonempty and unique. There is no channel/time inference.

## Artifact and native binding

`IOTDBFS-ARROW-LAYOUT/1\n` precedes a binary document. Integers are little-endian; counts, lengths and offsets are uint64, native field/path ordinals are uint32, flags/enums are uint8. Strings and byte vectors carry uint64 byte lengths. Signed dictionary IDs retain their exact 64-bit representation. No numeric metadata is routed through floating-point JSON.

The codec in `ArrowLayout.cpp` writes these sections in order:

1. Container byte `1` (stream), source size, total top-level rows, explicit-EOS flag, index bytes read and decoded.
2. Native fields: name, native type text, supported List depth, unsupported reason.
3. Messages: kind, stream offset, body offset/length, original padded FlatBuffer metadata, compression, row count, recursive nodes, optional dictionary ID, delta flag and optional preceding dictionary-message index.
4. Record batches: message index, top-level row begin/count, dictionary ID → current message index, and supported array indexes. Each array stores its native field ordinal, each List level's validated offsets and bitmap, leaf count/null count, and native leaf value/validity spans.

Vectors/maps are prefixed by a uint64 element count. Optional values have a byte presence flag. Every recursive node contains its native field-ordinal path, length, null count and ordered absolute buffer spans. The retained native metadata preserves the full schema, buffer descriptions and custom metadata needed by a subsequent native decoder.

Parsing validates the artifact version, exact source size, message order/end, schema, recursive nodes, spans, counts and compression against the original Arrow metadata. A dictionary delta links to the preceding message of the same ID; replacement starts a new chain. Each batch captures the dictionary epochs visible at its own position. A missing, stale or foreign dictionary reference is rejected.

`parse_arrow_layout` reports `CorruptLayout`, `UnsupportedLayoutVersion`, `ResourceExhausted`, or `StalePlan` for a different source size. The caller must still bind the artifact to the exact published source/version; equal byte size is not an identity check. The ingestion API retains its existing `IngestionError` contract: parsing failures are adapter failures, and real source short/failed reads propagate as source-read failures.

## Index access and resource accounting

`make_arrow_list_index(shared_layout, batch_index, field_index, limits)` supplies the A02 `ArrayListIndex` interface and keeps the immutable layout alive. Offsets stay native and may begin above zero or leave unused trailing child slots. Access validates row/path bounds; null parents short-circuit with the existing `null_list_level` provenance. Leaves remain source buffer spans; no float values are stored in L.

The first implementation stores complete offsets and parent/List validity within a bounded layout. It does not claim paged or unbounded indexes. Leaf validity is checked during ingestion and its native span is retained for window reads. Default limits are:

| Resource | Limit |
| --- | ---: |
| One message metadata | 8 MiB |
| Serialized layout and ingestion layout accounting | 64 MiB |
| Retained List indexes | 32 MiB |
| Index bytes read / decoded, each | 128 MiB |
| One index buffer read / decompressed, each | 64 MiB |
| Messages | 65,536 |
| Schema nodes / accumulated message nodes, each | 262,144 |
| Native nesting depth | 64 |

Passive value-buffer spans are not capped by the index-buffer limit. Ingestion already copies all original bytes to B once. `index_read_bytes` counts only additional offset/validity reads and compressed-value size prefixes; `index_decoded_bytes` counts the resulting index bytes, including uncompressed input. Detection reads the Schema separately. These are ingestion counters, not query or training amplification metrics.

Only five built-in Sigma fields are emitted: `arrow.container`, `arrow.layout_version`, `arrow.record_batches`, `arrow.rows`, and `arrow.fields`. They are exposed as `user.arrow.*` xattrs through persisted identity, regardless of path suffix. Full columns/schema remain in L. No value statistics, extrema, labels, time semantics or training views are inferred.

## Validation

The unit suites `ArrowLayout` and `ArrowFormatAdapter` cover native writer fixtures, malformed layouts and payload structure, dictionary epochs, chunk boundaries, compression, null provenance, source failures, publication rollback, unchanged B, identity and xattrs.

`tests/benchmarks/arrow_training/a03_ingestion_probe.cpp` ingests independently generated PB00 streams with 11-byte chunks. It records source rereads, then opens a separate Arrow batch reader as an oracle to compare schema, rows, every supported offset/bitmap, and leaf counts. Oracle value decoding happens only after the ingestion trace is checked. It is a validation tool, not a query implementation or performance benchmark.
