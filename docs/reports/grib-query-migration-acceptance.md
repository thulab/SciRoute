# GRIB typed-query migration acceptance matrix

## Scope

This matrix compares the legacy `GribExtensionService` FUSE query path with
the `GribFormatQueryAdapter` Implementation of the format-neutral
`FormatQueryAdapter` Seam. The legacy path remains a temporary acceptance
oracle; it is not called by the new Adapter and is not part of the typed query
contract.

The parity unit is the selected immutable File Version plus one GRIB message.
For numerical cases, both paths must use the same payload bytes and layout
record. Exact equality is required for encoded bytes, integer attributes,
coordinates, shape, and finite values produced by the shared ecCodes kernel.
NaN results are compared by position and `isnan`, rather than by equality.

## Matrix

| ID | Capability | Legacy query surface | Typed query surface | Acceptance oracle | Expected relation |
|---|---|---|---|---|---|
| GQ-01 | Format dispatch | Registry-selected GRIB service | `format_id == "grib"` and registered Adapter | Rename the same published GRIB path and query it | Equivalent suffix-independent dispatch; wrong Format Identity is rejected |
| GQ-02 | Message equality selection | `shortName`, `typeOfLevel`, `level`, `step`, `stepRange`, `edition`, `unit` string parameters | Typed equality predicate on the corresponding Logical Schema fields | Run each predicate against the same layout record | Same selected message; no implicit literal conversion in the typed path |
| GQ-03 | Compound message selection | Flat legacy key/value conjunction | `AND`, `OR`, `NOT`, and `IN` over equality-capable fields | Reduce the typed predicate to matching layout records | Conjunction parity; typed logical composition is an intentional extension |
| GQ-04 | Encoded message view | `function=read&type=binary` | `BinaryView` projecting only `grib.message.bytes` | Byte-for-byte comparison with the selected payload range | Exact parity |
| GQ-05 | Native values, default dtype | `function=read&type=data` or omitted type | `LogicalValues` projecting `grib.data.value` | Decode the same message and compare ordered values | Exact Float32 parity |
| GQ-06 | Float64 values | `dtype=f64` | Float64 override on `grib.data.value` | Compare ordered values and logical type | Exact Float64 parity |
| GQ-07 | Rectangular crop | `function=crop` with both corners | closed latitude/longitude `DomainSlice`s | Use bounds that coincide with native coordinates | Same cells, coordinates, and storage row order |
| GQ-08 | Equal bounds | equal legacy crop corners mean “start to grid end” | equal closed bounds mean one coordinate | Query one native coordinate | Intentional semantic difference: typed interval semantics win |
| GQ-09 | Bilinear interpolation by count | `outNi` and `outNj` | interpolation axes with `count` | Compare values, coordinates, and shape | Exact parity |
| GQ-10 | Bilinear interpolation by spacing | `dlon` and `dlat` | interpolation axes with typed `spacing` | Compare values, coordinates, and derived shape | Exact parity |
| GQ-11 | Crop plus interpolation | chained `read ? crop ? interp` | Domain Slices plus interpolation output | Compare with a non-constant sub-grid | Exact parity; crop must precede interpolation |
| GQ-12 | Border handling | `border=clamp` or `border=nan` | `BorderMode::Clamp` or `BorderMode::NaN` | Exercise output coordinates outside the native grid | Same finite values and NaN positions |
| GQ-13 | Message attributes | Index keys exposed mainly for selection/xattrs | projected typed message attributes | Compare projected values with the selected layout record | Typed query extension; nullable fields preserve missing labels |
| GQ-14 | Multiple matching messages | reject a non-unique singular virtual-file selection | one ordered batch stream per matching message | Use two matching layout records | Intentional difference: the singular legacy surface fails explicitly, while typed set semantics return every match in layout order |
| GQ-15 | Layout compatibility | unversioned private index text | `IOTDBFS-GRIB-LAYOUT/1` | remove or alter the protocol line | Intentional hardening: missing/malformed is `CorruptLayout`; unknown version is `UnsupportedLayoutVersion` |
| GQ-16 | Corrupt range/payload | legacy open/materialization failure | structured `QueryStatus` | truncate layout ranges and payload bytes independently | Typed path reports `CorruptLayout`, `ReadFailure`, or `CorruptPayload` at the responsible phase |
| GQ-17 | Dense-grid batching | one materialized byte buffer | bounded `QueryBatch` stream | set target rows below the result size | Values remain ordered; batches end only on full longitude rows and carry correct shape/origin |
| GQ-18 | Source stability | row time/path supplied on each legacy call | opaque plan bound to exact path/version | execute a plan with another Query Source | Typed path returns `StalePlan` |
| GQ-19 | Resource and cancellation policy | frontend-specific | Query Module layout/payload/result budgets and cancellation | exceed each limit and cancel an open cursor | Typed path fails with `ResourceExhausted` or `Cancelled`; no partial silent success |
| GQ-20 | Provenance | implicit path and row time | explicit `QuerySourceReference` in every batch | inspect every emitted batch | Exact path, version, and Format Identity are present |

## Automated evidence

| Matrix rows | Test evidence |
|---|---|
| GQ-02, GQ-03, GQ-04, GQ-13, GQ-14, GQ-20 | `GribFormatQueryAdapter.SelectsEveryMatchingMessageAsBinaryView`, `GribFormatQueryAdapter.ProjectsMessageRowsAfterTypedLogicalPredicate`, plus existing layout-match tests |
| GQ-05, GQ-06, GQ-17, GQ-20 | `GribFormatQueryAdapter.StreamsTypedGridInWholeLongitudeRows` |
| GQ-07, GQ-08 | `GribFormatQueryAdapter.UsesExactClosedSlicesIncludingOneCell` and existing GRIB crop tests |
| GQ-09, GQ-11 | `GribFormatQueryAdapter.AppliesDomainSliceBeforeBilinearInterpolation` and existing bilinear tests |
| GQ-10 | `GribFormatQueryAdapter.InterpolatesWithTypedAxisSpacing` and legacy spacing tests |
| GQ-01, GQ-15 | `GribFormatQueryAdapter.RejectsForeignIdentityAndUnversionedLayout` and `GribLayout.RequiresExplicitSupportedVersion` |
| GQ-12 | existing `GribRegularGrid.BilinearOutsideGridUsesSelectedBorderPolicy` |
| GQ-16 | `GribQueryKernel.ReturnsStructuredFailuresInsteadOfEmptyByteResults` |
| GQ-19 | Query Module budget/cancellation tests |
| GQ-18 | stale-source branch of `GribFormatQueryAdapter.RejectsForeignIdentityAndUnversionedLayout` |

## Release gate

The migration is accepted when:

1. all parity rows marked exact pass on GRIB1 and GRIB2 fixtures where ecCodes
   provides both samples;
2. every intentional difference is documented at the public query boundary
   and has a regression test;
3. no typed Adapter source includes or invokes `GribExtensionService` or its
   query-string parser;
4. layout version, source version, resource, and cancellation failures return
   structured `QueryStatus` values; and
5. the legacy path can be removed without changing the public
   `FormatQueryAdapter`, Query Module, or GRIB Query Kernel Interfaces.

## Execution record: 2026-09-25

The matrix was executed against the isolated TimechoDB database
`iotdb_fs_grib_acceptance` on port `33667`, with the FUSE mount at
`runtime/mount/grib_acceptance`. Both supplied real datasets were ingested as
GRIB File Versions and read back byte-for-byte:

| Dataset | Payload bytes | Layout bytes | Summary |
|---|---:|---:|---|
| `KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2` | 2,446,828 | 3,257 | `shortName={clwmr}`, 22 levels, `step={0}` |
| `KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2` | 549,606,683 | 707,244 | `shortName={clwmr}`, 22 levels, 209 steps over `[0,384]` |

Live-path observations were:

| Evidence | Result |
|---|---|
| Raw payload round trip | both files exactly equal their source files |
| Unique encoded message | 179 bytes and exactly equal to `grib_copy` output |
| Native Float32 / Float64 | 4,152,960 / 8,305,920 bytes |
| Crop crossing 0 degrees longitude | 26,244 bytes = `81 * 81 * sizeof(float)` |
| Crop then interpolation by count | 60 bytes = `5 * 3 * sizeof(float)` |
| Crop then interpolation by spacing | 100 bytes = `5 * 5 * sizeof(float)` |
| Ambiguous singular selection | rejected with a non-zero filesystem result and an explicit ambiguity warning |
| Cross-file exact membership pruning | two matches, two successful 4,152,960-byte results; absent member returns no files |
| Rename and format dispatch | query remained byte-exact after rename; original name was restored |

The focused migration suite passed **144/144** tests across 13 suites, and the
repository-wide unit suite passed **636/636** tests after adding the typed
spacing regression. Together with the live observations above, GQ-01 through
GQ-20 pass. Warnings
emitted by the automated run are expected assertions of corrupt-payload,
invalid-crop, read-failure, and ambiguity error paths.

The live run also exposed and fixed three namespace/lifecycle defects that
were outside the numerical GRIB kernel: the subtree `LIKE` predicate used an
invalid multi-character TimechoDB escape, rename enumerated `path,time` while
parsing the stable `time,path` projection, and the inode parser rejected the
canonical empty typed metadata document of a byte pass-through version.
