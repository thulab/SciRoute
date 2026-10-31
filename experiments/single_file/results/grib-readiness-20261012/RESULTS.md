# GRIB single-file readiness — 2026-10-12

The current daemon can execute GRIB message predicates, encoded-message
retrieval, numerical crops and fixed-query PreparedQuery sessions. This
read-only pilot passed eight retrievals against an existing published file.
It is **not** a formal FS/IoTDB-FS latency comparison or a Q=1/10/100 campaign.

## Frozen source and environment

- Raw GFS file: `KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2`,
  2,446,828 bytes, 22 messages, `clwmr`, 22 pressure levels, step 0.
- Published source: `/geos-pilot-9856a092a812/grib-control.grib2`,
  exact version `1791274591370`, database `h09probe1790939889dbfc1f`.
- Raw SHA-256: `a13c0e8698ef7897c4b32047ff5f2aea8221280b2e4d244e70d3b0499871cec1`.
- Daemon SHA-256: `3ac27a5bcd5bceee0b3b74fce9699b164ac954914bc689122707602352623699`.
- Native ecCodes 2.34.1 supplies both the daemon library and the independent
  oracle. Exact source provenance is checked on every nonempty result batch.
- Each case uses a fresh private daemon; point and ROI retain that daemon for
  two reads after a single fixed bind. Existing data and mounts are preserved.

## Retrieval acceptance

| Case | Result rows | Payload SQL | Requested payload bytes | Evidence |
| --- | ---: | ---: | ---: | --- |
| All message attributes | 22 | 0 | 0 | Variable, level type, level, step and offsets equal native directory |
| Compound predicate selecting level 500 | 1 | 1 | 66,606 | Encoded message SHA equals its raw source range |
| No matching level | 0 | 0 | 0 | Empty IPC with schema; no payload read |
| All encoded messages | 22 | 22 | 2,446,828 | Every message SHA and order match; ranges cover the entire raw source |
| Bound point, first read | 1 | 1 | 66,606 | Native coordinates and Float32 value match exactly |
| Bound point, second read | 1 | 0 | 0 | Fresh cursor and IPC encoding; zero SQL |
| Bound 4×4 ROI, first read | 16 | 1 | 66,606 | Native coordinates and Float32 values match exactly |
| Bound 4×4 ROI, second read | 16 | 0 | 0 | Fresh cursor and IPC encoding; zero SQL |

The compound predicate is `level=500 AND short_name=clwmr AND
type_of_level=isobaricInhPa AND step=0`. It selects 1/22 messages and 2.72% of
source bytes: **97.28% fewer payload bytes** than returning all messages.
This demonstrates physical message selection rather than filtering a fully
read file. The point and ROI read the same encoded message: GRIB spatial
cropping follows full selected-message decoding and does not prune bytes
inside that message.

The cold binary-one retrieval made one metadata SQL, one layout SQL and one
payload SQL. Bound numerical reads made zero metadata/layout SQL. The first
read fetched the selected message; the second made zero SQL calls. This does
not mean all GRIB decoding was absent: its private decoded-field/message
caches are outside the public runtime's decode/cache counters. Those public
counters must not be used to infer GRIB decode counts.

Diagnostic client times include socket registration and owned IPC consumption;
they are recorded per retrieval in `report.json`. This pilot has one observation
per cold case and two per bound case, uses an offline native oracle, and does
not time an equivalent native FS reader. Its timings cannot establish a speedup.

## Formal experiment readiness and remaining work

The server also has a GFS file of 549,606,683 bytes with 4,598 messages. ERA5
pressure-level raw files of 265,803,264 and 4,119,950,592 bytes are available;
their candidate distributions and geometry still need auditing for this
single-file campaign. Different products must not silently become an otherwise
identical size gradient.

The HDF5 `native_probe` and `session_runner.py` hardcode `/T`, HDF5 selections
and HDF5 version checks. A formal GRIB comparison therefore needs an ecCodes
native reader that includes message discovery in the unbound FS query and
charges discovery/binding once in the bound FS session. No external FS sidecar
is used in the primary single-file baseline. Both arms must return identical
encoded messages or identical decoded values/coordinates for the same case;
binary-message and numerical-crop results are separate workloads.

Use actual Q=1/10/100 sessions, include bind/release in their total, and record
candidate/selected messages, selected bytes, payload calls and GRIB-specific
decode/cache metrics. Preserve first-read versus reused-read measurements.
Do not place result `stat`/materialization before the query timer, or count
re-reading an existing IPC as a new query.

## Reproduction

Run `grib_readiness.py` with the verified session environment, raw source,
logical path, database and namespace lock above, and a **new** output directory.
The script requires a source at most 128 MiB, exactly one level-500 message,
and checks all encoded messages before numerical crops. Raw files, private
logs and failed attempts remain in ignored server `work/`; accepted evidence
is stored here. All private mounts were removed and the original three daemon
PIDs remained alive. No core code or published source was modified.
