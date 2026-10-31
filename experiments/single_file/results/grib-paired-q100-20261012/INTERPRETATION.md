# GRIB paired results: interpretation — 2026-10-12

All three campaigns completed: **3,996 queries per arm**, 7,992 in total.
The small-file smoke additionally passed 18 queries per arm. Eighteen local
runner/summary tests passed. Full source SHA checks, exact-version provenance,
message/value/coordinate/mask oracles, actual Q counts and fixed-session
release checks all passed. Independent local re-aggregation reproduced the
server summary exactly, and artifact/source hashes matched.

## Main findings

Numbers below are medians of three actual Q=100 sessions, in milliseconds.
Session totals include the once-only bind and actual close/release. See
`RESULTS.md` for paired ratios, Q=1/10, p95 and complete lifecycle tables.

| Source / case | Mode | FS session | IoTDB-FS session |
| --- | --- | ---: | ---: |
| 2.4 MiB / encoded message | Reopen / re-register | 630.64 | 1,121.58 |
| 2.4 MiB / point | Reopen / re-register | 2,559.63 | 670.62 |
| 2.4 MiB / ROI | Reopen / re-register | 2,556.28 | 707.88 |
| 524 MiB / encoded message | Reopen / re-register | 117,675.35 | 1,374.23 |
| 524 MiB / point | Reopen / re-register | 121,002.84 | 988.10 |
| 524 MiB / ROI | Reopen / re-register | 120,768.46 | 954.83 |
| 524 MiB / encoded message | Bind once | 1,240.70 | 671.57 |
| 524 MiB / point | Bind once | 1,235.17 | 273.61 |
| 524 MiB / ROI | Bind once | 1,242.83 | 248.12 |

The large file contains 4,598 physical messages. The unindexed native FS
reader constructs and interprets their ecCodes metadata on each unbound query.
It is a headers-only scan, not a forced full-payload read; cheap level/step
conditions precede costly concept resolution. Warm native discovery is still
about 1.17–1.18 seconds. IoTDB-FS reuses a compact parsed layout built from its
persistent ingestion index. Its warm ordinary queries take 8–12 ms; values
also benefit from reuse of the decoded native field. The binary case makes
no value-decoding claim and independently demonstrates the discovery benefit.

The small file exposes fixed costs more directly. Warm encoded retrieval is
5.73 ms for FS and 11.07 ms for B, so B is slower. Warm unbound numerical reads
are about 24.6–24.9 ms for FS and 6.1–6.3 ms for B. That numerical improvement
includes B's cross-query decoded-field reuse; FS deliberately retains no data
between ordinary queries. It must not be described as faster database payload
I/O or as a comparison with a persistent FS index.

## Bind-once and publication accounting

Both bound numerical readers reuse one full decoded field and make independent
crop/value outputs on each execution. FS's warm point/ROI reads take roughly
0.004 ms; B's take 1.09–1.61 ms across tiers. SQL is zero for warm bound values,
so B's remaining time is frontend/IPC/result consumption rather than payload
or catalog reads. FS remains much faster once both sides have their reusable
data and lookup state.

Large-file bound session totals nevertheless favor B within Q≤100 because
FS pays roughly 1.2 seconds of metadata discovery once, while B binds from its
already-ingested layout. The figures do **not** include construction of that
persistent layout; construction belongs to the publication phase.

Median publication costs are:

| Source | FS | IoTDB-FS | B exact-version locator |
| --- | ---: | ---: | ---: |
| 2.4 MiB | 12.74 ms | 415.50 ms | 30.94 ms |
| 524 MiB | 1.609 s | 11.706 s | 97.47 ms |

With publication and locator included, the large-file Q=100 bound numerical
paths cost about **2.84 s for FS versus 12.03–12.04 s for B**. B is slower for
all measured bound Q=1/10/100 lifecycles. In the unbound large-file paths, the
repeated scan cost changes the result: complete Q=100 lifecycles are roughly
119–123 s for FS versus 12.7–13.1 s for B. The measured Q=10 unbound lifecycles
already favor B modestly; Q=1 does not. These are observed Q points, not an
extrapolated or universal amortization threshold.

## Retrieval and evidence limits

Each query selects exactly one `clwmr`, isobaric 500 hPa, step-0 message:
1/22 messages and 66,606 bytes in the small file; 1/4,598 messages and 64,901
bytes in the large file. Fresh payload reads request/return only those bytes.
Point and ROI consume the same complete selected message; spatial crop is
performed after full native decode. The payload metrics quantify transfer to
the client, not database disk blocks or internal scans.

Bound FS encoded retrieval explicitly caches bytes; B BinaryView still reads
the source range on each execution. This stronger FS cache is declared in the
raw and aggregate reports. GRIB uses private decoder/message caches, so zero
public `runtime_delta.decode_count` is not evidence of no GRIB decode. Native
decode counts and B's payload SQL/bytes are the available direct observations.

The two sources share product, variable, grid geometry and packing, but their
selected compressed lengths and values differ. They are a real-data gradient,
not a byte-identical synthetic control. Both systems are page-cache warm and
the database cache is not reset. Three sequential paired campaigns cannot
establish cold-disk, distributed, multi-version or cross-file performance.
All owned mounts were removed; the three pre-existing daemons remained alive.

The supported conclusion is that GRIB's semantic message structure permits
compact reusable discovery and decoded-field state, avoiding substantial
repeated native parsing under the no-sidecar FS baseline. A native FS reader
with a reusable binding also removes those repeated costs. The experiment
therefore demonstrates an indexing/reuse benefit with an explicit preparation
cost, rather than a universal advantage of a database over filesystem reads.
