# GRIB arm-label and timing audit — 2026-10-12

The FS and IoTDB-FS results were **not swapped**. The reported 117–121 seconds
are the accumulated duration of **100 complete unbound queries**, rather than
one query or one selected-message payload read.

## Checks

- Classified all 3,996 observations per arm independently of their dictionary
  label: FS rows contain the native-reader protocol and ecCodes version;
  IoTDB-FS rows contain typed server profiles and mounted IPC result paths.
- Inspected dispatch: FS runs the native executable against `campaign-N/fs/`,
  outside the FUSE mount. IoTDB-FS uses socket registration and mounted IPC.
- Recomputed the complete summary from compressed raw records; it matches
  `summary.json` exactly. Raw durations use nanoseconds in both arms.
- Checked native process wall time measured independently by Python. It agrees
  with the sum of the 100 native query durations; no factor-of-100 or unit error.

For encoded-message retrieval on the 524 MiB source:

| Campaign | FS 100-query sum (s) | FS discovery component (s) | External FS process wall (s) | IoTDB-FS 100-query sum (s) |
| --- | ---: | ---: | ---: | ---: |
| 0 | 117.893700 | 117.885678 | 117.940769 | 1.374227 |
| 1 | 117.675354 | 117.668416 | 117.721730 | 1.298961 |
| 2 | 117.444581 | 117.436316 | 117.492559 | 1.380976 |

## Independent server replay

Re-ran the unchanged native executable on the ordinary-FS medium file, with
the same predicates and three repetitions. Each query scanned 4,598 messages
and selected the same message at offset 953,833, length 64,901 bytes. Its output
SHA-256 matches the original oracle.

| Repetition | Complete query (ms) | Metadata discovery (ms) | Selected payload read (µs) |
| --- | ---: | ---: | ---: |
| 0 | 1,224.117 | 1,224.028 | 38.239 |
| 1 | 1,179.643 | 1,179.460 | 34.648 |
| 2 | 1,188.161 | 1,188.086 | 34.848 |

The independently measured process wall time was 3.603891 seconds. As a
second sanity check, ecCodes' own `grib_ls -x -w ... -p offset,totalLength`
selected the same one of 4,598 messages in 1.607498 seconds, including CLI
startup and text output. This CLI check is not substituted into paired results.

## Meaning of the difference

The unbound FS baseline reconstructs message discovery on every query and
retains no index or decoded values. IoTDB-FS uses its ingestion-built index,
retains its parsed layout within the daemon, and also reuses decoded fields for
numerical queries. Thus the difference measures avoided repeated parsing and
reuse; it does not show that a database reads 65 KB faster than a filesystem.

The bound comparison already allows FS to retain discovery and decoded data.
Its warm point/ROI reads are about 0.004 ms, faster than IoTDB-FS's roughly
1–1.5 ms. Index construction/publication is outside the query-only table;
including publication, the medium Q=100 bound numerical lifecycle favors FS
(about 2.84 s versus 12.03–12.04 s).

No benchmark labels, durations, or core code required correction. Detailed
raw replay and classification evidence is in `label-audit.json`.
