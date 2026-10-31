# Single-file FS / IoTDB-FS — Q=1/10/100

Three paired campaigns; small/medium/large controlled synthetic HDF5 files, three physical subset cases. **2,700 complete queries per arm**. All full-file identities, exact versions, subset values, masks and shapes passed the runner's checks.

All time tables use milliseconds. First/p50/p95 are medians of three campaign statistics. Ratios are medians of paired campaign ratios and therefore need not equal ratios of the displayed medians. The 297 warm observations per case are distributed across three fresh runtimes. First means a fresh reader/runtime, not cold OS pages or cold IoTDB storage.

## Write, publication and exact-version binding

| Tier | Bytes | FS write+fsync+close | IoTDB-FS write+fsync+close | IoTDB exact-version binding |
| --- | ---: | ---: | ---: | ---: |
| small | 6,728,832 | 27.128 | 466.680 | 91.733 |
| medium | 537,684,072 | 1610.521 | 7515.220 | 51.969 |
| large | 3,441,125,376 | 10267.036 | 43170.094 | 59.218 |

## Complete query latency

| Tier / case | FS first | IoTDB first | FS warm p50 / p95 | IoTDB warm p50 / p95 | Paired all-query p50 B/A | <10x target |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| small / point | 1.109 | 26.742 | 0.160 / 0.191 | 6.733 / 8.178 | 41.64x | fail |
| small / small_roi | 1.398 | 27.312 | 0.222 / 0.274 | 6.705 / 7.993 | 30.26x | fail |
| small / cross_chunk | 2.477 | 50.887 | 0.512 / 0.617 | 8.983 / 10.640 | 17.57x | fail |
| medium / point | 1.166 | 26.834 | 0.166 / 0.204 | 6.045 / 7.527 | 35.80x | fail |
| medium / small_roi | 1.411 | 28.683 | 0.218 / 0.256 | 7.139 / 8.262 | 32.09x | fail |
| medium / cross_chunk | 1.934 | 37.758 | 0.514 / 0.586 | 6.113 / 7.732 | 12.01x | fail |
| large / point | 1.223 | 28.107 | 0.167 / 0.221 | 6.560 / 8.358 | 39.29x | fail |
| large / small_roi | 1.352 | 30.199 | 0.224 / 0.259 | 6.792 / 8.117 | 31.82x | fail |
| large / cross_chunk | 2.119 | 38.000 | 0.505 / 0.613 | 6.963 / 8.563 | 14.11x | fail |

## Actual Q prefix totals and lifecycle totals

Lifecycle = one write+file-fsync+close + one IoTDB exact-version binding + Q complete queries. The FS binding term is zero. Each case is an independent scenario; summing cases would count the write more than once.

| Tier / case | Q | FS query sum | IoTDB query sum | FS lifecycle | IoTDB lifecycle | Paired query B/A | Paired lifecycle B/A |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small / point | 1 | 1.109 | 26.742 | 28.206 | 589.622 | 24.11x | 19.67x |
| small / point | 10 | 3.326 | 86.991 | 30.264 | 649.275 | 27.74x | 20.15x |
| small / point | 100 | 17.806 | 690.288 | 44.935 | 1253.168 | 38.44x | 28.28x |
| small / small_roi | 1 | 1.398 | 27.312 | 28.375 | 589.311 | 21.90x | 19.48x |
| small / small_roi | 10 | 4.068 | 88.818 | 30.994 | 644.839 | 22.98x | 19.58x |
| small / small_roi | 100 | 24.340 | 703.686 | 51.468 | 1204.397 | 28.91x | 23.93x |
| small / cross_chunk | 1 | 2.477 | 50.887 | 29.419 | 613.768 | 21.30x | 19.59x |
| small / cross_chunk | 10 | 7.924 | 138.498 | 34.758 | 701.378 | 18.00x | 19.06x |
| small / cross_chunk | 100 | 55.167 | 939.643 | 82.295 | 1469.699 | 17.03x | 17.61x |
| medium / point | 1 | 1.166 | 26.834 | 1612.660 | 7631.472 | 21.95x | 4.89x |
| medium / point | 10 | 3.452 | 80.308 | 1614.853 | 7680.929 | 23.94x | 4.92x |
| medium / point | 100 | 18.822 | 628.599 | 1629.507 | 8231.384 | 33.60x | 5.24x |
| medium / small_roi | 1 | 1.411 | 28.683 | 1612.141 | 7633.321 | 20.13x | 4.88x |
| medium / small_roi | 10 | 4.107 | 89.122 | 1614.847 | 7693.761 | 21.83x | 4.92x |
| medium / small_roi | 100 | 24.063 | 735.989 | 1634.585 | 8340.627 | 30.14x | 5.25x |
| medium / cross_chunk | 1 | 1.934 | 37.758 | 1612.523 | 7643.801 | 19.40x | 4.88x |
| medium / cross_chunk | 10 | 7.454 | 98.898 | 1618.082 | 7703.536 | 13.37x | 4.91x |
| medium / cross_chunk | 100 | 53.756 | 660.384 | 1666.415 | 8265.023 | 12.41x | 5.15x |
| large / point | 1 | 1.223 | 28.107 | 10268.259 | 43253.540 | 22.98x | 4.52x |
| large / point | 10 | 3.357 | 85.931 | 10270.491 | 43307.646 | 26.02x | 4.52x |
| large / point | 100 | 19.271 | 709.629 | 10286.306 | 43828.405 | 36.76x | 4.57x |
| large / small_roi | 1 | 1.352 | 30.199 | 10268.388 | 43256.656 | 22.14x | 4.52x |
| large / small_roi | 10 | 4.048 | 90.561 | 10271.272 | 43316.195 | 22.70x | 4.52x |
| large / small_roi | 100 | 24.499 | 711.460 | 10291.534 | 43900.317 | 29.99x | 4.57x |
| large / cross_chunk | 1 | 2.119 | 38.000 | 10269.228 | 43263.232 | 17.33x | 4.52x |
| large / cross_chunk | 10 | 7.314 | 105.072 | 10275.046 | 43340.429 | 14.37x | 4.52x |
| large / cross_chunk | 100 | 54.109 | 735.915 | 10321.145 | 43964.165 | 14.30x | 4.57x |

## Warm query diagnostics

Pooled median of 297 warm server profiles per case. Stage times are inclusive and overlap; they are not added. SQL count covers daemon client executeQuery calls; payload bytes mean backend returned bytes, not physical disk I/O.

| Tier / case | Server total | Catalog | Prepare | Cursor | IPC encode | Client SQL calls | Payload calls / bytes | Decode count |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small / point | 5.533 | 4.849 | 0.116 | 0.067 | 0.097 | 1 | 0 / 0 | 0 |
| small / small_roi | 5.117 | 4.442 | 0.095 | 0.074 | 0.114 | 1 | 0 / 0 | 0 |
| small / cross_chunk | 7.273 | 6.551 | 0.134 | 0.098 | 0.121 | 1 | 0 / 0 | 0 |
| medium / point | 4.926 | 4.338 | 0.099 | 0.068 | 0.086 | 1 | 0 / 0 | 0 |
| medium / small_roi | 5.065 | 4.339 | 0.102 | 0.088 | 0.117 | 1 | 0 / 0 | 0 |
| medium / cross_chunk | 4.841 | 4.177 | 0.111 | 0.113 | 0.099 | 1 | 0 / 0 | 0 |
| large / point | 5.101 | 4.554 | 0.102 | 0.054 | 0.085 | 1 | 0 / 0 | 0 |
| large / small_roi | 5.560 | 4.852 | 0.105 | 0.080 | 0.120 | 1 | 0 / 0 | 0 |
| large / cross_chunk | 5.424 | 4.734 | 0.114 | 0.132 | 0.093 | 1 | 0 / 0 | 0 |

## Scope

The prespecified <10x paired median latency target is met by **0/9 cases**. Correctness completion does not imply performance acceptance; all failures are retained.

The FS arm uses native HDF5 1.14.4 without an external index. Each FS query opens/dataset-selects/reads/copies/closes; CLI startup, hashing and JSON output are outside query timing and reported separately. IoTDB-FS also uses HDF5 1.14.4 and performs a new registration, materialization, IPC mmap/read/copy and close per query. Its daemon startup is outside query timing. Nested server profiles exclude socket framing/profile sink, which remain within the client wall timer.

OS pages and the IoTDB server cache are not reset. Raw sources were fully hashed before timing; decoded fragments and verified layout are reused within each case. IoTDB-FS decoded cache: 256 MiB/65,536 entries, read parallelism 1, FUSE attribute/entry TTL 1 s; ordinary raw-file cache and full-prefetch caps are 1 byte. FS native handles close on every query and use recorded native HDF5 cache defaults. Profiling is enabled. Three campaigns use FS-first, IoTDB-first, FS-first order; no significance test or cold-disk claim is made. Input schema/chunks/filters and query coordinates remain frozen across tiers.

Both writes include file fsync and close; directory setup, environment build, data generation, daemon startup and correctness hashes are outside lifecycle totals. Backend WAL/sync policy is unchanged, so equivalent power-loss durability is not claimed. Synthetic values measure overhead under this fixed schema and do not establish scientific-workload indexing benefits.

See `summary.json` for machine-readable aggregates and profile counters, `campaign.json.gz` for all observations, and `run-manifest.json` for source/environment identity, cleanup and evidence hashes. Raw server logs and file copies remain in ignored `work/`.
