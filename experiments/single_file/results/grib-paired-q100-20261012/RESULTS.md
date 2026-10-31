# GRIB single-file paired experiment

Three paired campaigns use two real GFS file tiers, three selections, two reader modes and independent Q=1/10/100 sessions: **3,996 actual reads per arm**. Every output is checked against the raw-file oracle, with exact source versions, full-copy SHA verification and successful fixed-session release. Partial and smoke campaigns are rejected by this summary.

Times are milliseconds. Warm p50/p95 pool 297 Q=100 reads per tier/case/mode/arm, excluding each session's first read. Q totals are medians of three actual sessions; B/A ratios are campaign-paired medians. A fresh reader/daemon is not a cold OS or database cache.

## Warm reads

| Tier / selection | Mode | FS p50 / p95 | IoTDB-FS p50 / p95 |
| --- | --- | ---: | ---: |
| small / binary_one | unbound | 5.729 / 6.673 | 11.070 / 14.223 |
| small / binary_one | bound | 0.002 / 0.003 | 5.628 / 6.682 |
| small / point | unbound | 24.869 / 27.211 | 6.140 / 7.361 |
| small / point | bound | 0.004 / 0.004 | 1.610 / 2.540 |
| small / roi | unbound | 24.623 / 27.211 | 6.339 / 8.064 |
| small / roi | bound | 0.004 / 0.004 | 1.090 / 2.093 |
| medium / binary_one | unbound | 1175.203 / 1221.084 | 12.331 / 14.918 |
| medium / binary_one | bound | 0.002 / 0.004 | 5.987 / 7.625 |
| medium / point | unbound | 1206.863 / 1245.824 | 8.233 / 10.089 |
| medium / point | bound | 0.004 / 0.004 | 1.453 / 2.463 |
| medium / roi | unbound | 1205.629 / 1237.570 | 8.053 / 9.561 |
| medium / roi | bound | 0.004 / 0.004 | 1.101 / 2.013 |

## Actual Q query and session totals

Query total sums exactly Q reads. Session total adds one metadata bind and the actual close/release. Each Q has its own session; no Q=100 prefix substitutes for a Q=1/10 session.

| Tier / selection | Mode | Q | FS queries | B queries | FS session | B session | Paired session B/A |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| small / binary_one | unbound | 1 | 45.906 | 22.132 | 45.906 | 22.132 | 0.478x |
| small / binary_one | unbound | 10 | 100.843 | 122.193 | 100.843 | 122.193 | 1.286x |
| small / binary_one | unbound | 100 | 630.640 | 1121.581 | 630.640 | 1121.581 | 1.760x |
| small / binary_one | bound | 1 | 0.040 | 9.030 | 46.059 | 23.696 | 0.535x |
| small / binary_one | bound | 10 | 0.057 | 66.063 | 44.895 | 78.919 | 1.785x |
| small / binary_one | bound | 100 | 0.260 | 576.004 | 47.131 | 591.208 | 12.441x |
| small / point | unbound | 1 | 71.218 | 63.759 | 71.218 | 63.759 | 0.838x |
| small / point | unbound | 10 | 300.094 | 119.923 | 300.094 | 119.923 | 0.412x |
| small / point | unbound | 100 | 2559.631 | 670.620 | 2559.631 | 670.620 | 0.258x |
| small / point | bound | 1 | 26.333 | 47.294 | 72.610 | 61.403 | 0.864x |
| small / point | bound | 10 | 24.438 | 56.885 | 70.925 | 70.663 | 1.007x |
| small / point | bound | 100 | 25.461 | 216.254 | 74.388 | 229.070 | 2.900x |
| small / roi | unbound | 1 | 71.669 | 60.304 | 71.669 | 60.304 | 0.844x |
| small / roi | unbound | 10 | 295.872 | 120.640 | 295.872 | 120.640 | 0.397x |
| small / roi | unbound | 100 | 2556.281 | 707.883 | 2556.281 | 707.883 | 0.289x |
| small / roi | bound | 1 | 25.447 | 47.076 | 71.186 | 61.264 | 0.846x |
| small / roi | bound | 10 | 26.183 | 62.064 | 73.444 | 76.583 | 1.091x |
| small / roi | bound | 100 | 26.022 | 173.254 | 72.180 | 185.390 | 2.388x |
| medium / binary_one | unbound | 1 | 1216.455 | 87.909 | 1216.455 | 87.909 | 0.076x |
| medium / binary_one | unbound | 10 | 11663.544 | 213.590 | 11663.544 | 213.590 | 0.018x |
| medium / binary_one | unbound | 100 | 117675.354 | 1374.227 | 117675.354 | 1374.227 | 0.012x |
| medium / binary_one | bound | 1 | 0.052 | 9.394 | 1224.816 | 89.224 | 0.073x |
| medium / binary_one | bound | 10 | 0.060 | 70.826 | 1194.681 | 147.728 | 0.123x |
| medium / binary_one | bound | 100 | 0.242 | 596.034 | 1240.703 | 671.568 | 0.576x |
| medium / point | unbound | 1 | 1225.809 | 122.327 | 1225.809 | 122.327 | 0.100x |
| medium / point | unbound | 10 | 12140.943 | 197.754 | 12140.943 | 197.754 | 0.016x |
| medium / point | unbound | 100 | 121002.843 | 988.102 | 121002.843 | 988.102 | 0.008x |
| medium / point | bound | 1 | 25.996 | 48.629 | 1237.821 | 124.097 | 0.101x |
| medium / point | bound | 10 | 25.125 | 65.109 | 1235.794 | 136.398 | 0.110x |
| medium / point | bound | 100 | 26.573 | 199.873 | 1235.171 | 273.614 | 0.222x |
| medium / roi | unbound | 1 | 1229.121 | 116.244 | 1229.121 | 116.244 | 0.095x |
| medium / roi | unbound | 10 | 12195.247 | 199.620 | 12195.247 | 199.620 | 0.016x |
| medium / roi | unbound | 100 | 120768.457 | 954.831 | 120768.457 | 954.831 | 0.008x |
| medium / roi | bound | 1 | 25.823 | 48.243 | 1246.503 | 123.595 | 0.098x |
| medium / roi | bound | 10 | 25.395 | 64.879 | 1241.916 | 139.856 | 0.113x |
| medium / roi | bound | 100 | 26.783 | 172.606 | 1242.834 | 248.116 | 0.202x |

## Publication and complete lifecycle

Publication is shared by the scenarios within each campaign. Lifecycle = one publication + exact source locator (B only) + the chosen session. Separate scenario totals must not be summed as if they were one execution.

| Tier | Source bytes | FS publication | B publication | B exact-version locator |
| --- | ---: | ---: | ---: | ---: |
| small | 2,446,828 | 12.736 | 415.502 | 30.943 |
| medium | 549,606,683 | 1608.561 | 11706.339 | 97.466 |

| Tier / selection | Mode | Q | FS lifecycle | B lifecycle | Paired lifecycle B/A |
| --- | --- | ---: | ---: | ---: | ---: |
| small / binary_one | unbound | 1 | 59.012 | 465.869 | 8.035x |
| small / binary_one | unbound | 10 | 113.579 | 568.639 | 5.289x |
| small / binary_one | unbound | 100 | 646.370 | 1568.027 | 2.413x |
| small / binary_one | bound | 1 | 59.032 | 469.213 | 8.524x |
| small / binary_one | bound | 10 | 57.631 | 525.365 | 9.587x |
| small / binary_one | bound | 100 | 59.867 | 1045.681 | 17.551x |
| small / point | unbound | 1 | 83.954 | 504.230 | 6.113x |
| small / point | unbound | 10 | 312.619 | 571.643 | 1.829x |
| small / point | unbound | 100 | 2572.367 | 1134.426 | 0.441x |
| small / point | bound | 1 | 86.008 | 506.666 | 6.164x |
| small / point | bound | 10 | 83.451 | 513.090 | 6.148x |
| small / point | bound | 100 | 90.119 | 675.515 | 7.495x |
| small / roi | unbound | 1 | 84.405 | 506.540 | 6.452x |
| small / roi | unbound | 10 | 308.398 | 564.030 | 1.829x |
| small / roi | unbound | 100 | 2568.806 | 1136.541 | 0.462x |
| small / roi | bound | 1 | 83.922 | 507.710 | 6.128x |
| small / roi | bound | 10 | 87.509 | 518.283 | 6.464x |
| small / roi | bound | 100 | 87.910 | 631.836 | 7.007x |
| medium / binary_one | unbound | 1 | 2806.972 | 11874.886 | 4.203x |
| medium / binary_one | unbound | 10 | 13279.806 | 11976.823 | 0.904x |
| medium / binary_one | unbound | 100 | 119291.616 | 13087.758 | 0.110x |
| medium / binary_one | bound | 1 | 2795.249 | 11865.099 | 4.279x |
| medium / binary_one | bound | 10 | 2766.473 | 11931.702 | 4.313x |
| medium / binary_one | bound | 100 | 2796.199 | 12466.453 | 4.477x |
| medium / point | unbound | 1 | 2834.370 | 11903.227 | 4.202x |
| medium / point | unbound | 10 | 13749.504 | 11978.653 | 0.909x |
| medium / point | unbound | 100 | 122558.339 | 12766.031 | 0.107x |
| medium / point | bound | 1 | 2827.024 | 11909.518 | 4.238x |
| medium / point | bound | 10 | 2824.263 | 11917.298 | 4.196x |
| medium / point | bound | 100 | 2843.731 | 12044.460 | 4.301x |
| medium / roi | unbound | 1 | 2823.141 | 11897.144 | 4.237x |
| medium / roi | unbound | 10 | 13803.808 | 11980.519 | 0.868x |
| medium / roi | unbound | 100 | 122323.953 | 12718.810 | 0.104x |
| medium / roi | bound | 1 | 2841.276 | 11915.868 | 4.207x |
| medium / roi | bound | 10 | 2819.385 | 11915.820 | 4.205x |
| medium / roi | bound | 100 | 2844.677 | 12029.016 | 4.289x |

## Retrieval and warm diagnostics

Point and ROI selection still read/decode the complete selected GRIB message; crop selectivity does not imply intra-message physical pruning. Source payload bytes below measure transfer, not physical disk reads. Native metadata bytes are unavailable from the headers-only API and are not reported as zero. Shared-runtime decode counters do not cover the GRIB adapter's private decoder.

| Tier / selection | Selected / candidates | Selected encoded bytes | Mode | Warm FS payload bytes / decodes | Warm B SQL / payload calls | Warm B transferred payload bytes |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| small / binary_one | 1 / 22 | 66,606 | unbound | 66606 / 0 | 2 / 1 | 66606 |
| small / binary_one | 1 / 22 | 66,606 | bound | 0 / 0 | 1 / 1 | 66606 |
| small / point | 1 / 22 | 66,606 | unbound | 66606 / 1 | 1 / 0 | 0 |
| small / point | 1 / 22 | 66,606 | bound | 0 / 0 | 0 / 0 | 0 |
| small / roi | 1 / 22 | 66,606 | unbound | 66606 / 1 | 1 / 0 | 0 |
| small / roi | 1 / 22 | 66,606 | bound | 0 / 0 | 0 / 0 | 0 |
| medium / binary_one | 1 / 4598 | 64,901 | unbound | 64901 / 0 | 2 / 1 | 64901 |
| medium / binary_one | 1 / 4598 | 64,901 | bound | 0 / 0 | 1 / 1 | 64901 |
| medium / point | 1 / 4598 | 64,901 | unbound | 64901 / 1 | 1 / 0 | 0 |
| medium / point | 1 / 4598 | 64,901 | bound | 0 / 0 | 0 / 0 | 0 |
| medium / roi | 1 / 4598 | 64,901 | unbound | 64901 / 1 | 1 / 0 | 0 |
| medium / roi | 1 / 4598 | 64,901 | bound | 0 / 0 | 0 / 0 | 0 |

## Q=100 setup and client stages

Bind/release figures are campaign medians. Client stage figures are pooled warm medians; stage medians are not additive. Server stages are inclusive and may overlap.

| Tier / selection | Mode | FS bind / close | B bind / release | B registration | B mmap/open | B IPC conversion | B owned copy/close |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| small / binary_one | unbound | 0.000 / 0.000 | 0.000 / 0.000 | 9.733 | 0.668 | 0.310 | 0.172 |
| small / binary_one | bound | 46.859 / 0.005 | 12.643 / 0.677 | 4.702 | 0.489 | 0.226 | 0.138 |
| small / point | unbound | 0.000 / 0.000 | 0.000 / 0.000 | 5.212 | 0.509 | 0.194 | 0.166 |
| small / point | bound | 48.445 / 0.439 | 14.613 / 0.552 | 0.585 | 0.439 | 0.179 | 0.149 |
| small / roi | unbound | 0.000 / 0.000 | 0.000 / 0.000 | 5.344 | 0.526 | 0.183 | 0.156 |
| small / roi | bound | 45.619 / 0.544 | 13.689 / 0.351 | 0.466 | 0.319 | 0.144 | 0.131 |
| medium / binary_one | unbound | 0.000 / 0.000 | 0.000 / 0.000 | 11.037 | 0.671 | 0.336 | 0.181 |
| medium / binary_one | bound | 1240.457 / 0.004 | 74.923 / 0.668 | 4.833 | 0.594 | 0.266 | 0.159 |
| medium / point | unbound | 0.000 / 0.000 | 0.000 / 0.000 | 6.884 | 0.900 | 0.240 | 0.180 |
| medium / point | bound | 1207.148 / 0.621 | 72.870 / 0.772 | 0.716 | 0.352 | 0.157 | 0.134 |
| medium / roi | unbound | 0.000 / 0.000 | 0.000 / 0.000 | 6.809 | 0.707 | 0.215 | 0.171 |
| medium / roi | bound | 1215.398 / 0.643 | 75.104 / 0.407 | 0.473 | 0.321 | 0.150 | 0.134 |

## Scope and operational limits

FS uses native ecCodes headers-only scans with no sidecar or dataset-wide external index. Unbound reads rescan messages and retain no payload/decoded cache. Bound sessions scan metadata once, then lazily retain bounded encoded message bytes and native decoded arrays; every read recrops and makes an owned output copy. This FS bound raw-message cache may be stronger than IoTDB-FS BinaryView reuse; the recorded read counters expose this difference rather than assuming equivalent cache hits.

IoTDB-FS ingestion builds a persistent message layout and pays its cost in publication. Unbound queries perform normal registration/materialization. Fixed bound sessions hold one exact-version prepared selection, construct a fresh cursor and encode/publish an IPC result for every read. Client registration, mmap/open, IPC conversion, owned copy and close are included. Knowing offsets beforehand is confined to the raw correctness oracle, not the measured FS discovery operation.

The experimental binding requires an unchanged, read-only source and permissions for its lifetime. OS/database caches are not reset. Native process startup, hash checks and JSON printing are excluded from read timers and reported separately in raw observations; source locator and publication are excluded from query/session totals but included in lifecycle totals. Profiling is enabled on B; inclusive server stages overlap and must not be added. The binary-message case and decoded-value cases have different output work and are interpreted separately.

Complete observations are retained in `campaign.json` or `campaign.json.gz`; `summary.json` contains the checked aggregate. This experiment evaluates single-file retrieval and repeated-query amortization, and does not establish cross-file, multi-version or distributed indexing benefits.

Frozen session scope: Explicit private root-only fixed-query lease; no source deletion or permission mutation during session.

Publication scope: Same 1 MiB copy + file fsync + close; directory setup excluded; database WAL policy unchanged; power-loss durability not equated.

Timing scope: Native query phases versus complete typed registration+IPC open+owned consumption; hashes/JSON and native process startup excluded and process wall recorded separately.
