# Single-file paired bind-once experiment

Three paired campaigns, three size tiers and three physical selections: **5,697 actual reads per arm** (2,700 reopen/re-register and 2,997 bound). Every Q=1/10/100 bound group has a fresh native session and a fresh IoTDB-FS daemon; its bind and close/release are measured once. All full-file SHA, exact versions, selections, values and masks passed.

Times below are milliseconds; figures are medians across three campaigns. Ratios are paired medians and need not equal ratios of displayed medians. First means a fresh reader/runtime, not cold OS pages or cold database storage. Profiling is enabled in both IoTDB modes.

## Complete warm read comparison (Q=100 sessions)

| Tier / case | FS reopen p50 / p95 | FS bound p50 / p95 | IoTDB re-register p50 / p95 | IoTDB bound p50 / p95 | Bound warm B/A |
| --- | ---: | ---: | ---: | ---: | ---: |
| small / point | 0.159 / 0.195 | 0.002 / 0.002 | 6.417 / 7.898 | 1.475 / 2.112 | 804.17x |
| small / small_roi | 0.216 / 0.297 | 0.004 / 0.005 | 6.486 / 7.746 | 1.454 / 2.191 | 353.29x |
| small / cross_chunk | 0.511 / 0.652 | 0.008 / 0.009 | 9.229 / 10.751 | 1.590 / 2.229 | 183.83x |
| medium / point | 0.222 / 0.367 | 0.002 / 0.002 | 6.207 / 7.538 | 1.616 / 2.520 | 836.05x |
| medium / small_roi | 0.227 / 0.265 | 0.004 / 0.005 | 6.749 / 8.000 | 1.797 / 2.591 | 419.77x |
| medium / cross_chunk | 0.522 / 0.600 | 0.009 / 0.011 | 6.172 / 7.525 | 1.616 / 2.372 | 176.25x |
| large / point | 0.167 / 0.230 | 0.002 / 0.002 | 6.395 / 7.949 | 1.185 / 1.841 | 645.28x |
| large / small_roi | 0.227 / 0.297 | 0.004 / 0.005 | 6.664 / 7.904 | 1.759 / 2.486 | 401.12x |
| large / cross_chunk | 0.513 / 0.604 | 0.009 / 0.009 | 6.367 / 7.687 | 1.668 / 2.212 | 192.30x |

## Actual query/session totals

Reopen/re-register totals are actual Q-prefix sums. Bound totals include one bind + exactly Q reads + its own close/release, including the Q=1 close. Reads always materialize new IPC results and consume/copy them before release; repeated reads of an old IPC result are excluded.

| Tier / case | Q | FS reopen | FS bound session | IoTDB re-register | IoTDB bound session | FS bound / reopen | IoTDB bound / re-register | Bound B/A |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small / point | 1 | 1.290 | 1.249 | 27.165 | 26.217 | 0.968 | 1.034 | 20.55x |
| small / point | 10 | 3.883 | 1.204 | 89.748 | 39.185 | 0.330 | 0.437 | 32.21x |
| small / point | 100 | 18.647 | 1.620 | 672.955 | 181.728 | 0.087 | 0.254 | 110.02x |
| small / small_roi | 1 | 1.434 | 1.519 | 26.964 | 29.845 | 1.059 | 1.088 | 19.27x |
| small / small_roi | 10 | 4.144 | 1.562 | 84.383 | 50.044 | 0.377 | 0.597 | 32.11x |
| small / small_roi | 100 | 24.394 | 2.033 | 676.984 | 183.351 | 0.082 | 0.289 | 95.63x |
| small / cross_chunk | 1 | 2.618 | 2.193 | 49.071 | 37.902 | 0.857 | 0.772 | 18.67x |
| small / cross_chunk | 10 | 8.920 | 1.907 | 145.016 | 56.962 | 0.245 | 0.401 | 29.87x |
| small / cross_chunk | 100 | 53.916 | 3.035 | 959.495 | 207.543 | 0.054 | 0.216 | 58.75x |
| medium / point | 1 | 1.581 | 1.378 | 26.940 | 29.419 | 0.895 | 1.014 | 19.82x |
| medium / point | 10 | 5.263 | 1.242 | 82.693 | 42.805 | 0.214 | 0.536 | 33.53x |
| medium / point | 100 | 25.711 | 1.611 | 651.792 | 191.911 | 0.063 | 0.265 | 119.13x |
| medium / small_roi | 1 | 1.624 | 1.476 | 29.870 | 35.206 | 0.909 | 1.040 | 21.05x |
| medium / small_roi | 10 | 4.472 | 1.557 | 94.989 | 49.107 | 0.344 | 0.509 | 31.04x |
| medium / small_roi | 100 | 25.347 | 2.072 | 708.644 | 214.260 | 0.082 | 0.299 | 106.30x |
| medium / cross_chunk | 1 | 2.173 | 2.170 | 33.025 | 34.307 | 0.993 | 1.019 | 16.54x |
| medium / cross_chunk | 10 | 7.938 | 2.228 | 94.093 | 49.793 | 0.281 | 0.516 | 22.05x |
| medium / cross_chunk | 100 | 55.516 | 3.110 | 660.652 | 202.625 | 0.057 | 0.308 | 64.48x |
| large / point | 1 | 1.340 | 1.284 | 28.501 | 29.310 | 0.967 | 1.028 | 23.01x |
| large / point | 10 | 3.922 | 1.342 | 90.131 | 39.717 | 0.346 | 0.432 | 29.58x |
| large / point | 100 | 19.279 | 1.512 | 679.329 | 159.838 | 0.078 | 0.235 | 107.37x |
| large / small_roi | 1 | 1.524 | 1.553 | 27.607 | 30.084 | 1.118 | 1.100 | 19.37x |
| large / small_roi | 10 | 4.405 | 1.535 | 88.037 | 49.064 | 0.346 | 0.554 | 31.96x |
| large / small_roi | 100 | 25.856 | 1.985 | 691.781 | 208.567 | 0.079 | 0.303 | 105.05x |
| large / cross_chunk | 1 | 2.131 | 1.975 | 38.118 | 37.018 | 0.974 | 0.971 | 18.74x |
| large / cross_chunk | 10 | 7.772 | 2.419 | 99.593 | 53.244 | 0.320 | 0.535 | 21.54x |
| large / cross_chunk | 100 | 54.563 | 2.992 | 679.925 | 206.084 | 0.055 | 0.296 | 67.24x |

## Publication and lifecycle

Both modes share the same one-time publication and exact source locator per campaign. Each row is a separate scenario: lifecycle = shared write + IoTDB source locator (FS: zero) + the selected Q query/session total. Do not sum scenario rows as one execution.

| Tier | Bytes | FS write | IoTDB publish | IoTDB source locator |
| --- | ---: | ---: | ---: | ---: |
| small | 6,728,832 | 25.794 | 453.382 | 85.248 |
| medium | 537,684,072 | 1554.218 | 7119.675 | 60.596 |
| large | 3,441,125,376 | 9678.641 | 43485.420 | 56.976 |

| Tier / case | Q | FS reopen lifecycle | FS bound lifecycle | IoTDB re-register lifecycle | IoTDB bound lifecycle | Bound lifecycle B/A |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| small / point | 1 | 26.967 | 26.906 | 575.953 | 577.212 | 21.45x |
| small / point | 10 | 29.440 | 26.998 | 641.891 | 586.915 | 21.74x |
| small / point | 100 | 44.564 | 27.321 | 1186.010 | 710.393 | 26.00x |
| small / small_roi | 1 | 27.259 | 27.592 | 576.227 | 578.633 | 20.97x |
| small / small_roi | 10 | 29.976 | 27.208 | 629.332 | 598.832 | 22.01x |
| small / small_roi | 100 | 50.131 | 27.703 | 1184.034 | 732.139 | 26.43x |
| small / cross_chunk | 1 | 28.080 | 27.824 | 597.859 | 586.690 | 21.09x |
| small / cross_chunk | 10 | 35.488 | 27.665 | 693.804 | 606.895 | 21.94x |
| small / cross_chunk | 100 | 80.485 | 28.829 | 1542.202 | 778.769 | 27.01x |
| medium / point | 1 | 1555.541 | 1555.596 | 7232.497 | 7209.690 | 4.64x |
| medium / point | 10 | 1558.119 | 1555.680 | 7300.562 | 7217.072 | 4.65x |
| medium / point | 100 | 1573.095 | 1555.664 | 7888.817 | 7372.182 | 4.73x |
| medium / small_roi | 1 | 1555.843 | 1555.694 | 7215.908 | 7215.478 | 4.65x |
| medium / small_roi | 10 | 1558.546 | 1555.801 | 7275.260 | 7229.710 | 4.65x |
| medium / small_roi | 100 | 1578.814 | 1556.531 | 7915.955 | 7400.534 | 4.76x |
| medium / cross_chunk | 1 | 1556.629 | 1556.389 | 7213.296 | 7213.930 | 4.65x |
| medium / cross_chunk | 10 | 1562.372 | 1556.476 | 7273.745 | 7231.451 | 4.65x |
| medium / cross_chunk | 100 | 1609.734 | 1557.566 | 7840.923 | 7371.230 | 4.75x |
| large / point | 1 | 9679.802 | 9679.837 | 43566.272 | 43571.033 | 4.42x |
| large / point | 10 | 9682.261 | 9679.894 | 43628.816 | 43576.397 | 4.42x |
| large / point | 100 | 9698.335 | 9680.250 | 44245.989 | 43670.199 | 4.43x |
| large / small_roi | 1 | 9680.031 | 9680.194 | 43564.287 | 43569.276 | 4.42x |
| large / small_roi | 10 | 9682.878 | 9680.099 | 43624.716 | 43585.459 | 4.42x |
| large / small_roi | 100 | 9703.672 | 9680.626 | 44228.461 | 43745.977 | 4.43x |
| large / cross_chunk | 1 | 9680.668 | 9680.616 | 43574.798 | 43573.682 | 4.42x |
| large / cross_chunk | 10 | 9686.413 | 9680.790 | 43636.272 | 43589.924 | 4.42x |
| large / cross_chunk | 100 | 9733.204 | 9681.586 | 44216.605 | 43750.819 | 4.43x |

## Q=100 bound setup and warm diagnostics

Inclusive server stages overlap; do not add them. First reads may perform real payload SQL/decode; all warm reads are checked for zero SQL/payload reads/decode, and every read for zero catalog and decoder compilation plus prepared reuse. Bind-time SQL is retained in the setup cost.

| Tier / case | Bind | Release | Warm server | Warm IPC encode | Warm SQL | Warm catalog calls | Warm prepare calls | Warm payload calls | Warm decode count |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small / point | 19.146 | 0.467 | 0.275 | 0.069 | 0 | 0 | 0 | 0 | 0 |
| small / small_roi | 20.859 | 0.462 | 0.329 | 0.095 | 0 | 0 | 0 | 0 | 0 |
| small / cross_chunk | 21.530 | 0.493 | 0.362 | 0.081 | 0 | 0 | 0 | 0 | 0 |
| medium / point | 21.040 | 0.696 | 0.291 | 0.078 | 0 | 0 | 0 | 0 | 0 |
| medium / small_roi | 21.028 | 0.663 | 0.387 | 0.101 | 0 | 0 | 0 | 0 | 0 |
| medium / cross_chunk | 20.265 | 0.514 | 0.317 | 0.075 | 0 | 0 | 0 | 0 | 0 |
| large / point | 20.962 | 0.466 | 0.250 | 0.066 | 0 | 0 | 0 | 0 | 0 |
| large / small_roi | 20.264 | 0.867 | 0.448 | 0.129 | 0 | 0 | 0 | 0 | 0 |
| large / cross_chunk | 21.114 | 0.435 | 0.356 | 0.078 | 0 | 0 | 0 | 0 | 0 |

## Scope and operational limits

Private root-only 0600 socket; explicit experimental flag only on bound daemons; immutable query and source/permission assumptions; maximum five-minute TTL; not a production authorization cache.

FS uses native HDF5 1.14.4 with its recorded native adaptive metadata/raw chunk cache and no external index. Bound mode retains H5F/H5D handles; each read still selects a slab, creates a memory space, runs H5Dread and makes an owned copy. IoTDB-FS uses the same HDF5 core, shares verified layout/decoded fragments and freezes one exact source/version/plan; every read constructs a cursor and publishes a new bounded IPC stream, then mmap/read/copy/close through FUSE. All mappings close before session release.

The source namespace must remain read-only during fixed sessions. This is an explicitly gated experimental upper bound on amortization, not a production cache of changing permissions or latest-version metadata. OS and database caches are not reset. Daemons are private and sequential; existing mounts are untouched. Native process startup/hash/JSON output, daemon startup, generation and correctness hashes are excluded from query timers. Alternating campaign arm/mode order is recorded; no significance test or cold-disk claim is made.

Both arms fsync the file and close. Parent setup and daemon startup are excluded. Backend WAL policy is unchanged; equivalent power-loss durability is not claimed.

Machine-readable aggregates are in `summary.json`; complete observations are in `campaign.json` or `campaign.json.gz`. Raw logs, copies and settings remain under the ignored server `work/` directory. Controlled synthetic data establishes overhead for this fixed layout, not scientific indexing benefits.
