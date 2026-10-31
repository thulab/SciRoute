# Single-file preparation acceptance — 2026-10-12

Preparation is complete on `root@192.168.99.49`. This is **not** the formal
Q=1/10/100 performance campaign. The small-file smoke validates publication,
exact-version binding, registration, IPC, FUSE mmap/read, values and masks.

The server work root is
`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/single_file/work/prep-20261012`.
Large inputs, build products and private logs remain there. Accepted evidence
is mirrored in this directory; copied manifest paths refer to the server work
root, not to a second local copy of the data.

## Controlled input acceptance

All three files contain `/T`, little-endian Float32, shape
`[time,1,728,1152]`, chunks `[1,1,91,144]`, shuffle followed by gzip level 1,
and no Fletcher32. Values are deterministic finite random bit patterns. These
are controlled synthetic inputs, not public GEOS products. Every chunk is
allocated; actual payload storage and closed-file size gates passed.

| Tier | Time extent | Actual file bytes | Allocated chunks | Size gate |
| --- | ---: | ---: | ---: | --- |
| Small | 2 | 6,728,832 (6.42 MiB) | 128 | <=10 MiB: pass |
| Medium | 160 | 537,684,072 (512.78 MiB) | 10,240 | 0.3–0.7 GiB: pass |
| Large | 1,024 | 3,441,125,376 (3.20 GiB) | 65,536 | >=2 GiB: pass |

[data-manifest.json](https://github.com/SpriCoder/IoTDB-FS-C/blob/6e637945e13e107709aba733eb00ded350629750/experiments/single_file/results/preparation-20261012/data-manifest.json) records source SHA-256, shape,
storage inspection, generation/fsync/hash times and three common subset
oracles. Generation and whole-file hashing are offline preparation; hashing
warms OS pages. No claim of cold disk I/O is made.

## Environment and regression acceptance

- Production CMake, dependency lock, source-driver guard, parser version and
  current packaging/test entry points require **HDF5 1.14.4 EXACT**.
- Daemon, bounded helper and native FS probe resolve the **same core library
  path and SHA-256**. It is serial, thread-safe, core-only, DEFLATE-enabled;
  dynamic plugins remain disabled. Original mount binaries were not replaced.
- Offline h5py 3.12.1 uses its separate wheel HDF5 1.14.4. It is an input/oracle
  tool, not the timed native FS reader. NumPy is 2.1.3; PyArrow is 21.0.0.
- Python wheel dependencies are isolated from the C++ `LD_LIBRARY_PATH`.
  [environment.json](https://github.com/SpriCoder/IoTDB-FS-C/blob/6e637945e13e107709aba733eb00ded350629750/experiments/single_file/results/preparation-20261012/environment.json) records both profiles and library hashes.
- Linux C++ unit suite: **1,444 tests, zero failures/errors**; see
  [linux-unit-tests.json](https://github.com/SpriCoder/IoTDB-FS-C/blob/6e637945e13e107709aba733eb00ded350629750/experiments/single_file/results/preparation-20261012/linux-unit-tests.json). Experiment regression suite:
  **19/19 pass**, including wrong values/masks/descriptors, wrong full shape,
  short frames/writes, size-gate rejection and mount cleanup; see
  [experiment-tests.txt](https://github.com/SpriCoder/IoTDB-FS-C/blob/6e637945e13e107709aba733eb00ded350629750/experiments/single_file/results/preparation-20261012/experiment-tests.txt).
- Existing GEOS source `/geos-pilot-0e242137a15e/file1.nc4`, version
  `1791355040891`, was queried without re-ingestion by the new 1.14.4 build.
  The physical point `[0,10,360,576]` agrees with the independent oracle:
  `284.5534362792969`; see [existing-version.json](https://github.com/SpriCoder/IoTDB-FS-C/blob/6e637945e13e107709aba733eb00ded350629750/experiments/single_file/results/preparation-20261012/existing-version.json).

The initial official-archive download timed out; preparation recovered using
the independently SHA-verified official archive. A subsequent PyArrow import
failed because the C++ Arrow prefix shadowed its wheel; acceptance occurred
only after Python/C++ environment isolation was fixed. Initial snapshot and
follow-up script revisions are recorded in [preparation.json](https://github.com/SpriCoder/IoTDB-FS-C/blob/6e637945e13e107709aba733eb00ded350629750/experiments/single_file/results/preparation-20261012/preparation.json),
[source-hashes.json](https://github.com/SpriCoder/IoTDB-FS-C/blob/6e637945e13e107709aba733eb00ded350629750/experiments/single_file/results/preparation-20261012/source-hashes.json) and
[python-library-isolation.json](https://github.com/SpriCoder/IoTDB-FS-C/blob/6e637945e13e107709aba733eb00ded350629750/experiments/single_file/results/preparation-20261012/python-library-isolation.json).

## End-to-end smoke

One campaign, small tier only, two queries per case; profiles enabled. Each
case starts a fresh daemon/runtime and retains it for its second query. OS and
IoTDB server caches are not dropped. FS opens/reads/copies/closes with native
HDF5 and has no external index. Each IoTDB query registers/materializes a new
result and consumes/copies its IPC through FUSE.

| Case | FS first / second (ms) | IoTDB-FS first / second (ms) | Two-query median B/A |
| --- | ---: | ---: | ---: |
| Point | 2.343 / 0.655 | 42.393 / 8.955 | 17.13x |
| Small ROI | 1.459 / 0.738 | 34.608 / 8.821 | 19.77x |
| Cross-chunk ROI | 2.177 / 1.071 | 106.599 / 13.288 | 36.91x |

Correctness passes; none of these smoke median ratios meets the prespecified
<10x target. Two observations per case do not establish a formal p50/p95 or a
scale conclusion. These results are retained rather than discarded.

Small-file write+file-fsync+close: FS **28.011 ms**, IoTDB-FS **406.705 ms**.
One-time exact-version binding costs **56.043 ms**. Both arms' complete file
SHA-256 matches the source. [smoke-campaign.json](https://github.com/SpriCoder/IoTDB-FS-C/blob/6e637945e13e107709aba733eb00ded350629750/experiments/single_file/results/preparation-20261012/smoke-campaign.json) contains
raw timings, nested server profiles and actual Q=1/2 lifecycle totals. Directory
setup is outside write timing; backend WAL/fsync policy is unchanged, so equal
power-loss durability is not claimed. Nested stages are not added to wall time.

All owned mounts were closed; the three pre-existing daemon PIDs remained
alive. Only a new experiment prefix in `geostime20261007` was published.
Medium/large inputs were validated but **not yet published or benchmarked**.
The runner is ready for the formal three-tier Q=1/10/100 campaign described in
[the experiment README](../../README.md).
