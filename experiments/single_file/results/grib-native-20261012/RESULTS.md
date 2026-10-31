# Native ecCodes reader acceptance — 2026-10-12

The native GRIB filesystem reader is implemented and deployed. Both the
workstation (ecCodes 2.46.0) and server (2.34.1) passed **898 actual queries
across 30 waves**, plus **seven expected failure cases**, on the final source.
This accepts the reader; it is not a paired FS/IoTDB-FS performance campaign.

## Accepted behavior

| Case | Selection/output | Actual Q per mode |
| --- | --- | --- |
| Encoded sparse retrieval | One level-500 message, 66,606 bytes | 1, 10, 100 |
| No matching message | Zero rows/bytes | 1, 10, 100 |
| All encoded messages | 22 messages, full source byte SHA | 1 |
| Numerical point | One Float32 value, Float64 latitude/longitude | 1, 10, 100 |
| Numerical 4×4 ROI | 16 values and coordinates | 1, 10, 100 |
| Multiple numerical messages | 22 messages ×16 cells | 2 |
| Empty spatial intersection | Zero cells in the selected message | 2 |

Every case runs in both reopen/discover/read/close mode and the user-approved
bind-once mode. Q sessions are actual separate executions, with their own
binding and release. The 898 count is per machine, not per paired system arm.

An independent `grib_ls -x` message directory supplies offsets, lengths and
predicate labels. Raw source ranges supply binary SHA; a separate native
ecCodes numerical oracle supplies coordinates, Float32 values and physical
validity. Every per-message and concatenated hash, order, row count and shape
was checked. The input is the same 2,446,828-byte, 22-message GFS file accepted
by the earlier typed retrieval pilot, raw SHA
`a13c0e8698ef7897c4b32047ff5f2aea8221280b2e4d244e70d3b0499871cec1`.

Failures covered truncated source, NaN bounds, reversed bounds, output-byte
exhaustion, working-byte exhaustion, candidate exhaustion, and accumulation of
multiple bound fields beyond a budget sufficient for one field. Each failed
with nonzero exit status and no successful query record.

The standalone CMake build passed with `-Wall -Wextra -Werror`. It resolves
installed ecCodes headers/library directly after checking pkg-config version,
avoiding the server package's invalid advertised include prefix without
modifying that package. The server probe and current experiment daemon resolve
the **same library path and SHA**, recorded in `environment.json`.
The combined native build also passed: both readers were generated and the
existing HDF5 probe still reported thread-safe 1.14.4 with plugins disabled.

## Timing and cache contract

- Ordinary queries include fopen/fstat, direct native headers-only scan,
  equality selection, selected payload reads, full native decoding for values,
  fresh crop/conversion, owned output copy, cleanup and close. No sidecar or
  persistent FS index is constructed. Numeric filters precede costly concept
  and string resolution. Discovery does not duplicate indicator/footer reads.
- Bound setup only scans metadata and retains the FD and exact message
  ranges. First reads populate bounded encoded-byte/decoded-field caches;
  later reads make zero explicit payload reads and zero decode calls. Every
  read still crops and copies independent output; final query outputs are not
  reused. Cache hits/misses, encoded/decoded resident bytes and budgets are
  recorded. This is an explicit experiment-reader cache, not a claim about
  ecCodes's native cache. It is a stronger binary baseline than typed GRIB's
  direct per-execution range reads.
- Per-query stage durations plus `other_ns` equal `total_ns`. Bind cost and
  its open/discovery/other breakdown are nonzero only on record zero; release
  is charged only to the actual final record. Complete session cost is
  `sum(session_bind_ns) + sum(total_ns) + sum(session_close_ns)`.
- Hashing, JSON, process startup and initial library setup are outside query
  timers. The harness records complete process wall time separately. Its
  offline oracles read source bytes and warm OS pages, so these runs do not
  establish cold-disk latency.

The working budget constrains reader buffers/cache and estimated native
message copies. It is not an allocator quota on ecCodes internals or unpacking
workspace. Physical validity is all ones as in the current typed GRIB value
contract; bitmap sentinel values are not automatically masked or transformed.
Numerical views require a single-field regular_ll grid with ordinary row order,
increasing longitude and non-wrapping native-coordinate bounds. Other grids
and coordinate transformations are outside this reader's accepted scope.

## Reproduction and artifacts

Server root: `/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/single_file`.

```bash
cmake -S native/grib -B work/grib-native-20261012/build \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS='-Wall -Wextra -Werror'
cmake --build work/grib-native-20261012/build -j 4
env -u LD_LIBRARY_PATH -u PYTHONPATH work/session-20261012/venv/bin/python \
  grib_native_validation.py \
  --probe "$PWD/work/grib-native-20261012/build/grib_native_probe" \
  --raw /data_01/zzy/iotdb-fs-exp/datasets/grib/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 \
  --bound --output "$PWD/work/grib-native-reproduction"
```

Choose a new output directory. `grib_native_environment.py` verifies the
accepted probe against the daemon's existing environment lock and records
compiler flags, exact library identity, executable SHA and source hashes.
Accepted raw observations and oracle checks are retained in
`acceptance.json.gz`; the environment/source lock is `environment.json` and
artifact checksums are `run-manifest.json`. Builds, source files and failed
attempts remain under `work/` on the server. No mount, database or published
source was modified, and no production query code changed.
