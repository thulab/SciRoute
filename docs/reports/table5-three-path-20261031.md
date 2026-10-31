# Table 5 three-path lifecycle remeasurement

Status: formal-001 complete. All 135 lifecycles and 4,995 queries passed, followed
by independent statistical/source/resource and restoration audits. Elapsed
supervisor time was 7,149.458s. The original Table 5 archive and the separate
M1 changed-version campaign remain unchanged.

## Scope and qualification

Five frozen original inputs: GRIB with 22/4,598 messages; HDF5 shape
`[t,1,728,1152]`, t=2/160/1024. Same point values/masks and applicable GRIB
coordinates, Q=1/10/100, three repetitions, three freshly measured arms:
135 lifecycles and 4,995 queries. This is incremental new-file registration
into an initialized catalog; M1 separately measures changed-file updates.

`fs_index/lifecycle.py` stores every GRIB message header and native offset,
or the declared HDF5 dataset descriptor, with immutable source/version identity
in SQLite. Each registration processes one new file and commits under FULL WAL
synchronization. Each query selects persisted locators and opens fresh native
handles. Six catalog contract tests and all five actual source/oracle checks
passed, including close/reopen. Existing fixed-grid v1/v2 implementations are
unchanged. The large-file profile does not implement Latest resolution,
multi-writer publication or permission management.

The actual 2-core/2-GiB pilot (`pilot-003`) passed all 15 copy/query lifecycles,
full source/destination hashes and numeric oracles. Supervisor restored the
DataNode after 945.030 seconds. Maximum owned point output is 21 B (GRIB) and
5 B (HDF5); Q=100 retains at most 2,100 B of scientific output, excluding
small Python bookkeeping. Managed route count per formal block is 555, below
the configured 4,096 capacity. Formal admission reserves 279,389,695,082 B,
against 397,985,923,072 B observed free. The runtime projection with margin
is 11,248.46 seconds, below the 12,600-second worker deadline.

## Environment continuity and isolation

The original host had 377.695 GiB RAM; the current guest has 188.695 GiB.
Both have 32 virtual CPUs. Original DataNode heap/direct limits were
15,232/2,176 MiB; the current service uses 28,672/4,096 MiB. Both campaigns
use SYNC WAL and `enable_partial_insert=false`. In the old archives,
`service-report.json` describes restored ASYNC/true settings; the experiment
configuration is proved by `control/operations.jsonl` and live JVM flags.

An owned independent database deployment matched the old memory allocation,
but its existing license rejected schema/write operations after authentication.
It was stopped; no license was changed or bypassed. `pilot-001` and `pilot-002`
failed before timed cells because of the old admin tool's fixed port and the
custom release's configured authentication. These are qualification failures,
not performance observations. Failed deployment and logs remain private.

The accepted run uses the existing licensed DataNode, PID 281880, with a new
experiment database, independent owner namespaces and directories. No shared
service is restarted or reconfigured. The supervisor temporarily places client,
native readers, FUSE and DataNode in one two-core parent (CPUs 2–3); query
memory is 2 GiB, swap disabled; database memory is separately allocated.
DataNode affinity/cgroup state is saved for exact restoration. Live PID/RPC,
JVM flags, release JAR hashes and storage mounts are captured: source copies,
database data and WAL all use `/dev/sdc1` at `/data_01`. Other historical table
values are not combined with the new measurements.

## Local Claude Opus review

Actual model: `claude-opus-5-5`, independently reviewing the design and code
without executing experiments. Review output is retained in the ignored
`build/table5-three-path-20261031/opus-review.json`.

The substantive carryover concern was addressed before formal collection:
each arm begins and ends with scoped `FLUSH <experiment database>` and three
consecutive two-second windows below 0.10 DataNode CPU core and 1 MiB writes
per window. Timeout is 180 seconds. The standalone gate passed in 6.073 s.
This time is recorded outside copy-to-result latency and limits immediate
carryover into a following FS arm. Later background work remains observable
in the periodic resource samples; the gate does not establish causal isolation.

Other review dispositions:

- Live environment evidence supplements the shared connection binding;
  authentication and private configurations are never exported.
- Database initialization, managed startup, empty catalog setup and post-clock
  directory synchronization are recorded separately.
- `_profile` already attaches every typed execution profile to its query
  observation; the two source-visibility profiles are retained additionally.
- Capacity, runtime, Q=100 output size and route bounds are admission checks.
  The existing session loop and full matrix key-set assertions cover all
  three declared Q schedules; pilot timings are excluded.
- This table measures copy-to-result acknowledgment on exact immutable source
  point queries. It does not claim equal memory allocations by process role,
  identical management functionality, cold-cache behavior, power-loss recovery,
  or total CPU/storage work after the foreground clock.

## Frozen artifacts

Server root:
`/data_01/zzy/iotdb-fs-exp/table5-three-path-20261031/`.

- `source-formal-002/`: executed frozen Python source.
- `formal-001/`: completed three-path matrix and external restoring supervisor.
- `pilot-003/`: completed qualification, excluded from means.
- `adapter-gate-001/`: real-format/oracle and catalog reopening qualification.
- `shared-binding-001/deployment.json`: public original service identity.
- `shared-binding-001/connection.private.json`: private; never export.
- `db-001/`: stopped independent deployment; private logs/configuration retained.

`experiments/lifecycle_table5/export.py` independently validates unique matrix
keys, all output checks, mode/resource constraints, index registration counts,
catalog reopening and restored supervisor status. It recomputes arithmetic
means, medians, ranges and all three raw values from lifecycle clocks.

## Completed results and audit

| Source | Q=1: scan / index / managed (s) | Q=100: scan / index / managed (s) |
| --- | --- | --- |
| G-M22 | 0.050 / 0.055 / 2.270 | 1.726 / 0.997 / 2.365 |
| G-M4598 | 3.066 / 4.369 / 57.073 | 153.113 / 5.376 / 56.227 |
| H-T2 | 0.033 / 0.040 / 2.780 | 0.073 / 0.074 / 2.888 |
| H-T160 | 1.522 / 1.552 / 58.467 | 1.542 / 1.582 / 58.802 |
| H-T1024 | 9.072 / 8.428 / 350.967 | 9.374 / 8.168 / 351.279 |

All values are arithmetic means of three directly timed walls. Q=10, phase
times, ranges and raw values are preserved in `export-001/`. The independent
export verified 1,421 resource snapshots and 249 frozen source files. All
18 quiescence receipts passed (6.042–139.210s); this does not exclude later
background work. Three reopened catalogs each have 15 sources, 13,869 units
and a checkpointed 4,489,216B database with WAL=0 (SHM separately reported).

`formal-001/independent-restoration.json` confirms the original PID/start identity,
all 497 live thread affinities, original cgroup and saved parent/slice controls,
unchanged config/JVM memory/product/admin binaries, and no owned cgroup or
mount residue. The shared DataNode was not restarted. Namespaced data remains
for reproduction. The audit compares only controls actually saved before the run.

Public evidence: `formal-001-public.tar.gz`, SHA-256
`38713f21aa1db30b2cdbf073bafda8c548d676435d718b3caa73ff14646cf66c`
(4,839,970B; 69 archive members). The local archive and every selected
member hash were verified after download. It excludes private connection/config
files and logs. Final auditing scripts are in `postprocess-001/`, outside the
unchanged measured source tree.

The paper export is `VLDB-2027-IoTDB-FS/experiments/lifecycle-three-path-20261031/`.
The revised positioning recommends FS-index for static/infrequently updated
collections and database-backed management when large collections require
coordinated publication, versioning and access policies. This is a deployment
recommendation conditioned on management requirements, not a measured size
threshold or a claim that the managed path is always faster.
