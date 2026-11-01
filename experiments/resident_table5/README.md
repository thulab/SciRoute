# Resident Table 5 protocol (2026-10-31)

## Material Passport
- Type: preregistered code experiment protocol; raw-data access.
- RQ: How much does a complete scientific query session cost once source versions
  and persistent query structures are available in their intended storage systems?
- Status: protocol frozen before collection; no results claimed by this document.

The three-path import campaign answered a different question: external file
acquisition plus queries. Its results remain archived and must not be treated as
incremental indexing overhead on existing managed data.

## Fixed measurement

Reuse five original immutable inputs and three independent archived source/index
namespaces. No payload copy, insertion, re-publication or index registration occurs.
FS-scan and FS-index read exactly the same existing immutable local files. FS-index
opens a cloned persisted catalog. IoTDB-FS reopens existing database namespaces,
with a private clone of each owner journal and initialize=false. Original and
cloned journal hashes must remain unchanged; publication logs must be empty.

Same query, exact content, oracle, masks, coordinates and owned output as before.
All arms: one first query and two warmups per source, recorded separately. Then
independent Q=1/10/100 sessions, not prefixes/subtractions. Rotate arm and Q order
across three rounds. OS/DB caches retained. Native handles reopen for each query;
FS-index holds no payload/value cache; managed cache remains 256 MiB. Repeated
predicates describe a retained-state workload, not cold or changing-predicate cost.

Time each session directly from query entry through final result and reader
release. Each managed query includes normal catalog discovery and typed execution.
Hash/oracle/profile processing follows the measured session. 135 measured sessions,
4,995 measured queries plus 135 first/warmup queries. Arithmetic means; archive
all three values, medians and ranges. No significance claim from three repetitions.

Same shared two-core parent covers client/native/FUSE/DataNode. Query memory 2 GiB,
swap zero; existing DB 28/4 GiB recorded separately, ConfigNode outside. Scoped
quiet checks before/after blocks; independent supervisor restores DataNode cgroup
and every thread affinity. One-hour hard worker deadline. No other service reset.

## Cost accounting boundary

This measures query-ready service cost, not free index construction. Source storage
and persisted indexes are explicit preconditions for every indexed arm. Existing
import and FS-index registration receipts remain available as separate evidence.
They are not mixed into this query table or relabeled as incremental DB maintenance.

A separate incremental-maintenance comparison requires two matched states in each
backend: durable source/version storage alone and the same storage plus scientific
indexing. Index publication, invalidation, synchronization and subsequent queries
must be charged from arrival of an already stored version. The current managed
implementation couples these phases to ingestion, so no independent marginal
maintenance estimate is claimed until that boundary is executable and validated.
Local, NFS and database placement stay explicit; this single-host run does not
measure redistribution, replication or a multi-node distributed workload.

## Qualification amendment before accepted collection

Attempt formal-001 reached 30 native sessions but the inherited 90-second daemon
startup limit expired while reopening an existing managed namespace. All partial
measurements are excluded; supervisor confirmed resource restoration. Accepted
collection uses a separate 900-second service recovery deadline (outside query
clocks), records reopen duration, requires an existing empty publication log,
and compares independently read database heads before/after each managed block.
Per-session kernel memory.stat, io.stat and CPU counters are retained as envelopes
including out-of-clock audit work, not query-exclusive CPU costs. No run is removed
merely for memory pressure; all such counters remain attached to descriptive results.
The bounded owned-output retention is at most 2,100 scientific bytes per Q=100
session; Python/IPC bookkeeping remains included in measured process memory.

The deployed product and loaded ecCodes/HDF5 binaries are SHA-pinned. FS-index in
this profile is the Python lifecycle adapter with injected ctypes native readers;
it does not load the separate C++ maintenance extension. Cache-policy differences
are execution-setting conditions; results do not isolate an index-only causal
speedup. First requests follow source verification/service reopening and are not
cold-cache measurements. Three rounds describe these states without inferential
tests. Query session latency is the measured outcome, not total resource cost.

Attempt formal-002 ended before any query because this host does not expose io.stat in the experiment cgroup. The optional telemetry now records that absence explicitly; it does not enable or change the host I/O controller. DataNode /proc I/O and the available cgroup CPU/memory counters remain recorded.

The accepted run also archives `publication-log-initialization.jsonl`: the product
opens its publication diagnostic sink lazily on mutation, so a separate readiness
observer exclusively creates an absent empty target before managed requests. It
never truncates a log. Export verifies these timestamps and paths together with
independent head equality and owner-journal hashes; file existence alone is not
used to infer absence of writes. The observer source is preserved with the run.

## HDF5 handle-cache follow-up

`cache_run.py` and `cache_supervise.py` implement the separately archived native-only
follow-up. FS-index may retain eight native HDF5 file/dataset sessions through
`fs_index.hdf5_handles.Hdf5HandleCache`; all requests still resolve the catalog
and perform a fresh native value read. HDF5 chunk-cache state survives with the
handles. The two reopening controls are measured in the same follow-up. Managed
execution remains the original `iotdb-indexed` campaign. See the
completed report (local archive: `docs/reports/fs-index-hdf5-handles-20261031.md`) for direct
clocks, first/close timing, source identities and restoration evidence.
