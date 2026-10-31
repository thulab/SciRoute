# FS-index

FS-index is the standalone scientific-file index component of IoTDB-FS-C. It
provides persistent SQLite discovery over GRIB/HDF5 metadata and native reads
into client-owned Float32 arrays and uint8 validity masks. It requires no running
IoTDB service, FUSE mount, or experiment workspace.

The retained v1 interface preserves the accepted FM1 profile: six attribute
equalities, full-history version selection, and
complete 4,096-element selected units. HDF5 values must be finite Float32; GRIB
uses ecCodes bitmap/missing-value handling. General HDF5 scientific-role mapping,
Arrow adaptation and arbitrary-sized output are not implemented by this component.
The separate v2 maintenance interface adds incremental publication and reader
refresh for this same bounded profile; its implementation scope is specified in the
[incremental-maintenance plan](../docs/deliverables/fs-index-incremental-maintenance-plan.md).

## Build and verify

Use Linux/WSL2, C++17, thread-safe HDF5 **1.14.4**, ecCodes, SQLite, and Boost
headers, plus OpenSSL Crypto for publication identities. Tests additionally require
Python >=3.10, NumPy, and h5py. Use the same
native library installation as the retained query profile; do not substitute
another HDF5 release.

From the repository root (replace the HDF5 and Python paths):

```sh
cmake -S fs_index -B build/fs-index \
  -DCMAKE_BUILD_TYPE=Release \
  -DHDF5_DIR=/path/to/hdf5-1.14.4/cmake \
  -DFS_INDEX_BUILD_TESTS=ON \
  -DPython3_EXECUTABLE=/path/to/venv/bin/python
cmake --build build/fs-index --target fs_index_native --parallel 2
ctest --test-dir build/fs-index --output-on-failure
```

The output is `build/fs-index/libfs_index_native.so`. Optional integration into
an otherwise fully configured IoTDB-FS root build uses
`-DIOTDBFS_BUILD_FS_INDEX=ON`; the standalone command above avoids all
IoTDB/FUSE/Arrow dependencies. CTest sets the native-library path explicitly and
runs the original GRIB/HDF5 functional tests plus maintenance/recovery gates. Missing test dependencies
fail configuration, rather than producing a passing skipped suite.

Test builds contain explicitly gated process-kill hooks; `status()` exposes
`test_faults_enabled`. Use `-DFS_INDEX_BUILD_TESTS=OFF` in a separate Release build
for formal measurements. Keep source/binary/dependency identities with each run.

## Incremental publication (v2)

```python
from fs_index.maintenance import Writer, Reader

library = "/absolute/libfs_index_native.so"
root = "/absolute/new-store"  # parent exists; root must not exist on create
with Writer(library, root, create=True, indexed=True) as writer:
    with Reader(library, root) as reader:
        receipt = writer.publish(
            "/absolute/input.h5", logical_path="/weather/field.h5", format="hdf5",
            version=1, ordinal=1, expected_version=0, op_id="publish-weather-v1")
        result = reader.select("index", {"kind": "latest"},
            {"shortName": "t", "typeOfLevel": "isobaricInhPa", "level": 500,
             "step": 0, "validityDate": 20261021, "validityTime": 0},
            ["/weather/field.h5"])
        for locator in result["units"]:
            values, validity = reader.read(locator)
        completion = writer.checkpoint()
        assert not completion["busy"]
```

Reopen with `Writer(library, root)` / `Reader(library, root)`; no external catalog
JSON is needed. A retained reader discovers new sources at each `select()`.
Latest chooses the latest published version **before** scientific predicates.
Old locators remain readable while immutable source files remain present.
Handles are serial; use independent handles/processes for publication and queries.

Use `indexed=False` only on creation for the native FS comparison: it persists
the same version/source publication directory but no unit metadata. Query it with
`select("scan", ...)`. An indexed store also supports `scan` for equivalence checks.
The index mode of a store cannot change on reopen.

Publishing copies the source, hashes it, syncs the file and parent directory, then
atomically commits sources/units/latest/head/operation. Indexed publication also
checks every unit's supported shape, dtype and values. Native source-only stores
defer substructure validation to discovery/read. Use identical supported inputs
when comparing costs. HDF5 external/soft links, virtual datasets and external raw
storage are outside the immutable file profile and are rejected on the applicable
validation/read path.

Each logical path has increasing version and ordinal; `expected_version` is a
compare-and-set precondition (0 for a new path). Repeating an `op_id` with identical
content and semantic parameters returns the original `result`; a conflicting
request fails. Retry requires the stable input to remain available for hashing.
The `metrics` envelope describes the current attempt, not a replay of old timings.

`status()` inventories catalog/space/counters and belongs outside query clocks.
Measure initialization, publication, complete owned-output queries, checkpoint and
close with outer wall clocks. Explicitly account for checkpoint `busy`/remaining
WAL. `observed_wal_peak_bytes` is a per-handle observation, not an exact whole-run
peak; collect independent resource samples for lifecycle experiments.

There is no GC, deletion, multi-writer or distributed protocol. The directory is
application-owned and must not be modified externally. Read-only file mode and
size/mtime checks are not cryptographic protection from an external writer.
Atomic visibility applies to catalog clients, not arbitrary directory enumeration.
See the [v2 contract](../docs/contracts/fs-index-maintenance-v2.md) for recovery,
durability and source-only-path boundaries.

## Frozen v1 Python API and input

Import from the repository root (or place it on `PYTHONPATH`):

```python
from fs_index.native import NativeContext

config = {
    "catalog_path": "/absolute/catalog.json",
    "index_path": "/absolute/new-index.sqlite",
    "build_index": True,
}
with NativeContext("/absolute/libfs_index_native.so", config) as index:
    found = index.select(
        "index", {"kind": "latest"},
        {"shortName": "t", "typeOfLevel": "isobaricInhPa", "level": 500,
         "step": 0, "validityDate": 20261021, "validityTime": 0},
        ["/weather/field.grib"],
    )
    for unit in found["units"]:
        values, validity = index.read(unit)
```

The catalog is UTF-8 JSON with `protocol: "iotdbfs-fixed-native/1"` and a `sources`
array. Each source provides `path` (absolute native file), `logical_path`,
`format` (`grib`/`hdf5`), positive `version` and `ordinal`, `size`, and `mtime_ns`.
Sources must be retained unchanged. Logical-path/version and logical-path/ordinal
pairs must be unique. Metadata is extracted from source bytes, not supplied as
trusted query answers in this catalog. HDF5 datasets must carry the six scalar
attributes named in the query example.

`build_index=True` creates a **new** SQLite file; it does not append to or replace
an existing index. Reopen with `build_index=False`. The index binds the complete
catalog byte sequence, including its exact serialization. Contexts retain a
fixed source/version domain. Use immutable native files for their lifetime;
size/mtime checks detect ordinary changes but are not adversarial integrity or
transactional file-publication guarantees.

`select(mode, selection, predicate, logical_scope)` accepts `scan` and `index`.
Selection is `latest`, `all`, `ordinal` with one explicit `ordinals` entry, or
`between` with the exact explicit ordinals (not numeric interval endpoints).
Scope must be sorted, unique, and nonempty. Latest selects the newest registered
version of each path before applying the scientific predicate. Every selected
unit has an exact source association and is read in full.

`explain("index", ...)` returns the actual SQLite plan and bound predicate;
`stats()` reports native operations, versions, cache limits, and build accounting.
Run these outside performance clocks. `read()` includes client allocation and
returns owned arrays; discovery JSON contains no numerical payload. Each context
is serial and must not be called concurrently. Scan and index performance arms
use independent contexts.

## Layout, cache policy, and provenance

- `src/native.cpp`: catalog construction, selection, source checks, native reads.
- `native.py`: Python/ctypes boundary and owned-array allocation.
- `src/maintenance.inc`, `maintenance.py`: v2 publication, snapshot refresh and recovery.
- `include/fs_index.h`: public maintenance C ABI and shared allocation/error boundary.
- `tests/test_native.py`: real GRIB/HDF5 fixtures and rejection checks.
- `tests/test_maintenance.py`: incremental, crash recovery and concurrent-process gates.
- `CMakeLists.txt`: standalone build and CTest registration.

At the migration checkpoint, the C++ source and Python boundary were byte-identical to the pre-migration
`experiments/fixed_matrix/native/fixed_native.cpp` and
`experiments/fixed_matrix/native.py`. Maintenance now adds one included implementation
in the same translation unit to share inventory/selection/read functions; the
original Python boundary remains unchanged. C ABI names `fixed_native_*`, protocol
`iotdbfs-fixed-native/1`, cache limits, and numerical policies stay unchanged.
The historical names identify the retained profile; there is no duplicate active
implementation or legacy import shim. Importing the old Python path is no longer
supported. Current experiment callers import this component.

Each context retains at most eight files/eight datasets; HDF5 caches are 2 MiB
metadata per file and 256 KiB raw chunks per dataset. No decoded GRIB result or
final answer is cached. This migration does not change those limits or remove
the 4,096-element read bound.

Original experiment binaries, manifests, and accepted hashes remain frozen.
Source gates map the former Python path to `fs_index/native.py` while comparing
the **same** recorded digest. A newly built library has its own identity and
must receive a new run/build receipt; it cannot be relabeled as the old accepted
binary. Strictly pinned historical controller snapshots should run from their
archived source trees. The source migration is not a new performance result.

## Immutable-file registration profile

`lifecycle.py` provides a separate Python/SQLite profile for native point-query
lifecycles whose GRIB grids or HDF5 datasets exceed the fixed-matrix 4,096-value
bound. It leaves the v1/v2 native ABI and its tests unchanged. A caller supplies
the pinned native reader and an already copied, file-fsynced immutable file:

```python
from fs_index.lifecycle import Catalog

with Catalog("/private/run/index.sqlite", create=True) as catalog:
    identity = catalog.register(spec, reader, sha256=source_sha256, version=1)
    query = dict(spec, logical_path=identity["logical_path"], version=1)
    observation, owned = catalog.read(reader, query)
```

Registration inventories every GRIB message's scientific predicate, reference
and validity times, and native byte locator; it is independent of the query's
selected message. HDF5 registration describes the explicitly declared dataset
(the lifecycle workload uses `/T`); it does not infer scientific axis meanings
or inventory undeclared datasets. SQLite indexes the metadata and source/version
binding and commits each newly registered file with WAL and `synchronous=FULL`.
Existing registered sources are not rescanned. A query reads the persisted
catalog and opens fresh native handles, retaining no encoded or decoded payload.

For repeated HDF5 requests, `hdf5_handles.Hdf5HandleCache` optionally retains a
bounded number of native file/dataset sessions (default eight):

```python
from fs_index.hdf5_handles import Hdf5HandleCache

with Catalog("/private/run/index.sqlite") as catalog:
    with Hdf5HandleCache(catalog, reader, capacity=8) as handles:
        observation, owned = handles.read(query)
```

The injected HDF5 reader supplies `open_session`, `close_session`, and
`read(spec, session=...)`. Every request still performs catalog selection and
the immutable-source identity check. Cache keys include the exact version and
dataset; every request calls the native value reader into a fresh output.
HDF5's own chunk cache remains associated with each retained dataset handle.
The wrapper adds no payload or result cache and is single-threaded. Opening a
missing handle is charged to that request; eviction and final close release the
native resources. Keep the catalog open until the handle cache has closed.

This profile assumes a single publisher and immutable files, supports exact
publication versions, and rejects changed stat identities. It neither copies
nor publishes source bytes, resolves latest versions, handles deletions, nor
claims cross-process file/catalog publication atomicity. The supplied SHA-256
is an asserted source identity: the lifecycle caller verifies copied bytes
after the timed region. File and parent-directory durability remain the caller's
responsibility. Catalog creation and checkpoint clocks must be reported with
their actual timing boundary; registration metrics do not include constructor
work. `stats()` reports database/WAL space and `checkpoint()` yields retained
space after a FULL-durable WAL commit is consolidated.

`python3 -m unittest fs_index.tests.test_lifecycle -v` checks catalog persistence,
incremental insertion, exact-version binding and mutation rejection using a
metadata backend fixture. Real native-format output equality and performance
must additionally be verified by the lifecycle harness.

`python3 -m unittest fs_index.tests.test_hdf5_handles -v` checks fresh reads,
exact-version isolation, bounded eviction, replacement rejection, and exception
cleanup. The resident HDF5 follow-up is implemented in
`experiments/resident_table5/cache_run.py`; its separate export audits source,
oracle, native-read, clock, cache-lifetime, and resource-restoration receipts.
