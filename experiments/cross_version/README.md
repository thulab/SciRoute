# Versioned-query support modules

Formal query workloads use the [fixed matrix](../fixed_matrix/README.md).
This directory retains the shared corpus, publication, resource, native-reader,
and source-validation helpers needed by those drivers and the maintenance /
stored-data experiments. It is not a list of current standalone benchmarks.

| Retained area | Purpose |
| --- | --- |
| `pilot/` | Immutable corpus construction and publication helpers |
| `version_directory/` | Shared head worker, startup wrapper and historical support code |
| `version_matrix/` | Versioned corpus preparation and native helper dependencies |
| `hdf5_route3/` | Native HDF5 bindings, controls and fixtures imported by retained drivers |
| `resource_limited/` | Cgroup/resource supervision and shared scenario infrastructure |
| Named diagnostic wrappers | Source-bound dependencies of retained FM1 protocols |

Run commands from the repository root using full `experiments.*` module names.
Use the [HDF5 modes contract](../../docs/contracts/hdf5-query-modes.md) for the
current `native-hdf5` and `iotdb-indexed` semantics, and the
[identity contract](../../docs/contracts/experiment-identity.md) for corpus,
case and run provenance. In particular, candidate count, total file count,
selected versions and returned units are distinct workload quantities.

Superseded V00–V04 validation entry points, standalone large-file pilots,
unreferenced optimization probes, and old acceptance receipts remain local
archives. Historical runners may require their frozen binaries, source bundles,
private manifests and server paths; their presence here is not a claim that they
can run unchanged against the latest product. Keep the original source checks
and timing boundaries when using archived measurements.
