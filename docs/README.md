# Documentation

The public repository retains current query/storage contracts, operating guides,
and the frozen experiment protocol used by the retained drivers.

| Topic | Entry point |
| --- | --- |
| Build and deployment | [Main README](../README.md), [CI dependencies](../ci/README.md), [user manual](knowledge/user_manual/01-overview.md) |
| Standalone FS-index | [API and build](../fs_index/README.md), [maintenance contract](contracts/fs-index-maintenance-v2.md) |
| Query architecture | [Module layout](knowledge/query-module-layout.md), [scheduler](contracts/query-task-scheduler.md), [result delivery](contracts/result-fetch.md) |
| Scientific formats | [Format model](contracts/format.md), [Arrow queries](contracts/arrow-typed-query.md), [HDF5 modes](contracts/hdf5-query-modes.md), [GRIB cache](contracts/grib-query-cache.md) |
| Versioned sources | [Version directory and content sharing](contracts/version-directory-content-sharing-v1.md) |
| Experiments | [Retained experiment entry points](../experiments/README.md), [frozen FM1 protocol](deliverables/fixed-experiment-matrix-plan.md) |

Development diaries, superseded plans, intermediate reports, patent drafts, and
raw run outputs are not distributed as current documentation. References marked
as local archives identify historical context, not required public dependencies.
The 2026-11-02 cleanup only removed Git tracking: original local files and a
pre-cleanup snapshot remain available to the maintainers. Historical benchmark
results retain their original code, environment, and timing boundaries.
