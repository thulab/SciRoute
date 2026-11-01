# IoTDB-FS

**Layered query processing for structured scientific files.**

**English** | [简体中文](README.zh-CN.md)

IoTDB-FS connects scientific metadata, native file layouts, and exact source versions in a unified **Structured Blob** representation. It combines filesystem access with database query processing so that applications can select scientific fields across files and publication versions while retaining their original encoding and provenance.

This repository contains two execution paths: **FS-index**, a standalone index with native readers, and **IoTDB-FS**, a managed filesystem and query runtime backed by IoTDB. It accompanies the research manuscript *Beyond Opaque Byte Streams: Layered Query Processing for Structured Scientific Files*.

## Why index scientific files?

Selecting a small scientific result can still require repeated file discovery and decoding a much larger native unit. A reusable index must also keep scientific attributes and access locations bound to the correct file version.

![Scientific-file access: native decoding granularity, reusable discovery, and exact source-version binding.](resources/readme/iotdb-fs-motivation.png)

*Figure from the manuscript: small outputs, repeated discovery, and source identity are distinct parts of query cost and correctness.*

## Design

A Structured Blob associates publication identity, typed scientific metadata, native layout, and original bytes. Its semantic index maps scientific predicates to native units; native indexes or supplementary access structures locate those units.

![Structured Blob and layered query processing, from version binding through discovery and format planning to execution and typed results.](resources/readme/iotdb-fs-framework.png)

*Figure from the manuscript. The FS-index panel includes the paper's experimental profiles; the standalone component's implemented scope is listed below.*

The query path follows four steps:

1. **Bind sources and versions.** Resolve the publication policy before applying scientific predicates. Scientific time and publication order are separate dimensions.
2. **Discover relevant units.** Reuse scientific metadata to select candidate files, messages, datasets, or supported Arrow structures.
3. **Plan by format capability.** Place projection and selection at the catalog or native-access layer, retaining predicates that require decoded values.
4. **Execute and retain provenance.** Read and decode selected units, evaluate residual conditions, and deliver typed results with validity and source identity.

Indexing can save discovery work without reducing every decoding cost. The benefit depends on the file layout, available locators, selection, and reader. Directly addressable Arrow windows, for example, can leave little additional access work for an index to remove.

## Choose an execution path

| | FS-index | IoTDB-FS |
| --- | --- | --- |
| Intended setting | Static or infrequently updated native files | Data held in managed storage with coordinated publication and query execution |
| Discovery | Persistent SQLite metadata index | Versioned catalogs and format adapters |
| Reading | Native GRIB/HDF5 readers | Managed payload access and supported native-file modes |
| Versions and updates | Registered versions; bounded incremental publication and reader refresh | Integrated publication, version selection, and source validation |
| Access control | Supplied by the surrounding system | Validated in managed execution and result access |
| Runtime | No IoTDB service or FUSE mount required | Compatible IoTDB service; FUSE for filesystem access |

FS-index supports incremental maintenance; its intended use is not restricted to immutable, one-time indexes. Its current maintenance interface provides a single-writer catalog protocol within the documented profile, rather than a distributed management service.

### Format coverage

- **GRIB2:** scientific message metadata, message locations, and native decoding; managed queries include supported spatial selections and transformations.
- **HDF5:** dataset metadata and native selections; the managed adapter provides `iotdb-indexed` and explicitly bound `native-hdf5` modes. See the [HDF5 query-mode contract](docs/contracts/hdf5-query-modes.md).
- **Arrow IPC:** the managed adapter supports its declared tabular and nested-array profiles. Scientific roles require verified format or product metadata; arbitrary column names do not establish units or time semantics.

The standalone [`fs_index/`](fs_index/README.md) library currently implements the bounded GRIB/HDF5 profile: six attribute equalities and complete 4,096-element selected units. General HDF5 role mapping and Arrow adaptation are outside that library's current interface. The manuscript's agent-assisted bindings are checked adaptations, not automatic semantic inference for arbitrary files.

## Getting started

```bash
git clone https://github.com/thulab/IoTDB-FS.git
cd IoTDB-FS
```

### Standalone FS-index

Use Linux or WSL2, C++17, thread-safe HDF5 **1.14.4**, ecCodes, SQLite, Boost headers, and OpenSSL Crypto. This build does not require IoTDB, FUSE, or Arrow.

```bash
cmake -S fs_index -B build/fs-index \
  -DCMAKE_BUILD_TYPE=Release \
  -DHDF5_DIR=/path/to/hdf5-1.14.4/lib/cmake/hdf5 \
  -DFS_INDEX_BUILD_TESTS=OFF
cmake --build build/fs-index --target fs_index_native --parallel 2
```

The library is `build/fs-index/libfs_index_native.so`. See the [FS-index guide](fs_index/README.md) for the Python API, input requirements, incremental publication, and functional/recovery tests. Test builds enable fault-injection hooks; use a separate Release build with tests disabled for measurements.

### Managed IoTDB-FS

The locked Linux CI environment is Ubuntu 22.04 with GCC 11. The root build requires C++17, FUSE, ecCodes, OpenSSL, zlib, **Arrow 21.0.0** with IPC/LZ4/ZSTD, and thread-safe serial **HDF5 1.14.4** with DEFLATE. It also requires the custom **IoTDB C++ SDK with the `OBJECT` extension** and a compatible server; a standard SDK alone is insufficient.

Prepare the dependency prefixes using the [CI dependency guide](ci/README.md) and [dependency lock](ci/dependencies.lock.json), then configure their actual locations:

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=OFF \
  -DIOTDB_ROOT=/path/to/object-enabled-iotdb-client \
  -DArrow_DIR=/path/to/arrow-21/lib/cmake/Arrow \
  -DHDF5_DIR=/path/to/hdf5-1.14.4/lib/cmake/hdf5
cmake --build build --target iotdb_fs iotdb_fs_web --parallel 2
```

The custom SDK and server are external prerequisites, not bundled binaries. CI expects `IOTDB_SDK_URL` to identify the validated SDK archive. See [PR CI setup](docs/knowledge/pr-ci-design.md) for its configuration.

Create a configuration from [`resources/config.properties`](resources/config.properties), set the database endpoint, credentials, namespace, and mount path for your environment, and start the compatible IoTDB service before mounting:

```bash
cp resources/config.properties /path/to/iotdb-fs.properties
# Edit /path/to/iotdb-fs.properties before starting the service.
export IOTDB_CONFIG_FILE=/path/to/iotdb-fs.properties
mkdir -p /path/to/mount
./build/bin/iotdb_fs -f -o default_permissions /path/to/mount
```

In another terminal, ordinary file operations use the mount path. Optional web browsing is available at `http://127.0.0.1:8080`:

```bash
./build/bin/iotdb_fs_web --host 127.0.0.1 --port 8080 --root /path/to/mount
# Unmount after use (Linux).
fusermount -u /path/to/mount
```

For query syntax, deployment, and configuration details, see the [user manual](docs/knowledge/user_manual/01-overview.md) (Chinese) and the contracts below.

## Documentation and experiments

| Topic | Entry point |
| --- | --- |
| Scientific data and execution concepts | [Domain context](CONTEXT.md) |
| Standalone index and maintenance API | [FS-index](fs_index/README.md) · [Maintenance contract](docs/contracts/fs-index-maintenance-v2.md) |
| Query and adapter organization | [Module layout](docs/knowledge/query-module-layout.md) |
| Query scheduling and result delivery | [Task scheduler](docs/contracts/query-task-scheduler.md) · [Result fetch](docs/contracts/result-fetch.md) |
| User and deployment documentation | [User manual](docs/knowledge/user_manual/01-overview.md) · [Documentation index](docs/README.md) |
| Build, tests, and packaging | [CI](ci/README.md) · [Unit tests](tests/unit/README.md) · [Integration tests](tests/integration/README.md) · [Packaging](packaging/README.md) |
| Fixed query workloads | [Experiment matrix](experiments/fixed_matrix/README.md) |
| Cross-file and historical queries | [Multi-file experiments](experiments/multi_file/README.md) · [Version experiments](experiments/cross_version/README.md) |
| Maintenance and stored-data query sessions | [Maintenance](experiments/maintenance_lifecycle/README.md) · [Resident queries](experiments/resident_table5/README.md) |

The manuscript compares FS-scan, indexed native access, and managed execution under specified workloads. Reproduction requires the data, dependency versions, query semantics, cache state, and timing boundaries recorded for each experiment. Experiment directories include protocols and scripts; full datasets and archived run outputs are not all bundled in this repository. Some documentation is currently available in Chinese only.

## Repository layout

```text
fs_index/            Standalone GRIB/HDF5 index and Python bindings
include/iotdbfs/      C++ interfaces
src/                 Filesystem, storage, query runtime, and format adapters
resources/           Configuration templates and README figures
tests/               Unit, integration, and performance tests
experiments/         Workload definitions and experiment drivers
ci/                  Dependency installation and test tooling
packaging/           Release packaging and runtime tooling
docs/                Current contracts, manuals, and frozen experiment protocol
```

## Contributing and license

Use the documented query/storage contracts and run checks relevant to your change. Contributor and coding-agent guidance is in [AGENTS.md](AGENTS.md). The project is licensed under [Apache License 2.0](LICENSE).
