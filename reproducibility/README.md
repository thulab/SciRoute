# Reproducibility entry point

From the repository root, with **Python 3.10 or newer**:

```bash
python3 reproduce.py
```

The default command uses only the Python standard library. It runs offline,
uses temporary fixture files, and needs no IoTDB server, FUSE, downloaded dataset,
root access or native GRIB/HDF5 installation. It creates a new `.tmp/reproduce-*`
directory and prints its report path. Existing output directories are rejected.

## What the quick run checks

1. Run the ten existing Python catalog and handle-cache tests with fixture
   readers: persisted source registration, exact versions, changed-source
   rejection, bounded handle reuse, fresh reads, and cleanup. These tests do not
   decode real scientific files or exercise the managed query engine.
2. Verify SHA-256 hashes of the two bundled CSVs against [manifest.json](manifest.json).
3. Read all 1,920 archived collection timing records, retain the 1,600 measurement
   records under the original warmup rule, and independently recompute all 64
   cell medians and min–max ranges. Check identities, sample counts, and medians
   against the accepted summary. Slow samples are retained.
4. Recompute the controlled GRIB N=C=512, S0, K=2 headline:

| Path | Warm request median (ms) | FS-scan / path |
| --- | ---: | ---: |
| FS-scan | 1984.135477 | 1.000 |
| FS-index | 8.082783 | 245.477 |
| IoTDB-FS, sequential schedule | 18.1773385 | 109.154 |

These are **recomputed archived measurements**, not new benchmark timings. The
projection includes timing and case/corpus identity fields, not full query
responses, source files or the original 121 MB raw archive. Its original archive
hash and manuscript revision are recorded in the manifest. Warm medians use ten
filesystem or forty managed requests per cell within one campaign. Observed
min–max ranges are not confidence intervals.

Outputs include `smoke.log`, `smoke.json`, `collection-summary.csv`,
`headline.json`, and `report.json`. The report records scope, input/driver/source
hashes and environment; `full_paper_reproduced` remains `false`. Failed checks
return nonzero and preserve their logs. Missing requested dependencies fail
rather than silently skipping a requested mode. Ordinary quick runs take seconds;
the exact runtime depends on the machine.

## Optional figure reconstruction

With Matplotlib >=3.5,<4 installed in your chosen Python environment:

```bash
python3 -m pip install 'matplotlib>=3.5,<4'
python3 reproduce.py --plots
```

This additionally creates `collection.png`, plotting the paper's eighteen serial
N=C points with their observed ranges. It uses a simplified layout and default
fonts; the data and selection match the collection comparison, not the exact
typesetting. The Matplotlib version is recorded in `report.json`. Other paper
figures are not reconstructed by this command.

## Native GRIB/HDF5 functional and recovery checks

On Linux/WSL2, install the [standalone FS-index dependencies](../fs_index/README.md):
C++17, CMake/CTest (the CI profile uses 3.26.5), thread-safe HDF5 **1.14.4**, ecCodes
with its sample definitions, SQLite, Boost, OpenSSL, and Python with NumPy/h5py.
Then supply the HDF5 CMake package directory:

```bash
python3 reproduce.py --mode native \
  --hdf5-dir /path/to/hdf5-1.14.4/lib/cmake/hdf5
```

This runs the quick checks, builds a new tests-enabled native library with at
most two build jobs, and executes both registered CTest gates:
`fs_index_native_functional` and `fs_index_maintenance`. Tests create their own
small GRIB/HDF5 fixtures and include incremental publication and recovery cases.
Configuration, build, CTest logs and JUnit output are retained in the run directory.
This mode does not install dependencies, mount FUSE, connect to IoTDB, or run
performance campaigns. Its fault-injection build is not a performance binary.

## Paper experiments and remaining external materials

The [main README](../README.md#reproducing-the-paper) maps studies to retained
drivers and defines data, cache, version and timing requirements. The
[experiment index](../experiments/README.md) describes their shared helpers.
Reproducing all central claims additionally needs:

- A retrievable compatible OBJECT-enabled IoTDB SDK/server, or buildable pinned
  sources and setup instructions. A dependency digest without an obtainable
  package is insufficient.
- Exact real-data inventories and conversion/mapping artifacts, including the
  accepted Beijing/Jena and extended Arrow sidecars, query manifests and oracles.
- Frozen controller/source closures and initialization receipts used by accepted
  collection, history, remote, resident and maintenance campaigns. Retained
  hash-pinned controllers are not generic fresh-install benchmark launchers.
- The remaining raw/derived results and scripts mapping each paper figure/table
  to its source campaign, including the precise aggregation rules.

These materials are not all packaged in the current repository. The quick
checks and optional native gates do not close these availability gaps.

## VLDB Research Track audit (2026-11-02)

The [PVLDB v20 submission guidelines](https://www.vldb.org/2027/submission-guidelines.html#transparency-and-reproducibility)
require supplemental implementation/data materials with a publicly accessible,
durable repository and readable reuse instructions, or a reasonable explanation
for unavailable materials. Time pressure is not an accepted explanation. For
Regular Research, the separate reproducibility evaluation is encouraged; EA&B
has stricter mandatory full-package and evaluation requirements.

The [separate evaluation criteria](https://vldb.org/pvldb/reproducibility)
cover system/build setup, inputs or generators, workload/measurement scripts,
and transformations from measured results to figures. Similar supported behavior
is the objective; identical hardware-dependent timings are not required.

| Item | Current evidence / assessment |
| --- | --- |
| Public repository | Anonymous GitHub API returned HTTP 200 with `private=false` for `thulab/SciRoute` on 2026-11-02. New local changes still need publication. |
| Source and license | Product, FS-index, Apache-2.0 license, CI dependency lock and build instructions are present. |
| Simple reviewer entry | `reproduce.py`: offline fixture checks and collection reanalysis; native gates are separately selected. |
| Scientific inputs and mappings | Partial: generators and upstream source links exist; exact full inputs/conversions and accepted Arrow bundles remain external. |
| Managed environment | Partial: custom SDK/server acquisition is not fully self-contained. |
| Paper-wide reruns and figures | Partial: retained drivers and this collection reanalysis do not cover every accepted campaign/figure. |
| Fixed archival identity | Git history is available; publish the final artifact revision and record its commit/archive hash in the submission. A release/DOI is useful, not claimed here as a mandatory specific mechanism. |

**Assessment: partially ready, not certified as complete.** Public code and a
passing smoke run alone do not establish that all research materials are available
or that the independent reproducibility evaluation would pass. Complete the
external-material handoff above, or state any applicable restriction and access
arrangement accurately in the submission. This audit does not inspect CMT or
submit a badge/evaluation application.

### Entry-point validation

On 2026-11-02, quick mode passed from a clean exported source tree with Python
3.11 on macOS: ten fixture tests and all 64 collection cells matched. Requests
to overwrite an output directory and runs with a modified input CSV were rejected.
The optional plot was generated and inspected using Matplotlib 3.11.2. The Linux
native build/tests were not executed in this audit; macOS platform rejection was
verified. No managed service or new performance campaign was run.
