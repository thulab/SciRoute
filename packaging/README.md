# Packaging

Release packaging is separated from development scripts:

On macOS, the optional Finder metadata schema is installed to
`share/mdimport/schema.xml` with the CMake install target. Mounting still
requires macFUSE and the normal IoTDB/ecCodes dependencies.

Native Windows builds are intentionally rejected in this release. Run the
Linux build under WSL2; a WinFsp/Dokany backend can be added later without
changing the IoTDB data model.

- `current/build_release.sh` builds a release package for the current host
  system under `build/dist/`.
- `current/verify_release.sh` validates the generated current-system package.
- `current/platform_check.sh` validates macOS or WSL2 build configuration. Use
  `--static` in a different host environment to inspect repository contracts;
  use `--runtime` only on the target host. Static checks never imply that a
  mount, Finder integration, or IoTDB service was exercised.
- `kylin/build_release.sh` builds the static Kylin package under `build/dist/`;
  the package includes both `bin/iotdb_fs` and `bin/iotdb_fs_web`.
- `kylin/verify_release.sh` validates the generated package layout and binary
  portability.
- `runtime/self_check.sh` is copied into release packages.
- `scripts/init_third_party.sh` prepares third-party dependencies used by the
  release build.

Package names use the C++ release artifact format:

```text
iotdb-fs-c-<version>-<platform>-<arch>-<linkage>.tar.gz
```

Examples:

```text
iotdb-fs-c-1.0.0-kylin-v10-x86_64-static.tar.gz
iotdb-fs-c-1.0.0-ubuntu-22.04-x86_64-native.tar.gz
```

Platform configuration checks:

```sh
packaging/current/platform_check.sh --platform macos --static
packaging/current/platform_check.sh --platform wsl2 --runtime
```

The runtime mode checks host identity and prerequisite discovery but does not
replace the target-platform build, mount, or end-to-end acceptance matrix.

The current HDF5 ingestion and query modules use the pinned **shared HDF5
1.14.4** C library, with thread safety and DEFLATE enabled and MPI disabled.
Native releases use
host-provided shared dependencies, including this exact HDF5 profile when the
driver is linked into an executable; they do not bundle a portable HDF5 runtime.
The native packaging script retains upstream `COPYING` and `COPYING_LBNL_HDF5`
under `share/doc/hdf5` from the resolved dependency prefix. See
[`ci/README.md`](../ci/README.md) for installation. The separate legacy Kylin
static build does not include the current HDF5/query modules and is not a current
release or validation path; no static HDF5 fallback is provided.
