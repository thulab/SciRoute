# Native baseline migration

The active GRIB/HDF5 FS-index implementation, Python boundary, and functional
fixtures now live in [`fs_index/`](../../../fs_index/README.md). Build that
standalone component and import `fs_index.native`. This directory no longer
contains an active native build or a second copy of the implementation.

The existing catalog protocol, C ABI, native-reader semantics, and accepted
experiment archives are unchanged. Historical binaries and hash-pinned
controller snapshots remain evidence of their original runs. Reproduce such a
run with its archived source closure; do not replace its binary or hashes with
a new build. Current source gates resolve the migrated Python source explicitly
and retain the original digest comparison.
