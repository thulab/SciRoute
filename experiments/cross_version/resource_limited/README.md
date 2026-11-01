# Shared resource and scenario helpers

Current experiment entry points are listed in [experiments](../../README.md).
This directory retains resource isolation, process supervision, native HDF5
setup and scenario helpers imported by the fixed-matrix, maintenance and
stored-data drivers. Keep their tests and transitive dependencies together.

The original standalone historical comparison and phase-ratio diagnostics are
local archives. Full runs need the documented Linux cgroup setup, compatible
IoTDB/native dependencies, private configuration and frozen inputs. Importing a
helper does not establish a new performance result or current-binary acceptance.
