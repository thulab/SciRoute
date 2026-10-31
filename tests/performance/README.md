# Performance Tests

This directory contains benchmark programs and orchestration scripts used for
paper/demo read-write performance checks.

- `write_perf.cpp`, `read_perf.cpp`, and `read_perf_origin.cpp` are benchmark
  sources.
- `scripts/build_perf_test.sh` compiles the benchmark binaries into `build/bin/`.
- `scripts/test.sh` runs the historical write/read benchmark sequence.

Most scripts assume local mount points such as `/mnt/dfs`, `/mnt/local`, and
`/mnt/oracle/staging_area_2`; override the documented environment variables in
each script when running in another environment.
