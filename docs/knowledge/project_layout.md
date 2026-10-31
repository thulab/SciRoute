# Project Layout

IoTDB-FS-C now follows a conventional C++ release-oriented layout:

See [本地开发与 Web 启停](development.md) for build-directory Web startup and shutdown using the existing runtime scripts.

```text
include/iotdbfs/     Public project headers
src/                 Implementation files and executable entry points
third_party/         Third-party static dependencies
build/               CMake build tree; runtime outputs are under build/bin/
resources/           Default configuration and resource files
tests/unit/          GTest tests built by default
tests/integration/   Environment-dependent FUSE/IoTDB/GRIB checks
tests/performance/   Benchmark programs and orchestration scripts
packaging/kylin/     Kylin release build and package verification
packaging/runtime/   Runtime scripts copied into release packages
packaging/scripts/   Packaging/dependency helper scripts
scripts/dev/         Local development helpers
docs/                Documentation
```

The root `CMakeLists.txt` builds shared project code through the
`iotdb_fs_core` static library and links app targets against it. Unit tests
stay intentionally narrower: they compile only the core source files needed by
the test target so they do not require a live IoTDB service.

## Agent Collaboration

`AGENTS.md` documents the repository's specialized subagents and is the source
of truth for agent coordination. Multi-agent work is allowed by default when
tasks can be split by module or responsibility, such as GRIB processing, IoTDB
schema/query work, FUSE behavior, semantic grep, packaging, or verification.
