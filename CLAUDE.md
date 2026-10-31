# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Summary

IoTDB-FS-C is a FUSE filesystem that stores and queries meteorological GRIB2 data using Apache IoTDB as the storage backend. It exposes GRIB messages as virtual files with URI-style query parameters for spatial cropping, interpolation, and format conversion. Written in C++17.

## Build Commands

```bash
# Full build (from repo root)
cd build && cmake .. && make -j$(nproc)

# Rebuild after changes
cd build && make -j$(nproc)

# Run unit tests
cd build && ctest --output-on-failure

# Run a single unit test by filter
cd build && ./tests/unit/unit_tests --gtest_filter='TestSuiteName.TestName'

# Initialize third-party static libs (ecCodes, IoTDB client)
bash packaging/scripts/init-third-party.sh
```

## Dev Scripts

```bash
scripts/dev/start.sh [mountpoint]   # launch iotdb_fs (reads mount.point from config)
scripts/dev/stop.sh  [mountpoint]   # fusermount -u
scripts/dev/clean.sh                # clear logs (CLEAN_BUILD=1 to also rm build/)
```

## Architecture

The system has four layers connected through the FUSE callback interface:

1. **FUSE layer** (`src/main.cpp`) — POSIX callbacks (getattr, read, write, readdir, etc.). Parses URI-style query paths (`file.grib2?function=read&level=500?function=crop&lat0=30&lon0=100`) and dispatches to the appropriate service.

2. **Metadata layer** (`src/InodeCache.cpp`, `src/CrossFileClient.cpp`) — LRU inode cache backed by IoTDB. The `Inode` struct (`include/iotdbfs/InodeTypes.h`) stores POSIX attrs, format identity, and a typed format-neutral metadata document.

3. **Storage layer** (`src/StructuredBlobStorage.cpp`, `src/IoTDBStructuredBlobBackend.cpp`) — format-neutral lifecycle over an inode manifest plus separate payload and layout OBJECT tables. Publication is the sole visibility gate.

4. **Ingestion and format adapters** (`src/Ingestion.cpp`, `src/FormatRegistry.cpp`, `src/formats/grib/GribFormatAdapter.cpp`) — the Ingestion Module copies authoritative bytes and owns publish/abort orchestration. Adapters only recognize content and derive typed metadata plus private layout bytes. GRIB decoding/crop/interp remains ecCodes-based; `GribFileWriter` handles standalone GRIB output.

5. **Semantic grep** (`src/SemanticGrepClient.cpp`) — local heuristic NL-to-query planner. Translates natural language into inode field filters + message index lookups. Replace the planner function to integrate an external LLM.

## Key Design Patterns

- **Query path chaining**: Virtual file reads use `?function=X&...?function=Y&...` segments processed left-to-right (read → crop → interp).
- **Generic ingestion**: `IngestionModule` copies payload chunks exactly once and publishes only after adapter-private layout and typed metadata are durable. Unrecognized content uses byte pass-through without an adapter.
- **Header-only utilities**: `PathUtils.h`, `QueryUtils.h`, `FuseSemantics.h`, `FeatureConfig.h` are header-only (no .cpp).

## Coding Conventions

- Classes: `PascalCase`. Methods/functions: `camelCase`. Variables: `camelCase` or `snake_case` (follow local file convention).
- Error handling via return codes + `Logging.h` macros. No exceptions.
- Project headers: `#include "iotdbfs/Foo.h"`. System/library headers: `#include <...>`.

## Configuration

`resources/config.properties` — key-value config read at startup. Important keys: `iotdb.host/port/user/pass/db`, `mount.point`, `cache.*`, `grib.extension.enabled`.

Environment overrides: `IOTDB_FS_LOG_LEVEL`, `IOTDB_MOUNT_POINT`, `IOTDB_FS_BIN`.

## Dependencies

- `libfuse-dev` (FUSE 2.6 API)
- `libeccodes-dev` (ecCodes for GRIB processing)
- `iotdb-client-cpp` (in `third_party/`, provides `iotdb_session` library)
- `libboost-all-dev`, `cmake >= 3.10`, GTest (fetched automatically if not found)

## Testing

- **Unit tests** (`tests/unit/`): Pure logic tests using GTest/GMock. No IoTDB or FUSE needed.
- **Integration tests** (`tests/integration/`): Require a running IoTDB instance and/or FUSE mount.
- **Performance tests** (`tests/performance/`): Benchmarking scripts for read/write throughput.

## Multi-Agent Collaboration

See `AGENTS.md` for subagent roles. Tasks can be split across `grib_specialist`, `iotdb_specialist`, `fuse_specialist`, and `semantic_grep_expert` based on file ownership. Git write operations (commit, push) are user-only unless explicitly overridden.

## User 工作规则（固定，优先级最高）

1. **决策确认**：遇到不确定的代码设计问题时，必须先询问 User，不得直接行动。
2. **代码兼容性**：不要写兼容性代码，除非 User 主动要求。
