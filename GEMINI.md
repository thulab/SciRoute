# IoTDB-FS-C Project Instructions

This project is a FUSE-based filesystem that leverages IoTDB to store and query meteorological GRIB data.

## Architecture Overview
- **FUSE App (`src/main.cpp`):** Maps POSIX filesystem calls to the backend logic.
- **Backend Logic (`src/CrossFileClient.cpp`, `src/InodeCache.cpp`):** Manages file metadata and content retrieval.
- **Storage Layer (`src/IoTDBConnector.cpp`):** Interfaces with Apache IoTDB.
- **GRIB Utilities (`src/formats/grib/gribUtils.cpp`):** Uses `ecCodes` to process GRIB data, supporting spatial cropping and interpolation.
- **Semantic Grep (`src/SemanticGrepClient.cpp`):** Translates natural language queries into IoTDB filters.

## Coding Conventions
- **Language:** C++17.
- **Build System:** CMake (minimum 3.10).
- **Naming:**
    - Classes: `PascalCase`.
    - Methods/Functions: `camelCase`.
    - Variables: `camelCase` or `snake_case` (follow local file convention).
- **Error Handling:** Use return codes and logging (`include/iotdbfs/Logging.h`).
- **Includes:** Prefer `#include "..."` for project headers and `#include <...>` for system/library headers.

## Development Workflows

### Agent Collaboration
`AGENTS.md` is the source of truth for repository agent roles. Multi-agent
collaboration is allowed by default: dispatch specialized subagents whenever a
task can be split into independent GRIB, IoTDB, FUSE, semantic grep, packaging,
or verification work. Keep ownership explicit, avoid overlapping write scopes,
and do not overwrite changes made by another agent.

### Building
```bash
mkdir build && cd build
cmake ..
make
```

### Running the Filesystem
```bash
./build/bin/iotdb_fs /path/to/mountpoint
```
Use `-f -d` for foreground and debug output.

### Adding GRIB Functions
GRIB data processing is exposed via query parameters in the filename (e.g., `file.grib2?function=read&level=500`).
When adding new functions:
1. Update `include/iotdbfs/formats/grib/gribUtils.h` and `src/formats/grib/gribUtils.cpp` to implement the processing logic.
2. Update the query parser in `src/CrossFileClient.cpp` to recognize the new function.

### Testing
- Place new unit tests in `tests/unit/`.
- Place FUSE/IoTDB/real-data checks in `tests/integration/`.
- Place benchmark sources and scripts in `tests/performance/`.

## Critical Dependencies
- `libfuse-dev`
- `libeccodes-dev`
- `iotdb-client-cpp` (located in `third_party/`)
- `libboost-all-dev`

## Important Files
- `resources/config.properties`: Primary configuration file.
- `include/iotdbfs/InodeTypes.h`: Defines the core metadata structures.
- `include/iotdbfs/PathUtils.h`: Utilities for URI-style path parsing.

## User 工作规则（固定，优先级最高）

1. **决策确认**：遇到不确定的代码设计问题时，必须先询问 User，不得直接行动。
2. **代码兼容性**：不要写兼容性代码，除非 User 主动要求。
