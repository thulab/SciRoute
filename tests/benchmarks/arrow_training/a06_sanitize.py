#!/usr/bin/env python3
"""Run focused sanitizers for the format-independent A06 training model.

Use an experiment directory containing IoTDB-FS-C and build symlinks to the
validated source and Unix Makefiles build. This reuses the A05 instrumented
link harness but keeps A06 artifacts under its own experiment directory.
"""

import a05_sanitize as harness


harness.SOURCES = {
    "TrainingQueryModel.cpp", "QueryModel.cpp", "test_TrainingQueryModel.cpp",
}
harness.DEFAULT_FILTER = "TrainingQueryModel.*"


if __name__ == "__main__":
    raise SystemExit(harness.main())
