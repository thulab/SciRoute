#!/usr/bin/env python3
"""Check query boundaries and the narrow format request-model validation seams."""

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
INCLUDES = ROOT / "include"
PATTERN = re.compile(r'^\s*#\s*include\s*[<"](iotdbfs/[^">]+)[">]', re.MULTILINE)
RULES = {
    "migration/ReingestionService": ("iotdbfs/formats/",),
    "migration/MigrationPolicy": ("iotdbfs/formats/",),
    "migration/MigrationJournal": ("iotdbfs/formats/",),
    "query/core": ("iotdbfs/formats/", "iotdbfs/query/training/"),
    "query/training/TrainingQueryModel": ("iotdbfs/formats/",),
    "formats/arrow": ("iotdbfs/formats/grib/", "iotdbfs/formats/hdf5/",
                      "iotdbfs/query/training/"),
    "formats/grib": ("iotdbfs/formats/arrow/", "iotdbfs/formats/hdf5/",
                     "iotdbfs/query/training/"),
    "formats/hdf5": ("iotdbfs/formats/arrow/", "iotdbfs/formats/grib/",
                     "iotdbfs/query/training/"),
    "query/training/arrow": ("iotdbfs/formats/grib/", "iotdbfs/formats/hdf5/"),
}

# QueryFragment still carries Arrow's existing physical_array request by value.
# Allow only its data declaration and the validation call at that boundary;
# layout readers, decoders and runtime implementations remain forbidden in core.
# HDF5 binds through existing FieldRefs rather than adding a core subject.
# Its only permitted include is the request-model validator in QueryPlanning.cpp;
# that model owns field syntax and source constraints, never payload decoding.
REQUEST_CONTRACT_INCLUDES = {
    ("include/iotdbfs/query/core/QueryPlanning.h",
     "iotdbfs/formats/arrow/ArrowQuerySubject.h"),
    ("src/query/core/QueryPlanning.cpp",
     "iotdbfs/formats/arrow/ArrowQueryModel.h"),
    ("src/query/core/QueryPlanning.cpp",
     "iotdbfs/formats/hdf5/Hdf5QueryModel.h"),
}


def forbidden(path: Path) -> tuple[str, ...]:
    relative = path.relative_to(ROOT).as_posix()
    for module, prefixes in RULES.items():
        if relative.startswith(f"src/{module}/") or relative.startswith(
            f"include/iotdbfs/{module}/"
        ) or relative in (
            f"src/{module}.cpp", f"include/iotdbfs/{module}.h"
        ):
            return prefixes
    return ()


def main() -> int:
    errors = []
    for subtree in ("include", "src", "tests"):
        for path in (ROOT / subtree).rglob("*"):
            if path.name.startswith("._"):
                continue
            if path.suffix not in (".h", ".hpp", ".cpp", ".cc"):
                continue
            disallowed = forbidden(path)
            for include in PATTERN.findall(path.read_text()):
                if not (INCLUDES / include).is_file():
                    errors.append(f"{path.relative_to(ROOT)}: missing {include}")
                if disallowed and include.startswith(disallowed) and (
                    path.relative_to(ROOT).as_posix(), include
                ) not in REQUEST_CONTRACT_INCLUDES:
                    errors.append(f"{path.relative_to(ROOT)}: forbidden {include}")
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        return 1
    print("query module include paths and dependency direction are valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
