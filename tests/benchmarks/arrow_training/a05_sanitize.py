#!/usr/bin/env python3
"""Instrument A05 modules against an existing Unix Makefiles unit-test build.

Run on the experimental server after the normal build succeeds, for example:
  python3 a05_sanitize.py /path/to/a05-decoded-cache-20260927 \
    --sanitizer address,undefined
  python3 a05_sanitize.py /path/to/a05-decoded-cache-20260927 \
    --sanitizer thread --repeat 5

This intentionally reuses unrelated release objects and third-party libraries.
The manifest lists exactly which translation units are instrumented; this is
focused sanitizer coverage, not a fully instrumented dependency build.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time


SOURCES = {
    "DecodedFragmentCache.cpp", "QueryRuntime.cpp", "ArrowRangeReader.cpp",
    "ArrowFragmentProvider.cpp", "ArrowFormatQueryAdapter.cpp", "ArrowLayout.cpp",
    "QueryPlanning.cpp", "QueryModule.cpp", "StructuredBlobStorage.cpp",
    "QueryModel.cpp", "ArrowQueryModel.cpp",
    "test_DecodedFragmentCache.cpp", "test_ArrowFormatQueryAdapter.cpp",
    "test_QueryPlanning.cpp", "test_QueryModule.cpp", "test_ArrowLayout.cpp",
    "test_StructuredBlobStorage.cpp", "test_QueryModel.cpp", "test_ArrowQueryModel.cpp",
}
DEFAULT_FILTER = ":".join(f"{name}.*" for name in (
    "DecodedFragmentCacheTest", "ArrowFormatQueryAdapter", "ArrowFragmentProvider",
    "ArrowRangeReader", "ArrowLayout", "QueryModule", "QueryCapabilities",
    "QueryRequest", "QuerySource", "PublishedVersionCatalog", "AdapterPlan",
    "PreparedQuery", "FormatQueryAdapter", "StructuredBlobStorage",
    "DomainSlice", "LogicalArray", "LogicalSchema", "QueryBatch",
    "QueryErrorModel", "TypedExpression", "ArrowQueryModel",
))


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base", type=Path, help="Directory containing IoTDB-FS-C and build")
    parser.add_argument("--sanitizer", choices=("address,undefined", "thread"),
                        default="address,undefined")
    parser.add_argument("--gtest-filter", default=DEFAULT_FILTER)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--build-only", action="store_true")
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error("--repeat must be positive")
    return args


def run_logged(command: list[str], log: Path, cwd: Path,
               env: dict[str, str] | None = None) -> None:
    with log.open("w") as output:
        subprocess.run(command, cwd=cwd, env=env, stdout=output,
                       stderr=subprocess.STDOUT, check=True)


def source_for_object(root: Path, name: str) -> Path:
    subtree = root / ("tests/unit" if name.startswith("test_") else "src")
    matches = list(subtree.rglob(name))
    if len(matches) != 1:
        raise RuntimeError(
            f"expected one source for {name}, found {len(matches)} in {subtree}")
    return matches[0]


def main() -> int:
    args = arguments()
    base = args.base.resolve()
    root = base / "IoTDB-FS-C"
    work = base / "build/tests/unit"
    cmake = work / "CMakeFiles/unit_tests.dir"
    dest = base / "sanitizers" / args.sanitizer.replace(",", "-")
    dest.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    manifest = {"base": str(base), "sanitizer": args.sanitizer,
                "compiler_workers": 4, "filter": args.gtest_filter,
                "repeat": args.repeat, "instrumented_sources": [], "phase": "configure"}
    try:
        flags = []
        for line in (cmake / "flags.make").read_text().splitlines():
            if line.startswith(("CXX_DEFINES =", "CXX_INCLUDES =", "CXX_FLAGS =")):
                flags.extend(shlex.split(line.split("=", 1)[1]))
        # Preserve defines, includes, language and platform flags. The final
        # optimization flag overrides release -O3 while retaining its ABI.
        instrumentation = [f"-fsanitize={args.sanitizer}", "-fno-omit-frame-pointer"]
        flags.extend(["-O1", "-g", *instrumentation])
        link = shlex.split((cmake / "link.txt").read_text())
        if not link or "-o" not in link:
            raise RuntimeError("unit_tests link.txt has no compiler/output command")
        compiler = link[0]
        jobs = []
        found = set()
        for index, argument in enumerate(link):
            name = Path(argument).name.removesuffix(".o")
            if not argument.endswith(".o") or name not in SOURCES:
                continue
            if name in found:
                raise RuntimeError(f"duplicate linked translation unit: {name}")
            found.add(name)
            source = source_for_object(root, name)
            target = dest / f"{name}.o"
            jobs.append((source, target))
            link[index] = str(target)
            manifest["instrumented_sources"].append({
                "path": str(source.relative_to(root)),
                "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            })
        if found != SOURCES:
            raise RuntimeError("missing linked A05 objects: " + ", ".join(sorted(SOURCES - found)))
        manifest["phase"] = "compile"
        print(f"Compiling {len(jobs)} instrumented objects with 4 workers; logs: {dest}", flush=True)

        def compile_one(pair: tuple[Path, Path]) -> None:
            source, target = pair
            run_logged([compiler, *flags, "-c", str(source), "-o", str(target)],
                       dest / f"{source.name}.log", work)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(compile_one, jobs))
        manifest["phase"] = "link"
        binary = dest / "a05_sanitized_tests"
        link[1:1] = instrumentation
        link[link.index("-o") + 1] = str(binary)
        run_logged(link, dest / "link.log", work)
        manifest["binary"] = str(binary)
        if not args.build_only:
            manifest["phase"] = "test"
            env = os.environ.copy()
            if args.sanitizer == "thread":
                env.setdefault("TSAN_OPTIONS", "halt_on_error=1:second_deadlock_stack=1")
            else:
                env.setdefault("ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1")
                env.setdefault("UBSAN_OPTIONS", "halt_on_error=1:print_stacktrace=1")
            run_logged([str(binary), f"--gtest_filter={args.gtest_filter}",
                        f"--gtest_repeat={args.repeat}",
                        f"--gtest_output=xml:{dest / 'tests.xml'}"],
                       dest / "tests.log", root, env)
            print((dest / "tests.log").read_text()[-2000:], flush=True)
        manifest["phase"] = "complete"
        manifest["success"] = True
        return 0
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        manifest["success"] = False
        manifest["error"] = str(error)
        print(f"A05 sanitizer {manifest['phase']} failed: {error}\nLogs: {dest}", file=sys.stderr)
        return 1
    finally:
        manifest["duration_seconds"] = round(time.monotonic() - started, 3)
        (dest / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
