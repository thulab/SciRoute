#!/usr/bin/env python3
"""Link the auxiliary probe using an existing, read-only unit_tests build.

No CMake or original build file is modified. Production object paths and flags
are recovered from link.txt/flags.make; tests, gtest and main.cpp are excluded.
The source-root may be a fresh overlay with this probe, while objects remain
in a previously verified build. The caller must audit source equivalence.
"""
import argparse
import hashlib
import json
import pathlib
import shlex
import shutil
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", required=True, type=pathlib.Path)
    parser.add_argument("--build-root", required=True, type=pathlib.Path)
    parser.add_argument("--output-root", required=True, type=pathlib.Path)
    parser.add_argument("--target", default="unit_tests")
    args = parser.parse_args()
    source = args.source_root.resolve()
    build = args.build_root.resolve()
    output = args.output_root.resolve()
    if output.exists():
        raise SystemExit("output-root exists; use a fresh path to preserve build evidence")
    output.mkdir(parents=True)
    candidates = list(build.rglob(f"{args.target}.dir/link.txt"))
    if len(candidates) != 1:
        raise SystemExit(f"expected one {args.target} link.txt, found {candidates}")
    link_file = candidates[0]
    # CMake's link script executes in the target's binary directory, which
    # can be build/tests/unit rather than the top-level build directory.
    link_cwd = link_file.parents[2]
    flags_file = link_file.with_name("flags.make")
    flags = {}
    for line in flags_file.read_text().splitlines():
        if line.startswith(("CXX_DEFINES =", "CXX_INCLUDES =", "CXX_FLAGS =")):
            name, value = line.split("=", 1)
            flags[name.strip()] = shlex.split(value)
    tokens = shlex.split(link_file.read_text())
    compiler = tokens[0]
    objects = []
    link_flags = []
    excluded = []
    skip = False
    for token in tokens[1:]:
        if skip:
            skip = False
            continue
        if token == "-o":
            skip = True
            continue
        if token.endswith((".o", ".obj")):
            path = pathlib.Path(token)
            absolute = path if path.is_absolute() else link_cwd / path
            # Typical object names retain src/... or test_...cpp.o after the
            # target directory. Look only at the suffix after *.dir/ because
            # the unit_tests target itself can live under tests/unit/.
            suffix = str(absolute).split(f"{args.target}.dir/", 1)[-1]
            if ("/src/" in "/" + suffix and not suffix.endswith("/main.cpp.o")
                    and "googletest" not in suffix and "gtest" not in suffix
                    and "gmock" not in suffix and "/tests/" not in "/" + suffix):
                objects.append(str(absolute.resolve()))
            else:
                excluded.append(str(absolute))
        elif "gtest" in token or "gmock" in token:
            excluded.append(token)
        elif token.startswith("-Wl,--wrap="):
            # Unit-test fault injection wrappers live in excluded test
            # objects and must not redirect production calls in a probe.
            excluded.append(token)
        elif token.startswith("-Wl,-rpath,"):
            link_flags.append(token)
        else:
            path = pathlib.Path(token)
            # CMake emits relative library paths; preserve their meaning
            # outside the original build directory.
            link_flags.append(str((link_cwd / path).resolve())
                              if not token.startswith("-") and not path.is_absolute() and (link_cwd / path).exists()
                              else token)
    if not objects:
        raise SystemExit("no production objects found")
    compile_flags = flags.get("CXX_DEFINES", []) + flags.get("CXX_INCLUDES", []) + flags.get("CXX_FLAGS", [])
    compile_flags += ["-I" + str(source / "include")]
    probe = source / "tests/benchmarks/query/query_task_mixed_probe.cpp"
    obj = output / "query_task_mixed_probe.o"
    binary = output / "query_task_mixed_probe"
    commands = [[compiler, *compile_flags, "-c", str(probe), "-o", str(obj)]]
    # Minimal frontend-only builds can omit ingestion. Main unit_tests builds
    # normally already contain it.
    for basename in ("Ingestion", "FormatRegistry"):
        if not any(path.endswith(f"/{basename}.cpp.o") for path in objects):
            extra_obj = output / f"{basename}.cpp.o"
            commands.append([compiler, *compile_flags, "-c", str(source / f"src/{basename}.cpp"), "-o", str(extra_obj)])
            objects.append(str(extra_obj))
    commands.append([compiler, str(obj), *objects, *link_flags, "-o", str(binary)])
    evidence = {
        "source_root": str(source), "build_root": str(build),
        "link_cwd": str(link_cwd),
        "link_file": str(link_file), "link_sha256": hashlib.sha256(link_file.read_bytes()).hexdigest(),
        "flags_file": str(flags_file), "flags_sha256": hashlib.sha256(flags_file.read_bytes()).hexdigest(),
        "probe_sha256": hashlib.sha256(probe.read_bytes()).hexdigest(),
        "production_object_count": len(objects), "excluded": excluded, "commands": commands,
    }
    def digest(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()
    reused = [pathlib.Path(p) for p in objects if not pathlib.Path(p).is_relative_to(output)]
    archives = [pathlib.Path(p) for p in link_flags if p.endswith(".a") and pathlib.Path(p).is_file()]
    compiler_path = pathlib.Path(shutil.which(compiler) or compiler).resolve()
    evidence["compiler"] = {"path": str(compiler_path), "sha256": digest(compiler_path)}
    evidence["reused_files"] = [
        {"path": str(p), "sha256": digest(p), "bytes": p.stat().st_size}
        for p in dict.fromkeys(reused + archives)
    ]
    support = source / "tests/support/InMemoryDatasetCatalog.h"
    evidence["fixture_support"] = {"path": str(support), "sha256": digest(support)}
    (output / "build-manifest.json").write_text(json.dumps(evidence, indent=2) + "\n")
    with (output / "build.log").open("w") as log:
        for command in commands:
            log.write(shlex.join(command) + "\n")
            log.flush()
            subprocess.run(command, cwd=link_cwd, check=True, stdout=log, stderr=subprocess.STDOUT)
    evidence["binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
    evidence["reused_files_unchanged_after_build"] = all(
        digest(pathlib.Path(item["path"])) == item["sha256"] for item in evidence["reused_files"]
    )
    if not evidence["reused_files_unchanged_after_build"]:
        raise SystemExit("a reused production object/archive changed during linking")
    (output / "build-manifest.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps({"ok": True, "binary": str(binary), "sha256": evidence["binary_sha256"]}))


if __name__ == "__main__":
    main()
