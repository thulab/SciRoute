#!/usr/bin/env python3
"""Build a separate read-only inspector with an 8 GiB cumulative scan bound.

Per-file 1 GiB, metadata, tree, extent, and 8 MiB batch limits are unchanged.
Reuses the frozen r3 libraries/flags without modifying its build or binary.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def build(frozen_build, output, kind="payload"):
    frozen_build, output = frozen_build.resolve(), output.resolve()
    output.mkdir(mode=0o700)
    commands = json.loads((frozen_build / "compile_commands.json").read_text())
    name = "v04_payload_probe" if kind == "payload" else "v01_store_probe"
    target_name = "large_payload_probe" if kind == "payload" else "large_store_probe"
    entries = [e for e in commands if e["file"].endswith("/" + name + ".cpp")]
    assert len(entries) == 1
    entry = entries[0]
    original = Path(entry["file"])
    source = original.read_text()
    if kind == "payload":
        old = 'check(result.size <= kPayloadRequestBytes - logical_payload_bytes, "inspect cumulative payload bound exceeded");'
        new = 'check(logical_payload_bytes <= 8ULL * 1024 * 1024 * 1024 && result.size <= 8ULL * 1024 * 1024 * 1024 - logical_payload_bytes, "inspect cumulative payload bound exceeded");'
    else:
        old = 'constexpr std::uint64_t row_limit = 65536, byte_limit = 256ULL * 1024 * 1024;'
        new = 'constexpr std::uint64_t row_limit = 65536, byte_limit = 8ULL * 1024 * 1024 * 1024;'
    assert source.count(old) == 1
    modified = source.replace(old, new)
    target = output / (target_name + ".cpp")
    target.write_text(modified)
    obj = output / (target_name + ".o")
    binary = output / target_name
    compile_command = shlex.split(entry["command"])
    assert compile_command.count(entry["file"]) == 1
    compile_command[compile_command.index(entry["file"])] = str(target)
    compile_command[compile_command.index("-o") + 1] = str(obj)
    ninja = subprocess.run(["ninja", "-C", str(frozen_build), "-t", "commands", name],
        check=True, text=True, stdout=subprocess.PIPE).stdout.splitlines()[-1]
    link = shlex.split(ninja)
    assert link[:2] == [":", "&&"] and link[-2:] == ["&&", ":"]
    link = link[2:-2]
    old_object = entry["output"]
    assert link.count(old_object) == 1
    link[link.index(old_object)] = str(obj)
    link[link.index("-o") + 1] = str(binary)
    previous_sha = sha(frozen_build / "bin" / name)
    with (output / "build.log").open("x") as log:
        for command in (compile_command, link):
            subprocess.run(command, cwd=frozen_build, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=600)
    assert previous_sha == sha(frozen_build / "bin" / name)
    receipt = {"complete": True, "original_source": str(original), "original_source_sha256": sha(original),
        "source": str(target), "source_sha256": sha(target), "binary": str(binary), "binary_sha256": sha(binary),
        "original_binary_sha256_unchanged": previous_sha, "compile_command": compile_command, "link_command": link,
        "only_source_replacement": {"before": old, "after": new}, "script_sha256": sha(__file__),
        "scope": "read-only inspector cumulative namespace scan only; production request limits and binaries unchanged"}
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"complete": True, "binary_sha256": sha(binary)}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frozen-build", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--kind", choices=("payload", "store"), default="payload")
    args = parser.parse_args()
    build(args.frozen_build, args.output, args.kind)
