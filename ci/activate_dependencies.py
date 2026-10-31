#!/usr/bin/env python3
"""Verify a restored dependency bundle and export its known paths to Actions."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile

from install_dependencies import DependencyError, LOCK_PATH, archive_path, verify_prefix


def extract_bundle(archive, destination):
    """Extract a dependency artifact while rejecting traversal and unsafe entries."""
    archive = Path(archive)
    destination = Path(destination)
    if destination.is_symlink() or (destination.exists() and any(destination.iterdir())):
        raise DependencyError(f"Extraction destination must be empty: {destination}")
    destination.mkdir(parents=True, exist_ok=True)
    links = []
    with tarfile.open(archive, "r:gz") as source:
        members = source.getmembers()
        seen = set()
        for member in members:
            name = member.name
            if name in {".", "./"}:
                continue
            relative = archive_path(name)
            normalized = relative.as_posix()
            if normalized in seen:
                raise DependencyError(f"Duplicate dependency artifact member: {name}")
            seen.add(normalized)
            if member.issym():
                target = relative.parent
                link = member.linkname
                if not link or "\\" in link or "\x00" in link:
                    raise DependencyError(f"Unsafe dependency artifact link: {name}")
                link_path = Path(*target.parts)
                if Path(link).is_absolute():
                    raise DependencyError(f"Dependency artifact link escapes prefix: {name}")
                parts = list(link_path.parts)
                for part in Path(link).parts:
                    if part in {"", "."}:
                        continue
                    if part == "..":
                        if not parts:
                            raise DependencyError(f"Dependency artifact link escapes prefix: {name}")
                        parts.pop()
                    else:
                        parts.append(part)
                links.append((relative, link))
            elif not (member.isdir() or member.isfile()):
                raise DependencyError(f"Unsupported dependency artifact entry: {name}")
        for member in members:
            if member.name in {".", "./"}:
                continue
            relative = archive_path(member.name)
            target = destination.joinpath(*relative.parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                with source.extractfile(member) as incoming, target.open("xb") as outgoing:
                    shutil.copyfileobj(incoming, outgoing)
                target.chmod(0o755 if member.mode & 0o111 else 0o644)
        for relative, link in links:
            target = destination.joinpath(*relative.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists() or target.is_symlink():
                raise DependencyError(f"Dependency artifact link conflicts with member: {relative}")
            target.symlink_to(link)
        root = destination.resolve()
        for relative, _ in links:
            resolved = destination.joinpath(*relative.parts).resolve()
            if not resolved.is_relative_to(root):
                raise DependencyError(f"Dependency artifact link escapes prefix: {relative}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", required=True, type=Path)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--github-env", type=Path)
    args = parser.parse_args()
    prefix = args.prefix.resolve()
    if any(ch in str(prefix) for ch in "\r\n"):
        parser.error("dependency prefix cannot contain line breaks")
    if args.archive:
        extract_bundle(args.archive, prefix)
    verify_prefix(prefix, json.loads(LOCK_PATH.read_text()))
    library_path = f"{prefix}/lib:{prefix}/iotdb/lib"
    environment = {
        "IOTDB_SDK_ROOT": str(prefix / "iotdb"),
        "CMAKE_PREFIX_PATH": str(prefix),
        "Arrow_DIR": str(prefix / "lib/cmake/Arrow"),
        "HDF5_DIR": str(prefix / "lib/cmake/hdf5"),
        "PKG_CONFIG_PATH": str(prefix / "lib/pkgconfig"),
        "LD_LIBRARY_PATH": library_path,
        "ECCODES_DEFINITION_PATH": str(prefix / "share/eccodes/definitions"),
        "ECCODES_SAMPLES_PATH": str(prefix / "share/eccodes/samples"),
    }
    result = subprocess.run(["ldd", str(prefix / "iotdb/lib/libiotdb_session.so")],
                            env={**os.environ, **environment}, text=True, capture_output=True,
                            check=False)
    if result.returncode or "not found" in result.stdout + result.stderr:
        raise DependencyError("SDK runtime libraries or libstdc++ ABI are unavailable:\n"
                              + result.stdout + result.stderr)
    if args.github_env:
        with args.github_env.open("a") as output:
            for key, value in environment.items():
                output.write(f"{key}={value}\n")
    else:
        print(json.dumps(environment, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (DependencyError, OSError, ValueError) as error:
        print(f"Dependency activation failed: {error}", file=sys.stderr)
        sys.exit(1)
