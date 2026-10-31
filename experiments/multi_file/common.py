"""Frozen experiment contracts and filesystem publication, independent of IoTDB."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import shutil
import tempfile

SPEC_PROTOCOL = "iotdbfs-multifile-spec/1"
MANIFEST_PROTOCOL = "iotdbfs-multifile-data/1"
TARGET = {"shortName": "t", "typeOfLevel": "isobaricInhPa", "level": 500,
          "step": 0, "validityDate": 20261013, "validityTime": 0}
MAX_UNITS = 262144
MAX_UNIT_BYTES = 64 * 1024 * 1024


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode()


def identity(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def sha_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def integer(value, name, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f"{name} must be an integer in [{low}, {high}]")
    return value


def validate_query(query):
    if not isinstance(query, dict) or not 1 <= len(query) <= 16:
        raise ValueError("query must contain 1..16 scalar equality predicates")
    for name, value in query.items():
        if (not isinstance(name, str) or not name or len(name.encode()) > 128 or
                type(value) not in (str, int, float) or
                (type(value) is str and len(value.encode()) > 1024) or
                (type(value) is int and not -(1 << 63) <= value < (1 << 63)) or
                (type(value) is float and not math.isfinite(value))):
            raise ValueError(f"invalid scalar query predicate: {name}")
    return dict(query)


def matches(attributes, query):
    return all(type(attributes.get(k)) is type(v) and attributes[k] == v
               for k, v in query.items())


def generated_attributes(is_match, index):
    if is_match:
        return dict(TARGET)
    step = index % 4
    return {**TARGET, "shortName": ("t", "u", "v")[index % 3],
            "level": (850, 1000, 700)[index % 3], "step": step,
            "validityTime": step * 100}


def validate_spec(value):
    allowed = {"protocol", "format", "N", "K", "D", "depth", "seed",
               "tree_width", "payload", "query", "max_output_bytes"}
    if not isinstance(value, dict) or set(value) - allowed:
        raise ValueError("unknown spec fields")
    if value.get("protocol") != SPEC_PROTOCOL or value.get("format") not in ("hdf5", "grib"):
        raise ValueError("unsupported spec protocol or format")
    spec = json.loads(canonical(value))
    n = integer(spec.get("N"), "N", 1, 4096)
    integer(spec.get("K"), "K", 0, n)
    d = integer(spec.get("D"), "D", 1, 512)
    if n * d > MAX_UNITS:
        raise ValueError("N * D exceeds bounded unit count")
    integer(spec.get("depth"), "depth", 1, 8)
    integer(spec.get("seed"), "seed", 0, (1 << 63) - 1)
    integer(spec.get("tree_width"), "tree_width", 1, 256)
    integer(spec.get("max_output_bytes"), "max_output_bytes", 1, 4 * 1024**4)
    spec["query"] = validate_query(spec.get("query", TARGET))
    p = spec.get("payload")
    if not isinstance(p, dict):
        raise ValueError("payload configuration required")
    if spec["format"] == "grib" and set(spec["query"]) - set(TARGET):
        raise ValueError("unsupported GRIB query keys")
    if p == {"policy": "preserve"}:
        return spec
    if spec["format"] == "hdf5":
        if set(p) != {"shape", "chunks", "gzip_level", "shuffle"}:
            raise ValueError("invalid HDF5 payload fields")
        shape, chunks = p["shape"], p["chunks"]
        if not isinstance(shape, list) or not isinstance(chunks, list) or len(shape) != 4 or len(chunks) != 4:
            raise ValueError("HDF5 shape and chunks must have rank 4")
        for extent, chunk in zip(shape, chunks):
            integer(extent, "extent", 1, 1048576)
            integer(chunk, "chunk", 1, extent)
        if math.prod(shape) * 4 > MAX_UNIT_BYTES or math.prod(chunks) * 4 > 1024 * 1024:
            raise ValueError("HDF5 unit/chunk working size exceeds limit")
        integer(p["gzip_level"], "gzip_level", 0, 9)
        if type(p["shuffle"]) is not bool:
            raise ValueError("shuffle must be boolean")
    else:
        if set(p) != {"Ni", "Nj", "bits_per_value", "packing"} or p["packing"] != "grid_simple":
            raise ValueError("GRIB payload requires grid_simple packing")
        integer(p["Ni"], "Ni", 2, 1440)
        integer(p["Nj"], "Nj", 2, 721)
        integer(p["bits_per_value"], "bits_per_value", 8, 24)
        if p["Ni"] * p["Nj"] * 24 > MAX_UNIT_BYTES:
            raise ValueError("GRIB oracle working size exceeds limit")
        if set(spec["query"]) - set(TARGET):
            raise ValueError("unsupported GRIB query keys")
    if estimated_payload_bytes(spec) > spec["max_output_bytes"]:
        raise ValueError("logical payload estimate exceeds max_output_bytes")
    return spec


def estimated_payload_bytes(spec):
    p = spec["payload"]
    if p == {"policy": "preserve"}:
        return None
    per = math.prod(p["shape"]) * 4 if spec["format"] == "hdf5" else p["Ni"] * p["Nj"] * p["bits_per_value"] // 8 + 512
    return spec["N"] * spec["D"] * per


def relative_file(spec, index):
    """Stable opaque leaf name; depth does not change values or query labels."""
    code = identity({"seed": spec["seed"], "file": index})
    width = spec["tree_width"]
    parts = [f"b{int(code[j * 4:j * 4 + 4], 16) % width:03d}"
             for j in range(spec["depth"])]
    suffix = ".h5" if spec["format"] == "hdf5" else ".grib"
    return "/".join(["raw", *parts, f"f{index:06d}-{code[:12]}{suffix}"])


def safe_file(root, relative):
    if not isinstance(relative, str):
        raise ValueError("manifest file path must be a string")
    path = PurePosixPath(relative)
    if path.is_absolute() or not path.parts or any(x in (".", "..") for x in path.parts):
        raise ValueError("unsafe relative manifest path")
    base = Path(root).resolve()
    target = base.joinpath(*path.parts)
    if not target.resolve().is_relative_to(base):
        raise ValueError("manifest path escapes artifact root")
    checked = [target]
    parent = target.parent
    while parent != base:
        checked.append(parent)
        parent = parent.parent
    if any(p.is_symlink() for p in checked):
        raise ValueError("managed source path contains a symlink")
    return target


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_json(path, value):
    with Path(path).open("xb") as stream:
        stream.write(canonical(value) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


@contextmanager
def publication(output):
    """Single local writer, same-filesystem staging; never replace an artifact."""
    output = Path(output).absolute()
    output.parent.mkdir(parents=True, exist_ok=True)
    lock = output.parent / ("." + output.name + ".writer.lock")
    fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    temporary = None
    try:
        if output.exists() or output.is_symlink():
            raise FileExistsError(f"artifact exists: {output}")
        temporary = Path(tempfile.mkdtemp(prefix="." + output.name + ".building-", dir=output.parent))
        yield temporary
        for directory, _, files in os.walk(temporary, topdown=False):
            for name in files:
                with (Path(directory) / name).open("rb") as stream:
                    os.fsync(stream.fileno())
            fsync_dir(directory)
        if output.exists() or output.is_symlink():
            raise FileExistsError(f"artifact exists: {output}")
        temporary.rename(output)
        temporary = None
        fsync_dir(output.parent)
    finally:
        if temporary is not None:
            shutil.rmtree(temporary)
        os.close(fd)
        lock.unlink()


def seal(manifest):
    manifest = dict(manifest)
    manifest.pop("manifest_sha256", None)
    manifest["manifest_sha256"] = identity(manifest)
    return manifest


def load_manifest(root):
    root = Path(root).resolve()
    path = root / "manifest.json"
    if path.stat().st_size > 256 * 1024 * 1024:
        raise ValueError("manifest exceeds bounded size")
    value = json.loads(path.read_text())
    if value.get("protocol") != MANIFEST_PROTOCOL or seal(value) != value:
        raise ValueError("manifest protocol or fingerprint mismatch")
    return value


def source_path(root, source):
    return safe_file(root, source["file"]) if "file" in source else Path(source["external_path"])


def counts(sources, query):
    matched = [sum(matches(u["attributes"], query) for u in s["units"]) for s in sources]
    return {"N": len(sources), "K": sum(n > 0 for n in matched),
            "K_unit": sum(matched), "D_per_file": [len(s["units"]) for s in sources],
            "matches_per_file": matched,
            "unique_unit_count": len({u["unit_id"] for s in sources for u in s["units"]})}


def assert_dimensions(sources, spec):
    actual = counts(sources, spec["query"])
    if (actual["N"] != spec["N"] or actual["K"] != spec["K"] or
            any(n != spec["D"] for n in actual["D_per_file"]) or
            any(n not in (0, 1) for n in actual["matches_per_file"])):
        raise ValueError(f"native oracle does not satisfy N/K/D contract: {actual}")
    return actual
