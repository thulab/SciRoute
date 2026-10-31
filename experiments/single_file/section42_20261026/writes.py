#!/usr/bin/env python3
"""Single-source copy/publication and directly measured Q lifecycles.

No historical receipts are accepted. The orchestrator supplies a new private
destination and exact source locator for every independently measured lifecycle.
The source file is never modified. Parent directory durability is not claimed.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import time
from typing import Callable

try:
    from .sessions import require
except ImportError:  # Direct script/test discovery invocation.
    from sessions import require


def _save(path: Path, value: dict) -> None:
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def exact_visibility_locator(*, control: Callable, logical_path: str,
                             format_id: str, grib_request: dict | None = None,
                             format_request: dict | None = None) -> dict:
    """Discover a just-published immutable identity, then verify Exact selection.

    GRIB caller supplies the normal selected-message metadata declaration; only
    kind and version selector change. Catalog response never transfers payload.
    """
    if format_id == "hdf5":
        request = {"kind": "dataset_catalog", "request": {
            "sources": {"paths": [{"kind": "exact", "path": logical_path}],
                        "versions": {"kind": "latest"}}, "dataset_path": "/T"}}
        field = "datasets"
    elif format_id in ("grib", "arrow"):
        declaration = format_request if format_request is not None else grib_request
        require(declaration is not None, "format metadata declaration required")
        request = json.loads(json.dumps(declaration))
        request["kind"] = "format_catalog"
        request["request"]["sources"]["paths"] = [{"kind": "exact", "path": logical_path}]
        request["request"]["sources"]["versions"] = {"kind": "latest"}
        field = "units"
    else:
        raise ValueError("publication lifecycle only covers GRIB/HDF5/Arrow")
    first, _ = control(request)
    rows = first.get(field, [])
    require(bool(rows) and all(row.get("path") == logical_path for row in rows),
            "published path missing from metadata catalog")
    versions = {row.get("version") for row in rows}
    require(len(versions) == 1, "publication visibility has multiple versions")
    version = versions.pop()
    require(type(version) is int and version >= 0, "publication has no exact version")
    request["request"]["sources"]["versions"] = {"kind": "exact", "exact": version}
    exact, _ = control(request)
    confirmed = exact.get(field, [])
    require(bool(confirmed) and all(row.get("path") == logical_path and
                                  row.get("version") == version for row in confirmed),
            "published exact version not visible")
    return {"logical_path": logical_path, "version": version, "format_id": format_id,
            "exact_visible": True, "catalog_units": len(confirmed),
            "visibility_protocol": request["kind"], "metadata_only": True}


def publish_sources(*, context: dict, sources: list[dict], prefix: str,
                    receipt_path, locator: Callable[[str, dict], dict],
                    verify_published: Callable[[Path, dict, dict], None] | None = None) -> dict:
    """Publish frozen raw inputs once into a new owner namespace path prefix.

    sources entries: id, raw, bytes, sha256, format_id; optional filename.
    context supplies an already mounted, resource-limited owner daemon.
    locator(logical_path, source) must prove exact visibility. Hash validation is
    outside each publication timer. Partial receipts survive failures.
    """
    require(prefix.startswith("/") and ".." not in Path(prefix).parts and prefix != "/",
            "new private absolute prefix required")
    mount = Path(context["mount"]).resolve()
    destination_root = mount / prefix.lstrip("/")
    require(not destination_root.exists(), "publication prefix already exists")
    destination_root.mkdir(parents=True)
    report = {"protocol": "iotdbfs-section42-publication-readiness/1", "complete": False,
              "prefix": prefix, "source_publication": "new private owner paths; raw sources unchanged",
              "mount": str(mount), "sources": [], "publication_bytes": 0}
    receipt_path = Path(receipt_path)
    _save(receipt_path, report)
    for source in sources:
        raw = Path(source["raw"])
        require(raw.stat().st_size == source["bytes"] and sha_file(raw) == source["sha256"],
                "frozen input identity differs")
        name = source.get("filename", source["id"] + raw.suffix)
        require(Path(name).name == name and name not in ("", ".", ".."), "invalid destination name")
        logical = prefix.rstrip("/") + "/" + name
        destination = mount / logical.lstrip("/")
        started = time.perf_counter_ns()
        write = checked_copy(raw, destination, expected_bytes=source["bytes"])
        locator_start = time.perf_counter_ns()
        exact = locator(logical, source)
        visible = time.perf_counter_ns()
        require(exact.get("exact_visible") is True and exact.get("logical_path") == logical,
                "new owner publication identity not confirmed")
        row = {"id": source["id"], "raw": str(raw), "bytes": source["bytes"],
               "sha256": source["sha256"], "format_id": source["format_id"],
               "write": write, "exact_source": exact,
               "publication_and_visibility_ns": visible-started,
               "source_locator_ns": visible-locator_start, "verification_complete": False}
        report["sources"].append(row)
        report["publication_bytes"] += source["bytes"]
        _save(receipt_path, report)
        if verify_published:
            verify_published(destination, exact, source)
        else:
            require(sha_file(destination) == source["sha256"], "owner publication hash differs")
        require(sha_file(raw) == source["sha256"], "frozen source changed")
        row["verification_complete"] = True
        _save(receipt_path, report)
    report["complete"] = True
    _save(receipt_path, report)
    return report


def sha_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def checked_copy(source, destination, *, expected_bytes: int,
                 clock: Callable[[], int] = time.perf_counter_ns) -> dict:
    """Identical 1 MiB user copies, full writes, file fsync and close."""
    source, destination = Path(source), Path(destination)
    require(source.is_file() and source.stat().st_size == expected_bytes,
            "source size differs from frozen manifest")
    require(not destination.exists(), "private destination already exists")
    begin = clock()
    source_stream = source.open("rb", buffering=0)
    try:
        target = destination.open("xb", buffering=0)
        opened = clock()
        written_bytes = 0
        try:
            for block in iter(lambda: source_stream.read(1 << 20), b""):
                view = memoryview(block)
                while view:
                    count = target.write(view)
                    require(type(count) is int and count > 0, "short or failed destination write")
                    written_bytes += count
                    view = view[count:]
            copied = clock()
            target.flush()
            os.fsync(target.fileno())
            synced = clock()
        finally:
            target.close()
    finally:
        source_stream.close()
    closed = clock()
    require(written_bytes == expected_bytes and destination.stat().st_size == expected_bytes,
            "copied byte count differs")
    return {"copy_begin_ns": begin, "copy_end_ns": closed, "total_ns": closed-begin,
            "open_ns": opened-begin, "copy_ns": copied-opened,
            "file_fsync_ns": synced-copied, "close_ns": closed-synced,
            "bytes": written_bytes, "transfer_chunk_bytes": 1 << 20,
            "file_fsync_acknowledged": True, "closed": True,
            "parent_directory_fsync_included": False}


def run_lifecycle(*, source, destination, expected_bytes: int, expected_sha256: str,
                  locator: Callable[[Path], dict],
                  run_queries: Callable[[dict], dict],
                  verify_published: Callable[[Path, dict], None] | None = None,
                  clock: Callable[[], int] = time.perf_counter_ns) -> dict:
    """One real copy→exact visibility→independent Q session.

    run_queries must directly time its own session and defer oracle/profile work
    until returning. Its audits occur inside lifecycle wall: this is recorded
    explicitly so this wall cannot be mislabelled as an audit-free latency.
    verify_published and full hashes execute after lifecycle wall stops.
    """
    source, destination = Path(source), Path(destination)
    require(len(expected_sha256) == 64 and sha_file(source) == expected_sha256,
            "source digest differs from frozen manifest")
    begin = clock()
    copied = checked_copy(source, destination, expected_bytes=expected_bytes, clock=clock)
    location_begin = clock()
    identity = locator(destination)
    located = clock()
    require(isinstance(identity, dict) and identity.get("exact_visible") is True,
            "new publication did not become exact-version visible")
    session = run_queries(identity)
    end = clock()
    require(session.get("complete") is True and session.get("session_wall_measured_directly") is True,
            "query lifecycle requires an actual complete session")
    if verify_published:
        verify_published(destination, identity)
    else:
        require(sha_file(destination) == expected_sha256, "published bytes differ")
    require(source.stat().st_size == expected_bytes and sha_file(source) == expected_sha256,
            "source changed during publication lifecycle")
    return {"protocol": "iotdbfs-section42-publication-lifecycle/1", "complete": True,
            "source": str(source), "destination": str(destination),
            "source_bytes": expected_bytes, "source_sha256": expected_sha256,
            "lifecycle_begin_ns": begin, "lifecycle_end_ns": end,
            "lifecycle_wall_ns": end-begin, "lifecycle_wall_measured_directly": True,
            "includes_query_callback_audit_overhead": not session.get("audit_deferred_to_caller", False),
            "write": copied, "source_locator_ns": located-location_begin,
            "exact_source": identity, "session": session,
            "historical_publication_receipts_used": False,
            "post_lifecycle_hash_and_publication_verification": True}
