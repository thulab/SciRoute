"""Persistent metadata/locator index for immutable native scientific files.

This bounded profile supports GRIB message predicates and explicitly registered
HDF5 datasets. It registers an already copied file; it neither publishes bytes
nor copies them again. The caller owns file fsync/publication and verifies the
declared SHA-256 against the copied bytes. Registration checks live native
metadata, commits SQLite with FULL synchronization, and pins a stat identity.
Every query selects persisted metadata and opens/closes fresh native handles.
No encoded message, decoded field, or query result is stored in this catalog.

The native reader is injected: GRIB uses the ecCodes ABI adapter's header
methods; HDF5 uses open_file/describe/close_file. Both implement read(spec,
metadata=...) returning an owned result. No experiment module is imported.
"""
from __future__ import annotations

import ctypes as C
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time

PROTOCOL = "iotdbfs-fs-index/native-lifecycle-1"
GRIB_KEYS = ("shortName", "typeOfLevel", "level", "step")


def _need(condition, message):
    if not condition:
        raise ValueError(message)


def _identity(path):
    value = Path(path).lstat()
    _need(stat.S_ISREG(value.st_mode), "indexed source must be a regular immutable file")
    return [value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns]


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _grib_inventory(reader, spec, source_size):
    """Inventory all headers, independently of the benchmark query predicate."""
    file = reader._open(spec["raw"])
    position, rows = 0, []
    try:
        while position < source_size:
            error = C.c_int()
            handle = reader.lib.grib_new_from_file(None, file, 1, C.byref(error))
            reader._check(error.value, "index headers-only extraction")
            _need(handle, "GRIB truncated before source EOF")
            try:
                offset = C.c_int64()
                reader._check(reader.lib.codes_get_message_offset(handle, C.byref(offset)), "index offset")
                length = reader._long(handle, "totalLength")
                _need(offset.value == position and 0 < length <= source_size-position,
                      "GRIB source range differs")
                row = {key: reader._string(handle, key) for key in GRIB_KEYS[:2]}
                row.update({key: reader._long(handle, key) for key in GRIB_KEYS[2:]})
                row.update(offset=offset.value, length=length)
                # Forecast reference/validity are kept separate from publication version.
                row["scientific_time"] = {key: reader._long(handle, key) for key in
                                          ("dataDate", "dataTime", "validityDate", "validityTime")}
                rows.append(row)
                _need(len(rows) <= 100000, "GRIB catalog exceeds supported message bound")
                position += length
            finally:
                reader._check(reader.lib.codes_handle_delete(handle), "delete index header")
    finally:
        reader._close(file)
    _need(position == source_size and bool(rows), "empty or incomplete GRIB source")
    return rows


def _hdf5_inventory(reader, spec):
    file = reader.open_file(spec["raw"])
    try:
        metadata = reader.describe(file, spec.get("dataset", "/T"))
    finally:
        reader.close_file(file)
    reader._verify_metadata(spec, metadata)
    return [metadata]


class Catalog:
    """Single-writer incremental catalog; each registration appends one version.

    Sources are selected by logical path plus exact publication version; old
    versions continue to resolve to their distinct immutable physical files.
    Registration never rescans previously indexed sources. This profile does
    not implement concurrent mutation, current-version resolution or deletion.
    """
    def __init__(self, path, *, create=False):
        self.path = Path(path).absolute()
        self.connection = None
        if create:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(descriptor)
        else:
            _need(self.path.is_file(), "catalog does not exist")
        connection = sqlite3.connect(self.path, isolation_level=None)
        self.connection = connection
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            if create:
                connection.executescript("""
                    BEGIN IMMEDIATE;
                    CREATE TABLE configuration(protocol TEXT NOT NULL);
                    CREATE TABLE sources(
                        id INTEGER PRIMARY KEY, logical_path TEXT NOT NULL,
                        version INTEGER NOT NULL, raw TEXT NOT NULL,
                        format TEXT NOT NULL, sha256 TEXT NOT NULL,
                        identity TEXT NOT NULL, unit_count INTEGER NOT NULL,
                        UNIQUE(logical_path,version));
                    CREATE TABLE units(
                        source_id INTEGER NOT NULL REFERENCES sources(id),
                        ordinal INTEGER NOT NULL, shortName TEXT, typeOfLevel TEXT,
                        level INTEGER, step INTEGER, dataset TEXT, metadata TEXT NOT NULL,
                        PRIMARY KEY(source_id,ordinal));
                    CREATE INDEX grib_predicates ON units(source_id,shortName,typeOfLevel,level,step);
                    CREATE UNIQUE INDEX hdf5_datasets ON units(source_id,dataset);
                    COMMIT;
                """)
                connection.execute("INSERT INTO configuration VALUES(?)", (PROTOCOL,))
                directory = os.open(self.path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            _need(connection.execute("SELECT protocol FROM configuration").fetchall() == [(PROTOCOL,)],
                  "catalog profile differs")
        except BaseException:
            self.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        self.close()

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def stats(self):
        sizes = {suffix or "database": Path(str(self.path)+suffix).stat().st_size
                 if Path(str(self.path)+suffix).exists() else 0 for suffix in ("", "-wal", "-shm")}
        return dict(index_bytes=sum(v for k,v in sizes.items() if k != "-shm"),
                    index_files_bytes=sizes,
                    sources=self.connection.execute("SELECT count(*) FROM sources").fetchone()[0],
                    units=self.connection.execute("SELECT count(*) FROM units").fetchone()[0])

    def checkpoint(self):
        result = self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        _need(result[0] == 0, "catalog checkpoint busy")
        return self.stats()

    def register(self, spec, reader, *, sha256, version=1):
        _need(re.fullmatch(r"[0-9a-f]{64}", sha256 or "") is not None, "invalid declared SHA-256")
        _need(type(version) is int and version >= 0, "invalid exact publication version")
        fmt = spec["format_id"]
        _need(fmt in ("grib", "hdf5"), "unsupported native catalog format")
        raw = str(Path(spec["raw"]).absolute())
        logical = str(spec.get("logical_path", raw))
        _need(logical.startswith("/"), "logical path must be absolute")
        identity = _identity(raw)
        start = time.perf_counter_ns()
        actual = dict(spec, raw=raw)
        rows = (_grib_inventory(reader, actual, identity[2]) if fmt == "grib"
                else _hdf5_inventory(reader, actual))
        extracted = time.perf_counter_ns()
        _need(_identity(raw) == identity, "source changed during metadata extraction")
        connection = self.connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            cursor = connection.execute("INSERT INTO sources(logical_path,version,raw,format,sha256,identity,unit_count) "
                                        "VALUES(?,?,?,?,?,?,?)",
                                        (logical, version, raw, fmt, sha256, _json(identity), len(rows)))
            source_id = cursor.lastrowid
            connection.executemany("INSERT INTO units VALUES(?,?,?,?,?,?,?,?)", [
                (source_id, ordinal, row.get("shortName"), row.get("typeOfLevel"),
                 row.get("level"), row.get("step"), row.get("dataset"), _json(row))
                for ordinal, row in enumerate(rows)])
            _need(_identity(raw) == identity, "source changed before catalog commit")
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        committed = time.perf_counter_ns()
        return dict(logical_path=logical, version=version, format_id=fmt, exact_visible=True,
                    raw=raw, catalog_units=len(rows), immutable_source_sha256=sha256,
                    source_hash_validation="caller verifies copied file after measurement",
                    filesystem_identity=dict(zip(("device","inode","bytes","mtime_ns","ctime_ns"), identity)),
                    visibility_protocol=PROTOCOL, metadata_only=True,
                    metrics=dict(extract_ns=extracted-start, commit_ns=committed-extracted,
                                 registration_ns=committed-start, extracted_files=1,
                                 extracted_units=len(rows), **self.stats()))

    def lookup(self, spec):
        raw = str(Path(spec["raw"]).absolute())
        logical, version = str(spec.get("logical_path", raw)), spec.get("version", 1)
        _need(type(version) is int and version >= 0, "exact publication version required")
        row = self.connection.execute("SELECT id,raw,format,identity,unit_count FROM sources "
                                      "WHERE logical_path=? AND version=?", (logical,version)).fetchone()
        _need(row is not None, "exact source absent from catalog")
        source_id, stored_raw, fmt, identity, count = row
        _need(raw == stored_raw and fmt == spec["format_id"], "catalog source binding differs")
        _need(_identity(raw) == json.loads(identity), "immutable indexed source changed")
        if fmt == "grib":
            predicate = spec["filter"]
            _need(isinstance(predicate, dict) and bool(predicate) and set(predicate) <= set(GRIB_KEYS),
                  "unsupported GRIB semantic predicate")
            keys = [key for key in GRIB_KEYS if key in predicate]
            sql = "SELECT metadata FROM units WHERE source_id=? AND " + " AND ".join(k+"=?" for k in keys)
            selected = [json.loads(r[0]) for r in self.connection.execute(sql,[source_id]+[predicate[k] for k in keys])]
            _need(len(selected) == 1, "GRIB point query requires one selected indexed message")
            return dict(candidates=count, selected=selected, filter=dict(predicate),
                        source_bytes=json.loads(identity)[2])
        dataset = spec.get("dataset", "/T")
        metadata = self.connection.execute("SELECT metadata FROM units WHERE source_id=? AND dataset=?",
                                           (source_id,dataset)).fetchone()
        _need(metadata is not None, "dataset absent from catalog")
        return json.loads(metadata[0])

    def read(self, reader, spec):
        begin = time.perf_counter_ns()
        metadata = self.lookup(spec)
        selected = time.perf_counter_ns()
        observation, owned = reader.read(spec, metadata=metadata)
        end = time.perf_counter_ns()
        return dict(observation, t0_ns=begin, t1_ns=selected+observation.get("discovery_ns",0),
                    t2_ns=end, discovery_ns=selected-begin+observation.get("discovery_ns",0),
                    value_ns=end-selected-observation.get("discovery_ns",0), total_ns=end-begin,
                    catalog_lookup_ns=selected-begin, persistent_index=True,
                    metadata_reused=True, handles_reused=False), owned
