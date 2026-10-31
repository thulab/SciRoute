"""Local immutable identity registry. Registration is outside query timing.

Documents are committed by atomic rename under an advisory process lock. The
documents themselves are the authoritative index, so interrupted writes cannot
lose a variable-to-corpus binding. No database or benchmark is invoked here.
"""
from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path

from .model import (IdentityError, build_case, build_corpus, build_run, digest,
                    need, seal, sha, text, verify)


def file_sha(path):
    hasher = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def read_json(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            need(key not in result, "duplicate JSON key: " + key)
            result[key] = value
        return result
    def invalid(value):
        raise IdentityError("nonfinite JSON number: " + value)
    with Path(path).open() as stream:
        return json.load(stream, object_pairs_hook=unique, parse_constant=invalid)


class Registry:
    _directories = {"corpus": "corpora", "case": "cases", "run": "runs",
                    "mapping": "mappings", "unbound": "unbound", "verification": "verifications"}

    def __init__(self, root):
        self.root = Path(root)

    def _path(self, kind, identity):
        return self.root / self._directories[kind] / (sha(identity) + ".json")

    @contextmanager
    def _lock(self):
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / ".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def _get(self, kind, identity):
        path = self._path(kind, identity)
        need(path.is_file(), "unknown " + kind + " identity: " + identity)
        document = read_json(path)
        need(verify(document, kind) == identity, "registry filename differs from identity")
        self._validate(kind, document)
        return document

    def _validate(self, kind, doc):
        verify(doc, kind)
        if kind == "corpus":
            rebuilt = build_corpus({"bound": True, "format": doc["parameters"]["format"],
                "N_total": doc["parameters"]["N"], "generation": doc["parameters"]["generation"],
                "inventory": doc["inventory"], "source_evidence": doc["source_evidence"],
                "publication_evidence": doc["publication_evidence"], "warnings": doc["evidence_notes"]})
        elif kind == "case":
            keys = ("scope", "selection", "predicate", "operation", "roi", "output", "expected_K")
            rebuilt = build_case(self.get_corpus(doc["corpus_id"]), {k: doc["parameters"][k] for k in keys})
        elif kind == "run":
            keys = ("execution", "arm", "implementation", "clock", "cache", "environment", "replication")
            rebuilt = build_run(self.get_case(doc["case_id"]), **{k: doc[k] for k in keys})
        else:
            return
        need(rebuilt == doc, "identity document differs from validated model")

    def _documents(self, kind):
        return [self._get(kind, path.stem) for path in sorted((self.root / self._directories[kind]).glob("*.json"))]

    def _write(self, kind, doc):
        path = self._path(kind, doc[kind + "_id"])
        if path.exists():
            need(read_json(path) == doc, "immutable registry conflict")
            return copy.deepcopy(doc)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
        try:
            with os.fdopen(fd, "w") as stream:
                json.dump(doc, stream, sort_keys=True, indent=2, allow_nan=False)
                stream.write("\n"); stream.flush(); os.fsync(stream.fileno())
            os.replace(temporary, path)
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return copy.deepcopy(doc)

    def get_corpus(self, identity): return self._get("corpus", identity)
    def get_case(self, identity): return self._get("case", identity)
    def get_run(self, identity): return self._get("run", identity)

    def register_corpus(self, document):
        with self._lock():
            self._validate("corpus", document)
            key = digest(document["parameters"])
            for prior in self._documents("corpus"):
                need(digest(prior["parameters"]) != key or prior["corpus_id"] == document["corpus_id"],
                     "same corpus variables already bind another publication; reuse the registered corpus")
            return self._write("corpus", document)

    def register_case(self, document):
        with self._lock():
            self._validate("case", document)
            for prior in self._documents("case"):
                need(prior["parameter_key"] != document["parameter_key"] or prior["case_id"] == document["case_id"],
                     "same case variables already bind another corpus or query")
            return self._write("case", document)

    def register_run(self, document):
        with self._lock():
            self._validate("run", document)
            return self._write("run", document)

    @staticmethod
    def _artifact(reference):
        need(isinstance(reference, dict) and set(reference) == {"path", "sha256"}, "artifact path/SHA required")
        path = Path(text(reference["path"]))
        need(path.is_file() and file_sha(path) == sha(reference["sha256"]), "historical artifact missing or changed")

    def register_mapping(self, payload):
        need("protocol" not in payload and "mapping_id" not in payload, "mapping requires unsealed payload")
        self._artifact(payload["artifact"])
        for reference in payload.get("evidence", []): self._artifact(reference)
        need(isinstance(payload.get("row_selector"), dict) and payload["row_selector"], "explicit historical row selector required")
        need(all(isinstance(k, str) and k and type(v) in (str, int, float, bool, type(None))
                 for k, v in payload["row_selector"].items()), "row selector requires scalar field equalities")
        text(payload.get("interpretation"))
        self.get_case(payload["case_id"])
        with self._lock():
            # One original row cannot acquire contradictory case identities.
            for prior in self._documents("mapping"):
                common = set(prior["row_selector"]) & set(payload["row_selector"])
                overlap = all(type(prior["row_selector"][k]) is type(payload["row_selector"][k]) and
                              prior["row_selector"][k] == payload["row_selector"][k] for k in common)
                if prior["artifact"]["sha256"] == payload["artifact"]["sha256"] and overlap:
                    need(prior["case_id"] == payload["case_id"], "historical row already binds another case")
            return self._write("mapping", seal("mapping", payload))

    def register_unbound(self, payload):
        need("protocol" not in payload and "unbound_id" not in payload, "unbound requires unsealed payload")
        text(payload.get("reason"))
        if "artifact" in payload: self._artifact(payload["artifact"])
        with self._lock():
            return self._write("unbound", seal("unbound", payload))

    def verify_raw(self, corpus_id, raw_root):
        corpus = self.get_corpus(corpus_id)
        root = Path(raw_root).resolve(strict=True)
        files = corpus["source_evidence"].get("raw_files", [])
        by_key = {(r["logical_file"], r["ordinal"]): r for r in files}
        need(len(by_key) == len(files) == len(corpus["inventory"]), "complete raw file mapping required")
        checked = []
        for record in corpus["inventory"]:
            source = by_key.get((record["logical_file"], record["ordinal"]))
            need(source is not None, "raw version mapping absent")
            from .model import relative
            path = (root / relative(source["file"])).resolve(strict=True)
            need(path.is_relative_to(root) and path.is_file(), "raw file escapes corpus root")
            need(path.stat().st_size == record["bytes"] and file_sha(path) == record["sha256"], "raw source bytes changed")
            checked.append({"logical_file": record["logical_file"], "ordinal": record["ordinal"],
                            "bytes": record["bytes"], "sha256": record["sha256"]})
        doc = seal("verification", {"corpus_id": corpus_id, "scope": "all raw file-version bytes; no database read",
                                    "verified_inventory_sha256": digest(checked)})
        with self._lock():
            return self._write("verification", doc)
