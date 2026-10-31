"""Read-only imports of existing experiment evidence, without data generation.

Ordinals identify raw corpus generations. Only an explicitly supplied publication
receipt can attach a database path/version. A manifest hash proves the imported
document, not a fresh native inspection or a fresh hash of its raw source files.
"""
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re

_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_MAX_MANIFEST_BYTES = 256 * 1024 * 1024


def _need(ok, reason):
    if not ok:
        raise ValueError(reason)


def _integer(value, reason, minimum=0):
    _need(type(value) is int and value >= minimum, reason)
    return value


def _relative(value):
    _need(isinstance(value, str) and value and "\\" not in value and "\0" not in value,
          "raw identity must be a nonempty relative POSIX path")
    path = PurePosixPath(value)
    _need(not path.is_absolute() and all(part not in ("", ".", "..") for part in value.split("/")),
          "unsafe or noncanonical raw identity")
    return value


def _published_path(value):
    _need(isinstance(value, str) and value.startswith("/") and value != "/" and "\0" not in value,
          "publication path must be an absolute file path")
    _relative(value[1:])
    return value


def _digest(value, reason):
    _need(isinstance(value, str) and _DIGEST.fullmatch(value), reason)
    return value


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        _need(key not in result, "duplicate JSON object key")
        result[key] = value
    return result


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("utf-8")


def _load(path, expected_sha256):
    expected = _digest(expected_sha256, "explicit accepted manifest SHA-256 required")
    source = Path(path)
    _need(source.is_file() and not source.is_symlink(), "manifest must be a regular nonsymlink file")
    _need(source.stat().st_size <= _MAX_MANIFEST_BYTES, "manifest exceeds bounded input size")
    data = source.read_bytes()
    actual = hashlib.sha256(data).hexdigest()
    _need(actual == expected, "manifest byte SHA-256 differs from accepted evidence")
    def nonfinite(_):
        raise ValueError("nonfinite JSON number")
    manifest = json.loads(data, object_pairs_hook=_unique_object, parse_constant=nonfinite)
    _need(isinstance(manifest, dict), "manifest must be an object")
    return manifest, {"manifest_sha256": actual, "expected_manifest_sha256": expected,
                      "manifest_bytes": len(data), "raw_source_hashes_reverified": False}


def _units(value, format_id, size, warnings):
    _need(isinstance(value, list) and value, "source must contain a nonempty unit inventory")
    units = deepcopy(value)
    identities = set()
    complete = True
    for unit in units:
        _need(isinstance(unit, dict) and isinstance(unit.get("attributes"), dict),
              "unit semantic attributes absent")
        if format_id == "grib":
            offset = _integer(unit.get("offset"), "GRIB message offset missing")
            length = _integer(unit.get("length"), "GRIB message length missing", 1)
            _need(offset <= size and length <= size-offset, "GRIB message range exceeds source")
            identity = (offset, length)
        else:
            identity = unit.get("dataset")
            _published_path(identity)
        _need(identity not in identities, "duplicate source unit identity")
        identities.add(identity)
        oracles = unit.get("oracles") if "oracles" in unit else [unit.get("oracle")]
        if not isinstance(oracles, list) or not oracles or any(oracle is None for oracle in oracles):
            complete = False
            warnings.append("native oracle missing; import remains unbound without synthesized evidence")
            continue
        for oracle in oracles:
            _need(isinstance(oracle, dict), "native oracle absent")
            _integer(oracle.get("elements"), "native oracle element count missing", 1)
            if "shape" in oracle:
                shape = oracle["shape"]
                _need(isinstance(shape, list) and shape and all(type(n) is int and n > 0 for n in shape),
                      "native oracle shape malformed")
                _need(math.prod(shape) == oracle["elements"], "native oracle shape/count differs")
            if not oracle.get("value_sha256"):
                complete = False
                warnings.append("native value hash missing; no oracle hash synthesized")
            else:
                _digest(oracle["value_sha256"], "invalid native value hash")
            if "mask_sha256" in oracle:
                _digest(oracle["mask_sha256"], "invalid native mask hash")
            else:
                warnings.append("native mask hash absent; original oracle preserved without synthesis")
    return units, complete


def _geometry(value):
    return isinstance(value, list) and bool(value) and all(type(n) is int and n > 0 for n in value)


def _matches(attributes, predicate):
    return all(type(attributes.get(key)) is type(value) and attributes[key] == value
               for key, value in predicate.items())


def _multi_geometry(format_id, spec):
    if not spec or type(spec.get("seed")) is not int or not isinstance(spec.get("payload"), dict):
        return False
    payload = spec["payload"]
    if format_id == "grib":
        return all(type(payload.get(key)) is int and payload[key] > 0 for key in ("Ni", "Nj"))
    return _geometry(payload.get("shape")) and _geometry(payload.get("chunks")) and len(payload["shape"]) == len(payload["chunks"])


def _source_hash(value, warnings):
    if value is None or value == "":
        warnings.append("raw source hash missing; record cannot be publication-bound")
        return ""
    return _digest(value, "invalid raw source SHA-256")


def _bind(inventory, publication_evidence, warnings):
    if publication_evidence is None:
        warnings.append("publication receipt absent; raw generation ordinals are not database versions")
        return None, False
    _need(isinstance(publication_evidence, dict), "publication evidence must be an object")
    evidence = deepcopy(publication_evidence)
    _digest(evidence.get("receipt_sha256"), "accepted publication receipt SHA-256 required")
    _need(isinstance(evidence.get("namespace"), str) and evidence["namespace"].strip(),
          "explicit published namespace required")
    bindings = evidence.get("bindings")
    _need(isinstance(bindings, list), "publication bindings must be an array")
    expected = {(record["logical_file"], record["ordinal"]): record for record in inventory}
    seen, published = set(), set()
    for binding in bindings:
        _need(isinstance(binding, dict), "publication binding must be an object")
        key = (_relative(binding.get("logical_file")),
               _integer(binding.get("ordinal"), "binding raw ordinal absent", 1))
        _need(key in expected, "extraneous publication binding outside complete corpus inventory")
        _need(key not in seen, "duplicate publication binding for raw identity")
        seen.add(key)
        record = expected[key]
        path = _published_path(binding.get("path"))
        version = _integer(binding.get("version"), "explicit database version required")
        _need((path, version) not in published, "published exact identity bound to multiple raw records")
        published.add((path, version))
        size = _integer(binding.get("bytes"), "publication source byte count missing", 1)
        digest = _digest(binding.get("sha256"), "publication source hash missing")
        _need(size == record["bytes"], "publication source size differs from raw inventory")
        if record["sha256"]:
            _need(digest == record["sha256"], "publication source hash differs from raw inventory")
            record["binding"] = {"path": path, "version": version}
    complete = len(seen) == len(expected) and all("binding" in record for record in inventory)
    by_file = {}
    for record in inventory:
        if "binding" in record:
            by_file.setdefault(record["logical_file"], []).append(record)
    for records in by_file.values():
        records.sort(key=lambda record: record["ordinal"])
        _need(len({record["binding"]["path"] for record in records}) == 1,
              "logical file publication path changes across raw versions")
        versions = [record["binding"]["version"] for record in records]
        _need(versions == sorted(set(versions)), "published version order differs from raw history")
    if not complete:
        warnings.append("publication evidence covers only part of the complete raw inventory")
    return evidence, complete


def _finish(format_id, generation, inventory, query_oracles, evidence,
            publication_evidence, warnings, n_total, generation_complete, hashes_complete):
    publication, bound = _bind(inventory, publication_evidence, warnings)
    inventory.sort(key=lambda record: (record["logical_file"], record["ordinal"]))
    return {"format": format_id, "generation": generation, "inventory": inventory,
            "query_oracles": query_oracles, "source_evidence": evidence,
            "publication_evidence": publication, "warnings": sorted(set(warnings)),
            "bound": bool(bound and generation_complete and hashes_complete), "N_total": n_total}


def import_multi_file(path, *, expected_sha256, publication_evidence=None):
    """Import the ENTIRE independent multi_file manifest, never a query subset."""
    manifest, evidence = _load(path, expected_sha256)
    _need(manifest.get("protocol") == "iotdbfs-multifile-data/1", "unsupported multi_file manifest protocol")
    seal = dict(manifest)
    fingerprint = seal.pop("manifest_sha256", None)
    _digest(fingerprint, "multi_file internal fingerprint missing")
    _need(hashlib.sha256(_canonical(seal)).hexdigest() == fingerprint,
          "multi_file internal fingerprint differs")
    evidence["internal_manifest_sha256"] = fingerprint
    format_id = manifest.get("format")
    _need(format_id in ("grib", "hdf5"), "unsupported manifest format")
    sources = manifest.get("sources")
    _need(isinstance(sources, list) and sources, "complete source inventory absent")
    actual = manifest.get("actual")
    _need(isinstance(actual, dict), "actual source counts absent")
    _need(_integer(actual.get("N"), "actual N absent", 1) == len(sources), "actual N differs from complete inventory")
    spec = manifest.get("spec")
    _need(spec is None or isinstance(spec, dict), "generation specification malformed")
    if spec:
        _need(_integer(spec.get("N"), "generation N absent", 1) == len(sources) and spec.get("format") == format_id,
              "generation specification relabels source inventory")
    warnings = []
    generation = deepcopy(spec) if spec else {key:deepcopy(manifest[key]) for key in
        ("protocol", "kind", "provenance", "tree") if key in manifest}
    generation_complete = _multi_geometry(format_id, spec)
    if not generation_complete:
        warnings.append("complete seed/geometry generation evidence absent; import remains unbound")
    inventory, seen, hashes_complete = [], set(), True
    for source in sources:
        _need(isinstance(source, dict), "source inventory record malformed")
        logical = _relative(source.get("file"))
        _need(logical not in seen, "duplicate raw file identity")
        seen.add(logical)
        size = _integer(source.get("bytes"), "raw source size absent", 1)
        digest = _source_hash(source.get("sha256"), warnings)
        units, valid_oracles = _units(source.get("units"), format_id, size, warnings)
        if generation_complete:
            for unit in units:
                schema = unit.get("schema", {})
                fields = ("Ni", "Nj") if format_id == "grib" else ("shape", "chunks")
                _need(all(schema.get(field) == spec["payload"][field] for field in fields),
                      "native unit geometry differs from generation specification")
        hashes_complete = hashes_complete and bool(digest) and valid_oracles
        inventory.append({"logical_file": logical, "ordinal": 1, "bytes": size,
                          "sha256": digest, "units": units})
    _need(sum(record["bytes"] for record in inventory) == manifest.get("raw_bytes"), "raw byte total differs")
    _need(actual.get("D_per_file") == [len(record["units"]) for record in inventory], "per-file D differs")
    query = manifest.get("query")
    _need(isinstance(query, dict) and query, "source query evidence absent")
    if spec:
        _need(spec.get("query") == query, "query differs from generation specification")
        _need(all(len(record["units"]) == spec.get("D") for record in inventory), "generation D differs")
    matches = [sum(_matches(unit["attributes"], query) for unit in record["units"])
               for record in inventory]
    _need(_integer(actual.get("K"), "actual K absent") == sum(value > 0 for value in matches) and
          _integer(actual.get("K_unit"), "actual unit K absent") == sum(matches),
          "declared selective query cardinality differs")
    query_oracles = {"predicate": deepcopy(query), "query_template": deepcopy(manifest.get("query_template")),
                     "unit_oracles": [{"logical_file":record["logical_file"], "ordinal":1, "units":deepcopy(record["units"])}
                                      for record in inventory]}
    evidence["raw_files"] = sorted(
        ({"logical_file":record["logical_file"], "ordinal":1, "file":record["logical_file"]}
         for record in inventory), key=lambda entry:(entry["logical_file"],entry["ordinal"]))
    return _finish(format_id, generation, inventory, query_oracles, evidence,
                   publication_evidence, warnings, len(sources), generation_complete, hashes_complete)


def import_route3(path, *, expected_sha256, publication_evidence=None):
    """Import every route3 file and raw version ordinal, retaining backing N."""
    manifest, evidence = _load(path, expected_sha256)
    _need(manifest.get("protocol") == "iotdbfs-route3-hdf5-data/1", "unsupported route3 manifest protocol")
    n = _integer(manifest.get("N"), "route3 complete N absent", 1)
    rounds = _integer(manifest.get("rounds"), "route3 generation rounds absent", 1)
    dimension = _integer(manifest.get("D"), "route3 D absent", 1)
    entries = manifest.get("entries")
    _need(isinstance(entries, list) and len(entries) == n*rounds, "route3 manifest is a partial or relabelled corpus")
    warnings, inventory, seen, names, ids = [], [], set(), {}, {}
    hashes_complete = True
    for source in entries:
        _need(isinstance(source, dict), "route3 source record malformed")
        logical = _relative(source.get("logical_file"))
        ordinal = _integer(source.get("ordinal"), "route3 raw ordinal absent", 1)
        _need(ordinal <= rounds and (logical, ordinal) not in seen, "duplicate or out-of-range raw version")
        seen.add((logical, ordinal))
        _need(_relative(source.get("file")) == "v%03d/%s" % (ordinal, logical), "raw file path differs from generation identity")
        file_id = _integer(source.get("file_id"), "route3 file_id absent")
        _need(file_id < n and names.setdefault(logical, file_id) == file_id and ids.setdefault(file_id, logical) == logical,
              "route3 file identities differ across rounds")
        size = _integer(source.get("bytes"), "raw source size absent", 1)
        digest = _source_hash(source.get("sha256"), warnings)
        units, valid_oracles = _units(source.get("units"), "hdf5", size, warnings)
        _need(len(units) == dimension, "route3 D differs from unit inventory")
        if any(not isinstance(unit.get("oracles"), list) or not unit["oracles"] or
               not all(isinstance(oracle, dict) for oracle in unit["oracles"]) for unit in units):
            valid_oracles = False
            warnings.append("route3 native ROI oracle inventory absent; import remains unbound")
        hashes_complete = hashes_complete and bool(digest) and valid_oracles
        inventory.append({"logical_file": logical, "ordinal": ordinal, "bytes": size,
                          "sha256": digest, "units": units})
    _need(len(names) == n and set(ids) == set(range(n)) and
          all((logical, ordinal) in seen for logical in names for ordinal in range(1, rounds+1)),
          "route3 history omits raw identities or rounds")
    _need(sum(record["bytes"] for record in inventory) == manifest.get("physical_bytes"), "route3 full byte count differs")
    generation = {key:deepcopy(manifest[key]) for key in
        ("protocol", "seed", "N", "D", "K", "rounds", "shape", "chunks", "roi", "origins", "storage") if key in manifest}
    generation_complete = type(generation.get("seed")) is int and all(
        _geometry(generation.get(key)) for key in ("shape", "chunks", "roi")) and \
        isinstance(generation.get("origins"), list) and bool(generation["origins"])
    if not generation_complete:
        warnings.append("complete seed/geometry generation evidence absent; import remains unbound")
    else:
        shape, chunks, roi, origins = (generation[key] for key in ("shape", "chunks", "roi", "origins"))
        rank = len(shape)
        _need(len(chunks) == len(roi) == rank and all(isinstance(origin, list) and len(origin) == rank and
              all(type(a) is int and 0 <= a <= size-width for a,size,width in zip(origin, shape, roi))
              for origin in origins), "route3 ROI geometry exceeds generated shape")
        _need(len({_canonical(origin) for origin in origins}) == len(origins), "route3 ROI origins repeated")
        for record in inventory:
            for unit in record["units"]:
                if not isinstance(unit.get("oracles"), list) or not unit["oracles"] or \
                        not all(isinstance(oracle, dict) for oracle in unit["oracles"]):
                    continue
                _need(isinstance(unit.get("oracles"), list) and len(unit["oracles"]) == len(origins) and
                      sorted(_canonical(o.get("origin")) for o in unit["oracles"]) == sorted(_canonical(o) for o in origins) and
                      all(o.get("shape") == roi for o in unit["oracles"]), "route3 native ROI oracle inventory differs")
    predicate = manifest.get("predicate")
    _need(isinstance(predicate, dict) and predicate, "route3 query predicate absent")
    for ordinal in range(1, rounds+1):
        hits = sum(_matches(unit["attributes"], predicate) for record in inventory if record["ordinal"] == ordinal for unit in record["units"])
        _need(hits == _integer(manifest.get("K"), "route3 K absent"), "route3 declared per-round selective cardinality differs")
    query_oracles = {"predicate": deepcopy(predicate), "roi":deepcopy(manifest.get("roi")),
                     "origins":deepcopy(manifest.get("origins")),
                     "unit_oracles":[{"logical_file":record["logical_file"], "ordinal":record["ordinal"], "units":deepcopy(record["units"])}
                                     for record in inventory]}
    evidence["raw_files"] = sorted(
        ({"logical_file":source["logical_file"], "ordinal":source["ordinal"], "file":source["file"]}
         for source in entries), key=lambda entry:(entry["logical_file"],entry["ordinal"]))
    return _finish("hdf5", generation, inventory, query_oracles, evidence,
                   publication_evidence, warnings, n, generation_complete, hashes_complete)
