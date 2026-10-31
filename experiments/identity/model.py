"""Strict corpus / query case / execution identities (standard library only).

N always counts the full independently published logical-file population.
Subsetting is a query property and can never construct a smaller corpus.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from collections import Counter
from pathlib import PurePosixPath


class IdentityError(ValueError):
    pass


def need(condition, message):
    if not condition:
        raise IdentityError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def sha(value):
    need(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "SHA-256 required")
    return value


def number(value, minimum=0):
    need(type(value) is int and value >= minimum, "nonnegative integer required")
    return value


def text(value):
    need(isinstance(value, str) and value.strip(), "nonempty text required")
    return value


def relative(value):
    text(value)
    path = PurePosixPath(value)
    need("\\" not in value and "\x00" not in value and not path.is_absolute() and all(p not in (".", "..") for p in value.split("/"))
         and str(path) == value and not value.endswith("/"), "canonical relative logical file required")
    return value


def seal(kind, payload):
    result = copy.deepcopy(payload)
    result["protocol"] = "iotdbfs-experiment-" + kind + "/1"
    result[kind + "_id"] = digest(result)
    return result


def verify(document, kind):
    value = copy.deepcopy(document)
    identity = value.pop(kind + "_id", None)
    need(value.get("protocol") == "iotdbfs-experiment-" + kind + "/1" and
         identity == digest(value), "invalid " + kind + " identity")
    return identity


def inventory_key(record):
    return record["logical_file"], record["ordinal"]


def unit_key(unit):
    if "dataset" in unit:
        return "dataset:" + text(unit["dataset"])
    need("offset" in unit and "length" in unit, "unit requires dataset or encoded message range")
    return "message:%d:%d" % (number(unit["offset"]), number(unit["length"], 1))


def size_summary(inventory):
    sizes = [number(r["bytes"], 1) for r in inventory]
    return {"unit": "encoded_bytes_per_file_version", "count": len(sizes),
            "min": min(sizes), "max": max(sizes), "total_logical_version_bytes": sum(sizes),
            "constant": sizes[0] if min(sizes) == max(sizes) else None,
            "histogram": [[size, count] for size, count in sorted(Counter(sizes).items())]}


def build_corpus(imported):
    need(imported.get("bound") is True,
         "unbound corpus evidence: explicit complete publication required")
    fmt = imported.get("format")
    need(fmt in ("grib", "hdf5"), "unsupported corpus format")
    generation = imported.get("generation")
    need(isinstance(generation, dict) and generation, "generation parameters required")
    source = imported.get("source_evidence", {})
    sha(source.get("manifest_sha256"))
    publication = imported.get("publication_evidence", {})
    sha(publication.get("receipt_sha256")); text(publication.get("namespace"))
    published = {}
    for binding in publication.get("bindings", []):
        key = inventory_key(binding)
        need(key not in published, "duplicate publication binding")
        published[key] = binding
    inventory = copy.deepcopy(imported.get("inventory"))
    need(isinstance(inventory, list) and inventory, "complete inventory required")
    seen = set(); versions = {}; counts = []
    for record in inventory:
        relative(record["logical_file"]); number(record["ordinal"], 1)
        key = inventory_key(record)
        need(key not in seen, "duplicate file / ordinal")
        seen.add(key); sha(record["sha256"]); number(record["bytes"], 1)
        binding = record.get("binding", {})
        need(text(binding.get("path")).startswith("/"), "published absolute path required")
        number(binding.get("version"))
        evidence = published.get(key, {})
        need(all(evidence.get(k) == v for k, v in {"path": binding["path"], "version": binding["version"],
             "bytes": record["bytes"], "sha256": record["sha256"]}.items()), "publication binding differs from inventory")
        versions.setdefault(record["logical_file"], []).append(record["ordinal"])
        need(isinstance(record.get("units"), list) and record["units"], "native unit inventory required")
        keys = [unit_key(u) for u in record["units"]]
        for unit in record["units"]:
            if fmt == "grib":
                need("dataset" not in unit and "offset" in unit and "length" in unit and
                     unit["offset"] + unit["length"] <= record["bytes"], "GRIB message range outside file")
            else:
                need("dataset" in unit and "offset" not in unit, "HDF5 dataset locator required")
        need(len(keys) == len(set(keys)), "duplicate unit locator")
        counts.append(len(keys))
    N = number(imported.get("N_total"), 1)
    need(set(published) == seen, "publication does not bind exactly the full corpus")
    need(N == len(versions), "N_total differs from complete independently published inventory")
    for ordinals in versions.values():
        need(sorted(ordinals) == list(range(1, len(ordinals) + 1)), "version history has gaps")
    for path in versions:
        records = [r for r in inventory if r["logical_file"] == path]
        need(len({r["binding"]["path"] for r in records}) == 1, "logical file rebound across versions")
        need(len({r["binding"]["version"] for r in records}) == len(records), "published version reused")
        actual = [r["binding"]["version"] for r in sorted(records, key=lambda r: r["ordinal"])]
        need(actual == sorted(actual), "scientific ordinals disagree with database Latest order")
    need(len({r["binding"]["path"] for r in inventory}) == N, "published paths merged logical files")
    inventory.sort(key=inventory_key)
    V = {path: len(v) for path, v in sorted(versions.items())}
    parameters = {"format": fmt, "N": N, "V_stored": next(iter(V.values())) if len(set(V.values())) == 1 else None,
                  "version_counts": V, "D": counts[0] if len(set(counts)) == 1 else None,
                  "D_histogram": sorted([[d, c] for d, c in Counter(counts).items()]),
                  "S": size_summary(inventory), "generation": generation}
    return seal("corpus", {"population_kind": "independent", "parameters": parameters,
        "inventory": inventory, "source_evidence": source, "publication_evidence": publication,
        "evidence_notes": imported.get("warnings", [])})


def _selected(corpus, scope, selection):
    paths = scope.get("logical_files")
    need(isinstance(paths, list) and paths and paths == sorted(set(paths)), "explicit sorted candidate paths required")
    all_paths = {r["logical_file"] for r in corpus["inventory"]}
    need(set(paths) <= all_paths, "candidate outside full corpus")
    kind = scope.get("kind")
    need(kind in ("all", "paths", "buckets"), "explicit candidate scope kind required")
    text(scope.get("selection_basis"))
    if kind == "all":
        need(set(paths) == all_paths and scope.get("buckets") == [], "all scope must retain full population")
    if kind == "buckets":
        buckets = scope.get("buckets")
        need(isinstance(buckets, list) and buckets and buckets == sorted(set(buckets)), "explicit sorted buckets required")
        for bucket in buckets:
            relative(bucket)
            need(any(p.startswith(bucket + "/") for p in all_paths), "empty or nonexistent bucket")
        need(all(not a.startswith(b + "/") for a in buckets for b in buckets if a != b), "overlapping buckets")
        expected = {p for p in all_paths if any(p.startswith(b + "/") for b in buckets)}
        need(expected == set(paths), "bucket scope is not the exact set of complete buckets")
    elif kind == "paths":
        need(scope.get("buckets") == [], "path scope cannot claim buckets")
    candidates = [r for r in corpus["inventory"] if r["logical_file"] in paths]
    mode = selection.get("kind")
    need(mode in ("latest", "all", "exact", "between"), "selection must be latest/all/exact/between")
    if mode in ("latest", "all"):
        need(set(selection) == {"kind"}, "latest/all cannot hide an ordinal filter")
    else:
        need(set(selection) == {"kind", "ordinals"}, "explicit ordinal selection required")
        ordinals = selection["ordinals"]
        need(isinstance(ordinals, list) and ordinals and ordinals == sorted(set(ordinals)), "invalid ordinal set")
        for ordinal in ordinals:
            number(ordinal, 1)
        if mode == "exact":
            need(len(ordinals) == 1, "exact selects one scientific ordinal")
        else:
            need(ordinals == list(range(ordinals[0], ordinals[-1] + 1)), "between requires a contiguous ordinal window")
        for path in paths:
            available = {r["ordinal"] for r in candidates if r["logical_file"] == path}
            need(set(ordinals) <= available, "selected version is absent")
    if mode == "latest":
        latest = {p: max(r["ordinal"] for r in candidates if r["logical_file"] == p) for p in paths}
        candidates = [r for r in candidates if r["ordinal"] == latest[r["logical_file"]]]
    elif mode != "all":
        candidates = [r for r in candidates if r["ordinal"] in selection["ordinals"]]
    return candidates


def _oracle(unit, roi, generation):
    schema = unit.get("schema", {})
    geometry = schema.get("shape", generation.get("shape"))
    if geometry is None and "Ni" in schema and "Nj" in schema:
        geometry = [schema["Nj"], schema["Ni"]]
    if geometry is not None:
        need(len(geometry) == len(roi["shape"]), "ROI rank differs from native geometry")
        need(all(o + s <= n for o, s, n in zip(roi["origin"], roi["shape"], geometry)), "ROI outside native geometry")
    if roi["kind"] == "whole":
        need(geometry is not None and list(geometry) == roi["shape"] and
             not any(roi["origin"]), "whole ROI differs from full native geometry")
    if "oracles" in unit:
        found = [o for o in unit["oracles"] if o.get("origin") == roi["origin"] and o.get("shape") == roi["shape"]]
        need(len(found) == 1, "native oracle for exact ROI missing or duplicated")
        value = found[0]
    else:
        need(roi.get("kind") == "whole", "whole-unit oracle cannot certify a smaller ROI")
        value = unit.get("oracle")
    need(isinstance(value, dict) and value, "native oracle missing")
    need(number(value.get("elements"), 1) == math.prod(roi["shape"]), "native oracle element count differs from ROI")
    sha(value.get("value_sha256"))
    if "mask_sha256" in value: sha(value["mask_sha256"])
    return value


def build_case(corpus, request):
    verify(corpus, "corpus")
    need(set(request) == {"scope", "selection", "predicate", "operation", "roi", "output", "expected_K"},
         "case requires exact scope/selection/predicate/operation/ROI/output/expected_K fields")
    request = copy.deepcopy(request)
    candidates = _selected(corpus, request["scope"], request["selection"])
    predicate = request["predicate"]
    need(isinstance(predicate, dict) and predicate, "explicit scalar equality predicate required")
    for key, value in predicate.items():
        text(key)
        need(type(value) in (str, int, float, bool) and value is not None, "unsupported predicate literal")
        need(type(value) is not float or math.isfinite(value), "nonfinite predicate literal")
    need(request["operation"] in ("discover", "discover-values"), "explicit operation required")
    roi = request["roi"]
    need(isinstance(roi, dict) and set(roi) == {"kind", "origin", "shape"} and roi["kind"] in ("whole", "window"),
         "explicit ROI kind/origin/shape required")
    need(isinstance(roi["origin"], list) and isinstance(roi["shape"], list) and
         0 < len(roi["origin"]) == len(roi["shape"]), "ROI rank mismatch")
    for x in roi["origin"]: number(x)
    for x in roi["shape"]: number(x, 1)
    output = request["output"]
    need(isinstance(output, dict) and set(output) == {"projection", "dtype"}, "explicit projection/dtype output contract required")
    projection = output["projection"]
    need(isinstance(projection, list) and projection and len(projection) == len(set(projection)) and
         set(projection) <= {"locators", "value", "mask"}, "unsupported output projection")
    text(output["dtype"])
    if request["operation"] == "discover":
        need(projection == ["locators"] and output["dtype"] == "none", "discover returns locators only")
    else:
        need("value" in projection and output["dtype"] in ("float32", "float64"), "value output dtype required")
    matches = []
    for record in candidates:
        for unit in record["units"]:
            attrs = unit.get("attributes", {})
            if all(type(attrs.get(k)) is type(v) and attrs[k] == v for k, v in predicate.items()):
                oracle = _oracle(unit, roi, corpus["parameters"]["generation"]) if "value" in projection else None
                if "mask" in projection:
                    sha(oracle.get("mask_sha256"))
                # Preserve full native evidence, never create an oracle from an
                # optimized arm's answer or alter expected K after execution.
                matches.append({"logical_file": record["logical_file"], "ordinal": record["ordinal"],
                    "unit": unit_key(unit), "binding": record["binding"], "native_oracle": oracle})
    counts = {"K_files": len({m["logical_file"] for m in matches}),
              "K_file_versions": len({(m["logical_file"], m["ordinal"]) for m in matches}), "K_units": len(matches)}
    need(isinstance(request["expected_K"], dict) and set(request["expected_K"]) == set(counts), "three explicit K counts required")
    for value in request["expected_K"].values(): number(value)
    need(request["expected_K"] == counts, "declared K differs from full-candidate native inventory")
    C = len(request["scope"]["logical_files"])
    N = corpus["parameters"]["N"]
    B = len(request["scope"].get("buckets", []))
    label = "N%d" % N
    if request["scope"]["kind"] != "all":
        label += ("/B%d" % B if B else "") + "/C%d" % C
    parameters = {**corpus["parameters"], **request, "C_paths": C,
                  "C_versions": len(candidates), "B": B, "N_label": label}
    expected = sorted(matches, key=lambda m: (m["logical_file"], m["ordinal"], m["unit"]))
    return seal("case", {"corpus_id": corpus["corpus_id"], "parameters": parameters,
        "parameter_key": digest(parameters), "expected_matches": expected,
        "selected_bindings_sha256": digest([r["binding"] for r in candidates])})


def build_run(case, *, execution, arm, implementation, clock, cache, environment, replication):
    verify(case, "case")
    text(execution); text(arm)
    need(isinstance(implementation, dict) and set(implementation) == {"binary_sha256", "runner_sha256", "engine"},
         "explicit binary/runner/engine identity required")
    sha(implementation["binary_sha256"]); sha(implementation["runner_sha256"]); text(implementation["engine"])
    for value in (clock, cache, environment, replication):
        need(isinstance(value, dict) and value, "clock/cache/environment/replication protocol required")
    return seal("run", {"case_id": case["case_id"], "corpus_id": case["corpus_id"],
        "execution": execution, "arm": arm, "implementation": implementation,
        "clock": clock, "cache": cache, "environment": environment, "replication": replication})


def require_comparable(left, right):
    verify(left, "run"); verify(right, "run")
    for field in ("case_id", "corpus_id", "arm", "clock", "cache", "environment", "replication"):
        need(left[field] == right[field], "not an equivalent A/B: different " + field)
    return True


def validate_result(case, result):
    verify(case, "case")
    need(result.get("case_id") == case["case_id"] and result.get("corpus_id") == case["corpus_id"],
         "result has missing or different case/corpus identity")
    need(result.get("actual_matches") == case["expected_matches"], "output differs from frozen native oracle")
    return True
