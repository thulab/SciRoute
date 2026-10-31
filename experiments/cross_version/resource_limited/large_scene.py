"""Bind the accepted existing N8/V8 large GRIB scene without generating data.

Only JSON evidence is read here. Physical files, the database head, the native
binary and SQLite must be reverified by the execution preflight, outside timing.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path, PurePosixPath

from experiments.identity.model import build_case, build_corpus, digest, need, number, relative, sha, text


WORK = Path("experiments/cross_version/work")
STAGE = WORK / "large-file-upper-r3-20261017"
MATERIALS = {
    "manifest": (STAGE / "verified-evidence/corpus/manifest.json",
                 "0818d8bbe67f335764c146c794db8d68c1ed7a443c62e2552a533b8cd3d7c7e3"),
    "template": (STAGE / "verified-evidence/corpus/grib-base-manifest.json",
                 "100e29fda74cbca3e9683e9b97dba446cc41c5ec56dd7fbe7ded44e555fa0158"),
    "publication": (STAGE / "report.json",
                    "f7e1ed13af10338df18551d9ca506d2a6081d2c169f9628bc840073f78648d1c"),
}


def _sealed(document, protocol):
    need(document.get("protocol") == protocol, "unexpected frozen manifest protocol")
    body = deepcopy(document)
    fingerprint = body.pop("manifest_sha256", None)
    need(sha(fingerprint) == digest(body), "frozen manifest internal seal differs")


def _absolute(value):
    text(value)
    path = PurePosixPath(value)
    need(path.is_absolute() and str(path) == value and value != "/", "noncanonical absolute evidence path")
    relative(value[1:])
    return value


def _one(rows, label, **fields):
    found = [row for row in rows if all(type(row.get(k)) is type(v) and row[k] == v for k, v in fields.items())]
    need(len(found) == 1, "missing or duplicate " + label)
    return found[0]


def _object(pairs):
    result = {}
    for key, value in pairs:
        need(key not in result, "duplicate accepted JSON object key")
        result[key] = value
    return result


def _nonfinite(value):
    need(False, "nonfinite accepted JSON number: " + value)


def _normalise(manifest, template, report, evidence):
    """Normalize accepted documents; callers must bind their original byte SHAs."""
    _sealed(manifest, "iotdbfs-large-version-data/1")
    _sealed(template, "iotdbfs-multifile-data/1")
    for item in evidence.values():
        sha(item["sha256"])
    need(report.get("complete") is True and report.get("original_service_restored") is True,
         "accepted publication report incomplete or unrestored")
    need(report.get("corpus_sha256") == manifest["manifest_sha256"], "publication report names another corpus")
    need(template.get("format") == "grib", "base template is not GRIB")
    controls = manifest["controls"]
    need(all(type(controls.get(k)) is int and controls[k] == v for k, v in {"N": 8, "D": 4, "K": 2}.items()),
         "large scene requires N8/D4/K2 controls")
    need(type(manifest.get("versions")) is int and manifest["versions"] == 8, "large scene requires stored V8")
    spec = template["spec"]
    need(all(spec.get(k) == controls[k] for k in ("N", "D", "K", "depth", "seed", "tree_width")),
         "base generation controls differ")
    predicate = controls["query"]
    need(predicate == template["query"] == spec["query"], "native predicate evidence differs")
    need(set(predicate) == {"shortName", "typeOfLevel", "level", "step", "validityDate", "validityTime"},
         "large scene requires its original six-field predicate")
    payload = spec["payload"]
    ni, nj = number(payload["Ni"], 1), number(payload["Nj"], 1)
    base = manifest["bases"]["grib"]
    need(base["manifest_sha256"] == template["manifest_sha256"], "large corpus base seal differs")
    raw_root = _absolute(base["root"])
    sources = template["sources"]
    need(len(sources) == 8 and template["actual"]["N"] == 8, "base inventory does not contain exactly eight files")
    by_file = {}
    for source in sources:
        logical = relative(source["file"])
        need(logical not in by_file, "duplicate base logical file")
        sha(source["sha256"]); number(source["bytes"], 1)
        need(len(source["units"]) == 4, "base D differs")
        by_file[logical] = source
    namespace = _one(report["namespaces"], "accepted V04 identical V8 namespace", system="V04", scenario="identical", V=8)
    need(namespace.get("prepared") is True, "namespace publication not prepared")
    head = namespace["head"]
    for key in ("namespace", "incarnation"):
        text(head[key])
    for key in ("commit", "root"):
        sha(head[key])
    number(head["seq"])
    original_owner = _absolute(namespace["journal"])
    _absolute(namespace["logical_prefix"]); text(namespace["prefix"])
    bindings = [binding for binding in namespace["bindings"] if binding["format"] == "grib"]
    by_binding = {}
    for binding in bindings:
        key = (relative(binding["logical_file"]), number(binding["ordinal"], 1))
        need(key not in by_binding, "duplicate scientific publication binding")
        by_binding[key] = binding
    inventory, published, raw_files = [], [], []
    seen = set()
    entries = [entry for entry in manifest["entries"] if entry["format"] == "grib" and entry["scenario"] == "identical"]
    for entry in entries:
        key = (relative(entry["logical_file"]), number(entry["ordinal"], 1))
        need(key not in seen and key in by_binding, "duplicate or unbound scientific history entry")
        seen.add(key)
        source = by_file.get(key[0])
        need(source is not None and entry["blob"] == {"kind": "base", "file": key[0]},
             "identical history must reference its original base bytes")
        need(entry["bytes"] == source["bytes"] and entry["sha256"] == source["sha256"] and
             entry["units"] == source["units"], "identical history bytes or native unit evidence changed")
        for unit in entry["units"]:
            schema, oracle = unit["schema"], unit["oracle"]
            need(schema["Ni"] == ni and schema["Nj"] == nj and oracle["elements"] == ni * nj,
                 "native full-field geometry differs")
            need(oracle["physical_dtype"] == "<f4", "native oracle must certify Float32")
            for field in ("value_sha256", "mask_sha256", "latitudes_sha256", "longitudes_sha256"):
                sha(oracle.get(field))
            sha(unit["unit_id"])
        binding = by_binding[key]
        need(binding["sha256"] == entry["sha256"], "published source SHA differs")
        path = _absolute(binding["logical_path"])
        need(path == namespace["logical_prefix"] + "/grib/" + key[0], "published logical path differs")
        exact = {"path": path, "version": number(binding["version"])}
        inventory.append({"logical_file": key[0], "ordinal": key[1], "bytes": entry["bytes"],
                          "sha256": entry["sha256"], "units": deepcopy(entry["units"]), "binding": exact})
        published.append({"logical_file": key[0], "ordinal": key[1], "bytes": entry["bytes"],
                          "sha256": entry["sha256"], **exact})
        raw_files.append({"logical_file": key[0], "ordinal": key[1], "file": source["file"]})
    expected = {(logical, ordinal) for logical in by_file for ordinal in range(1, 9)}
    need(seen == set(by_binding) == expected, "complete eight-file eight-version scientific history required")
    fs = _one(report["fs"], "accepted complete FS history", format="grib", scenario="identical", V=8)
    fs_root, index = _absolute(fs["root"]), _absolute(fs["index"])
    sha(fs["index_sha256"])
    fs_files = {item["path"]: item for item in fs["files"]}
    need(len(fs_files) == len(fs["files"]) == 64, "complete FS physical-copy inventory required")
    for record in inventory:
        file = fs_root + "/v%03d/" % record["ordinal"] + record["logical_file"]
        actual = fs_files.get(file, {})
        need(actual.get("bytes") == record["bytes"] and actual.get("sha256") == record["sha256"] and
             actual.get("nlink") == 1, "FS copy binding differs from scientific source")
    build = fs["index_build"]
    need(build["files"] == 64 and build["units"] == 256 and build["version_count"] == 8,
         "FS-index does not describe complete history")
    generation = {"kind": "existing-sealed-large-GRIB", "scenario": "identical", "versions": 8,
                  "spec": deepcopy(spec), "mutation_protocol": manifest["mutation"],
                  "payload": deepcopy(payload), "shape": [nj, ni]}
    imported = {"bound": True, "format": "grib", "N_total": 8, "generation": generation,
        "inventory": inventory, "warnings": [], "source_evidence": {
            "manifest_sha256": evidence["manifest"]["sha256"],
            "internal_manifest_sha256": manifest["manifest_sha256"],
            "base_manifest_sha256": evidence["template"]["sha256"],
            "base_internal_manifest_sha256": template["manifest_sha256"],
            "artifacts": deepcopy(evidence), "raw_files": sorted(raw_files, key=lambda r: (r["logical_file"], r["ordinal"])),
            "raw_source_hashes_reverified": False,
            "physical_mapping": "identical versions reference the same existing base file; no new file or hash synthesized"},
        "publication_evidence": {"receipt_sha256": evidence["publication"]["sha256"],
            "namespace": head["namespace"], "head": deepcopy(head), "table_prefix": namespace["prefix"],
            "bindings": published}}
    corpus = build_corpus(imported)
    case = build_case(corpus, {"scope": {"kind": "all", "logical_files": sorted(by_file), "buckets": [],
        "selection_basis": "approved existing N8 identical/V8 full-history scene; six-field selective predicate"},
        "selection": {"kind": "all"}, "predicate": deepcopy(predicate), "operation": "discover-values",
        "roi": {"kind": "whole", "origin": [0, 0], "shape": [nj, ni]},
        "output": {"projection": ["value", "mask"], "dtype": "float32"},
        "expected_K": {"K_files": 2, "K_file_versions": 16, "K_units": 16}})
    scene = {"protocol": "iotdbfs-resource-limited-large-grib-scene/1", "N": 8, "V_stored": 8, "D": 4,
        "selection": "all", "format": "grib", "scenario": "identical", "raw_root": raw_root,
        "native_fs_root": fs_root, "native_index": index, "native_index_sha256": fs["index_sha256"],
        "native_index_build_ns": build["total_ns"], "original_owner": original_owner,
        "head": deepcopy(head), "prefix": namespace["prefix"], "logical_prefix": namespace["logical_prefix"],
        "actual_sizes": deepcopy(corpus["parameters"]["S"]), "owned_output_bytes": 16 * ni * nj * 5,
        "native_binary": str(PurePosixPath(raw_root).parents[3] / "n8-version-matrix-pilot-20261017/native-build/version_matrix_native"),
        "native_binary_sha256": sha(report["native_sha256"]),
        "frozen_inputs": {"manifest": deepcopy(manifest), "template": deepcopy(template), "namespace": deepcopy(namespace)},
        "legacy_loader": {"matrix_module": "experiments.cross_version.version_matrix.n8_matrix",
            "function": "iotdb_query", "arguments": ["paired", "context", "namespace", "manifest", "template", "grib", "all", "offset"],
            "paired_runner": "experiments/multi_file/work/hdf5-predicate-compiled-20261015/runner-source/experiments/multi_file/small_runner.py"},
        "preflight_required": ["all raw byte hashes", "all FS-copy byte hashes", "native SQLite SHA",
                               "native binary SHA", "scientific head", "owner journal", "resource isolation"],
        "raw_hashes_reverified": False, "database_read": False}
    return corpus, case, scene


def prepare_large_scene(repo, registry):
    """Register accepted historical corpus/case; return IDs and execution inputs.

    ``repo`` is IoTDB-FS-C; ``registry`` is an identity.registry.Registry. This
    function performs no native imports, subprocesses, raw reads or database IO.
    """
    repo = Path(repo)
    documents, evidence = {}, {}
    for key, (relative_path, accepted_sha) in MATERIALS.items():
        path = repo / relative_path
        need(path.is_file() and not path.is_symlink(), "accepted large-scene artifact missing: " + str(path))
        need(path.stat().st_size <= 256 * 1024 * 1024, "large-scene JSON exceeds input bound")
        data = path.read_bytes()
        actual = hashlib.sha256(data).hexdigest()
        need(actual == accepted_sha, "accepted large-scene artifact SHA differs: " + str(path))
        documents[key] = json.loads(data, object_pairs_hook=_object, parse_constant=_nonfinite)
        evidence[key] = {"path": str(relative_path), "sha256": actual}
    corpus, case, scene = _normalise(documents["manifest"], documents["template"], documents["publication"], evidence)
    registry.register_corpus(corpus)
    registry.register_case(case)
    return {"corpus_id": corpus["corpus_id"], "case_id": case["case_id"], "scene": scene}
