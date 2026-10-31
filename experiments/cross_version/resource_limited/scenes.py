"""Existing scientific scenes and their frozen full-query adapters.

Preparation imports JSON identities only. ``inspect`` hashes all source/copy
bytes and executable/index inputs; the controller separately verifies the live
database head and resources before opening a mount. No API generates data,
publishes versions, builds an index, or controls a service.
"""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from experiments.identity.model import need
from experiments.identity.registry import Registry, file_sha, read_json
from . import large_scene


IDENTITY = Path("experiments/cross_version/work/experiment-identity-20261020/registry")
SMALL_CORPUS = "fcfcc777268da6cb86c6724d3ef114803f2b4e880e718b72381454cd2feb2ffc"
SMALL_CASE = "241e127c3583b454ba3149968b0f4e1a9643d153ed80d0f63e7123536082fcbc"
SMALL_PREPARED = Path("experiments/cross_version/work/grib-global-index-20261020/public-ab/final/prepared.json")
SMALL_PREPARED_SHA = "93a446b8567ddb0884d768979acd14cedb7b58e78f997d3e632a80c37dd946eb"
SMALL_TEMPLATE = Path("experiments/cross_version/work/experiment-identity-20261020/imported-manifests/multi-file-n128-grib.json")
SMALL_TEMPLATE_SHA = "2805f23f7a2bb3bfb9f7627eec4668009e31d2ac57483be67c350ff1e992c4e9"
CACHE_FIX = Path("experiments/cross_version/work/grib-cache-fix-20261018/perf/report.json")
CACHE_FIX_SHA = "d8444e0eff66f270bd49b0852c8c412e701d72be8a4fe0e10e7a7fd786287acc"


def _accepted(repo, path, digest):
    full = Path(repo) / path
    need(full.is_file() and not full.is_symlink() and file_sha(full) == digest,
         "accepted scene evidence missing or changed: " + str(path))
    return read_json(full)


def _register_existing(repo, registry, corpus_id, case_id):
    source = Registry(Path(repo) / IDENTITY)
    corpus, case = source.get_corpus(corpus_id), source.get_case(case_id)
    need(case["corpus_id"] == corpus_id, "historical case binds another corpus")
    registry.register_corpus(corpus); registry.register_case(case)
    return corpus, case


def _base_scene(name, corpus, case):
    return {"scene_id": name, "corpus_id": corpus["corpus_id"], "case_id": case["case_id"],
            "format": corpus["parameters"]["format"], "N": corpus["parameters"]["N"],
            "V_stored": corpus["parameters"]["V_stored"], "D": corpus["parameters"]["D"],
            "expected_matches": deepcopy(case["expected_matches"]),
            "expected_K": deepcopy(case["parameters"]["expected_K"]),
            "predicate": deepcopy(case["parameters"]["predicate"]),
            "selection": case["parameters"]["selection"]["kind"],
            "owned_output_bytes": sum(m["native_oracle"]["elements"] * 5 for m in case["expected_matches"]),
            "preflight_complete": False}


def _prepare_small(repo, registry):
    corpus, case = _register_existing(repo, registry, SMALL_CORPUS, SMALL_CASE)
    accepted = _accepted(repo, SMALL_PREPARED, SMALL_PREPARED_SHA)
    old = accepted["baseline"]
    template = _accepted(repo, SMALL_TEMPLATE, SMALL_TEMPLATE_SHA)
    large_scene._sealed(template, "iotdbfs-multifile-data/1")
    need(accepted["complete"] is True and old["prepared"] is True and
         old["manifest_sha256"] == SMALL_TEMPLATE_SHA and old["N"] == 128 and old["V"] == 1,
         "small GRIB accepted preparation differs")
    need(old["head"] == corpus["publication_evidence"]["head"], "small GRIB publication head differs")
    sources = {s["file"]: s for s in template["sources"]}
    need(len(sources) == 128 and len(corpus["inventory"]) == 128, "small GRIB complete population missing")
    for record in corpus["inventory"]:
        source = sources.get(record["logical_file"], {})
        need(source.get("sha256") == record["sha256"] and source.get("bytes") == record["bytes"] and
             source.get("units") == record["units"] and record["ordinal"] == 1 and
             old["versions"].get(record["binding"]["path"]) == record["binding"]["version"],
             "small GRIB native bytes/unit/version identity differs")
    data = Path(old["data_work"])
    expected_paths = {str(data / "grib-index.sqlite")}
    for record in corpus["inventory"]:
        expected_paths.update(str(data / folder / record["logical_file"]) for folder in ("grib-data", "grib-fs"))
    checks = deepcopy(old["frozen_inputs"])
    need(len(checks) == 257 and {entry["path"] for entry in checks} == expected_paths,
         "small GRIB raw/copy/index byte inventory incomplete")
    for entry in checks:
        if Path(entry["path"]).is_relative_to(data / "grib-fs"):
            entry["nlink"] = 1
    scene = _base_scene("grib-small", corpus, case)
    scene.update(raw_root=str(data / "grib-data"), native_fs_root=str(data / "grib-fs/raw"),
        native_index=str(data / "grib-index.sqlite"), native_binary=old["native"],
        native_binary_sha256=old["native_sha256"], byte_checks=deepcopy(checks),
        original_owner=old["owner_journal"], journal_digest=deepcopy(old["journal_digest"]),
        head=deepcopy(old["head"]), prefix=old["prefix"], logical_prefix=old["logical_prefix"],
        native_index_build=deepcopy(old["index_build_inherited"]),
        frozen_runner=old["runner"], frozen_runner_sha256=old["runner_sha256"],
        frozen_inputs={"template": template, "versions": deepcopy(old["versions"])},
        clock={"IoTDB-FS": "client discovery through owned NumPy Float32/mask; profile wait and oracle excluded",
               "FS-index": "native C++ owned Float32/mask; digest, JSON and pipe excluded"})
    return scene


def _prepare_large(repo, registry):
    bound = large_scene.prepare_large_scene(repo, registry)
    scene = bound["scene"]
    corpus, case = registry.get_corpus(bound["corpus_id"]), registry.get_case(bound["case_id"])
    scene.update(_base_scene("grib-large", corpus, case))
    fix = _accepted(repo, CACHE_FIX, CACHE_FIX_SHA)
    need(fix["complete"] is True and fix["original_service_restored"] is True,
         "accepted large GRIB owner audit incomplete")
    candidates = [n for item in fix["inputs"] if item["size_mib"] == 8 for n in item["namespaces"]
                  if n["V"] == 8 and n["scenario"] == "identical"]
    need(len(candidates) == 1, "large GRIB owner identity absent or ambiguous")
    owner = candidates[0]
    need(owner["head"] == scene["head"] and owner["prefix"] == scene["prefix"] and
         owner["original_journal"] == scene["original_owner"], "large GRIB original owner differs")
    scene["journal_digest"] = deepcopy(owner["original_journal_digest"])
    report = _accepted(repo, *large_scene.MATERIALS["publication"])
    fs = large_scene._one(report["fs"], "large FS byte checks", format="grib", scenario="identical", V=8)
    scene["byte_checks"] = [{"path": f["path"], "bytes": f["bytes"], "sha256": f["sha256"], "nlink": 1}
                            for f in fs["files"]]
    scene["byte_checks"].append({"path": fs["index"], "sha256": fs["index_sha256"]})
    scene["clock"] = {"IoTDB-FS": "legacy n8 full query through owned NumPy Float32/mask; profile wait and oracle excluded",
                      "FS-index": "native version selection through C++ owned Float32/mask; digest, JSON and pipe excluded"}
    return scene


def prepare(repo, registry):
    """Return three JSON scenes on the source host; HDF also hashes existing bytes.

    Does not start native readers, query databases or rebuild source/index data.
    """
    from .hdf_scene import prepare_hdf
    small = _prepare_small(repo, registry)
    hdf = prepare_hdf(repo, registry)
    hdf.update(_base_scene("hdf5-c128", registry.get_corpus(hdf["corpus_id"]), registry.get_case(hdf["case_id"])))
    hdf["native_binary"] = hdf["native_library"]
    hdf["native_binary_sha256"] = hdf["native_sha256"]
    hdf["clock"] = {"IoTDB-FS": "route3 client discovery through owned NumPy Float32/mask; profile wait and oracle excluded",
                    "FS-index": "route3 client version/SQLite selection through native C ABI NumPy-owned Float32/mask"}
    large = _prepare_large(repo, registry)
    need([s["scene_id"] for s in (small, hdf, large)] == ["grib-small", "hdf5-c128", "grib-large"],
         "unexpected scene IDs")
    return [small, hdf, large]


def _journal(directory):
    root = Path(directory)
    need(root.is_dir() and not root.is_symlink(), "original owner directory absent or symlinked")
    result = {}
    for file in root.rglob("*"):
        need(file.is_file() and not file.is_symlink(), "unexpected original owner journal entry")
        result[file.relative_to(root).as_posix()] = file_sha(file)
    need(set(result) == {"state", "owner.lock"}, "owner journal contains unexpected pending entries")
    return result


def inspect(scene, registry):
    """Verify all physical bytes; live head/resource checks remain controller-owned."""
    if scene["scene_id"] == "hdf5-c128":
        from .hdf_scene import inspect_hdf
        return inspect_hdf(scene, registry)
    corpus, case = registry.get_corpus(scene["corpus_id"]), registry.get_case(scene["case_id"])
    need(case["corpus_id"] == corpus["corpus_id"] and case["expected_matches"] == scene["expected_matches"],
         "scene differs from frozen scientific case")
    verification = registry.verify_raw(corpus["corpus_id"], scene["raw_root"])
    checked = []
    for entry in [*scene["byte_checks"], {"path": scene["native_binary"], "sha256": scene["native_binary_sha256"]}]:
        path = Path(entry["path"])
        need(path.is_file() and not path.is_symlink(), "physical scene input missing or symlinked: " + str(path))
        stat = path.stat()
        need(("bytes" not in entry or stat.st_size == entry["bytes"]) and
             ("nlink" not in entry or stat.st_nlink == entry["nlink"]) and file_sha(path) == entry["sha256"],
             "physical scene bytes or identity changed: " + str(path))
        checked.append({"path": str(path), "sha256": entry["sha256"], "bytes": stat.st_size})
    journal = _journal(scene["original_owner"])
    need(journal == scene["journal_digest"], "original owner journal changed")
    return {"complete": True, "scene_id": scene["scene_id"], "corpus_id": corpus["corpus_id"],
            "case_id": case["case_id"], "raw_verification": verification, "checked_files": checked,
            "journal_digest": journal, "database_read": False, "live_head_check_required": True}


class NativeHandle:
    def __init__(self, reader, paired, kind):
        self.reader, self.paired, self.kind = reader, paired, kind

    def close(self):
        self.reader.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def open_native(scene, paired, env=None):
    """Open retained native state with index creation disabled in every scene."""
    if scene["scene_id"] == "hdf5-c128":
        from .hdf_scene import open_hdf_native
        return NativeHandle(open_hdf_native(scene, paired, env), paired, "hdf5")
    return NativeHandle(paired.NativeWorker(Path(scene["native_binary"]), env), paired, "grib")


def expected(scene, registry=None):
    if registry is not None:
        case = registry.get_case(scene["case_id"])
        need(case["corpus_id"] == scene["corpus_id"] and case["expected_matches"] == scene["expected_matches"],
             "scene expected output differs from registry")
    return deepcopy(scene["expected_matches"])


def _result(scene, row):
    need(row.get("oracle_verified") is True, "query adapter did not verify its scientific result")
    need(row["selected_units"] == scene["expected_K"]["K_units"] and
         row["selected_owned_rows"] == sum(m["native_oracle"]["elements"] for m in scene["expected_matches"]),
         "query result count differs from complete frozen output")
    # Legacy adapters independently validate all source locators, scientific
    # versions, output hashes and masks before reaching this normalization.
    return {**row, "scene_id": scene["scene_id"], "corpus_id": scene["corpus_id"], "case_id": scene["case_id"],
            "actual_matches": deepcopy(scene["expected_matches"]), "owned_output_bytes": scene["owned_output_bytes"]}


def query_native(native, scene):
    if scene["scene_id"] == "hdf5-c128":
        from .hdf_scene import query_hdf_native
        return _result(scene, query_hdf_native(native.reader, scene))
    request = {"op": "query", "format": "grib", "mode": "index", "values": True,
               "root": scene["native_fs_root"], "index": scene["native_index"], "predicate": scene["predicate"]}
    if scene["scene_id"] == "grib-large":
        from experiments.cross_version.version_matrix import n8_matrix
        request.update(version_count=8, selection="all")
        row = native.reader.call(request)
        frozen = scene["frozen_inputs"]
        oracle = n8_matrix.oracle_for(frozen["manifest"], "identical", "grib", 8, "all")
        n8_matrix.check_native(row, oracle, "grib", 8, "all", "FS-index")
    else:
        row = native.reader.call(request)
        manifest = scene["frozen_inputs"]["template"]
        native.paired.check_native(row, manifest, native.paired.expected(manifest), True)
    row.update(oracle_verified=True, selected_owned_rows=sum(unit["rows"] for unit in row["units"]),
               clock=scene["clock"]["FS-index"])
    return _result(scene, row)


def query_iotdb(paired, context, scene, offset=0):
    if scene["scene_id"] == "hdf5-c128":
        from .hdf_scene import query_hdf_iotdb
        row, offset = query_hdf_iotdb(paired, context, scene, offset)
    elif scene["scene_id"] == "grib-large":
        from experiments.cross_version.version_matrix import n8_matrix
        frozen = scene["frozen_inputs"]
        row, offset = n8_matrix.iotdb_query(paired, context, frozen["namespace"], frozen["manifest"],
                                           frozen["template"], "grib", "all", offset)
    else:
        frozen = scene["frozen_inputs"]
        row, offset = paired.iotdb_query(context, frozen["template"], scene["logical_prefix"], frozen["versions"], offset, True)
        row.update(oracle_verified=True,
                   selected_owned_rows=sum(m["native_oracle"]["elements"] for m in scene["expected_matches"]))
    row["clock"] = scene["clock"]["IoTDB-FS"]
    return _result(scene, row), offset
