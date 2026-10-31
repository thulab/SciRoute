"""Identity gate for fresh small_runner measurements of existing publications.

This module uses no database connector and never generates or publishes data.
"""
from __future__ import annotations

import copy
import hashlib
from pathlib import Path
import uuid

from .model import build_run, canonical, digest, need, sha, verify
from .registry import read_json

FORMATS = ("grib", "hdf5")
PHASES = ("discover", "discover-values")


def file_sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def output_contract(phase):
    need(phase in PHASES, "unsupported measurement operation")
    return {"projection": ["locators"], "dtype": "none"} if phase == "discover" else {
        "projection": ["value", "mask"], "dtype": "float32"}


def measurement_session(args, registry=None):
    """Call before connection_environment/private_mount, including on CLI errors."""
    if args.prepare_only:
        return None
    need(getattr(args, "identity_registry", None) is not None and
         getattr(args, "identity_cases", None) is not None,
         "measurement requires --identity-registry and --identity-cases")
    need(args.reuse_preparation is not None and args.reuse_work is not None,
         "measurement requires existing --reuse-preparation and --reuse-work; no generation/publication")
    return MeasurementSession(args.identity_registry, args.identity_cases, registry=registry)


class MeasurementSession:
    def __init__(self, registry_path, bundle_path, *, registry=None):
        if registry is None:
            from .registry import Registry
            registry = Registry(registry_path)
        self.registry = registry
        bundle = read_json(bundle_path)
        need(isinstance(bundle, dict) and set(bundle) == set(FORMATS), "exact two-format case bundle required")
        self.cases, self.corpora = {}, {}
        self.local_verified, self.live_verified = set(), set()
        self.execution = "small-runner-" + uuid.uuid4().hex
        self.runs = {}
        for fmt in FORMATS:
            need(isinstance(bundle[fmt], dict) and set(bundle[fmt]) == set(PHASES),
                 "each format requires discover and discover-values case IDs")
            for phase in PHASES:
                sha(bundle[fmt][phase])
                case = registry.get_case(bundle[fmt][phase]); verify(case, "case")
                corpus = registry.get_corpus(case["corpus_id"]); verify(corpus, "corpus")
                p = case["parameters"]; cp = corpus["parameters"]
                need(cp["format"] == fmt and p["format"] == fmt and p["operation"] == phase,
                     "case format/operation differs from runner slot")
                need(corpus["population_kind"] == "independent" and p["scope"]["kind"] == "all" and
                     p["C_paths"] == p["N"] == cp["N"] and p["C_versions"] == cp["N"],
                     "official small runner requires full independent N, not a candidate subset")
                need(cp["V_stored"] == 1 and p["V_stored"] == 1 and p["selection"] == {"kind": "latest"},
                     "official small runner requires stored V1 and Latest")
                need(p["output"] == output_contract(phase), "case output contract differs from runner")
                if phase == "discover-values":
                    for match in case["expected_matches"]:
                        sha(match["native_oracle"].get("value_sha256"))
                        sha(match["native_oracle"].get("mask_sha256"))
                self.cases[fmt, phase] = copy.deepcopy(case)
                previous = self.corpora.setdefault(fmt, copy.deepcopy(corpus))
                need(previous == corpus, "two phases must bind the same full corpus")

    def report_bindings(self):
        return {"corpora": {fmt: {"corpus_id": corpus["corpus_id"], "parameters": copy.deepcopy(corpus["parameters"])}
                            for fmt, corpus in self.corpora.items()},
                "cases": {fmt: {phase: {"case_id": self.cases[fmt, phase]["case_id"],
                    "corpus_id": self.cases[fmt, phase]["corpus_id"],
                    "parameters": copy.deepcopy(self.cases[fmt, phase]["parameters"])} for phase in PHASES}
                          for fmt in FORMATS}}

    def verify_inputs(self, fmt, *, spec, manifest, manifest_path, raw_root, fs_root,
                      logical_prefix, versions, preparation_sha256):
        corpus = self.corpora[fmt]; cp = corpus["parameters"]
        need(canonical(spec) == canonical(cp["generation"]) == canonical(manifest["spec"]), "runner spec differs from full corpus generation")
        need(spec["N"] == cp["N"] == manifest["actual"]["N"] == len(manifest["sources"]),
             "runner N differs from full independently published inventory")
        need(file_sha(manifest_path) == corpus["source_evidence"]["manifest_sha256"],
             "manifest bytes differ from accepted corpus")
        need(preparation_sha256 == corpus["publication_evidence"]["receipt_sha256"],
             "preparation receipt differs from accepted publication")
        sources = {s["file"]: s for s in manifest["sources"]}
        need(len(sources) == len(manifest["sources"]), "duplicate manifest source")
        inventory = {r["logical_file"]: r for r in corpus["inventory"]}
        need(set(sources) == set(inventory), "manifest is not the exact full corpus")
        expected_versions = {}
        for logical, record in inventory.items():
            source = sources[logical]
            need(record["ordinal"] == 1 and source["bytes"] == record["bytes"] and
                 source["sha256"] == record["sha256"] and canonical(source["units"]) == canonical(record["units"]),
                 "manifest content/native oracle differs from accepted corpus")
            need(record["binding"]["path"] == logical_prefix.rstrip("/") + "/" + logical,
                 "publication prefix/path differs from accepted corpus")
            expected_versions[record["binding"]["path"]] = record["binding"]["version"]
            for root in (raw_root, fs_root):
                root = Path(root).resolve(strict=True); path = root / logical
                need(path.is_file() and not path.is_symlink() and path.resolve().is_relative_to(root),
                     "source must be an owned regular file within its declared root")
                need(path.stat().st_size == record["bytes"] and file_sha(path) == record["sha256"],
                     "raw/publication copy bytes differ from accepted corpus")
        need(canonical(versions) == canonical(expected_versions), "preparation publication map differs from accepted corpus")
        for phase in PHASES:
            p = self.cases[fmt, phase]["parameters"]
            payload = spec["payload"]
            shape = payload["shape"] if fmt == "hdf5" else [payload["Nj"], payload["Ni"]]
            need(canonical(p["predicate"]) == canonical(manifest["query"]) and p["roi"] == {
                "kind": "whole", "origin": [0] * len(shape), "shape": shape},
                "case predicate/whole-unit geometry differs from measured request")
        self.local_verified.add(fmt)

    def verify_publication(self, fmt, versions):
        need(fmt in self.local_verified, "local full-corpus verification required first")
        expected = {r["binding"]["path"]: r["binding"]["version"] for r in self.corpora[fmt]["inventory"]}
        need(canonical(versions) == canonical(expected), "live publication map differs from frozen case")
        self.live_verified.add(fmt)

    def run_metadata(self, fmt, phase, arm, campaign, *, binary_sha256, runner_sha256,
                     engine, clock, cache, environment, campaigns, repeats):
        need(fmt in self.live_verified, "live exact publication must be verified before measurement")
        key = fmt, phase, arm, campaign
        case = self.cases[fmt, phase]
        run = build_run(case, execution=self.execution, arm=arm,
            implementation={"binary_sha256": binary_sha256, "runner_sha256": runner_sha256, "engine": engine},
            clock=clock, cache=cache, environment=environment,
            replication={"campaign": campaign, "campaigns": campaigns, "repeats": repeats,
                         "warmup_repeat": 0, "ordering": "campaign-rotated arms within repeat/phase"})
        if key not in self.runs:
            self.registry.register_run(run)
            self.runs[key] = run
        else:
            need(self.runs[key] == run, "implementation/protocol changed inside one measurement run")
        config = {k: copy.deepcopy(run[k]) for k in ("clock", "cache", "environment", "replication")}
        config["replication"].pop("campaign")
        return {"case_id": run["case_id"], "corpus_id": run["corpus_id"], "run_id": run["run_id"],
                "implementation": copy.deepcopy(run["implementation"]),
                "implementation_id": digest(run["implementation"]),
                "measurement_config": config, "measurement_config_id": digest(config)}


def result_group_key(row):
    """Never merge an old/unbound row, another case, or another implementation."""
    for field in ("case_id", "corpus_id", "run_id", "implementation_id", "measurement_config_id"):
        sha(row.get(field))
    need(isinstance(row.get("implementation"), dict) and
         row["implementation_id"] == digest(row["implementation"]), "result implementation binding differs")
    need(isinstance(row.get("measurement_config"), dict) and
         row["measurement_config_id"] == digest(row["measurement_config"]), "result measurement configuration differs")
    return (row["format"], row["phase"], row["arm"], row["case_id"],
            row["corpus_id"], row["implementation_id"], row["measurement_config_id"])


def registered_result_group_key(row, registry):
    key = result_group_key(row)
    run = registry.get_run(row["run_id"]); verify(run, "run")
    case = registry.get_case(run["case_id"]); verify(case, "case")
    need(row["case_id"] == run["case_id"] and row["corpus_id"] == run["corpus_id"] and
         row["format"] == case["parameters"]["format"] and
         row["phase"] == case["parameters"]["operation"] and row["arm"] == run["arm"] and
         type(row["campaign"]) is int and row["campaign"] == run["replication"]["campaign"],
         "result slot differs from registered run/case")
    need(canonical(row["implementation"]) == canonical(run["implementation"]),
         "result implementation differs from registered run")
    config = {k: copy.deepcopy(run[k]) for k in ("clock", "cache", "environment", "replication")}
    config["replication"].pop("campaign")
    need(canonical(row["measurement_config"]) == canonical(config),
         "result protocol differs from registered run")
    return key
