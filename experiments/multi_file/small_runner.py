#!/usr/bin/env python3
"""Three-arm immutable-corpus pilot, with native FS and persistent SQLite index."""
from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import select
import statistics
import socket
import struct
import subprocess
import sys
import time
from urllib.parse import quote
import uuid

import numpy as np
import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from experiments.identity.session import measurement_session, registered_result_group_key

import manager
from common import load_manifest, matches, sha_file

SINGLE = Path(__file__).resolve().parents[1] / "single_file"
sys.path.insert(0, str(SINGLE))
import runner as sf
from environment import connection_environment


class NativeWorker:
    def __init__(self, probe, env):
        self.process = subprocess.Popen([str(probe)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, text=True, env=env)

    def call(self, request):
        start = time.perf_counter_ns()
        self.process.stdin.write(json.dumps(request, separators=(",", ":")) + "\n")
        self.process.stdin.flush()
        ready, _, _ = select.select([self.process.stdout], [], [], 180)
        sf.require(bool(ready), "native worker timeout")
        line = self.process.stdout.readline(1048577)
        sf.require(line and len(line) <= 1048576, "native worker failed or response exceeds bound")
        response = json.loads(line)
        response["worker_client_ns"] = time.perf_counter_ns() - start
        sf.require(response.get("ok"), "native worker failed: " + str(response.get("error")))
        return response

    def close(self):
        self.process.stdin.close()
        self.process.wait(timeout=20)
        sf.require(self.process.returncode == 0, "native worker exited unsuccessfully")
        self.process.stdout.close()
        self.process.stderr.close()


def ipc(context, response, values=False):
    """Return owned arrays and batch provenance; release mapped owners here."""
    path = Path(response["path"])
    sf.require(path.is_relative_to(context["mount"]), "IPC path outside owned mount")
    result, size = [], 0
    with pa.memory_map(str(path), "r") as stream:
        size = stream.size()
        reader = pa.ipc.open_stream(stream)
        try:
            for item in reader.iter_batches_with_custom_metadata():
                batch = item.batch
                meta = dict(item.custom_metadata or {})
                if values:
                    sf.require(batch.num_columns == 1 and batch.column(0).type == pa.float32(), "value dtype differs")
                    array = np.array(batch.column(0).to_numpy(zero_copy_only=False), dtype="<f4", copy=True)
                    mask = np.array(batch.column(0).is_valid().to_numpy(zero_copy_only=False), dtype="u1", copy=True)
                    result.append({"meta": meta, "array": array, "mask": mask})
                else:
                    result.append({"meta": meta, "columns": [column.to_pylist() for column in batch.columns]})
                del batch, item
        finally:
            reader.close()
            del reader
    return result, size


def profile_call(context, request, offset, values=False):
    begin = time.perf_counter_ns()
    response, control_ns = query_control(context["socket"], request)
    ready = time.perf_counter_ns()
    batches, ipc_bytes = (ipc(context, response, values) if "path" in response else ([], 0))
    end = time.perf_counter_ns()
    profiles, offset = wait_profiles(context["root"] / "queries.jsonl", offset)
    sf.require(len(profiles) == 1 and profiles[0].get("ok"), "one successful profile required per request")
    instrumentation_ns = time.perf_counter_ns() - end
    return response, batches, {"total_ns": end - begin, "control_ns": control_ns,
        "ipc_read_owned_copy_ns": end - ready, "ipc_bytes": ipc_bytes,
        "instrumentation_ns": instrumentation_ns, "server_profile": profiles[0]}, offset


def wait_profiles(path, offset, timeout=5):
    """The server writes its profile after the socket response; don't resubmit."""
    deadline = time.monotonic() + timeout
    while True:
        if path.exists():
            with path.open("rb") as stream:
                stream.seek(offset)
                data = stream.read(1048577)
            sf.require(len(data) <= 1048576, "profile frame exceeds bound")
            end = data.rfind(b"\n")
            if end >= 0:
                complete = data[:end+1]
                return [json.loads(line) for line in complete.splitlines() if line.strip()], offset + len(complete)
        sf.require(time.monotonic() < deadline, "profile sink did not finish after query response")
        time.sleep(.001)


FIELDS = ["short_name", "type_of_level", "level", "step", "validity_date", "validity_time", "offset"]


def query_control(path, request):
    started = time.perf_counter_ns()
    payload = json.dumps(request, separators=(",", ":"), allow_nan=False).encode()
    sf.require(len(payload) <= 1048576, "request exceeds frame budget")
    with socket.socket(socket.AF_UNIX) as stream:
        stream.settimeout(300)
        stream.connect(str(path))
        stream.sendall(struct.pack(">I", len(payload)) + payload)
        length, = struct.unpack(">I", sf.receive(stream, 4))
        sf.require(0 < length <= 1048576, "response exceeds frame budget")
        response = json.loads(sf.receive(stream, length))
    sf.require(response.get("ok") is True, "registration failed: " + json.dumps(response, sort_keys=True))
    return response, time.perf_counter_ns() - started


def selector(prefix, fmt, version=None, exact=None):
    return {"paths": [{"kind": "subtree" if exact is None else "exact", "path": prefix if exact is None else exact}],
        "versions": {"kind": "latest"} if version is None else {"kind": "exact", "exact": version},
        "formats": {"adapters": "listed", "adapter_ids": [fmt], "include_byte_passthrough": False}}


def discovery_request(prefix, manifest, predicate=True):
    fmt = manifest["format"]
    expression = manifest["query_template"]["predicate"]
    if fmt == "hdf5":
        request = {"sources": selector(prefix, fmt)}
        if predicate:
            request["predicate"] = expression
        return {"kind": "dataset_catalog", "request": request}
    subset = {"projection": ["grib.message." + name for name in FIELDS]}
    if predicate:
        subset["predicate"] = expression
    return {"kind": "typed", "request": {"sources": selector(prefix, fmt), "subset": subset}}


def discovered(response, batches, fmt, prefix):
    rows = []
    if fmt == "hdf5":
        for row in response["datasets"]:
            rows.append({"path": row["path"], "version": row["version"], "dataset": row["dataset_path"]})
    else:
        for batch in batches:
            meta = batch["meta"]
            sf.require(meta[b"iotdbfs.sources"] == b"1", "missing exact provenance")
            path, version = meta[b"iotdbfs.source.0.path"].decode(), int(meta[b"iotdbfs.source.0.version"])
            for fields in zip(*batch["columns"]):
                rows.append({"path": path, "version": version, "offset": fields[-1]})
    for row in rows:
        sf.require(row["path"].startswith(prefix + "/"), "source escapes scoped subtree")
        row["relative"] = row["path"][len(prefix) + 1:]
    return rows


def key(row, fmt):
    return (row.get("relative", row.get("path")), row["dataset"] if fmt == "hdf5" else row["offset"])


def expected(manifest):
    result = {}
    for source in manifest["sources"]:
        for unit in source["units"]:
            if matches(unit["attributes"], manifest["query"]):
                relative = source["file"].removeprefix("raw/")
                result[(relative, unit["dataset"] if manifest["format"] == "hdf5" else unit["offset"])] = unit
    return result


def check_native(response, manifest, oracle, values):
    actual = {key(row, manifest["format"]): row for row in response["units"]}
    sf.require(len(actual) == len(response["units"]) and set(actual) == set(oracle), "native discovery identities differ")
    if values:
        for identifier, row in actual.items():
            unit = oracle[identifier]
            sf.require(row["rows"] == unit["oracle"]["elements"] and
                       row["value_sha256"] == unit["oracle"]["value_sha256"], "native value oracle differs")
            mask = unit["oracle"].get("mask_sha256", hashlib.sha256(b"\1" * row["rows"]).hexdigest())
            sf.require(row["mask_sha256"] == mask, "native validity mask differs")


def check_values(batches, rows, fmt, versions, oracle):
    by_path = {row["path"]: key(row, fmt) for row in rows}
    owned = defaultdict(list)
    for batch in batches:
        meta = batch["meta"]
        sf.require(meta[b"iotdbfs.sources"] == b"1", "missing batch provenance")
        path = meta[b"iotdbfs.source.0.path"].decode()
        sf.require(path in by_path and int(meta[b"iotdbfs.source.0.version"]) == versions[path], "source/version differs")
        owned[by_path[path]].append(batch)
    sf.require(set(owned) == set(oracle), "value stream omitted or added a matching unit")
    for identifier, parts in owned.items():
        unit = oracle[identifier]
        values = np.concatenate([p["array"] for p in parts])
        mask = np.concatenate([p["mask"] for p in parts])
        sf.require(len(values) == unit["oracle"]["elements"] and
            hashlib.sha256(values.tobytes()).hexdigest() == unit["oracle"]["value_sha256"], "IoTDB value hash differs")
        wanted = unit["oracle"].get("mask_sha256", hashlib.sha256(b"\1" * len(mask)).hexdigest())
        sf.require(hashlib.sha256(mask.tobytes()).hexdigest() == wanted, "IoTDB mask hash differs")


def iotdb_query(context, manifest, prefix, versions, offset, values):
    start = time.perf_counter_ns()
    response, batches, discovery, offset = profile_call(context, discovery_request(prefix, manifest), offset)
    rows = discovered(response, batches, manifest["format"], prefix)
    ready = time.perf_counter_ns()
    # Count mapping/schema work in the client; keep hash checks outside clocks.
    value_batches, value_calls = [], []
    if values:
        groups = defaultdict(list)
        if manifest["format"] == "hdf5":
            for row in rows:
                groups[(row["dataset"], row["path"], row["version"])].append(row)
        else:
            groups["grib"].extend(rows)
        for dataset, group in groups.items():
            if manifest["format"] == "hdf5":
                dataset, path, version = dataset
                sources = selector(prefix, "hdf5", version, path)
            else:
                sources = selector(prefix, "grib")
                sources["paths"] = [{"kind": "exact", "path": row["path"]} for row in group]
            # The immutable experiment permits latest selection; every emitted
            # exact version is checked against the separately frozen locator.
            subset = ({"projection": ["hdf5.value:" + quote(dataset, safe="")]} if manifest["format"] == "hdf5" else
                      {"projection": ["grib.data.value"], "predicate": manifest["query_template"]["predicate"]})
            request = {"kind": "typed", "request": {"sources": sources, "subset": subset}}
            _, parts, stats, offset = profile_call(context, request, offset, True)
            value_batches.extend(parts)
            value_calls.append(stats)
    ended = time.perf_counter_ns()
    oracle = expected(manifest)
    sf.require(len(rows) == len(oracle) and {key(row, manifest["format"]) for row in rows} == set(oracle), "IoTDB discovered wrong units")
    sf.require(all(versions[row["path"]] == row["version"] for row in rows), "latest source changed during frozen experiment")
    if values:
        check_values(value_batches, rows, manifest["format"], versions, oracle)
    return {"total_ns": ended-start-sum(c["instrumentation_ns"] for c in [discovery, *value_calls]),
            "discovery_client_ns": ready-start-discovery["instrumentation_ns"],
            "selected_value_client_ns": ended-ready-sum(c["instrumentation_ns"] for c in value_calls), "selected_units": len(rows),
            "discovery": discovery, "value_calls": value_calls,
            "server_total_ns": sum(c["server_profile"]["server_total_ns"] for c in [discovery, *value_calls]),
            "registration_requests": 1 + len(value_calls),
            "iotdb_sql_queries": sum(c["server_profile"].get("iotdb_delta", {}).get("query_calls", 0) for c in [discovery, *value_calls])}, offset


def publish(context, root, manifest, prefix, fs_root):
    root, fs_root = Path(root), Path(fs_root)
    files = []
    timings = {"fs": {}, "iotdb": {}}
    for arm in ("fs", "iotdb"):
        started = time.perf_counter_ns()
        base = fs_root if arm == "fs" else context["mount"] / prefix.lstrip("/")
        directories = {base}
        for source in manifest["sources"]:
            path = base / source["file"]
            directories.update(path.parents[p] for p in range(len(path.parents)) if path.parents[p].is_relative_to(base))
            path.parent.mkdir(parents=True, exist_ok=True)
        setup_ns = time.perf_counter_ns()-started
        writes=[]
        for source in manifest["sources"]:
            writes.append(sf.checked_copy(root / source["file"], base / source["file"]))
        timings[arm] = {"directory_setup_ns": setup_ns, "files": writes,
                        "file_publish_sum_ns": sum(w["total_ns"] for w in writes),
                        "total_ns": time.perf_counter_ns()-started, "directory_count": len(directories),
                        "parent_directory_fsync": False}
    return timings


def summarize(records, registry):
    groups = defaultdict(list)
    for row in records:
        groups[registered_result_group_key(row, registry)].append(row)
    result = []
    for (fmt, phase, arm, case_id, corpus_id, implementation_id, measurement_config_id), rows in sorted(groups.items()):
        warm = [r for r in rows if r["repeat"] > 0]
        sf.require(bool(warm), "summary requires measured samples after warmup")
        result.append({"format": fmt, "phase": phase, "arm": arm,
            "case_id": case_id, "corpus_id": corpus_id, "implementation_id": implementation_id,
            "implementation": rows[0]["implementation"], "measurement_config_id": measurement_config_id,
            "measurement_config": rows[0]["measurement_config"], "run_ids": sorted({r["run_id"] for r in rows}),
            "observations": len(rows), "warm_observations": len(warm),
            "all_p50_ns": statistics.median(r["total_ns"] for r in rows),
            "warm_p50_ns": statistics.median(r["total_ns"] for r in warm),
            "first_by_campaign_ns": [r["total_ns"] for r in rows if r["repeat"] == 0],
            "warm_client_p50_ns": statistics.median(r.get("worker_client_ns", r["total_ns"]) for r in warm),
            "warm_sql_queries": statistics.median(r["iotdb_sql_queries"] for r in warm) if arm == "iotdb" else None})
    return result


def run(args):
    identity = measurement_session(args)
    specs = {fmt: manager.read_spec(path) for fmt, path in
             (("hdf5", args.hdf5_spec), ("grib", args.grib_spec))}
    sf.require(all(specs[fmt]["format"] == fmt for fmt in specs), "spec format mismatch")
    sf.require(all(specs["hdf5"][k] == specs["grib"][k]
                   for k in ("N", "K", "D", "depth", "seed", "tree_width", "query")), "paired spec controls differ")
    # Fail closed on local evidence before reading connection credentials or mounting.
    if identity is not None:
        accepted = json.loads(args.reuse_preparation.read_text())
        sf.require(accepted.get("prepared") is True, "completed existing preparation required")
        for fmt in ("hdf5", "grib"):
            source_root = args.reuse_work.resolve() / (fmt + "-data")
            identity.verify_inputs(fmt, spec=specs[fmt], manifest=load_manifest(source_root),
                manifest_path=source_root / "manifest.json", raw_root=source_root,
                fs_root=args.reuse_work.resolve() / (fmt + "-fs"),
                logical_prefix=accepted["logical_prefix"] + "/" + fmt,
                versions=accepted["preparation"][fmt]["versions"],
                preparation_sha256=sha_file(args.reuse_preparation))
    output=args.output.resolve();output.mkdir(parents=True,exist_ok=False)
    work=args.work.resolve();work.mkdir(parents=True,exist_ok=False)
    environment=json.loads(args.environment.read_text())
    daemon=Path(environment["daemon"])
    sf.require(sha_file(daemon)==environment["binaries"]["daemon"]["sha256"],"daemon identity changed")
    env=dict(os.environ,**connection_environment(args.source_pid,args.database))
    env["LD_LIBRARY_PATH"]=environment["runtime_library_path"]
    # Protect existing baseline namespace: new files live only under this scope.
    sf.require(args.database=="h09probe1790939889dbfc1f", "runner uses the existing idle experimental namespace")
    versions=json.loads(subprocess.check_output([str(args.native),"--versions"],env=env,text=True))
    sf.require(versions["hdf5"]=="1.14.4" and versions["plugins"] is False,"native library policy differs")
    links=subprocess.check_output(["ldd",str(args.native)],env=env,text=True)
    hdf_line=next(line for line in links.splitlines() if "libhdf5" in line)
    hdf_library=Path(hdf_line.split("=> ",1)[1].split(" (",1)[0]).resolve()
    sf.require(sha_file(hdf_library) in environment["binaries"]["daemon"]["hdf5_dependency_hashes"].values(),"native HDF5 differs from daemon")
    report={"protocol":"iotdbfs-multifile-small-campaign/1","complete":False,"campaigns":args.campaigns,
            "repeats":args.repeats,"database":args.database,"preparation":{},"records":[],
            "native_versions":versions,"daemon_sha256":sha_file(daemon),"native_sha256":sha_file(args.native),
            "native_hdf5_sha256":sha_file(hdf_library),"code_sha256":sha_file(__file__),
            "cache_policy":{"OS":"not dropped; preparation hashes warm source pages","FS":"no decoded-array cache, native file handles reopened per query", "FS_index":"persistent SQLite composite index, connection retained within campaign", "IoTDB":"fresh daemon each campaign; shared runtime retained within campaign"},
            "measurement":{"FS_total":"native clock; SHA/JSON/pipe outside; worker round-trip also reported", "IoTDB_total":"control socket + result mmap/IPC decode + owned arrays; validation SHA outside", "scope":"fixed immutable corpus, root user, no source/permission mutations; no cold-disk claim"}}
    if identity is not None:
        report["identity"] = {"case_bundle_sha256": sha_file(args.identity_cases),
            **identity.report_bindings(),
            "registry": str(args.identity_registry.resolve()), "execution": identity.execution}
        repo = Path(__file__).resolve().parents[2]
        # Bind active Python helpers as well as the runner; never copy connection env.
        report["public_source_closure_sha256"] = {relative: sha_file(repo / relative) for relative in (
            "experiments/multi_file/small_runner.py", "experiments/multi_file/common.py",
            "experiments/multi_file/manager.py", "experiments/multi_file/hdf5_data.py",
            "experiments/multi_file/grib_data.py", "experiments/single_file/runner.py",
            "experiments/single_file/environment.py", "experiments/identity/session.py",
            "experiments/identity/model.py", "experiments/identity/registry.py")}
    sf.save(output/"report.json",report)
    manifests={}
    report["controls"] = {k: specs["hdf5"][k] for k in ("N", "K", "D", "depth", "seed", "tree_width")}
    data_work = work
    if args.reuse_preparation:
        sf.require(args.reuse_work is not None, "reuse needs the original data work root")
        prior=json.loads(args.reuse_preparation.read_text())
        sf.require(prior["native_sha256"]==report["native_sha256"],"prepared native worker differs; frozen FS-index format requires the same worker")
        data_work=args.reuse_work.resolve()
        report["preparation"]=prior["preparation"]
        report["preparation_reused"]={"report":str(args.reuse_preparation.resolve()),
            "sha256":sha_file(args.reuse_preparation),"costs":"carried from the completed preparation, not re-incurred in this query run",
            "prepared_daemon_sha256":prior["daemon_sha256"],"query_daemon_sha256":report["daemon_sha256"],
            "implementation_policy":"corpus publication is fixed; daemon is a run treatment; native worker and existing FS-index remain frozen"}
        for fmt in ("hdf5","grib"):
            manager.verify(data_work/(fmt+"-data"))
            manifest=load_manifest(data_work/(fmt+"-data"))
            sf.require(manifest["spec"]==specs[fmt],"reused data spec differs")
            manifests[fmt]=manifest
            sf.save(output/(fmt+"-data-manifest.json"),manifest)
        prefix=prior["logical_prefix"]
    else:
        for fmt in ("hdf5","grib"):
            started=time.perf_counter_ns()
            spec=specs[fmt]
            manifest=manager.generate(spec,work/(fmt+"-data"));manifests[fmt]=manifest
            sf.save(output/(fmt+"-data-manifest.json"),manifest)
            report["preparation"][fmt]={"manager_preparation_ns":time.perf_counter_ns()-started,"raw_bytes":manifest["raw_bytes"],"actual":manifest["actual"]}
        prefix="/multi-file-"+uuid.uuid4().hex[:16]
    report["logical_prefix"]=prefix
    report["data_work"]=str(data_work)
    with sf.private_mount(daemon,work/"publisher-validation",env,args.writer_lock) as context:
        for fmt,manifest in manifests.items():
            logical=prefix+"/"+fmt
            prep=report["preparation"][fmt]
            if not args.reuse_preparation:
                prep["publication"]=publish(context,data_work/(fmt+"-data"),manifest,logical,data_work/(fmt+"-fs"))
            verified_started=time.perf_counter_ns()
            for source in manifest["sources"]:
                sf.require(sha_file(data_work/(fmt+"-fs")/source["file"])==source["sha256"],"FS publication bytes differ")
                sf.require(sha_file(context["mount"]/logical.lstrip("/")/source["file"])==source["sha256"],"IoTDB full published bytes differ")
            prep["query_run_publication_verification_ns"]=time.perf_counter_ns()-verified_started
            # Unfiltered locator resolves every file version outside online timing.
            response,batches,stats,_=profile_call(context,discovery_request(logical+"/raw",manifest,False),
                sf.profiles_since(context["root"]/"queries.jsonl",0)[1])
            rows=discovered(response,batches,fmt,logical+"/raw")
            mapping={row["path"]:row["version"] for row in rows}
            sf.require(len(mapping)==manifest["actual"]["N"] and len(rows)==manifest["spec"]["N"]*manifest["spec"]["D"],"published inventory differs")
            if args.reuse_preparation:
                sf.require(mapping==prep["versions"],"published versions changed after preparation")
            if identity is not None:
                identity.verify_publication(fmt, mapping)
            prep["versions"]=mapping;prep["query_run_source_binding"]=stats
            if not args.reuse_preparation:
                native=NativeWorker(args.native,env)
                try:
                    prep["fs_index_build"]=native.call({"op":"build","root":str(data_work/(fmt+"-fs/raw")),"format":fmt,
                        "index":str(data_work/(fmt+"-index.sqlite")),"predicate":manifest["query"]})
                finally:
                    native.close()
            sf.save(output/"report.json",report)
    report["prepared"] = True
    sf.save(output/"report.json",report)
    if args.prepare_only:
        print(json.dumps({"prepared": True, "controls": report["controls"]}), flush=True)
        return
    for campaign in range(args.campaigns):
        for fmt,manifest in manifests.items():
            native=NativeWorker(args.native,env)
            try:
                with sf.private_mount(daemon,work/f"c{campaign}-{fmt}",env,args.writer_lock) as context:
                    offset=0;oracle=expected(manifest)
                    for repeat in range(args.repeats):
                        for phase in ("discover","discover-values"):
                            values=phase=="discover-values"
                            arms=["fs-scan","fs-index","iotdb"]
                            arms=arms[campaign%3:]+arms[:campaign%3]
                            for arm in arms:
                                # Register outside the unchanged query clock, after live binding verification.
                                binding = identity.run_metadata(fmt, phase, arm, campaign,
                                    binary_sha256=report["daemon_sha256"] if arm == "iotdb" else report["native_sha256"],
                                    runner_sha256=report["code_sha256"],
                                    engine=("iotdb-indexed" if fmt == "hdf5" else "grib-typed") if arm == "iotdb" else arm,
                                    clock={"protocol": "multifile-owned-query/1", "unit": "ns", "phase": phase,
                                           "boundary": report["measurement"]["IoTDB_total" if arm == "iotdb" else "FS_total"]},
                                    cache=report["cache_policy"],
                                    environment={"capture_scope": "explicit host/source/dependency/publication fields; host load and complete environment not captured",
                                        "host": socket.gethostname(), "platform": sys.platform,
                                        "uid": os.getuid(), "gid": os.getgid(), "groups": sorted(os.getgroups()),
                                        "hdf5_dependency_sha256": report["native_hdf5_sha256"],
                                        "source_closure_sha256": report["public_source_closure_sha256"],
                                        "native_binary_sha256": report["native_sha256"],
                                        "publication": {"namespace": identity.corpora[fmt]["publication_evidence"]["namespace"],
                                            "receipt_sha256": sha_file(args.reuse_preparation),
                                            "logical_prefix": prefix + "/" + fmt,
                                            "head": {k: accepted.get("head", {}).get(k) for k in
                                                     ("namespace", "incarnation", "seq", "root", "commit")
                                                     if k in accepted.get("head", {})}}},
                                    campaigns=args.campaigns, repeats=args.repeats)
                                if arm=="iotdb":
                                    row,offset=iotdb_query(context,manifest,prefix+"/"+fmt+"/raw",report["preparation"][fmt]["versions"],offset,values)
                                else:
                                    row=native.call({"op":"query","root":str(data_work/(fmt+"-fs/raw")),"format":fmt,
                                        "mode":"scan" if arm=="fs-scan" else "index","values":values,
                                        "predicate":manifest["query"],"index":str(data_work/(fmt+"-index.sqlite"))})
                                    check_native(row,manifest,oracle,values)
                                row.update(campaign=campaign,repeat=repeat,format=fmt,phase=phase,arm=arm,**binding)
                                report["records"].append(row)
                            sf.save(output/"report.json",report)
                    print(json.dumps({"campaign":campaign,"format":fmt,"status":"passed"}),flush=True)
            finally:
                native.close()
    report["summary"]=summarize(report["records"], identity.registry);report["complete"]=True
    sf.save(output/"report.json",report)
    print(json.dumps({"complete":True,"records":len(report["records"]),"summary":report["summary"]}))


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--native",type=Path,required=True);p.add_argument("--environment",type=Path,required=True)
    p.add_argument("--database",default="h09probe1790939889dbfc1f");p.add_argument("--source-pid",type=int,default=1001546)
    p.add_argument("--writer-lock",type=Path,required=True);p.add_argument("--output",type=Path,required=True);p.add_argument("--work",type=Path,required=True)
    p.add_argument("--campaigns",type=int,default=3);p.add_argument("--repeats",type=int,default=5)
    p.add_argument("--reuse-preparation",type=Path);p.add_argument("--reuse-work",type=Path)
    p.add_argument("--hdf5-spec", type=Path, default=Path(__file__).parent/"specs/runner-hdf5-small.json")
    p.add_argument("--grib-spec", type=Path, default=Path(__file__).parent/"specs/runner-grib-small.json")
    p.add_argument("--identity-registry", type=Path)
    p.add_argument("--identity-cases", type=Path, help="strict grib/hdf5 discover and discover-values case-ID bundle")
    p.add_argument("--prepare-only", action="store_true")
    a=p.parse_args();sf.require(1<=a.campaigns<=5 and 2<=a.repeats<=20,"invalid bounded campaign size");run(a)
