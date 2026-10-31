#!/usr/bin/env python3
"""Run fixed-trace Electricity cache/read-lane ablations on an existing mount.

Restarts the daemon between cold runs; never drops OS or database caches. Saves
configuration variants separately and restores the original mount configuration
in finally. Credentials stay in memory, inherited from the original daemon.
Run only on a dedicated, otherwise idle experiment mount.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

from compare_public_ab import compare
from summarize_query_profile import client_requests, read_server_log, summarize

ARMS = (("baseline", 1024, 1), ("cache", 16384, 1),
        ("cache_p2", 16384, 2), ("cache_p4", 16384, 4))


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


class Mount:
    def __init__(self, args):
        self.args, self.pid, self.child = args, args.daemon_pid, None
        self.env = os.environ.copy()
        proc = Path(f"/proc/{self.pid}")
        if str(args.mount).encode() not in proc.joinpath("cmdline").read_bytes().split(b"\0"):
            raise ValueError("daemon PID does not serve the requested mount")
        for item in proc.joinpath("environ").read_bytes().split(b"\0"):
            if b"=" in item:
                key, value = item.split(b"=", 1)
                self.env[key.decode()] = value.decode()
        self.original_config = self.env["IOTDB_CONFIG_FILE"]

    def stop(self):
        if subprocess.run(["mountpoint", "-q", str(self.args.mount)]).returncode == 0:
            subprocess.run(["fusermount", "-u", str(self.args.mount)], check=True)
        for _ in range(300):
            if self.child is not None:
                if self.child.poll() is not None:
                    self.pid = None
                    return
            elif self.pid is None or not Path(f"/proc/{self.pid}").exists():
                self.pid = None
                return
            elif Path(f"/proc/{self.pid}/stat").read_text().split(")", 1)[1].split()[0] == "Z":
                self.pid = None
                return
            time.sleep(.1)
        raise RuntimeError("daemon has not released the mount after unmount")

    def start(self, config, profile, tag, binary=None):
        self.stop()
        env = dict(self.env, IOTDB_CONFIG_FILE=str(config))
        env.pop("IOTDBFS_QUERY_PROFILE_JSONL", None)
        if profile is not None:
            env["IOTDBFS_QUERY_PROFILE_JSONL"] = str(profile)
        with (self.args.output / f"{tag}-mount.log").open("ab") as log:
            self.child = subprocess.Popen(
                [str(binary or self.args.binary), "-f", str(self.args.mount)],
                env=env, stdin=subprocess.DEVNULL, stdout=log,
                stderr=subprocess.STDOUT, start_new_session=True)
        self.pid = self.child.pid
        for _ in range(300):
            if self.child.poll() is not None:
                raise RuntimeError(f"daemon failed: see {tag}-mount.log")
            if subprocess.run(["mountpoint", "-q", str(self.args.mount)]).returncode == 0:
                try:
                    with socket.socket(socket.AF_UNIX) as connection:
                        connection.settimeout(.2)
                        connection.connect(str(self.args.socket))
                    return
                except OSError:
                    pass
            time.sleep(.1)
        raise RuntimeError("daemon did not become ready")


def train(args, model, backend, tag):
    report = args.output / f"{tag}.json"
    command = [sys.executable, str(Path(__file__).with_name("train_hf.py")),
               "--work-dir", str(args.work_dir), "--dataset", "UCI_Electricity_370",
               "--backend", backend, "--model", model, "--max-steps", "50",
               "--batch-size", "2", "--eval-windows", "4", "--seed", "20260929",
               "--output-dir", str(args.output / "runs" / tag), "--report", str(report)]
    if backend == "iotdbfs":
        command += ["--mount", str(args.mount), "--socket", str(args.socket),
                    "--source-version", str(args.source_version),
                    "--registration-mode", getattr(args, "registration_mode", "full"),
                    "--conversion-mode", getattr(args, "conversion_mode", "batch")]
    print(f"START {tag}", flush=True)
    with (args.output / f"{tag}.log").open("w") as log:
        subprocess.run(command, check=True, env=dict(os.environ, CUDA_VISIBLE_DEVICES=args.gpu),
                       stdout=log, stderr=subprocess.STDOUT, timeout=600)
    result = json.loads(report.read_text())
    print(f"DONE {tag}: train={result['train_wall_seconds']:.3f}s", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("work-dir", "mount", "socket", "binary", "fallback", "output"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--daemon-pid", type=int, required=True)
    parser.add_argument("--source-version", type=int, required=True)
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--gpu", default="0")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    mount = Mount(args)
    original = Path(mount.original_config).read_text()
    settings = {"query.cache.max_entries": 0, "query.cache.max_inflight": 256,
                "query.cache.worker_threads": 4, "query.cache.memory_budget_bytes": 1073741824,
                "query.training.read_parallelism": 0}
    base = "\n".join(line for line in original.splitlines()
                     if line.split("=", 1)[0].strip() not in settings)
    results = []
    try:
        for repetition in range(1, args.repetitions + 1):
            for model in ("patchtst", "patchtsmixer"):
                local = train(args, model, "local", f"r{repetition}-{model}-local")
                # Reverse order on the second replication to reduce time-order confounding.
                arms = ARMS if repetition % 2 else tuple(reversed(ARMS))
                for arm, entries, lanes in arms:
                    tag = f"r{repetition}-{model}-{arm}"
                    config = args.output / f"{tag}.properties"
                    values = dict(settings, **{"query.cache.max_entries": entries,
                                              "query.training.read_parallelism": lanes})
                    config.write_text(base + "\n" + "\n".join(f"{k}={v}" for k,v in values.items()) + "\n")
                    config.chmod(0o600)
                    profile = args.output / f"{tag}-profile.jsonl"
                    mount.start(config, profile, tag)
                    for temperature in ("cold", "warm"):
                        run = f"{tag}-{temperature}"
                        result = train(args, model, "iotdbfs", run)
                        comparison = compare(local, result)
                        save(args.output / f"{run}-comparison.json", comparison)
                        if not comparison["passed"]:
                            raise RuntimeError(f"A/B correctness failed: {run}")
                        records = read_server_log(profile)
                        summary = summarize(records, client_requests(result, "train"))
                        save(args.output / f"{run}-profile-summary.json", summary)
                        if summary["excluded_count"] or summary["request_count"] != 51:
                            raise RuntimeError(f"incomplete/overlapping profile: {run}")
                        servers = [item["server"] for item in summary["requests"]]
                        for server in servers:
                            actual = server["runtime_config"]
                            if actual["max_entries"] != entries or actual["max_inflight"] != 256 or \
                               server["server_training_execution"]["read_parallelism"] != lanes:
                                raise RuntimeError(f"configuration mismatch: {run}")
                        results.append({"run": run, "model": model, "arm": arm,
                            "repetition": repetition, "temperature": temperature,
                            "train_wall_seconds": result["train_wall_seconds"],
                            "local_train_wall_seconds": local["train_wall_seconds"],
                            "eval_wall_seconds": result["eval_wall_seconds"],
                            "passed": comparison["passed"],
                            "server_seconds": {k:v["sum"] for k,v in summary["server_seconds"].items()},
                            "runtime_delta": {k:v["sum"] for k,v in summary["runtime_delta"].items()},
                            "payload_reads_peak": max(s["runtime_after"]["payload_reads_peak"] for s in servers),
                            "runtime_final": servers[-1]["runtime_after"]})
                        save(args.output / "results.json", results)
    finally:
        try:
            mount.start(mount.original_config, None, "restored")
        except Exception:
            mount.start(mount.original_config, None, "fallback", args.fallback)
        print(f"RESTORED daemon pid={mount.pid}, original configuration, profile off", flush=True)


if __name__ == "__main__":
    main()
