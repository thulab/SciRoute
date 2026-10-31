#!/usr/bin/env python3
"""Measure registration preparation using the fixed 50-step Electricity trace.

Uses the cache ablation's daemon lifecycle and restores the original mount in
finally. It reuses saved local controls only for correctness, not timing claims.
"""
import argparse
import json
from pathlib import Path

from run_cache_parallel_ablation import Mount, save, train
from compare_public_ab import compare
from summarize_query_profile import client_requests, read_server_log, summarize


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("work-dir", "mount", "socket", "binary", "fallback", "output", "controls"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--daemon-pid", type=int, required=True)
    parser.add_argument("--source-version", type=int, required=True)
    parser.add_argument("--gpu", default="0")
    parser.add_argument("--registration-mode", choices=("full", "prepared", "prepared_compact"), default="full")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    mount = Mount(args)
    settings = {"query.cache.max_entries": 16384, "query.cache.max_inflight": 256,
                "query.cache.worker_threads": 4, "query.cache.memory_budget_bytes": 1073741824,
                "query.training.read_parallelism": 4}
    original = Path(mount.original_config).read_text()
    base = "\n".join(line for line in original.splitlines()
                     if line.split("=", 1)[0].strip() not in settings)
    config = args.output / "profile.properties"
    config.write_text(base + "\n" + "\n".join(f"{k}={v}" for k,v in settings.items()) + "\n")
    config.chmod(0o600)
    results = []
    try:
        for model in ("patchtst", "patchtsmixer"):
            profile = args.output / f"{model}-profile.jsonl"
            mount.start(config, profile, model)
            control = json.loads((args.controls / f"r1-{model}-local.json").read_text())
            for temperature in ("cold", "warm"):
                tag = f"{model}-{temperature}"
                report = train(args, model, "iotdbfs", tag)
                checked = compare(control, report)
                save(args.output / f"{tag}-comparison.json", checked)
                if not checked["passed"]:
                    raise RuntimeError(f"correctness failure: {tag}")
                summary = summarize(read_server_log(profile), client_requests(report, "train"))
                save(args.output / f"{tag}-summary.json", summary)
                if summary["request_count"] != 51 or summary["excluded_count"]:
                    raise RuntimeError(f"incomplete profile: {tag}")
                results.append({"run": tag, "train_wall_seconds": report["train_wall_seconds"],
                    "passed": True,
                    "request_json_bytes": sum(x["request_json_bytes"] for x in client_requests(report,"train")),
                    "server_seconds": {k:v["sum"] for k,v in summary["server_seconds"].items()},
                    "client_seconds": {k:v["sum"] for k,v in summary["client_seconds"].items()},
                    "runtime_delta": {k:v["sum"] for k,v in summary["runtime_delta"].items()}})
                save(args.output / "results.json", results)
    finally:
        try:
            mount.start(mount.original_config, None, "restored")
        except Exception:
            mount.start(mount.original_config, None, "fallback", args.fallback)
        print(f"RESTORED daemon pid={mount.pid}, original configuration, profile off", flush=True)


if __name__ == "__main__":
    main()
