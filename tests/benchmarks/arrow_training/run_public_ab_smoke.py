#!/usr/bin/env python3
"""Run one single-GPU, two-step public Arrow versus IoTDB-FS Trainer pair."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

from compare_public_ab import compare
from public_iotdbfs import source_version


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--mount", type=Path, required=True)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--dataset", choices=("ETTm1", "UCI_Electricity_370"), required=True)
    parser.add_argument("--model", choices=("patchtst", "patchtsmixer"), required=True)
    parser.add_argument("--source-version", type=int)
    parser.add_argument("--gpu", default="0")
    parser.add_argument("--seed", type=int, default=20260929)
    parser.add_argument("--max-steps", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--eval-windows", type=int)
    parser.add_argument("--tag", default="ab-smoke")
    args = parser.parse_args()
    if args.eval_windows is None:
        args.eval_windows = 8 if args.dataset == "ETTm1" else 4
    logical = "/" + json.loads(
        (args.work_dir / "manifests" / f"{args.dataset}-pb00.json").read_text()
    )["arrow"]["path"]
    version = args.source_version
    if version is None:
        version = source_version(args.socket, logical)
    prefix = f"{args.dataset}-{args.model}-{args.tag}"
    env = os.environ.copy()
    env["CUDA_VISIBLE_DEVICES"] = args.gpu
    reports = {}
    for backend in ("local", "iotdbfs"):
        report = args.work_dir / "manifests" / f"{prefix}-{backend}.json"
        command = [
            sys.executable, str(Path(__file__).with_name("train_hf.py")),
            "--work-dir", str(args.work_dir), "--dataset", args.dataset,
            "--backend", backend, "--model", args.model,
            "--max-steps", str(args.max_steps), "--batch-size", str(args.batch_size),
            "--eval-windows", str(args.eval_windows), "--seed", str(args.seed),
            "--output-dir", str(args.work_dir / "runs" / f"{prefix}-{backend}"),
            "--report", str(report),
        ]
        if backend == "iotdbfs":
            command += ["--mount", str(args.mount), "--socket", str(args.socket),
                        "--source-version", str(version)]
        subprocess.run(command, check=True, env=env)
        reports[backend] = json.loads(report.read_text())
    result = compare(reports["local"], reports["iotdbfs"])
    result["reports"] = {
        backend: f"manifests/{prefix}-{backend}.json" for backend in reports
    }
    result_path = args.work_dir / "manifests" / f"{prefix}-comparison.json"
    result_path.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    if not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
