#!/usr/bin/env python3
"""Paired F1/Q2 D1 loader and RegionalCNN-v1 training runs.

All timed runs replay an immutable no-replacement training trace. Reader
profiles are reported as measured; Q2 materializes a result before the socket
registration call returns, then maps and converts the IPC stream separately.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import resource
import statistics
import sys
import time

os.environ.setdefault("CUBLAS_WORKSPACE_CONFIG", ":4096:8")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

import numpy as np

from dataset import (FrozenERA5Dataset, fit_training_statistics, load_statistics,
                     load_trace, make_trace, save_statistics, save_trace)
from local_reader import LocalGribReader
from sample_contract import Catalog, VIEWS, digest


DEFAULT_SEEDS = {"V0": 20260930, "V1": 20260931,
                 "V2": 20260932, "V3": 20260933}


def _percentiles(values: list[int]) -> dict:
    if not values:
        return {"p50": None, "p95": None, "p99": None, "mean": None}
    ordered = np.asarray(sorted(values), dtype=np.float64)
    return {"p50": float(np.percentile(ordered, 50)),
            "p95": float(np.percentile(ordered, 95)),
            "p99": float(np.percentile(ordered, 99)),
            "mean": float(statistics.fmean(values))}


def _state_hash(model: object) -> str:
    result = hashlib.sha256()
    for name, value in model.state_dict().items():
        result.update(name.encode("utf-8"))
        result.update(value.detach().cpu().contiguous().numpy().tobytes())
    return result.hexdigest()


def _sync(torch: object, device: object) -> None:
    if device.type == "cuda":
        torch.cuda.synchronize(device)


def _collate(samples: list) -> dict:
    import torch

    started = time.perf_counter_ns()
    x = np.stack([sample.x for sample in samples])
    y = np.stack([sample.y for sample in samples])
    x_mask = np.stack([sample.x_mask for sample in samples])
    y_mask = np.stack([sample.y_mask for sample in samples])
    rows = np.asarray([sample.row for sample in samples], dtype=np.int64)
    stack_ns = time.perf_counter_ns() - started
    started = time.perf_counter_ns()
    tensors = {
        "x": torch.from_numpy(x), "y": torch.from_numpy(y),
        "x_mask": torch.from_numpy(x_mask),
        "y_mask": torch.from_numpy(y_mask),
        "rows": torch.from_numpy(rows),
    }
    tensor_ns = time.perf_counter_ns() - started
    profile: dict[str, int | float] = defaultdict(int)
    for sample in samples:
        for key, value in sample.reader_profile_delta.items():
            profile[key] += value
    return {
        **tensors,
        "sample_ids": [sample.sample_id for sample in samples],
        "fetch_ns": sum(sample.fetch_ns for sample in samples),
        "transform_ns": sum(sample.transform_ns for sample in samples),
        "collate_ns": stack_ns + tensor_ns,
        "tensor_ns": tensor_ns,
        "reader_profile_delta": dict(profile),
        "logical_value_bytes": x.nbytes + y.nbytes,
        "mask_bytes": x_mask.nbytes + y_mask.nbytes,
    }


def _make_reader(args: argparse.Namespace, catalog: Catalog):
    if args.backend == "f1":
        return LocalGribReader(catalog, max_cache_bytes=args.cache_bytes)
    if args.backend == "q2":
        from iotdb_reader import IoTDBGribReader

        if args.bindings is None or args.socket is None:
            raise ValueError("Q2 requires --bindings and --socket")
        bindings = json.loads(args.bindings.read_text())
        return IoTDBGribReader(catalog, bindings["sources"], args.socket)
    if args.backend == "z1":
        from zarr_backend import ZarrReader

        if args.zarr_store is None:
            raise ValueError("Z1 requires --zarr-store")
        return ZarrReader(args.zarr_store, expected_fingerprint=catalog.fingerprint)
    raise ValueError(f"unknown backend {args.backend}")


def _validate_contract(catalog: Catalog, contract: dict, view_id: str) -> None:
    # JSON turns the tuple-valued split bounds in Catalog.freeze() into lists.
    # Compare the canonical serialized content, not Python container types.
    if view_id not in VIEWS or digest(contract) != digest(catalog.freeze()):
        raise ValueError("frozen D1 contract differs from validated source and sample rules")


def _run(args: argparse.Namespace) -> dict:
    setup_started = time.perf_counter_ns()
    cpu_started = time.process_time_ns()
    contract = json.loads(args.contract.read_text())
    if args.mode == "freeze-trace":
        if args.trace_out is None:
            raise ValueError("freeze-trace requires --trace-out")
        seed = args.seed if args.seed is not None else DEFAULT_SEEDS[args.view]
        trace = make_trace(contract, args.view, seed, args.batch_size, args.steps)
        save_trace(args.trace_out, trace)
        return {"mode": "freeze-trace", "view": args.view,
                "snapshot_fingerprint": trace["snapshot_fingerprint"],
                "trace_id": trace["trace_id"],
                "sample_count": len(trace["sample_ids"]),
                "trace_path": str(args.trace_out)}
    if args.single_manifest is None or args.pressure_manifest is None:
        raise ValueError("training and statistics fitting require both source manifests")
    catalog = Catalog.from_paths(args.single_manifest, args.pressure_manifest, "d1")
    _validate_contract(catalog, contract, args.view)
    region = VIEWS[args.view]["region"]
    if args.mode == "fit-stats":
        if args.backend != "f1" or args.stats_out is None:
            raise ValueError("fit-stats requires --backend f1 and --stats-out")
        reader = LocalGribReader(catalog, max_cache_bytes=args.cache_bytes)
        fit_started = time.perf_counter_ns()
        artifact = fit_training_statistics(catalog, region, reader)
        fit_ns = time.perf_counter_ns() - fit_started
        save_statistics(args.stats_out, artifact)
        report = {"mode": "fit-stats", "view": args.view,
                  "region": region, "artifact_id": artifact["artifact_id"],
                  "snapshot_fingerprint": catalog.fingerprint,
                  "fit_ns": fit_ns, "total_wall_ns": time.perf_counter_ns() - setup_started,
                  "reader_profile": reader.profile() if hasattr(reader, "profile") else
                  {"decoded_messages": reader.decoded_messages}}
        if args.output_dir:
            args.output_dir.mkdir(parents=True, exist_ok=True)
            (args.output_dir / "summary.json").write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n")
        return report

    if args.stats is None or args.output_dir is None:
        raise ValueError("train/replay require --stats and --output-dir")
    artifact = load_statistics(args.stats, catalog, region)
    if args.trace:
        trace = load_trace(args.trace, contract, args.view, args.batch_size, args.steps)
        if args.seed is not None and args.seed != trace["seed"]:
            raise ValueError("--seed differs from the frozen trace seed")
        seed = trace["seed"]
    else:
        seed = args.seed if args.seed is not None else DEFAULT_SEEDS[args.view]
        trace = make_trace(contract, args.view, seed, args.batch_size, args.steps)
        if args.trace_out is None:
            raise ValueError("generated traces require --trace-out for reproducibility")
        save_trace(args.trace_out, trace)

    import torch
    from torch.utils.data import DataLoader
    from model import RegionalCNN, cosine_latitude_mse

    torch.set_num_threads(args.threads)
    torch.manual_seed(seed)
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    torch.backends.cudnn.benchmark = False
    torch.backends.cudnn.deterministic = True
    torch.use_deterministic_algorithms(True)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
    device = torch.device("cpu" if args.mode == "replay" else args.device)
    if args.mode == "train" and device.type == "cuda" and not torch.cuda.is_available():
        raise ValueError("CUDA device requested but unavailable")

    reader = _make_reader(args, catalog)
    dataset = FrozenERA5Dataset(contract, args.view, trace, artifact, reader)
    loader = DataLoader(dataset, batch_size=args.batch_size, shuffle=False,
                        num_workers=0, drop_last=True, collate_fn=_collate)
    model = None
    optimizer = None
    initial_weight_hash = None
    if args.mode == "train":
        model = RegionalCNN(len(dataset.channel_ordinals)).to(device=device,
                                                              dtype=torch.float32)
        optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3,
                                      weight_decay=1e-4)
        initial_weight_hash = _state_hash(model)
    if device.type == "cuda":
        torch.cuda.reset_peak_memory_stats(device)
    setup_ns = time.perf_counter_ns() - setup_started
    args.output_dir.mkdir(parents=True, exist_ok=True)
    records: list[dict] = []
    loop_started = time.perf_counter_ns()
    iterator = iter(loader)
    first_batch_ready_ns = None
    with (args.output_dir / "steps.jsonl").open("w") as stream:
        for step in range(args.steps):
            _sync(torch, device)
            step_started = time.perf_counter_ns()
            batch = next(iterator)
            wait_ns = time.perf_counter_ns() - step_started
            if first_batch_ready_ns is None:
                first_batch_ready_ns = time.perf_counter_ns() - setup_started
            h2d_ns = forward_backward_ns = optimizer_ns = 0
            loss_value = None
            if model is not None:
                _sync(torch, device)
                started = time.perf_counter_ns()
                x = batch["x"].to(device, non_blocking=False)
                y = batch["y"].to(device, non_blocking=False)
                y_mask = batch["y_mask"].to(device, non_blocking=False)
                rows = batch["rows"].to(device, non_blocking=False)
                _sync(torch, device)
                h2d_ns = time.perf_counter_ns() - started
                optimizer.zero_grad(set_to_none=True)
                _sync(torch, device)
                started = time.perf_counter_ns()
                prediction = model(x)
                loss = cosine_latitude_mse(prediction, y, y_mask, rows)
                loss.backward()
                _sync(torch, device)
                forward_backward_ns = time.perf_counter_ns() - started
                started = time.perf_counter_ns()
                optimizer.step()
                _sync(torch, device)
                optimizer_ns = time.perf_counter_ns() - started
                loss_value = float(loss.detach().cpu())
            profile = batch["reader_profile_delta"]
            record = {
                "step": step, "sample_ids": batch["sample_ids"],
                "batch_wait_ns": wait_ns,
                "fetch_ns": batch["fetch_ns"],
                "transform_ns": batch["transform_ns"],
                "collate_ns": batch["collate_ns"],
                "tensor_ns": batch["tensor_ns"],
                "h2d_ns": h2d_ns,
                "forward_backward_ns": forward_backward_ns,
                "optimizer_ns": optimizer_ns,
                "step_wall_ns": time.perf_counter_ns() - step_started,
                "loss": loss_value,
                "query_ipc_ns": profile.get("query_ns", 0),
                "query_count": profile.get("query_count", 0),
                "ipc_bytes": profile.get("ipc_bytes", 0),
                "decode_count": profile.get("decoded_messages", 0),
                "logical_value_bytes": batch["logical_value_bytes"],
                "mask_bytes": batch["mask_bytes"],
                "reader_profile_delta": profile,
            }
            records.append(record)
            stream.write(json.dumps(record, separators=(",", ":")) + "\n")
            stream.flush()
    loop_ns = time.perf_counter_ns() - loop_started
    profile_total: dict[str, int | float] = defaultdict(int)
    for record in records:
        for key, value in record["reader_profile_delta"].items():
            profile_total[key] += value
    summary = {
        "protocol": "iotdbfs-era5-regional-training/1",
        "mode": args.mode, "backend": args.backend, "view": args.view,
        "seed": seed, "batch_size": args.batch_size, "steps": args.steps,
        "device": str(device), "torch_version": torch.__version__,
        "cuda_version": torch.version.cuda,
        "snapshot_fingerprint": catalog.fingerprint,
        "statistics_artifact_id": artifact["artifact_id"],
        "statistics_fit_domain": artifact["fit_domain"],
        "statistics_fit_time": [artifact["fit_time_start"],
                                artifact["fit_time_end_exclusive"]],
        "trace_id": trace["trace_id"],
        "initial_weight_sha256": initial_weight_hash,
        "final_weight_sha256": _state_hash(model) if model else None,
        "setup_ns": setup_ns,
        "loop_ns": loop_ns,
        "total_wall_ns": time.perf_counter_ns() - setup_started,
        "process_cpu_ns": time.process_time_ns() - cpu_started,
        "process_peak_rss_bytes": (
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            * (1 if sys.platform == "darwin" else 1024)),
        "cuda_peak_allocated_bytes": (torch.cuda.max_memory_allocated(device)
                                      if device.type == "cuda" else None),
        "cuda_peak_reserved_bytes": (torch.cuda.max_memory_reserved(device)
                                     if device.type == "cuda" else None),
        "first_batch_ready_ns": first_batch_ready_ns,
        "step_wall_ns": _percentiles([r["step_wall_ns"] for r in records]),
        "batch_wait_ns": _percentiles([r["batch_wait_ns"] for r in records]),
        "phase_sum_ns": {key: sum(r[key] for r in records) for key in (
            "fetch_ns", "transform_ns", "collate_ns", "tensor_ns",
            "h2d_ns", "forward_backward_ns", "optimizer_ns")},
        "reader_profile_sum": dict(profile_total),
        "logical_value_bytes": sum(r["logical_value_bytes"] for r in records),
        "ipc_bytes": sum(r["ipc_bytes"] for r in records),
        "loss_first": records[0]["loss"], "loss_last": records[-1]["loss"],
    }
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("freeze-trace", "fit-stats", "replay", "train"),
                        required=True)
    parser.add_argument("--backend", choices=("f1", "q2", "z1"), default="f1")
    parser.add_argument("--single-manifest", type=Path)
    parser.add_argument("--pressure-manifest", type=Path)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--view", choices=tuple(VIEWS), default="V0")
    parser.add_argument("--stats", type=Path)
    parser.add_argument("--stats-out", type=Path)
    parser.add_argument("--trace", type=Path)
    parser.add_argument("--trace-out", type=Path)
    parser.add_argument("--steps", type=int, default=50)
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--device", default="cuda:0")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--cache-bytes", type=int, default=4 << 30)
    parser.add_argument("--bindings", type=Path)
    parser.add_argument("--socket", type=Path)
    parser.add_argument("--zarr-store", type=Path)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    if args.threads <= 0 or args.cache_bytes < 0:
        parser.error("threads must be positive and cache bytes nonnegative")
    print(json.dumps(_run(args), sort_keys=True))


if __name__ == "__main__":
    main()
