#!/usr/bin/env python3
"""Shared HF Trainer driver for local Arrow and IoTDB-FS public A/B runs."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import time

import numpy as np
import torch
from torch.utils.data import Dataset
from transformers import (
    PatchTSTConfig, PatchTSTForPrediction, PatchTSMixerConfig,
    PatchTSMixerForPrediction, Trainer, TrainingArguments,
)

from prepare_public_windows import verify_seal
from public_backends import LocalArrowWindowReader
from public_iotdbfs import IoTDBFSWindowReader, WindowArrays


class WindowIndexDataset(Dataset):
    def __init__(self, manifest: dict, split: str, limit: int = 0):
        verify_seal(manifest)
        self.manifest = manifest
        self.split = split
        item = manifest["splits"][split]
        self.first = item["first_anchor"]
        self.stride = item["stride"]
        self.count = min(item["count"], limit) if limit else item["count"]

    def __len__(self) -> int:
        return self.count

    def __getitem__(self, index: int) -> dict:
        if not 0 <= index < self.count:
            raise IndexError(index)
        anchor = self.first + index * self.stride
        return {"anchor": anchor, "split": self.split,
                "sample_id": f"{self.manifest['sample_id_prefix']}/{self.split}/{anchor}"}


class LocalBackend:
    def __init__(self, work_dir: Path, pb00: dict, normalization: dict,
                 windows: dict, channels: tuple[int, ...], cache_bytes: int):
        self.reader = LocalArrowWindowReader(
            work_dir / "arrow" / pb00["arrow"]["path"],
            row_index=pb00["arrow"]["rows"], max_cache_bytes=cache_bytes)
        self.length = windows["input_length"]
        self.horizon = windows["prediction_length"]
        self.channels = channels
        self.mean = np.asarray([normalization["statistics"][i]["mean"]
                                for i in channels], dtype=np.float64)
        self.stddev = np.asarray([normalization["statistics"][i]["stddev"]
                                  for i in channels], dtype=np.float64)

    def read_windows(self, anchors: list[int], _split: str) -> WindowArrays:
        n, length, horizon, width = len(anchors), self.length, self.horizon, len(self.channels)
        input_values = np.zeros((n, length, width), dtype=np.float32)
        input_valid = np.zeros((n, length, width), dtype=np.bool_)
        target_values = np.zeros((n, horizon, width), dtype=np.float32)
        target_valid = np.zeros((n, horizon, width), dtype=np.bool_)
        for index, anchor in enumerate(anchors):
            raw, valid = self.reader.read_window(anchor - length, length + horizon,
                                                 self.channels)
            scaled = ((raw.astype(np.float64) - self.mean) / self.stddev).astype(np.float32)
            scaled[~valid] = 0
            input_values[index], input_valid[index] = scaled[:length], valid[:length]
            target_values[index], target_valid[index] = scaled[length:], valid[length:]
        return WindowArrays(input_values, input_valid, target_values, target_valid)

    @property
    def metrics(self) -> dict:
        return self.reader.metrics

    def close(self) -> None:
        self.reader.close()


class IoTDBFSBackend:
    def __init__(self, socket_path: Path, mount: Path, pb00: dict,
                 normalization: dict, windows: dict, channels: tuple[int, ...],
                 source_version: int, registration_mode: str = "prepared_compact",
                 conversion_mode: str = "batch"):
        self.readers = {
            split: IoTDBFSWindowReader(
                socket_path=socket_path, mount=mount, pb00=pb00,
                normalization=normalization, windows=windows, split=split,
                channels=channels, source_version_exact=source_version,
                registration_mode=registration_mode, conversion_mode=conversion_mode)
            for split in ("train", "validation", "test")
        }

    def read_windows(self, anchors: list[int], split: str) -> WindowArrays:
        return self.readers[split].read_windows(anchors)

    @property
    def metrics(self) -> dict:
        return {split: reader.metrics for split, reader in self.readers.items()}

    def close(self) -> None:
        for reader in self.readers.values():
            reader.close()


class WindowCollator:
    def __init__(self, backend, model: str):
        self.backend = backend
        self.mask_name = "past_observed_mask" if model == "patchtst" else "observed_mask"
        self.traces: dict[str, list[str]] = {name: [] for name in ("train", "validation", "test")}
        self.fetch_seconds: dict[str, float] = {name: 0.0 for name in self.traces}
        self.batch_count: dict[str, int] = {name: 0 for name in self.traces}

    def __call__(self, features: list[dict]) -> dict[str, torch.Tensor]:
        if not features or len({feature["split"] for feature in features}) != 1:
            raise ValueError("collator requires one nonempty split per batch")
        split = features[0]["split"]
        anchors = [int(feature["anchor"]) for feature in features]
        start = time.perf_counter()
        arrays = self.backend.read_windows(anchors, split)
        self.fetch_seconds[split] += time.perf_counter() - start
        self.batch_count[split] += 1
        if not arrays.target_valid.all():
            raise ValueError("first public baseline requires a complete target mask")
        self.traces[split].extend(feature["sample_id"] for feature in features)
        return {
            "past_values": torch.from_numpy(arrays.input_values),
            "future_values": torch.from_numpy(arrays.target_values),
            self.mask_name: torch.from_numpy(arrays.input_valid),
        }


def make_model(name: str, channels: int, length: int, horizon: int):
    if name == "patchtst":
        return PatchTSTForPrediction(PatchTSTConfig(
            num_input_channels=channels, context_length=length,
            prediction_length=horizon, patch_length=16, patch_stride=16,
            num_hidden_layers=1, d_model=32, num_attention_heads=4,
            ffn_dim=64, scaling=None))
    if name == "patchtsmixer":
        return PatchTSMixerForPrediction(PatchTSMixerConfig(
            num_input_channels=channels, context_length=length,
            prediction_length=horizon, patch_length=16, patch_stride=16,
            num_layers=1, d_model=16, mode="mix_channel", dropout=0.0,
            scaling=None))
    raise ValueError(f"unknown model {name}")


def trace_summary(trace: list[str]) -> dict:
    digest = hashlib.sha256("\n".join(trace).encode("utf-8")).hexdigest()
    return {"count": len(trace), "sha256": digest, "sample_ids": trace}


def run(args: argparse.Namespace, *, backend_factory=None) -> dict:
    work_dir = args.work_dir
    manifests = work_dir / "manifests"
    pb00 = json.loads((manifests / f"{args.dataset}-pb00.json").read_text())
    normalization = json.loads((manifests / f"{args.dataset}-pb01-normalization.json").read_text())
    windows = json.loads((manifests / f"{args.dataset}-pb01-windows-L96-H24-S1.json").read_text())
    verify_seal(normalization)
    verify_seal(windows)
    if windows["normalization_artifact_id"] != normalization["artifact_id"]:
        raise ValueError("window manifest is bound to another normalization artifact")
    channels = tuple(range(len(pb00["channels"]))) if args.channels is None else tuple(args.channels)
    if not channels or len(set(channels)) != len(channels):
        raise ValueError("channel selection must be nonempty and unique")
    if backend_factory is not None:
        backend = backend_factory(pb00, normalization, windows, channels)
    elif args.backend == "local":
        backend = LocalBackend(work_dir, pb00, normalization, windows, channels,
                               args.cache_bytes)
    else:
        if args.source_version is None:
            raise ValueError("--source-version is required for IoTDB-FS")
        backend = IoTDBFSBackend(args.socket, args.mount, pb00, normalization,
                                 windows, channels, args.source_version,
                                 args.registration_mode, args.conversion_mode)
    try:
        train_data = WindowIndexDataset(windows, "train", args.train_windows)
        eval_data = WindowIndexDataset(windows, "validation", args.eval_windows)
        collator = WindowCollator(backend, args.model)
        torch.manual_seed(args.seed)
        model = make_model(args.model, len(channels), windows["input_length"],
                           windows["prediction_length"])
        training_args = TrainingArguments(
            output_dir=str(args.output_dir), max_steps=args.max_steps,
            per_device_train_batch_size=args.batch_size,
            per_device_eval_batch_size=args.batch_size,
            save_strategy="no", report_to=[],
            label_names=["future_values"], remove_unused_columns=False,
            dataloader_num_workers=0, group_by_length=False,
            dataloader_drop_last=False, disable_tqdm=True,
            seed=args.seed, data_seed=args.seed,
        )
        trainer = Trainer(model=model, args=training_args,
                          train_dataset=train_data, eval_dataset=eval_data,
                          data_collator=collator)
        if trainer.args.n_gpu != args.expected_gpus:
            raise RuntimeError(
                f"expected {args.expected_gpus} visible GPU(s), Trainer found "
                f"{trainer.args.n_gpu}; set CUDA_VISIBLE_DEVICES explicitly")
        start = time.perf_counter()
        train_result = trainer.train()
        train_wall = time.perf_counter() - start
        start = time.perf_counter()
        eval_result = trainer.evaluate()
        eval_wall = time.perf_counter() - start
        return {
            "dataset": args.dataset, "backend": args.backend, "model": args.model,
            "source_sha256": pb00["arrow"]["sha256"],
            "source_version": args.source_version,
            "registration_mode": args.registration_mode if args.backend == "iotdbfs" else None,
            "conversion_mode": args.conversion_mode if args.backend == "iotdbfs" else None,
            "normalization_artifact_id": normalization["artifact_id"],
            "window_artifact_id": windows["artifact_id"],
            "channels": list(channels), "batch_size": args.batch_size,
            "max_steps": args.max_steps, "seed": args.seed,
            "visible_gpus": trainer.args.n_gpu,
            "samplers": {"train": type(trainer._get_train_sampler()).__name__,
                         "validation": type(trainer._get_eval_sampler(eval_data)).__name__},
            "train_window_population": len(train_data),
            "eval_window_population": len(eval_data),
            "train_wall_seconds": train_wall, "eval_wall_seconds": eval_wall,
            "train_loss": float(train_result.training_loss),
            "eval_loss": float(eval_result["eval_loss"]),
            "traces": {name: trace_summary(trace) for name, trace in collator.traces.items()},
            "fetch_seconds": collator.fetch_seconds,
            "fetch_batches": collator.batch_count,
            "backend_metrics": backend.metrics,
        }
    finally:
        backend.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--dataset", choices=("ETTm1", "UCI_Electricity_370"), required=True)
    parser.add_argument("--backend", choices=("local", "iotdbfs"), required=True)
    parser.add_argument("--model", choices=("patchtst", "patchtsmixer"), required=True)
    parser.add_argument("--channels", type=int, nargs="+")
    parser.add_argument("--mount", type=Path)
    parser.add_argument("--socket", type=Path)
    parser.add_argument("--source-version", type=int)
    parser.add_argument("--registration-mode", choices=("full", "prepared", "prepared_compact"),
                        default="prepared_compact")
    parser.add_argument("--conversion-mode", choices=("sample", "batch", "buffers"), default="buffers")
    parser.add_argument("--cache-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--train-windows", type=int, default=0)
    parser.add_argument("--eval-windows", type=int, default=8)
    parser.add_argument("--max-steps", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--seed", type=int, default=20260929)
    parser.add_argument("--expected-gpus", type=int, default=1)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.backend == "iotdbfs" and (args.mount is None or args.socket is None):
        parser.error("IoTDB-FS requires --mount and --socket")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    result = run(args)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({key: value for key, value in result.items()
                      if key not in ("traces", "channels")}, indent=2))


if __name__ == "__main__":
    main()
