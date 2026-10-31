"""Verify the public ETTm1 Trainer environment, not benchmark performance.

Usage: python smoke_hf_environment.py --work-dir /path/to/public-arrow-baseline
The work directory must contain raw/ETTm1.csv. No private training code is used.
"""

import argparse
import hashlib
import json
from pathlib import Path

import accelerate
import datasets
import numpy as np
import pandas as pd
import pyarrow
import torch
import transformers
from torch.utils.data import Dataset
from transformers import (
    PatchTSTConfig,
    PatchTSTForPrediction,
    PatchTSMixerConfig,
    PatchTSMixerForPrediction,
    Trainer,
    TrainingArguments,
    default_data_collator,
)


ETTM1_SHA256 = "6ce1759b1a18e3328421d5d75fadcb316c449fcd7cec32820c8dafda71986c9e"
CHANNELS = ("HUFL", "HULL", "MUFL", "MULL", "LUFL", "LULL", "OT")
CONTEXT = 96
HORIZON = 24
TRAIN_END = 12 * 30 * 24 * 4


class ETTWindowDataset(Dataset):
    def __init__(self, values: np.ndarray, starts: tuple[int, ...], mask_name: str):
        self.values = values
        self.starts = starts
        self.mask_name = mask_name

    def __len__(self) -> int:
        return len(self.starts)

    def __getitem__(self, index: int) -> dict[str, torch.Tensor]:
        start = self.starts[index]
        past = torch.from_numpy(self.values[start : start + CONTEXT].copy())
        future = torch.from_numpy(
            self.values[start + CONTEXT : start + CONTEXT + HORIZON].copy()
        )
        return {
            "past_values": past,
            "future_values": future,
            self.mask_name: torch.ones_like(past, dtype=torch.bool),
        }


def load_values(path: Path) -> np.ndarray:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != ETTM1_SHA256:
        raise ValueError(f"ETTm1 SHA-256 mismatch: {digest}")
    frame = pd.read_csv(path)
    if len(frame) != 69680 or tuple(frame.columns[1:]) != CHANNELS:
        raise ValueError("Unexpected ETTm1 shape or channel order")
    values = frame.loc[:, CHANNELS].to_numpy(dtype=np.float32, copy=True)
    if not np.isfinite(values).all():
        raise ValueError("ETTm1 contains non-finite numeric values")
    train_mean = values[:TRAIN_END].mean(axis=0)
    train_std = values[:TRAIN_END].std(axis=0)
    if np.any(train_std == 0):
        raise ValueError("ETTm1 training channel has zero variance")
    return np.ascontiguousarray((values - train_mean) / train_std, dtype=np.float32)


def make_model(name: str):
    if name == "patchtst":
        config = PatchTSTConfig(
            num_input_channels=len(CHANNELS),
            context_length=CONTEXT,
            prediction_length=HORIZON,
            patch_length=16,
            patch_stride=16,
            num_hidden_layers=1,
            d_model=32,
            num_attention_heads=4,
            ffn_dim=64,
            scaling=None,
        )
        return PatchTSTForPrediction(config), "past_observed_mask"
    config = PatchTSMixerConfig(
        num_input_channels=len(CHANNELS),
        context_length=CONTEXT,
        prediction_length=HORIZON,
        patch_length=16,
        patch_stride=16,
        num_layers=1,
        d_model=16,
        mode="mix_channel",
        dropout=0.0,
        scaling=None,
    )
    return PatchTSMixerForPrediction(config), "observed_mask"


def train_one(name: str, values: np.ndarray, work_dir: Path) -> dict:
    torch.manual_seed(20260927)
    model, mask_name = make_model(name)
    train_data = ETTWindowDataset(values, (0, 16, 32, 48), mask_name)
    eval_data = ETTWindowDataset(values, (40000, 40016), mask_name)
    batch = default_data_collator([train_data[0], train_data[1]])
    device = torch.device("cuda:0")
    model.to(device)
    output = model(**{key: value.to(device) for key, value in batch.items()})
    if not torch.isfinite(output.loss):
        raise AssertionError(f"{name}: non-finite forward loss")
    if tuple(output.prediction_outputs.shape) != (2, HORIZON, len(CHANNELS)):
        raise AssertionError(f"{name}: wrong prediction shape")
    output.loss.backward()
    model.zero_grad(set_to_none=True)

    model_dir = work_dir / "runs" / f"smoke-{name}"
    args = TrainingArguments(
        output_dir=str(model_dir),
        max_steps=1,
        per_device_train_batch_size=2,
        per_device_eval_batch_size=2,
        save_strategy="steps",
        save_steps=1,
        save_total_limit=2,
        report_to=[],
        label_names=["future_values"],
        remove_unused_columns=False,
        dataloader_num_workers=0,
        disable_tqdm=True,
        seed=20260927,
    )
    trainer = Trainer(
        model=model,
        args=args,
        train_dataset=train_data,
        eval_dataset=eval_data,
        data_collator=default_data_collator,
    )
    trainer.train()
    if trainer.state.global_step != 1:
        raise AssertionError(f"{name}: first training step did not finish")
    first_eval = trainer.evaluate()["eval_loss"]
    checkpoint = model_dir / "checkpoint-1"
    if not checkpoint.is_dir():
        raise AssertionError(f"{name}: checkpoint was not saved")

    resumed_model, _ = make_model(name)
    resumed_args = TrainingArguments(
        output_dir=str(model_dir),
        max_steps=2,
        per_device_train_batch_size=2,
        per_device_eval_batch_size=2,
        save_strategy="steps",
        save_steps=1,
        save_total_limit=2,
        report_to=[],
        label_names=["future_values"],
        remove_unused_columns=False,
        dataloader_num_workers=0,
        disable_tqdm=True,
        seed=20260927,
    )
    resumed = Trainer(
        model=resumed_model,
        args=resumed_args,
        train_dataset=train_data,
        eval_dataset=eval_data,
        data_collator=default_data_collator,
    )
    resumed.train(resume_from_checkpoint=str(checkpoint))
    if resumed.state.global_step != 2:
        raise AssertionError(f"{name}: resume did not reach step 2")
    resumed_eval = resumed.evaluate()["eval_loss"]
    resumed.save_model(str(model_dir / "final"))
    return {
        "forward_loss": float(output.loss.detach().cpu()),
        "first_eval_loss": float(first_eval),
        "resumed_eval_loss": float(resumed_eval),
        "global_step": resumed.state.global_step,
        "checkpoint": str(checkpoint),
        "final_model": str(model_dir / "final"),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir", type=Path, required=True)
    opts = parser.parse_args()
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is unavailable in the isolated environment")
    values = load_values(opts.work_dir / "raw" / "ETTm1.csv")
    result = {
        "purpose": "environment smoke only; not a throughput benchmark",
        "source_sha256": ETTM1_SHA256,
        "versions": {
            "python": __import__("sys").version.split()[0],
            "torch": torch.__version__,
            "cuda": torch.version.cuda,
            "transformers": transformers.__version__,
            "accelerate": accelerate.__version__,
            "datasets": datasets.__version__,
            "pyarrow": pyarrow.__version__,
            "numpy": np.__version__,
            "pandas": pd.__version__,
        },
        "gpu": torch.cuda.get_device_name(0),
        "models": {name: train_one(name, values, opts.work_dir) for name in ("patchtst", "patchtsmixer")},
    }
    destination = opts.work_dir / "manifests" / "hf_environment_smoke.json"
    destination.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
