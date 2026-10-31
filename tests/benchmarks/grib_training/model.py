"""The public RegionalCNN-v1 model and its area-weighted training objective."""

from __future__ import annotations

import math

import torch
from torch import nn


class RegionalCNN(nn.Module):
    """Predict one regional ERA5 field from the previous two time points."""

    def __init__(self, channels: int):
        super().__init__()
        if channels <= 0:
            raise ValueError("channels must be positive")
        self.channels = channels
        self.network = nn.Sequential(
            nn.Conv2d(2 * channels, 64, 3, padding=1, padding_mode="reflect"),
            nn.ReLU(),
            nn.Conv2d(64, 64, 3, padding=1, padding_mode="reflect"),
            nn.ReLU(),
            nn.Conv2d(64, 64, 3, padding=1, padding_mode="reflect"),
            nn.ReLU(),
            nn.Conv2d(64, channels, 1),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        if x.ndim != 5 or x.shape[1] != 2 or x.shape[2] != self.channels:
            raise ValueError("expected input [batch, 2, channels, height, width]")
        batch, _, channels, height, width = x.shape
        return self.network(x.reshape(batch, 2 * channels, height, width)).unsqueeze(1)


def cosine_latitude_mse(
    prediction: torch.Tensor,
    target: torch.Tensor,
    target_mask: torch.Tensor,
    patch_rows: torch.Tensor,
) -> torch.Tensor:
    """Normalize valid cos(latitude) weights within each channel, then average channels."""
    if prediction.shape != target.shape or target.shape != target_mask.shape:
        raise ValueError("prediction, target and mask shapes differ")
    if target.ndim != 5 or target.shape[1] != 1:
        raise ValueError("expected one target time point")
    batch, _, channels, height, width = target.shape
    if patch_rows.shape != (batch,):
        raise ValueError("one patch origin row is required per sample")
    if not prediction.is_floating_point() or not target.is_floating_point():
        raise ValueError("prediction and target must be floating point")
    latitude = 90.0 - (patch_rows.to(target.device, dtype=target.dtype)[:, None]
                       + torch.arange(height, device=target.device,
                                      dtype=target.dtype)[None, :]) * 0.25
    weight = torch.cos(latitude * (math.pi / 180.0))[:, None, None, :, None]
    valid = target_mask.to(torch.bool)
    safe_target = torch.where(valid, target, torch.zeros_like(target))
    safe_prediction = torch.where(valid, prediction, torch.zeros_like(prediction))
    weighted = weight * valid
    denominator = weighted.sum(dim=(0, 1, 3, 4))
    if torch.any(denominator <= 0):
        raise ValueError("a target channel has no valid area weight")
    squared = (safe_prediction - safe_target).square()
    numerator = (squared * weighted).sum(dim=(0, 1, 3, 4))
    loss = (numerator / denominator).mean()
    if not torch.isfinite(loss):
        raise ValueError("non-finite area-weighted MSE")
    return loss
