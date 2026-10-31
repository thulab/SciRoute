#!/usr/bin/env python3
"""Compare local Arrow and IoTDB-FS typed windows on pinned public data."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from public_backends import LocalArrowWindowReader
from public_iotdbfs import IoTDBFSWindowReader, source_version
from prepare_public_windows import verify_seal


def check(work_dir: Path, mount: Path, socket_path: Path, dataset: str,
          channels: tuple[int, ...]) -> dict:
    manifests = work_dir / "manifests"
    pb00 = json.loads((manifests / f"{dataset}-pb00.json").read_text())
    norm = json.loads((manifests / f"{dataset}-pb01-normalization.json").read_text())
    windows = json.loads((manifests / f"{dataset}-pb01-windows-L96-H24-S1.json").read_text())
    verify_seal(norm)
    verify_seal(windows)
    version = source_version(socket_path, "/" + pb00["arrow"]["path"])
    result = {"dataset": dataset, "source_version": version,
              "source_sha256": pb00["arrow"]["sha256"],
              "channel_count": len(channels),
              "channels": list(channels) if len(channels) <= 16 else "all-in-order",
              "splits": {}}
    means = np.asarray([norm["statistics"][i]["mean"] for i in channels], dtype=np.float64)
    stddev = np.asarray([norm["statistics"][i]["stddev"] for i in channels], dtype=np.float64)
    with LocalArrowWindowReader(work_dir / "arrow" / pb00["arrow"]["path"],
                                row_index=pb00["arrow"]["rows"]) as local:
        for split in ("train", "validation", "test"):
            spec = windows["splits"][split]
            anchors = [spec["first_anchor"]]
            if split == "train":
                boundary = 8192 - 12
                if spec["first_anchor"] <= boundary < spec["stop_anchor_exclusive"]:
                    anchors.append(boundary)
            remote = IoTDBFSWindowReader(
                socket_path=socket_path, mount=mount, pb00=pb00,
                normalization=norm, windows=windows, split=split,
                channels=channels, source_version_exact=version)
            try:
                batch = remote.read_windows(anchors)
            finally:
                remote.close()
            for index, anchor in enumerate(anchors):
                raw, valid = local.read_window(anchor - windows["input_length"],
                                               windows["input_length"] + windows["prediction_length"],
                                               channels)
                transformed = ((raw.astype(np.float64) - means) / stddev).astype(np.float32)
                transformed[~valid] = 0
                length = windows["input_length"]
                np.testing.assert_array_equal(batch.input_valid[index], valid[:length])
                np.testing.assert_array_equal(batch.target_valid[index], valid[length:])
                np.testing.assert_allclose(batch.input_values[index][valid[:length]],
                                           transformed[:length][valid[:length]], rtol=1e-6, atol=1e-6)
                np.testing.assert_allclose(batch.target_values[index][valid[length:]],
                                           transformed[length:][valid[length:]], rtol=1e-6, atol=1e-6)
            result["splits"][split] = {"anchors": anchors, "remote_metrics": remote.metrics}
        result["local_metrics"] = local.metrics
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--mount", type=Path, required=True)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--dataset", choices=("ETTm1", "UCI_Electricity_370"), required=True)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--channels", type=int, nargs="+")
    selection.add_argument("--all-channels", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    channels = (tuple(range(7 if args.dataset == "ETTm1" else 370))
                if args.all_channels else tuple(args.channels))
    result = check(args.work_dir, args.mount, args.socket, args.dataset, channels)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
