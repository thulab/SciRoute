"""Lossless, bounded ERA5 GRIB-to-Zarr control and regional sample reader.

The store contains base fields, never materialized training windows.  The
encoded-byte counters describe reads made by Zarr's Python store; they do not
claim to measure physical disk I/O beneath the OS page cache.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import time
import uuid

import numpy as np
import zarr
from numcodecs import Blosc

from local_reader import LocalGribReader, Sample
from sample_contract import (CHANNELS, HEIGHT, PATCH, SPLITS, WIDTH, Catalog,
                             iso, parse_iso)


PROTOCOL = "iotdbfs-era5-zarr/1"
CHUNKS = (1, 1, PATCH, PATCH)
CODEC = {"name": "blosc", "cname": "zstd", "clevel": 3,
         "shuffle": "bitshuffle"}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _source_sha256_check(catalog: Catalog) -> None:
    for source in catalog.sources:
        path = catalog.paths[source["kind"]]
        if _sha256(path) != source["sha256"]:
            raise ValueError(f"source SHA-256 mismatch: {source['kind']}")


def _store_sizes(path: Path) -> dict[str, int]:
    totals = {"store_bytes": 0, "store_files": 0,
              "value_chunk_bytes": 0, "value_chunk_files": 0,
              "mask_chunk_bytes": 0, "mask_chunk_files": 0}
    for current, _, names in os.walk(path):
        for name in names:
            file = Path(current) / name
            size = file.stat().st_size
            totals["store_bytes"] += size
            totals["store_files"] += 1
            relative = file.relative_to(path).parts
            if len(relative) == 2 and not relative[-1].startswith("."):
                prefix = relative[0]
                if prefix in ("values", "mask"):
                    totals[f"{prefix[:-1] if prefix == 'values' else 'mask'}_chunk_bytes"] += size
                    totals[f"{prefix[:-1] if prefix == 'values' else 'mask'}_chunk_files"] += 1
    return totals


def convert_grib_to_zarr(
    catalog: Catalog,
    destination: Path,
    *,
    manifest_paths: dict[str, Path] | None = None,
) -> dict:
    """Convert one frozen D0/D1 catalog, one decoded field at a time.

    ``destination`` must not exist.  Work is written to a sibling directory
    and becomes visible only after every field has been verified and written.
    """
    destination = Path(destination)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(destination)
    if not destination.parent.is_dir():
        raise FileNotFoundError(destination.parent)
    if manifest_paths is not None and set(manifest_paths) != {
            "single-levels", "pressure-levels"}:
        raise ValueError("both source manifests are required")
    started = time.perf_counter_ns()
    _source_sha256_check(catalog)
    source_verify_ns = time.perf_counter_ns() - started
    identity = {
        kind: _sha256(Path(path))
        for kind, path in (manifest_paths or {}).items()
    }
    temporary = destination.with_name(
        f".{destination.name}.building-{uuid.uuid4().hex}"
    )
    store = zarr.DirectoryStore(str(temporary))
    try:
        root = zarr.group(store=store, overwrite=False)
        shape = (len(catalog.times), len(CHANNELS), HEIGHT, WIDTH)
        codec = Blosc(cname="zstd", clevel=3, shuffle=Blosc.BITSHUFFLE)
        values = root.create_dataset(
            "values", shape=shape, chunks=CHUNKS, dtype="<f4",
            compressor=codec, fill_value=np.nan, order="C",
        )
        # The frozen ERA5 catalogs reject messages with missing values.  A
        # Zarr boolean array with true fill is an exact, compact saved mask:
        # no duplicate all-true chunk files need to be materialized.
        root.create_dataset(
            "mask", shape=shape, chunks=CHUNKS, dtype="bool",
            compressor=codec, fill_value=True, order="C",
        )
        root.attrs.update({
            "protocol": PROTOCOL,
            "tier": catalog.tier,
            "snapshot_fingerprint": catalog.fingerprint,
            "sources": catalog.sources,
            "manifest_sha256": identity,
            "channels": [asdict(channel) for channel in CHANNELS],
            "utc_times": [iso(when) for when in catalog.times],
            "grid": {"rows": HEIGHT, "cols": WIDTH,
                     "latitude_north_to_south": True,
                     "longitude_0_to_360": True},
            "layout": ["time", "channel", "y", "x"],
            "chunks": list(CHUNKS),
            "codec": CODEC,
            "value_cast": "ecCodes float64 to IEEE Float32",
            "mask_semantics": "true means valid; source manifests have no missing values",
        })
        reader = LocalGribReader(catalog, max_cache_bytes=0)
        source_bytes_read = 0
        decoded_float32_bytes = 0
        write_started = time.perf_counter_ns()
        for time_index, when in enumerate(catalog.times):
            for channel in CHANNELS:
                row = catalog.rows[when][channel.ordinal]
                field = reader._decode(when, channel.ordinal)
                if field.shape != (HEIGHT, WIDTH) or field.dtype != np.float32:
                    raise ValueError("unexpected ecCodes field shape or dtype")
                values[time_index, channel.ordinal, :, :] = field
                source_bytes_read += int(row["length"])
                decoded_float32_bytes += field.nbytes
        write_ns = time.perf_counter_ns() - write_started
        del values, reader, root, store
        sizes_started = time.perf_counter_ns()
        sizes = _store_sizes(temporary)
        scan_ns = time.perf_counter_ns() - sizes_started
        os.rename(temporary, destination)
    except BaseException:
        # Keep an interrupted conversion as an inspectable, non-published
        # temporary store.  A retry always chooses a new directory.
        raise
    return {
        "protocol": PROTOCOL,
        "tier": catalog.tier,
        "snapshot_fingerprint": catalog.fingerprint,
        "store": str(destination),
        "source_sha256_verify_ns": source_verify_ns,
        "conversion_write_ns": write_ns,
        "size_scan_ns": scan_ns,
        "elapsed_ns": time.perf_counter_ns() - started,
        "decoded_fields": len(catalog.times) * len(CHANNELS),
        "source_message_bytes_read": source_bytes_read,
        "decoded_float32_bytes": decoded_float32_bytes,
        **sizes,
    }


class CountingDirectoryStore(zarr.DirectoryStore):
    def __init__(self, path: str):
        super().__init__(path)
        self.read_bytes = 0
        self.read_calls = 0

    def __getitem__(self, key: str) -> bytes:
        result = super().__getitem__(key)
        if key.startswith("values/") or key.startswith("mask/"):
            if not key.rsplit("/", 1)[-1].startswith("."):
                self.read_calls += 1
                self.read_bytes += len(result)
        return result


class ZarrReader:
    def __init__(self, store_path: Path, *, expected_fingerprint: str | None = None):
        self.store = CountingDirectoryStore(str(store_path))
        root = zarr.open_group(store=self.store, mode="r")
        attrs = root.attrs.asdict()
        if attrs.get("protocol") != PROTOCOL:
            raise ValueError("unsupported ERA5 Zarr store protocol")
        if (expected_fingerprint is not None
                and attrs.get("snapshot_fingerprint") != expected_fingerprint):
            raise ValueError("Zarr store snapshot fingerprint mismatch")
        self.fingerprint = attrs["snapshot_fingerprint"]
        if (attrs.get("channels") != [asdict(channel) for channel in CHANNELS]
                or attrs.get("layout") != ["time", "channel", "y", "x"]
                or attrs.get("chunks") != list(CHUNKS)):
            raise ValueError("Zarr store channel order or layout mismatch")
        times = attrs.get("utc_times", [])
        if (len(times) not in (8, 124)
                or times != sorted(set(times))
                or attrs.get("tier") != ("d0" if len(times) == 8 else "d1")):
            raise ValueError("Zarr store time coverage mismatch")
        self.time_index = {value: index for index, value in enumerate(times)}
        self.values = root["values"]
        self.mask = root["mask"]
        expected_shape = (len(times), len(CHANNELS), HEIGHT, WIDTH)
        if (self.values.shape != expected_shape or self.mask.shape != expected_shape
                or self.values.chunks != CHUNKS or self.mask.chunks != CHUNKS
                or self.values.dtype != np.dtype("<f4")
                or self.mask.dtype != np.dtype("bool")):
            raise ValueError("Zarr array shape, chunks, or dtype mismatch")
        self._read_ns = 0
        self._logical_bytes = 0
        self._samples = 0
        self._chunks_requested = 0
        self.store.read_calls = 0
        self.store.read_bytes = 0

    def metrics(self) -> dict[str, int]:
        return {
            "read_ns": self._read_ns,
            "read_calls": self.store.read_calls,
            "read_bytes": self.store.read_bytes,
            "logical_bytes": self._logical_bytes,
            "chunks_requested": self._chunks_requested,
            "samples": self._samples,
        }

    @staticmethod
    def _chunks_for_patch(row: int, col: int) -> int:
        row_last = row + PATCH - 1
        col_last = col + PATCH - 1
        return ((row_last // PATCH - row // PATCH + 1)
                * (col_last // PATCH - col // PATCH + 1))

    def sample(self, row: dict, channel_ordinals: list[int]) -> Sample:
        if (not channel_ordinals or len(channel_ordinals) != len(set(channel_ordinals))
                or any(ordinal < 0 or ordinal >= len(CHANNELS)
                       for ordinal in channel_ordinals)):
            raise ValueError("invalid channel ordinal list")
        y0, x0 = int(row["row"]), int(row["col"])
        if y0 < 0 or x0 < 0 or y0 + PATCH > HEIGHT or x0 + PATCH > WIDTH:
            raise ValueError("patch exceeds ERA5 grid")
        times = list(row["input_times"]) + [row["target_time"]]
        if len(times) != 3 or any(when not in self.time_index for when in times):
            raise ValueError("sample time is outside the Zarr store")
        parsed = [parse_iso(when) for when in times]
        if not (parsed[0] < parsed[1] < parsed[2]):
            raise ValueError("sample time order is invalid")
        split = row.get("split")
        if split not in SPLITS:
            raise ValueError("sample has no frozen split")
        left, right = (parse_iso(value) for value in SPLITS[split])
        if any(not left <= when < right for when in parsed):
            raise ValueError("sample crosses its frozen split")
        x = np.empty((2, len(channel_ordinals), PATCH, PATCH), dtype=np.float32)
        y = np.empty((1, len(channel_ordinals), PATCH, PATCH), dtype=np.float32)
        x_mask = np.empty_like(x, dtype=np.bool_)
        y_mask = np.empty_like(y, dtype=np.bool_)
        read_started = time.perf_counter_ns()
        for channel_index, ordinal in enumerate(channel_ordinals):
            for position, when in enumerate(times):
                index = self.time_index[when]
                value = self.values[index, ordinal, y0:y0 + PATCH, x0:x0 + PATCH]
                mask = self.mask[index, ordinal, y0:y0 + PATCH, x0:x0 + PATCH]
                if position < 2:
                    x[position, channel_index] = value
                    x_mask[position, channel_index] = mask
                else:
                    y[0, channel_index] = value
                    y_mask[0, channel_index] = mask
        self._read_ns += time.perf_counter_ns() - read_started
        self._samples += 1
        self._logical_bytes += sum(array.nbytes for array in (x, y, x_mask, y_mask))
        self._chunks_requested += 2 * len(times) * len(channel_ordinals) * (
            self._chunks_for_patch(y0, x0))
        return Sample(row["sample_id"], x, y, x_mask, y_mask)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--single-manifest", type=Path, required=True)
    parser.add_argument("--pressure-manifest", type=Path, required=True)
    parser.add_argument("--tier", choices=("d0", "d1"), required=True)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    catalog = Catalog.from_paths(args.single_manifest, args.pressure_manifest, args.tier)
    report = convert_grib_to_zarr(catalog, args.store, manifest_paths={
        "single-levels": args.single_manifest,
        "pressure-levels": args.pressure_manifest,
    })
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
