"""Replay the published Electricity 50-step trace against candidate value layouts.

This is an idealized byte/range calculation, not a storage benchmark. It assumes
the uncompressed 370-channel Electricity Arrow layout used by PB00 and excludes
bitmap, layout, protocol, and cache metadata. Run with the IoTDB-FS client JSON
report from the 2026-09-29 tile-batch experiment.
"""

import json
from pathlib import Path
import sys


CHANNELS = 370
ROW_STEPS = 8192
WINDOW_STEPS = 120


def channel_major_keys(anchor, tile_steps):
    keys = set()
    step = anchor
    end = anchor + WINDOW_STEPS
    while step < end:
        row = step // ROW_STEPS
        row_begin = row * ROW_STEPS
        take_end = min(end, row_begin + ROW_STEPS)
        batch = row // 2
        row_in_batch = row % 2
        for channel in range(CHANNELS):
            leaf = (row_in_batch * CHANNELS + channel) * ROW_STEPS
            first = (leaf + step - row_begin) // tile_steps
            last = (leaf + take_end - row_begin - 1) // tile_steps
            keys.update((batch, tile) for tile in range(first, last + 1))
        step = take_end
    return keys


def step_major_keys(anchor, tile_steps):
    return set(range(anchor // tile_steps,
                     (anchor + WINDOW_STEPS - 1) // tile_steps + 1))


def contiguous_ranges(keys):
    count = 0
    previous = None
    for key in sorted(keys):
        if previous is None:
            count += 1
        elif isinstance(key, tuple):
            if key[0] != previous[0] or key[1] != previous[1] + 1:
                count += 1
        elif key != previous + 1:
            count += 1
        previous = key
    return count


def replay(requests, name, key_fn, block_bytes):
    groups = [set().union(*(key_fn(anchor) for anchor in anchors))
              for anchors in requests]
    distinct = set().union(*groups)
    return {
        "layout": name,
        "requests": len(groups),
        "key_demands_without_cross_request_cache": sum(map(len, groups)),
        "distinct_keys": len(distinct),
        "value_bytes_without_cross_request_cache":
            sum(map(len, groups)) * block_bytes,
        "value_bytes_with_perfect_cross_request_cache":
            len(distinct) * block_bytes,
        "ideal_contiguous_value_ranges_without_wave_splits":
            sum(contiguous_ranges(group) for group in groups),
    }


def main(report_path):
    report = json.loads(Path(report_path).read_text())
    requests = [entry["anchors"] for entry in
                report["backend_metrics"]["train"]["request_timings"]]
    if len(requests) != 51 or sum(map(len, requests)) != 102:
        raise ValueError("expected the 51-request, 102-anchor Electricity trace")
    rows = []
    for tile_steps in (4096, 256, 128):
        rows.append(replay(
            requests, f"channel_major_{tile_steps}",
            lambda anchor, width=tile_steps: channel_major_keys(anchor, width),
            tile_steps * 4))
    rows.append(replay(requests, "step_major_128x370",
                       lambda anchor: step_major_keys(anchor, 128),
                       128 * CHANNELS * 4))
    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: replay_electricity_layout.py REPORT.json")
    main(sys.argv[1])
