"""Frozen ERA5 regional forecast coordinates shared by every EG01 reader."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path


PROTOCOL = "iotdbfs-era5-regional-samples/1"
UTC = timezone.utc
STEP = timedelta(hours=6)
HEIGHT = 721
WIDTH = 1440
PATCH = 128


@dataclass(frozen=True)
class Channel:
    ordinal: int
    name: str
    source: str
    param_id: int
    level: int
    type_of_level: str
    unit: str


CHANNELS = tuple(
    [Channel(0, "t2m", "single-levels", 167, 0, "surface", "K"),
     Channel(1, "u10", "single-levels", 165, 0, "surface", "m s**-1"),
     Channel(2, "v10", "single-levels", 166, 0, "surface", "m s**-1"),
     Channel(3, "msl", "single-levels", 151, 0, "surface", "Pa")]
    + [
        Channel(4 + 4 * variable + level_index,
                f"{name}{level}", "pressure-levels", param_id, level,
                "isobaricInhPa", unit)
        for variable, (name, param_id, unit) in enumerate(
            [("z", 129, "m**2 s**-2"), ("t", 130, "K"),
             ("u", 131, "m s**-1"), ("v", 132, "m s**-1")]
        )
        for level_index, level in enumerate((1000, 850, 700, 500))
    ]
)

REGIONS = {"east_asia": (156, 400), "north_america": (100, 920)}
OFFSETS = (0, 16, 32, 48)
VIEWS = {
    "V0": {"region": "east_asia", "channels": list(range(20)), "lead_steps": 1},
    "V1": {"region": "north_america", "channels": list(range(20)), "lead_steps": 1},
    "V2": {"region": "east_asia",
           "channels": [0, 1, 2, 3, 7, 9, 13, 17], "lead_steps": 1},
    "V3": {"region": "east_asia", "channels": list(range(20)), "lead_steps": 4},
}
SPLITS = {
    "train": ("2018-01-01T00:00:00Z", "2018-01-22T00:00:00Z"),
    "validation": ("2018-01-22T00:00:00Z", "2018-01-27T00:00:00Z"),
    "test": ("2018-01-27T00:00:00Z", "2018-02-01T00:00:00Z"),
}


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")).hexdigest()


def iso(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def message_time(row: dict) -> datetime:
    date = int(row["validityDate"])
    clock = int(row["validityTime"])
    if clock not in (0, 600, 1200, 1800):
        raise ValueError(f"unexpected ERA5 UTC time: {date}/{clock}")
    return datetime.strptime(f"{date:08d}{clock:04d}", "%Y%m%d%H%M").replace(
        tzinfo=UTC)


def channel_key(channel: Channel) -> tuple[str, int, int]:
    return channel.source, channel.param_id, channel.level


def patch_rows(region: str) -> list[dict]:
    base_row, base_col = REGIONS[region]
    patches = []
    for dr in OFFSETS:
        for dc in OFFSETS:
            row, col = base_row + dr, base_col + dc
            assert row + PATCH <= HEIGHT and col + PATCH <= WIDTH
            patches.append({
                "id": f"{region}:r{row}:c{col}", "row": row, "col": col,
                "height": PATCH, "width": PATCH,
                "lat_first": 90.0 - row * 0.25,
                "lat_last": 90.0 - (row + PATCH - 1) * 0.25,
                "lon_first": col * 0.25,
                "lon_last": (col + PATCH - 1) * 0.25,
            })
    return patches


class Catalog:
    def __init__(self, manifests: dict[str, dict], tier: str):
        if tier not in ("d0", "d1"):
            raise ValueError("tier must be d0 or d1")
        self.tier = tier
        self.paths: dict[str, Path] = {}
        self.sources = []
        self.rows: dict[datetime, dict[int, dict]] = {}
        by_key = {channel_key(c): c for c in CHANNELS}
        if set(manifests) != {"single-levels", "pressure-levels"}:
            raise ValueError("both ERA5 source categories are required")
        for kind in ("single-levels", "pressure-levels"):
            manifest = manifests[kind]
            if (not manifest.get("complete_decode")
                    or manifest.get("messages_with_missing") != 0
                    or manifest.get("grid") != [HEIGHT, WIDTH]):
                raise ValueError(f"unvalidated or incompatible {kind} manifest")
            path = Path(manifest["path"])
            if not path.is_file() or path.stat().st_size != manifest["bytes"]:
                raise ValueError(f"{kind} source file is missing or has changed size")
            self.paths[kind] = path
            self.sources.append({
                "kind": kind, "bytes": manifest["bytes"],
                "sha256": manifest["sha256"],
                "message_count": manifest["message_count"],
            })
            if len(manifest["messages"]) != manifest["message_count"]:
                raise ValueError(f"{kind} message count differs from manifest")
            for row in manifest["messages"]:
                source_key = (kind, int(row["paramId"]),
                              int(row["level"]) if kind == "pressure-levels" else 0)
                channel = by_key.get(source_key)
                if channel is None:
                    raise ValueError(f"unexpected channel {source_key}")
                if (row["typeOfLevel"] != channel.type_of_level
                        or row["units"] != channel.unit
                        or row["Ni"] != WIDTH or row["Nj"] != HEIGHT
                        or row["gridType"] != "regular_ll"
                        or row["step"] != 0
                        or row["numberOfMissing"] != 0):
                    raise ValueError(f"invalid message metadata {source_key}")
                when = message_time(row)
                slot = self.rows.setdefault(when, {})
                if channel.ordinal in slot:
                    raise ValueError(f"duplicate channel {channel.ordinal} at {iso(when)}")
                slot[channel.ordinal] = row
        self.times = sorted(self.rows)
        expected_times = 8 if tier == "d0" else 124
        if len(self.times) != expected_times:
            raise ValueError(f"{tier} requires {expected_times} UTC time points")
        if any(b - a != STEP for a, b in zip(self.times, self.times[1:])):
            raise ValueError("ERA5 time coverage has a gap")
        if self.times[0] != parse_iso("2018-01-01T00:00:00Z"):
            raise ValueError("unexpected ERA5 coverage start")
        if tier == "d0" and self.times[-1] != parse_iso("2018-01-02T18:00:00Z"):
            raise ValueError("unexpected D0 coverage end")
        if tier == "d1" and self.times[-1] != parse_iso("2018-01-31T18:00:00Z"):
            raise ValueError("unexpected D1 coverage end")
        if any(set(slot) != set(range(20)) for slot in self.rows.values()):
            raise ValueError("one or more time points lack a channel")
        self.fingerprint = digest({
            "protocol": PROTOCOL, "sources": self.sources,
            "channels": [asdict(c) for c in CHANNELS],
        })

    @classmethod
    def from_paths(cls, single: Path, pressure: Path, tier: str) -> "Catalog":
        return cls({
            "single-levels": json.loads(single.read_text()),
            "pressure-levels": json.loads(pressure.read_text()),
        }, tier)

    def sample_rows(self, view_id: str) -> list[dict]:
        view = VIEWS[view_id]
        view_fingerprint = digest({
            "protocol": PROTOCOL, "view": view_id, "spec": view,
            "patches": patch_rows(view["region"]), "transform": "raw",
        })
        available = set(self.times)
        result = []
        for split, (start, end) in SPLITS.items():
            left, right = parse_iso(start), parse_iso(end)
            for init in self.times:
                previous, target = init - STEP, init + view["lead_steps"] * STEP
                if not (left <= previous and target < right):
                    continue
                if not {previous, init, target}.issubset(available):
                    continue
                for patch in patch_rows(view["region"]):
                    coordinates = {"init_time": iso(init), "patch_id": patch["id"]}
                    result.append({
                        "sample_id": digest({
                            "snapshot": self.fingerprint, "view": view_fingerprint,
                            **coordinates,
                        }),
                        "view_id": view_id, "view_fingerprint": view_fingerprint,
                        "split": split, **coordinates,
                        "input_times": [iso(previous), iso(init)],
                        "target_time": iso(target),
                        "row": patch["row"], "col": patch["col"],
                    })
        return result

    def freeze(self) -> dict:
        views = {}
        for view_id, spec in VIEWS.items():
            rows = self.sample_rows(view_id)
            views[view_id] = {
                "spec": spec, "patches": patch_rows(spec["region"]),
                "counts": {split: sum(row["split"] == split for row in rows)
                           for split in SPLITS},
                "samples": rows,
            }
        return {
            "protocol": PROTOCOL, "tier": self.tier,
            "snapshot_fingerprint": self.fingerprint,
            "sources": self.sources,
            "channels": [asdict(c) for c in CHANNELS],
            "grid": {"rows": HEIGHT, "cols": WIDTH, "lat_first": 90.0,
                     "lat_last": -90.0, "lon_first": 0.0, "lon_last": 359.75,
                     "step_degrees": 0.25},
            "splits": SPLITS, "utc_times": [iso(t) for t in self.times],
            "views": views,
        }
