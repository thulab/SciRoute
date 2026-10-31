"""Compare fixed-output scale points using contemporaneous warm baselines."""
import argparse
import json
from pathlib import Path
import statistics

from analyze_small import analyze


def summarize(root):
    scale = json.loads((root / "scale-report.json").read_text())
    if not scale.get("complete"):
        raise ValueError("scale campaign is incomplete")
    rows, preparations = [], []
    for name, point in scale["points"].items():
        report = json.loads((root / name / "report.json").read_text())
        analysis = analyze(report)
        controls = report["controls"]
        if any(controls[k] != v for k, v in {"N": point["N"], "D": point["D"], "K": 2, "depth": 2}.items()):
            raise ValueError("scale controls do not agree")
        for fmt in ("hdf5", "grib"):
            prep = report["preparation"][fmt]
            preparations.append({"point": name, "format": fmt, "raw_bytes": prep["raw_bytes"],
                "manager_ms": prep["manager_preparation_ns"] / 1e6,
                "fs_publish_ms": prep["publication"]["fs"]["total_ns"] / 1e6,
                "iotdb_publish_ms": prep["publication"]["iotdb"]["total_ns"] / 1e6,
                "fs_index_build_ms": prep["fs_index_build"]["total_ns"] / 1e6})
            for phase in ("discover", "discover-values"):
                cells = {s["arm"]: s for s in analysis["summary"] if s["format"] == fmt and s["phase"] == phase}
                scan, index, io = [cells[a]["warm_p50_ns"] / 1e6 for a in ("fs-scan", "fs-index", "iotdb")]
                diagnostics = next(d for d in analysis["iotdb_diagnostics"] if d["format"] == fmt and d["phase"] == phase)
                campaign_medians = {}
                for arm in cells:
                    campaign_medians[arm] = [statistics.median(r["total_ns"] for r in report["records"] if
                        r["format"] == fmt and r["phase"] == phase and r["arm"] == arm and
                        r["campaign"] == campaign and r["repeat"] > 0) / 1e6 for campaign in range(3)]
                rows.append({"point": name, "N": point["N"], "D": point["D"], "units": point["N"] * point["D"],
                    "format": fmt, "phase": phase, "fs_scan_ms": scan, "fs_index_ms": index, "iotdb_ms": io,
                    "iotdb_over_scan": io / scan, "iotdb_minus_scan_ms": io - scan,
                    "iotdb_over_index": io / index, "campaign_medians_ms": campaign_medians,
                    "iotdb_diagnostics": diagnostics})
    return {"protocol": "iotdbfs-multifile-scale-analysis/1", "observations": 900,
            "rows": rows, "preparations": preparations,
            "scope": "descriptive warm medians; same final database state; no cold-disk or statistical significance claim"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.output.open("x") as stream:
        json.dump(summarize(args.root), stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
