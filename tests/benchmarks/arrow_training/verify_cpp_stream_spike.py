"""Compare the native Arrow batch spike with an independent PyArrow oracle."""

import argparse
import json
import math
from pathlib import Path
import subprocess

import pyarrow.ipc as ipc


CASES = (
    ("fixtures/nested-target-only-none.arrows", 0, 0, 0, 0, 3),
    ("fixtures/nested-multibatch-lz4.arrows", 0, 0, 0, 0, 3),
    ("fixtures/nested-multibatch-zstd.arrows", 0, 0, 0, 0, 3),
    ("ETTm1-none-8192.arrows", 0, 0, 2, 3, 11),
    ("UCI_Electricity_370-none-8192.arrows", 0, 0, 200, 100, 108),
)


def check_case(binary: Path, root: Path, case: tuple):
    relative, batch_index, row, channel, start, end = case
    path = root / relative
    output = subprocess.check_output(
        [str(binary), str(path), str(batch_index), str(row), str(channel),
         str(start), str(end)], text=True
    )
    actual = json.loads(output)
    batches = list(ipc.open_stream(path))
    batch = batches[batch_index]
    expected = batch.column(batch.schema.get_field_index("target"))[row].as_py()[channel][start:end]
    if actual["batch_rows"] != batch.num_rows or len(actual["values"]) != len(expected):
        raise AssertionError(f"Wrong row/value count for {relative}")
    for index, (value, oracle) in enumerate(zip(actual["values"], expected)):
        if oracle is None:
            equal = value is None
        else:
            equal = value is not None and math.isclose(value, oracle, rel_tol=1e-8, abs_tol=1e-6)
        if not equal:
            raise AssertionError(f"{relative} step {start + index}: {value} != {oracle}")
    return {"source": relative, "batch": batch_index, "row": row,
            "channel": channel, "steps": [start, end], "values_checked": len(expected)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    results = [check_case(args.binary, args.data_root, case) for case in CASES]
    report = {"cases": results, "total_values_checked": sum(x["values_checked"] for x in results)}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
