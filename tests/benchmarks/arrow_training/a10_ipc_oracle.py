"""Independent PyArrow check for IPC emitted by the A10 C++ unit fixtures.

Run with IOTDBFS_A10_ORACLE_DIR set to a temporary directory while executing
QueryBatchIpc.RetainsValuesMasksFieldMetadataAndPerBatchProvenance and
WindowIpcEncoder.FixedFlatSchemaPreservesRoleMaskTimeAndProvenance, then pass
WindowIpcEncoder.StandardizedValuesCarryCompleteTrainingStatistics, then pass
that directory to this script.
"""

import json
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc


def meta(metadata, key):
    assert metadata is not None
    return metadata[key.encode()].decode()


def check_query(root: Path):
    with ipc.open_stream(root / "query.ipc") as reader:
        schema = reader.schema
        assert schema.names == ["index", "value.2", "observed_at", "payload"]
        assert meta(schema.metadata, "iotdbfs.protocol") == "iotdbfs-query-batch-ipc/1"
        assert meta(schema.field(1).metadata, "iotdbfs.unit") == "K"
        assert meta(schema.field(2).metadata, "iotdbfs.timezone") == "UTC"
        assert schema.field(2).type == pa.timestamp("ns", tz="UTC")
        for origin, source in ((3, "/first"), (17, "/second")):
            batch, metadata = reader.read_next_batch_with_custom_metadata()
            assert batch.num_rows == 2
            assert batch.column(0).to_pylist() == [origin, origin + 1]
            assert batch.column(1).to_pylist() == [10.5, None]
            assert batch.column(3).to_pylist() == [b"\x00\x01\xff", None]
            assert meta(metadata, "iotdbfs.domain.kind") == "rows"
            assert meta(metadata, "iotdbfs.domain.dimension.0.origin") == str(origin)
            assert meta(metadata, "iotdbfs.source.0.path") == source
            assert meta(metadata, "iotdbfs.source.0.version") == "7"
        try:
            reader.read_next_batch()
        except StopIteration:
            pass
        else:
            raise AssertionError("unexpected extra query batch")


def check_window(root: Path):
    with ipc.open_stream(root / "window.ipc") as reader:
        schema = reader.schema
        assert schema.names == [
            "sample_id", "sample_index", "role", "step", "time_ns",
            "descriptor_json", "input.0", "input.1", "target.0",
        ]
        assert meta(schema.metadata, "iotdbfs.protocol") == "iotdbfs-window-flat/3"
        assert meta(schema.metadata, "iotdbfs.transform.kind") == "identity"
        assert meta(schema.metadata, "iotdbfs.transform.artifact_id") == ""
        assert meta(schema.metadata, "iotdbfs.transform.statistics.count") == "0"
        assert schema.field("sample_id").nullable
        assert meta(schema.metadata, "iotdbfs.sample_id_location") == "first_row_per_sample"
        assert [meta(schema.metadata, f"iotdbfs.input.{i}.ordinal") for i in range(2)] == ["0", "1"]
        assert meta(schema.metadata, "iotdbfs.target.0.ordinal") == "0"
        assert [meta(schema.metadata, key) for key in (
            "iotdbfs.input_length", "iotdbfs.gap", "iotdbfs.target_length", "iotdbfs.sample_count",
        )] == ["2", "0", "1", "1"]
        rows = reader.read_all().to_pylist()
        assert len(rows) == 3
        assert [row["role"] for row in rows] == ["input", "input", "target"]
        assert [row["step"] for row in rows] == [0, 1, 2]
        assert [row["time_ns"] for row in rows] == [1000, 1010, 1020]
        assert [row["input.0"] for row in rows] == [0.0, None, None]
        assert [row["input.1"] for row in rows] == [100.0, 101.0, None]
        assert [row["target.0"] for row in rows] == [None, None, 2.0]
        assert rows[0]["sample_id"] is not None
        assert [row["sample_id"] for row in rows[1:]] == [None, None]
        assert [row["sample_index"] for row in rows] == [0, 0, 0]
        assert rows[1]["descriptor_json"] is None
        assert rows[2]["descriptor_json"] is None
        descriptor = json.loads(rows[0]["descriptor_json"])
        assert descriptor["sample_id"] == rows[0]["sample_id"]
        assert descriptor["series_id"] == "series"
        assert descriptor["anchor"] == 2
        assert descriptor["input"] == [0, 2]
        assert descriptor["target"] == [2, 3]
        assert descriptor["source_spans"]
        assert {span["path"] for span in descriptor["source_spans"]} == {"/training/source"}


def check_standardized_window(root: Path):
    with ipc.open_stream(root / "window-standardized.ipc") as reader:
        schema = reader.schema
        metadata = schema.metadata
        assert meta(metadata, "iotdbfs.protocol") == "iotdbfs-window-flat/3"
        assert meta(metadata, "iotdbfs.transform.kind") == "standardize"
        assert meta(metadata, "iotdbfs.transform.artifact_id") == "train-statistics-v1"
        assert meta(metadata, "iotdbfs.transform.statistics.count") == "2"
        assert [meta(metadata, f"iotdbfs.transform.statistics.{i}.ordinal")
                for i in range(2)] == ["0", "1"]
        assert [meta(metadata, f"iotdbfs.transform.statistics.{i}.field_id")
                for i in range(2)] == ["target", "target"]
        assert float(meta(metadata, "iotdbfs.transform.statistics.0.mean")) == -1.0000000000000002
        assert float(meta(metadata, "iotdbfs.transform.statistics.0.stddev")) == 2.0
        assert float(meta(metadata, "iotdbfs.transform.statistics.1.mean")) == 100.0
        assert float(meta(metadata, "iotdbfs.transform.statistics.1.stddev")) == 1.0
        rows = reader.read_all().to_pylist()
        assert [row["input.0"] for row in rows] == [0.5, None, None]
        assert [row["input.1"] for row in rows] == [0.0, 1.0, None]
        assert [row["target.0"] for row in rows] == [None, None, 1.5]


if __name__ == "__main__":
    directory = Path(sys.argv[1])
    check_query(directory)
    check_window(directory)
    check_standardized_window(directory)
    print("A10 PyArrow IPC oracle: query, identity, and standardized streams match native fixtures")
