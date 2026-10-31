"""Independent tensor-oracle tests for both flat window IPC conversion paths."""

import gc
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from public_iotdbfs import IoTDBFSWindowReader, WindowArrays, convert_window_columns


LENGTH = 5
HORIZON = 3
CHANNEL_ORDER = (8, 1, 5)
ARTIFACT = "frozen-statistics"
SERIES = "conversion-fixture"


def chunked(values, dtype, style, column_index):
    """Use offset buffers and unrelated column chunk boundaries, including empty chunks."""
    if style == "single":
        return pa.chunked_array([pa.array(values, type=dtype)])
    if style == "offset":
        padded = pa.array([None] * 3 + values + [None] * 2, type=dtype)
        return pa.chunked_array([padded.slice(3, len(values))])
    total = len(values)
    cuts = {0, total}
    # Both within-role and sample-boundary splits. Different columns must not
    # rely on matching Arrow record-batch boundaries during conversion.
    for position in range(column_index % 3 + 1, total, column_index % 4 + 2):
        cuts.add(position)
    cuts.update(position for position in (LENGTH - 1, LENGTH, LENGTH + 1,
                                         LENGTH + HORIZON, LENGTH + HORIZON + 1)
                if position < total)
    cuts = sorted(cuts)
    empty = pa.array([None, None], type=dtype).slice(1, 0)
    chunks = [empty]
    for begin, end in zip(cuts, cuts[1:]):
        padded = pa.array([None] * 3 + values[begin:end] + [None], type=dtype)
        chunks.extend((padded.slice(3, end - begin), empty))
    return pa.chunked_array(chunks, type=dtype)


def fixture(n, style="mixed"):
    """Create tensors directly; flat serialization is separate from the oracle."""
    width = len(CHANNEL_ORDER)
    inputs = np.empty((n, LENGTH, width), dtype=np.float32)
    targets = np.empty((n, HORIZON, width), dtype=np.float32)
    input_valid = np.empty(inputs.shape, dtype=np.bool_)
    target_valid = np.empty(targets.shape, dtype=np.bool_)
    for sample in range(n):
        for step in range(LENGTH):
            for channel, physical in enumerate(CHANNEL_ORDER):
                inputs[sample, step, channel] = sample * 1000 + physical * 10 + step + .25
                input_valid[sample, step, channel] = (sample + step + channel) % 5 != 0
        for step in range(HORIZON):
            for channel, physical in enumerate(CHANNEL_ORDER):
                targets[sample, step, channel] = -sample * 1000 - physical * 10 - step - .75
                target_valid[sample, step, channel] = (sample + step + channel) % 4 != 0
    # IEEE NaN is an observed Float32 value here, not an Arrow null.
    inputs[0, 1, 0] = np.nan
    input_valid[0, 1, 0] = True
    targets[0, 0, 1] = np.nan
    target_valid[0, 0, 1] = True
    inputs[~input_valid] = 0
    targets[~target_valid] = 0
    expected = WindowArrays(inputs, input_valid, targets, target_valid)
    anchors = [100 + 19 * sample for sample in range(n)]
    columns = {"sample_index": [], "role": [], "step": [], "descriptor_json": []}
    for sample, anchor in enumerate(anchors):
        columns["sample_index"].extend([sample] * (LENGTH + HORIZON))
        columns["role"].extend(["input"] * LENGTH + ["target"] * HORIZON)
        columns["step"].extend(range(anchor - LENGTH, anchor + HORIZON))
        columns["descriptor_json"].extend([
            json.dumps({"anchor": anchor, "series_id": SERIES})] +
            [None] * (LENGTH + HORIZON - 1))
    for channel in range(width):
        input_column, target_column = [], []
        for sample in range(n):
            input_column.extend(float(inputs[sample, step, channel])
                                if input_valid[sample, step, channel] else None
                                for step in range(LENGTH))
            input_column.extend([None] * HORIZON)
            target_column.extend([None] * LENGTH)
            target_column.extend(float(targets[sample, step, channel])
                                 if target_valid[sample, step, channel] else None
                                 for step in range(HORIZON))
        columns[f"input.{channel}"] = input_column
        columns[f"target.{channel}"] = target_column
    names = ["sample_index", "role", "step", "descriptor_json",
             "target.2", "input.0", "input.2", "target.0", "input.1", "target.1"]
    arrays = []
    for index, name in enumerate(names):
        dtype = (pa.uint64() if name in ("sample_index", "step") else
                 pa.string() if name in ("role", "descriptor_json") else pa.float32())
        arrays.append(chunked(columns[name], dtype, style, index))
    table = pa.Table.from_arrays(arrays, names=names).replace_schema_metadata({
        b"iotdbfs.protocol": b"iotdbfs-window-flat/3",
        b"iotdbfs.transform.kind": b"standardize",
        b"iotdbfs.transform.artifact_id": ARTIFACT.encode(),
    })
    return table, expected, anchors


def reader(mode):
    # _decode only depends on the immutable expected view declarations below;
    # avoid sockets, source lookup and preparation in conversion-only tests.
    result = IoTDBFSWindowReader.__new__(IoTDBFSWindowReader)
    result.normalization = {"artifact_id": ARTIFACT}
    result.windows = {"input_length": LENGTH, "prediction_length": HORIZON}
    result.channels = CHANNEL_ORDER
    result.pb00 = {"dataset_id": SERIES}
    result.conversion_mode = mode
    return result


class WindowConversionTests(unittest.TestCase):
    def assert_arrays(self, actual, expected):
        for name in ("input_values", "input_valid", "target_values", "target_valid"):
            with self.subTest(array=name):
                value, oracle = getattr(actual, name), getattr(expected, name)
                self.assertEqual(value.shape, oracle.shape)
                self.assertEqual(value.dtype, oracle.dtype)
                np.testing.assert_array_equal(value, oracle)
                self.assertTrue(value.flags.owndata)
                self.assertTrue(value.flags.c_contiguous)
                self.assertTrue(value.flags.writeable)
        self.assertTrue(np.isnan(actual.input_values[0, 1, 0]))
        self.assertTrue(actual.input_valid[0, 1, 0])
        self.assertTrue(np.isnan(actual.target_values[0, 0, 1]))
        self.assertTrue(actual.target_valid[0, 0, 1])

    def test_both_modes_match_independent_oracle_for_samples_chunks_nulls_and_order(self):
        for n in (1, 2, 5):
            for style in ("single", "offset", "mixed"):
                table, expected, anchors = fixture(n, style)
                if style == "mixed":
                    self.assertGreater(table.column("input.0").num_chunks, 1)
                    self.assertTrue(any(len(chunk) == 0 for chunk in table.column("input.0").chunks))
                    self.assertTrue(any(chunk.offset > 0 for chunk in table.column("input.0").chunks))
                for mode in ("sample", "batch", "buffers"):
                    with self.subTest(n=n, style=style, mode=mode):
                        result = convert_window_columns(table, n, LENGTH, HORIZON,
                                                        len(CHANNEL_ORDER), mode=mode)
                        self.assert_arrays(result, expected)
                        self.assert_arrays(reader(mode)._decode(table, anchors), expected)

    def test_outputs_survive_closed_mapping_and_deleted_arrow_buffers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "windows.ipc"
            table, expected, anchors = fixture(5)
            with pa.OSFile(str(path), "wb") as sink:
                with ipc.new_stream(sink, table.schema) as writer:
                    writer.write_table(table, max_chunksize=4)
            del table
            outputs = []
            for mode in ("sample", "batch", "buffers"):
                with pa.memory_map(str(path), "r") as mapped:
                    stream = ipc.open_stream(mapped)
                    loaded = stream.read_all()
                    outputs.append(reader(mode)._decode(loaded, anchors))
                del loaded, stream, mapped
            path.unlink()
            gc.collect()
            for output in outputs:
                self.assert_arrays(output, expected)
                # Writing one output must not mutate the oracle or another
                # independently converted result through aliased buffers.
                output.input_values[0, 0, 0] = 12345
                output.input_valid[0, 0, 0] = True
                self.assertEqual(expected.input_values[0, 0, 0], 0)
                self.assertFalse(expected.input_valid[0, 0, 0])

    def test_reader_recomputes_column_positions_for_each_table(self):
        table, expected, anchors = fixture(2)
        reordered = table.select(list(reversed(table.column_names)))
        for mode in ("sample", "batch", "buffers"):
            with self.subTest(mode=mode):
                decoder = reader(mode)
                self.assert_arrays(decoder._decode(table, anchors), expected)
                self.assert_arrays(decoder._decode(reordered, anchors), expected)
                self.assert_arrays(decoder._decode(table, anchors), expected)

    def test_batch_rejects_missing_and_ambiguous_value_columns(self):
        table, _, _ = fixture(2)
        for changed in (table.drop(["input.1"]),
                        table.append_column("input.1", table.column("input.1"))):
            with self.assertRaisesRegex(ValueError, "missing or ambiguous.*input.1"):
                convert_window_columns(changed, 2, LENGTH, HORIZON,
                                       len(CHANNEL_ORDER), mode="batch")

    def test_buffers_handle_all_valid_all_null_and_nonbyte_aligned_offsets(self):
        # Exercise absent bitmap and garbage-under-null without deriving nulls
        # from NaN values. Offset 11 spans both a byte boundary and a slice.
        for null in (False, True):
            raw = np.arange(29, dtype=np.float32)
            raw[12] = np.nan
            array = pa.array(raw, mask=np.full(29, null)).slice(11, 16)
            table = pa.Table.from_arrays([array, array], names=["input.0", "target.0"])
            actual = convert_window_columns(table, 2, 5, 3, 1, "buffers")
            expected = convert_window_columns(table, 2, 5, 3, 1, "batch")
            for name in ("input_values", "input_valid", "target_values", "target_valid"):
                np.testing.assert_array_equal(getattr(actual, name), getattr(expected, name))

    def test_buffers_reject_wrong_type_length_and_ambiguous_names(self):
        table, _, _ = fixture(2)
        for changed in (table.drop(["input.1"]),
                        table.append_column("input.1", table.column("input.1"))):
            with self.assertRaisesRegex(ValueError, "missing or ambiguous"):
                convert_window_columns(changed, 2, LENGTH, HORIZON, 3, "buffers")
        wrong_type = table.set_column(table.schema.get_field_index("input.0"), "input.0",
                                      table.column("input.0").cast(pa.float64()))
        for changed in (wrong_type, table.slice(0, table.num_rows - 1)):
            with self.assertRaisesRegex(ValueError, "Float32 rows"):
                convert_window_columns(changed, 2, LENGTH, HORIZON, 3, "buffers")

    def test_reader_keeps_structural_and_identity_validation_in_both_modes(self):
        table, _, anchors = fixture(2)
        mutations = []
        for name, replacement, message in (
                ("role", "target", "roles"),
                ("step", 9999, "step positions"),
                ("sample_index", 99, "sample order"),
                ("descriptor_json", json.dumps({"anchor": 9999, "series_id": SERIES}), "another sample"),
                ("descriptor_json", json.dumps({"anchor": anchors[0], "series_id": "other"}), "another sample")):
            values = table.column(name).to_pylist()
            values[0] = replacement
            changed = table.set_column(table.schema.get_field_index(name), name,
                                       pa.array(values, type=table.schema.field(name).type))
            mutations.append((name, changed, message))
        for key, value, message in (
                (b"iotdbfs.protocol", b"other", "protocol"),
                (b"iotdbfs.transform.kind", b"identity", "transform"),
                (b"iotdbfs.transform.artifact_id", b"other", "transform")):
            metadata = dict(table.schema.metadata)
            metadata[key] = value
            mutations.append((key, table.replace_schema_metadata(metadata), message))
        mutations.append(("rows", table.slice(0, table.num_rows - 1), "number of rows"))
        for mode in ("sample", "batch", "buffers"):
            for name, changed, message in mutations:
                with self.subTest(mode=mode, invalid=name):
                    with self.assertRaisesRegex(ValueError, message):
                        reader(mode)._decode(changed, anchors)


if __name__ == "__main__":
    unittest.main()
