"""Headless regression checks for the read-only supplementary components."""
import copy
import json
from pathlib import Path
import tempfile
import struct
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow as pa

from extensions_cases import case_registry, source_registry
from extensions_reader import (ArrowLocalReader, digest_owned, discovery_request,
                               typed_execute, typed_request, validate_typed, arrow_index_only_request, control)


class ExtensionTests(unittest.TestCase):
    def test_registry_closes_precise_source_identity_without_import_side_effects(self):
        sources=source_registry()
        self.assertEqual(len(sources),5)
        for source in sources.values():
            self.assertEqual(len(source["sha256"]),64)
            self.assertGreater(source["bytes"],0)
        cases=case_registry()
        self.assertEqual(len(cases),14)
        self.assertEqual(len([c for c in cases if c["id"].startswith("A-F4")]),4)
        self.assertEqual(sources["arrow_e370_lz4"]["bytes"],93676296)

    def test_native_arrow_reexecutes_and_owns_null_masks_and_reverse_projection(self):
        for codec in (None,"lz4"):
            with self.subTest(codec=codec),tempfile.TemporaryDirectory() as directory:
                path=Path(directory)/"source.arrows"
                schema=pa.schema([pa.field("target",pa.list_(pa.list_(pa.float32())))])
                with path.open("wb") as stream,pa.ipc.new_stream(stream,schema,
                        options=pa.ipc.IpcWriteOptions(compression=codec)) as writer:
                    writer.write_batch(pa.record_batch([pa.array([[[1.,None,0.,-2.],[8.,7.,6.,5.]]],type=schema.field(0).type)],schema=schema))
                    writer.write_batch(pa.record_batch([pa.array([[[20.,21.,22.,23.],[24.,25.,26.,27.]]],type=schema.field(0).type)],schema=schema))
                case=dict(id="test",format="arrow",batch=0,row=0,field="target",children=[1,0],begin=1,length=3)
                reader=ArrowLocalReader(dict(raw=str(path)),"FS-native")
                reader.prepare()
                record,owned=reader.execute(case)
                self.assertEqual(record["T_ns"],record["d_ns"]+record["q_ns"])
                self.assertEqual(record["d_ns"],0)
                self.assertEqual(record["t0_ns"],record["t1_ns"])
                self.assertGreaterEqual(record["native_parse_ns"],0)
                self.assertGreater(record["native_batch_decoded_bytes"],0)
                np.testing.assert_array_equal(owned["columns"][0][0],[7.,6.,5.])
                np.testing.assert_array_equal(owned["columns"][1][0],[0.,0.,-2.])
                np.testing.assert_array_equal(owned["columns"][1][1],[0,1,1])
                first=digest_owned(case,owned)
                owned["columns"][0][0][:]=999.
                _,fresh=reader.execute(case)
                self.assertEqual(digest_owned(case,fresh),first)
                case2=dict(case,batch=1)
                _,second=reader.execute(case2)
                np.testing.assert_array_equal(second["columns"][0][0],[25.,26.,27.])

    def test_exact_requests_use_dynamic_owner_identity(self):
        source=source_registry()["arrow_e370"]
        source.update(logical_path="/private/new.arrows",version=1234)
        case=next(c for c in case_registry() if c["id"]=="A-F4-E370-C370-R128")
        request=typed_request(source,case)
        self.assertEqual(request["request"]["sources"]["versions"]["exact"],1234)
        self.assertEqual(request["request"]["subset"]["projection"][1],"value.369")
        with self.assertRaisesRegex(ValueError,"not unit catalog"):
            discovery_request(source,case)
        visibility=arrow_index_only_request(source,case,latest=True)
        self.assertEqual(visibility["kind"],"typed")
        self.assertEqual(visibility["request"]["subset"]["projection"],["index"])
        self.assertEqual(visibility["request"]["sources"]["versions"],dict(kind="latest"))
        self.assertEqual(len(visibility["request"]["subset"]["physical_array"]["values"]),1)
        self.assertEqual(arrow_index_only_request(source)["request"]["sources"]["versions"],dict(kind="exact",exact=1234))
        json.dumps(request,allow_nan=False)

    def test_typed_owned_clock_and_batch_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"result.arrows"
            source=dict(logical_path="/private/source.arrows",version=1234)
            case=dict(id="test",format="arrow",batch=0,row=0,field="target",children=[0],begin=1,length=2)
            schema=pa.schema([pa.field("index",pa.uint64()),pa.field("value.0",pa.float32())])
            batch=pa.record_batch([pa.array([1,2],type=pa.uint64()),pa.array([None,0.],type=pa.float32())],schema=schema)
            with path.open("wb") as stream,pa.ipc.new_stream(stream,schema) as writer:
                writer.write_batch(batch,custom_metadata={b"iotdbfs.sources":b"1",b"iotdbfs.source.0.path":b"/private/source.arrows",b"iotdbfs.source.0.version":b"1234"})
            calls=[]
            def send(socket,request):
                calls.append(request["kind"])
                return dict(ok=True,path=str(path))
            record,owned=typed_execute("unused",source,case,send=send)
            self.assertEqual(calls,["typed"])
            self.assertEqual(record["T_ns"],record["d_ns"]+record["q_ns"])
            self.assertEqual(record["d_ns"],0)
            self.assertEqual(record["q_ns"],record["T_ns"])
            self.assertEqual(record["t0_ns"],record["t1_ns"])
            self.assertIsNone(record["catalog"])
            self.assertEqual(record["expected_profile_count"],1)
            self.assertTrue(validate_typed(record,source,case))
            self.assertEqual(digest_owned(case,owned)["rows"],2)
            bad=copy.deepcopy(record)
            bad["batch_metadata"][0]["iotdbfs.source.0.version"]="1235"
            with self.assertRaisesRegex(ValueError,"version mismatch"):
                validate_typed(bad,source,case)

    def test_hdf5_keeps_real_catalog_plus_typed_profile_count(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"hdf5-result.arrows"
            source=dict(logical_path="/private/source.h5",version=1234,dataset="/T",
                        view="physical",dtype="<f4",attribute="standard_name",attribute_value="air_temperature")
            case=dict(id="hdf5-test",format="hdf5",origin=[0],shape=[1],selectors={},coordinates=[])
            schema=pa.schema([pa.field("value",pa.float32())],metadata={
                b"iotdbfs.result_descriptor.0.axis.0.selection_origin":b"0",
                b"iotdbfs.result_descriptor.0.axis.0.selection_extent":b"1"})
            batch=pa.record_batch([pa.array([2.],type=pa.float32())],schema=schema)
            with path.open("wb") as stream,pa.ipc.new_stream(stream,schema) as writer:
                writer.write_batch(batch,custom_metadata={b"iotdbfs.sources":b"1",
                    b"iotdbfs.source.0.path":b"/private/source.h5",b"iotdbfs.source.0.version":b"1234"})
            calls=[]
            def send(socket,request):
                calls.append(request["kind"])
                if request["kind"]=="dataset_catalog":
                    return dict(ok=True,datasets=[dict(path=source["logical_path"],version=1234,dataset_path="/T")])
                return dict(ok=True,path=str(path))
            record,owned=typed_execute("unused",source,case,send=send)
            self.assertEqual(calls,["dataset_catalog","typed"])
            self.assertEqual(record["expected_profile_count"],2)
            self.assertGreater(record["d_ns"],0)
            self.assertEqual(record["T_ns"],record["d_ns"]+record["q_ns"])
            self.assertTrue(validate_typed(record,source,case))
            np.testing.assert_array_equal(owned["values"],[2.])

    def test_control_failure_preserves_actual_top_level_status(self):
        response=dict(ok=False,code=16,phase="encoding",message="typed result byte budget exceeded")
        body=json.dumps(response).encode()
        class Socket:
            def __init__(self):self.wire=struct.pack(">I",len(body))+body
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def settimeout(self,*args):pass
            def connect(self,*args):pass
            def sendall(self,*args):pass
            def recv(self,count):
                part=self.wire[:count];self.wire=self.wire[count:];return part
        with patch("extensions_reader.socket.socket",return_value=Socket()):
            try:control("unused",dict(kind="typed"))
            except ValueError as error:
                self.assertEqual(error.response,response)
                self.assertIn("code=16",str(error))
                self.assertIn(response["message"],str(error))
            else:self.fail("structured failure must propagate")

    def test_typed_rejects_wrong_wire_types_before_numpy_can_coerce(self):
        fixtures=[
            ("arrow-index", "arrow", "physical", ["index","value.0"], [pa.int64(),pa.float32()], [], "index must be UInt64"),
            ("arrow-value", "arrow", "physical", ["index","value.0"], [pa.uint64(),pa.float64()], [], "values must be Float32"),
            ("hdf5-physical", "hdf5", "physical", ["value"], [pa.float64()], [], "values must be Float32"),
            ("hdf5-science", "hdf5", "science", ["value"], [pa.float32()], [], "values must be Float64"),
            ("hdf5-coordinate", "hdf5", "physical", ["value","latitude"], [pa.float32(),pa.float32()], ["latitude"], "coordinates must be Float64"),
        ]
        for label,format_id,view,names,types,coordinates,error_pattern in fixtures:
            with self.subTest(label=label),tempfile.TemporaryDirectory() as directory:
                path=Path(directory)/"wrong-schema.arrows"
                source=dict(logical_path="/private/source",version=1234,dataset="/T",
                    view=view,dtype="<f4",attribute="standard_name",attribute_value="air_temperature")
                case=dict(id=label,format=format_id,batch=0,row=0,field="target",children=[0],
                    begin=0,length=1,origin=[0],shape=[1],selectors={},coordinates=coordinates)
                schema=pa.schema([pa.field(name,dtype) for name,dtype in zip(names,types)])
                batch=pa.record_batch([pa.array([0],type=dtype) for dtype in types],schema=schema)
                with path.open("wb") as stream,pa.ipc.new_stream(stream,schema) as writer:
                    writer.write_batch(batch)
                def send(socket,request):
                    if request["kind"]=="dataset_catalog":return dict(ok=True,datasets=[])
                    return dict(ok=True,path=str(path))
                with self.assertRaisesRegex(ValueError,error_pattern):
                    typed_execute("unused",source,case,send=send)


if __name__=="__main__":
    unittest.main()
