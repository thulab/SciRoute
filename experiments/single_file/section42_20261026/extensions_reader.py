"""Read-only Section 4.2 components; caller owns services and resource limits.

Arrow FS-index retains framing metadata only, never decoded batches. Every
execute reads and decodes a new native Arrow record batch and owns all output.
No mount, publication, configuration mutation or scientific run at import.
"""
from __future__ import annotations
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import struct
import time


def require(condition, message):
    if not condition:
        raise ValueError(message)


def _libs():
    import numpy as np
    import pyarrow as pa
    return np, pa


def _identity(path):
    value = os.stat(path)
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)


def verify_source(source):
    """Outside query clocks; hash original source once per campaign."""
    identity = _identity(source["raw"])
    require(identity[2] == source["bytes"], "source size mismatch")
    with open(source["raw"], "rb") as stream:
        actual = hashlib.file_digest(stream, "sha256").hexdigest()
    require(actual == source["sha256"], "source SHA-256 mismatch")
    require(_identity(source["raw"]) == identity, "source changed while hashing")
    return dict(stat_identity=list(identity), sha256=actual, bytes=identity[2])


def _parser():
    # Existing metadata-only IPC framing parser; rejects dictionaries and
    # unsupported schemas before a scientific measurement can be accepted.
    path = Path(__file__).resolve().parents[3] / "tests/benchmarks/arrow_training/a00_range_spike.py"
    spec = importlib.util.spec_from_file_location("section42_arrow_framing", path)
    import sys
    module = sys.modules.get(spec.name)
    if module is None:
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    return module


def _owned(array):
    np, pa = _libs()
    if isinstance(array, pa.ChunkedArray):
        array = array.combine_chunks()
    require(array.type == pa.float32(), "expected Arrow Float32")
    values = np.array(array.fill_null(0).to_numpy(zero_copy_only=False), dtype="<f4", copy=True)
    mask = np.array(array.is_valid().to_numpy(zero_copy_only=False), dtype="u1", copy=True)
    require(values.flags.owndata and mask.flags.owndata, "Arrow output ownership failure")
    return values, mask


class ArrowLocalReader:
    """Native Arrow batch decoding baseline, with/without framing directory."""
    def __init__(self, source, method):
        require(method in ("FS-native", "FS-scan", "FS-index"), "unknown local Arrow method")
        self.source = source.copy()
        self.method = method
        self.identity = _identity(source["raw"])
        self.framing = None if method == "FS-native" else _parser()
        _libs()  # imports/setup stay outside every timed query
        self.schema = self.batches = None
        self.preparation = dict(prepare_ns=0, metadata_read_calls=0, metadata_read_bytes=0,
                                retains_decoded_batch=False)

    def _index(self):
        reader = self.framing.RecordingRangeReader(Path(self.source["raw"]))
        try:
            schema, batches, messages = self.framing.index_stream(reader)
            require(all(m["type"] != "dictionary" for m in messages), "dictionary baseline unsupported")
            return schema, batches, reader.reads
        finally:
            reader.close()

    def prepare(self):
        start = time.perf_counter_ns()
        if self.method == "FS-index":
            self.schema, self.batches, reads = self._index()
        else:
            reads = []
        end = time.perf_counter_ns()
        self.preparation.update(prepare_ns=end-start, metadata_read_calls=len(reads),
                                metadata_read_bytes=sum(r["length"] for r in reads))
        return self.preparation.copy()

    def execute(self, case):
        if self.method == "FS-native":
            return self._execute_native(case)
        np, pa = _libs()
        start = time.perf_counter_ns()
        require(_identity(self.source["raw"]) == self.identity, "source identity changed")
        if self.method == "FS-scan":
            schema, batches, metadata_reads = self._index()
        else:
            require(self.batches is not None, "FS-index prepare must precede execute")
            schema, batches, metadata_reads = self.schema, self.batches, []
        require(case["batch"] < len(batches), "Arrow batch absent")
        batch_meta = batches[case["batch"]]
        selected = time.perf_counter_ns()
        with pa.memory_map(self.source["raw"], "r") as mapped:
            mapped.seek(batch_meta.message_offset)
            message = pa.ipc.read_message(mapped)
            batch = pa.ipc.read_record_batch(message, schema)
            scalar = batch.column(case.get("field", "target"))[case["row"]]
            require(scalar.is_valid, "null parent list unsupported in frozen PB00 baseline")
            outer = scalar.values
            columns = []
            for child in case["children"]:
                child_scalar = outer[child]
                require(child_scalar.is_valid, "null child list unsupported in frozen PB00 baseline")
                leaf = child_scalar.values
                require(case["begin"] + case["length"] <= len(leaf), "Arrow selection outside child")
                columns.append(_owned(leaf.slice(case["begin"], case["length"])))
            index = np.arange(case["begin"], case["begin"]+case["length"], dtype="<u8")
            del batch, message, scalar, outer
        require(_identity(self.source["raw"]) == self.identity, "source changed during query")
        end = time.perf_counter_ns()
        owned = dict(columns=columns, index=index)
        return dict(d_ns=selected-start, q_ns=end-selected, T_ns=end-start,
            t0_ns=start,t1_ns=selected,t2_ns=end,
            metadata_read_calls=len(metadata_reads), metadata_read_bytes=sum(r["length"] for r in metadata_reads),
            batch_body_bytes=batch_meta.body_length, logical_batch_read_bytes=(batch_meta.body_offset-
                batch_meta.message_offset+batch_meta.body_length),
            native_decode_count=1, output_rows=case["length"], output_columns=len(columns),
            owned_bytes=sum(v.nbytes+m.nbytes for v,m in columns)+index.nbytes,
            first_is_application_only=True), owned

    def _execute_native(self,case):
        np,pa=_libs()
        start=time.perf_counter_ns()
        require(_identity(self.source["raw"])==self.identity,"source identity changed")
        with pa.memory_map(self.source["raw"],"r") as mapped:
            reader=pa.ipc.open_stream(mapped)
            selected=time.perf_counter_ns()
            batch=None
            for _ in range(case["batch"]+1):
                batch=reader.read_next_batch()
            native_batch_bytes=batch.nbytes
            scalar=batch.column(case.get("field","target"))[case["row"]]
            require(scalar.is_valid,"null parent list unsupported in frozen PB00 baseline")
            outer=scalar.values
            columns=[]
            child_scalar=leaf=None
            for child in case["children"]:
                child_scalar=outer[child]
                require(child_scalar.is_valid,"null child list unsupported in frozen PB00 baseline")
                leaf=child_scalar.values
                require(case["begin"]+case["length"]<=len(leaf),"Arrow selection outside child")
                columns.append(_owned(leaf.slice(case["begin"],case["length"])))
            index=np.arange(case["begin"],case["begin"]+case["length"],dtype="<u8")
            del batch,scalar,outer,child_scalar,leaf,reader
        require(_identity(self.source["raw"])==self.identity,"source changed during query")
        end=time.perf_counter_ns()
        return dict(d_ns=0,q_ns=end-start,T_ns=end-start,
            t0_ns=start,t1_ns=start,t2_ns=end,
            native_parse_ns=selected-start,
            native_parse_scope="source identity check, mmap open and native stream schema parse",
            native_decode_count=case["batch"]+1,native_batch_decoded_bytes=native_batch_bytes,
            output_rows=case["length"],output_columns=len(columns),
            owned_bytes=sum(v.nbytes+m.nbytes for v,m in columns)+index.nbytes,
            source_mmap_extent_bytes=self.identity[2],
            physical_read_bytes=None,io_note="mmap extent is not physical I/O; native IPC stream navigation"),dict(columns=columns,index=index)


def selector(source):
    return dict(paths=[dict(kind="exact", path=source["logical_path"])],
                versions=dict(kind="exact", exact=source["version"]))


def typed_request(source, case):
    src = selector(source)
    if case["format"] == "arrow":
        def bound(value, kind):
            return dict(kind=kind, value=dict(type="uint64", value=value))
        src["formats"] = dict(adapters="listed", adapter_ids=["arrow"], include_byte_passthrough=False)
        subset = dict(projection=["index"]+[f"value.{child}" for child in case["children"]],
            physical_array=dict(field_id=case.get("field","target"), record_batch_index=case["batch"],
                row_index=case["row"], values=[dict(child_indices=[child]) for child in case["children"]]),
            slices=[dict(dimension="index", lower=bound(case["begin"],"inclusive"),
                upper=bound(case["begin"]+case["length"],"exclusive"))])
    else:
        from urllib.parse import quote
        field = "hdf5.science.value:" if source["view"] == "science" else "hdf5.value:"
        subset = dict(projection=[field+quote(source["dataset"],safe="")]+
            ["hdf5.coord."+role for role in case.get("coordinates",[])], slices=[])
        for role, (lower,upper) in case.get("selectors",{}).items():
            subset["slices"].append(dict(dimension="hdf5.coord."+role,
                lower=dict(kind="inclusive",value=dict(type="float64",value=lower)),
                upper=dict(kind="exclusive",value=dict(type="float64",value=upper))))
    return dict(kind="typed",request=dict(sources=src,subset=subset))


def discovery_request(source, case):
    if case["format"] == "arrow":
        raise ValueError("Arrow supports known Exact physical selection, not unit catalog discovery")
    return dict(kind="dataset_catalog",request=dict(sources=selector(source),
        dataset_path=source["dataset"],predicate=dict(kind="compare",field="hdf5.attr:"+source["attribute"],
            operator="eq",value=dict(type="utf8",value=source["attribute_value"]))))


def arrow_index_only_request(source, case=None, *, latest=False):
    """Publication visibility check through an actual one-row typed result.

    Arrow has no unit-catalog endpoint. A valid physical value address remains
    in the declaration, while index-only projection requires no float payload.
    The caller must independently verify zero payload reads in its profile and
    obtain exact identity from each result batch's provenance metadata.
    This helper is not used by online latency collection.
    """
    case = dict(case or dict(format="arrow", batch=0, row=0, field="target", children=[0], begin=0))
    case.update(format="arrow", length=1, children=case.get("children",[0])[:1])
    known = dict(source, version=source.get("version", 1))
    request = typed_request(known, case)
    request["request"]["subset"]["projection"] = ["index"]
    if latest:
        request["request"]["sources"]["versions"] = dict(kind="latest")
    return request


def control(socket_path, request):
    payload = json.dumps(request,separators=(",",":"),allow_nan=False).encode()
    require(len(payload) <= 1048576,"request frame exceeded")
    def receive(stream,size):
        parts=bytearray()
        while len(parts)<size:
            part=stream.recv(size-len(parts))
            require(part,"short response")
            parts.extend(part)
        return bytes(parts)
    with socket.socket(socket.AF_UNIX) as stream:
        stream.settimeout(600)
        stream.connect(str(socket_path))
        stream.sendall(struct.pack(">I",len(payload))+payload)
        size,=struct.unpack(">I",receive(stream,4))
        require(0<size<=1048576,"response frame exceeded")
        response=json.loads(receive(stream,size))
    if response.get("ok") is not True:
        error=ValueError("query failed: code="+str(response.get("code"))+" phase="+
            str(response.get("phase"))+" message="+str(response.get("message",response.get("error","unspecified failure"))))
        error.response=response
        raise error
    return response


def _validate_ipc_types(table,source,case):
    """Check wire types before NumPy conversions can coerce a wrong schema."""
    _,pa=_libs()
    if case["format"]=="arrow":
        require(table.column("index").type==pa.uint64(),"Arrow IPC index must be UInt64")
        for child in case["children"]:
            require(table.column(f"value.{child}").type==pa.float32(),"Arrow IPC values must be Float32")
    else:
        coordinates=case.get("coordinates",[])
        require(table.num_columns==1+len(coordinates),"HDF5 IPC column count differs")
        expected=pa.float64() if source["view"]=="science" else pa.float32()
        require(table.column(0).type==expected,
                "HDF5 IPC values must be "+("Float64" if source["view"]=="science" else "Float32"))
        for axis,_ in enumerate(coordinates):
            require(table.column(axis+1).type==pa.float64(),"HDF5 IPC coordinates must be Float64")


def typed_execute(socket_path, source, case, send=control):
    """Known Exact Arrow, or HDF5 discovery, then fresh cursor and owned output.

    Discovery response and result descriptor are validated after both timed
    intervals, alongside the returned arrays by the caller.
    """
    np, pa = _libs()
    discovery = None if case["format"] == "arrow" else discovery_request(source,case)
    request = typed_request(source,case)
    start=time.perf_counter_ns()
    if discovery is None:
        catalog=None
        discovered=start
    else:
        catalog=send(socket_path,discovery)
        discovered=time.perf_counter_ns()
    response=send(socket_path,request)
    registered=time.perf_counter_ns()
    with pa.memory_map(response["path"],"r") as mapped:
        size=mapped.size()
        reader=pa.ipc.open_stream(mapped)
        batches=[]
        batch_metadata=[]
        while True:
            try:
                item=reader.read_next_batch_with_custom_metadata()
            except StopIteration:
                break
            batches.append(item.batch)
            batch_metadata.append(item.custom_metadata)
        table=pa.Table.from_batches(batches,schema=reader.schema)
        decoded=time.perf_counter_ns()
        _validate_ipc_types(table,source,case)
        if case["format"] == "arrow":
            columns=[_owned(table.column(f"value.{child}")) for child in case["children"]]
            index=np.array(table.column("index").to_numpy(),dtype="<u8",copy=True)
            owned=dict(columns=columns,index=index)
        else:
            column=table.column(0)
            mask=np.array(column.is_valid().to_numpy(),dtype="u1",copy=True)
            dtype="<f8" if source["view"]=="science" else source["dtype"]
            values=np.array(column.fill_null(0).to_numpy(zero_copy_only=False),dtype=dtype,copy=True)
            coords={role:np.array(table.column(axis+1).to_numpy(),dtype="<f8",copy=True)
                for axis,role in enumerate(case.get("coordinates",[]))}
            owned=dict(values=values,mask=mask,coordinates=coords)
        metadata={k.decode():v.decode() for k,v in (table.schema.metadata or {}).items()}
        names=table.column_names
        types=[str(field.type) for field in table.schema]
        column=None
        item=None
        del table,batches,reader,column,item
    end=time.perf_counter_ns()
    return dict(d_ns=discovered-start,q_ns=end-discovered,T_ns=end-start,
        t0_ns=start,t1_ns=discovered,t2_ns=end,
        register_ns=registered-discovered,fetch_decode_ns=decoded-registered,
        owned_and_close_ns=end-decoded,encoded_bytes=size,path=response["path"],
        catalog=catalog,metadata=metadata,column_names=names,column_types=types,
        expected_profile_count=1 if discovery is None else 2,
        batch_metadata=[{k.decode():v.decode() for k,v in (m or {}).items()} for m in batch_metadata]),owned


def digest_owned(case,owned):
    """Correctness hashing is deliberately outside T."""
    np,_=_libs()
    if case["format"]=="arrow":
        h=hashlib.sha256()
        for values,mask in owned["columns"]:
            require(values.dtype==np.dtype("<f4") and mask.dtype==np.dtype("u1"),"output dtype mismatch")
            require(values.flags.owndata and mask.flags.owndata,"output not owned")
            h.update(values.tobytes());h.update(mask.tobytes())
        index=owned["index"]
        require(np.array_equal(index,np.arange(case["begin"],case["begin"]+case["length"],dtype="<u8")),"index order mismatch")
        return dict(digest=h.hexdigest(),index_digest=hashlib.sha256(index.tobytes()).hexdigest(),rows=len(index),
                    owned_bytes=sum(v.nbytes+m.nbytes for v,m in owned["columns"])+index.nbytes)
    values,mask=owned["values"],owned["mask"]
    require(values.flags.owndata and mask.flags.owndata,"HDF5 output not owned")
    h=hashlib.sha256();h.update(values.tobytes());h.update(mask.tobytes())
    return dict(digest=h.hexdigest(),rows=len(values),masked=int(np.count_nonzero(mask==0)),dtype=values.dtype.str,
        coordinates={role:hashlib.sha256(arr.astype("<f8",copy=False).tobytes()).hexdigest()
                     for role,arr in owned.get("coordinates",{}).items()},
        owned_bytes=values.nbytes+mask.nbytes+sum(arr.nbytes for arr in owned.get("coordinates",{}).values()))


def validate_typed(record,source,case):
    """Validate exact identity and requested schema independently of clocks."""
    if case["format"]=="arrow":
        require(record.get("catalog") is None,"Arrow must not invent a unit catalog")
    else:
        entries=record["catalog"]["datasets"]
        require(entries,"catalog selected no dataset")
        require(all(e["path"]==source["logical_path"] and e["version"]==source["version"] for e in entries),
                "catalog exact source/version mismatch")
    require(record["batch_metadata"],"no scientific result batch")
    for metadata in record["batch_metadata"]:
        require(metadata.get("iotdbfs.sources")=="1","result provenance source count mismatch")
        require(metadata.get("iotdbfs.source.0.path")==source["logical_path"] and
                metadata.get("iotdbfs.source.0.version")==str(source["version"]),"IPC exact source/version mismatch")
    if case["format"]=="arrow":
        expected=["index"]+[f"value.{child}" for child in case["children"]]
        require(record["column_names"]==expected,"Arrow projection ordering mismatch")
    else:
        require(len(entries)==1 and entries[0]["dataset_path"]==source["dataset"],"dataset identity mismatch")
        prefix="iotdbfs.result_descriptor.0."
        for axis,(origin,extent) in enumerate(zip(case["origin"],case["shape"])):
            require(record["metadata"].get(prefix+f"axis.{axis}.selection_origin")==str(origin) and
                    record["metadata"].get(prefix+f"axis.{axis}.selection_extent")==str(extent),
                    "HDF5 result descriptor selection mismatch")
    return True


def hdf5_oracle(source,case,*,with_arrays=True):
    """h5py reference only: never use its latency as the production-library arm."""
    import h5py
    np,_=_libs()
    with h5py.File(source["raw"],"r") as file:
        data=file[source["dataset"]]
        require(list(data.shape)==source["source_shape"],"native source shape mismatch")
        require(list(data.chunks)==source["chunk_shape"],"native source chunk mismatch")
        text=data.attrs[source["attribute"]]
        if isinstance(text,bytes): text=text.decode()
        require(str(text)==source["attribute_value"],"dataset predicate mismatch")
        axes={role:np.array(file[path][...],dtype="<f8",copy=True).ravel()
              for role,path in zip(source["roles"],source["coordinate_datasets"])}
        origins=[];shape=[]
        for axis,role in enumerate(source["roles"]):
            positions=np.arange(len(axes[role]))
            if role in case.get("selectors",{}):
                lower,upper=case["selectors"][role]
                positions=np.flatnonzero((axes[role]>=lower)&(axes[role]<upper))
            require(len(positions)>0 and np.array_equal(positions,np.arange(positions[0],positions[-1]+1)),
                    "coordinate query has nonrectangular selection")
            origins.append(int(positions[0]));shape.append(len(positions))
        require(origins==case["origin"] and shape==case["shape"],"coordinate selection differs from declared parameters")
        attrs={name:np.array(value).tolist() for name,value in data.attrs.items()
               if name in ("_FillValue","missing_value","valid_range","valid_min","valid_max","scale_factor","add_offset")}
        metadata=dict(origin=origins,shape=shape,coordinates={k:v.tolist() for k,v in axes.items()},
                      numeric_attributes=attrs,dtype=data.dtype.str,chunks=list(data.chunks))
        if not with_arrays:
            return metadata
        raw=np.array(data[tuple(slice(o,o+n) for o,n in zip(origins,shape))],copy=True).ravel(order="C").copy()
        mask=np.ones(raw.size,dtype="u1")
        if source["view"]=="science":
            mask[~np.isfinite(raw)]=0
            for attr in ("_FillValue","missing_value"):
                for value in np.asarray(attrs.get(attr,[])).ravel():
                    mask[raw==value]=0
            valid_range=np.asarray(attrs.get("valid_range",[])).ravel()
            lo=valid_range[0] if valid_range.size==2 else attrs.get("valid_min")
            hi=valid_range[1] if valid_range.size==2 else attrs.get("valid_max")
            if lo is not None: mask[raw<lo]=0
            if hi is not None: mask[raw>hi]=0
            values=raw.astype("<f8")
            values*=float(np.asarray(attrs.get("scale_factor",1.)).item())
            values+=float(np.asarray(attrs.get("add_offset",0.)).item())
            require(np.isfinite(values[mask.astype(bool)]).all(),"scientific transform overflow")
            values[mask==0]=0
        else:
            values=raw.astype(source["dtype"],copy=True)
        coordinates={}
        # No rank-sized np.indices allocation. Broadcasting only materializes
        # each required final independent coordinate vector.
        for axis,role in enumerate(source["roles"]):
            if role not in case.get("coordinates",[]): continue
            vector=axes[role][origins[axis]:origins[axis]+shape[axis]]
            selected_shape=[1]*len(shape);selected_shape[axis]=len(vector)
            coordinates[role]=np.broadcast_to(vector.reshape(selected_shape),shape).ravel(order="C").copy()
    return metadata,dict(values=values,mask=mask,coordinates=coordinates)


class ScientificNativeReader:
    """GEOS physical / OISST CF reads through the daemon's HDF5 core ABI.

    The caller supplies read_matrix_native.NativeHdf5 from the exact daemon
    linked library. No h5py calls occur in this reader's timed operation.
    Native embedded chunk/B-tree indexes are retained by normal HDF5 H5Dread.
    """
    NUMERIC_ATTRIBUTES=("_FillValue","missing_value","valid_range","valid_min","valid_max","scale_factor","add_offset")

    def __init__(self,native,source):
        self.native=native
        self.source=source.copy()
        self.identity=_identity(source["raw"])
        self.text_attribute=_NativeHdf5Text(native).read
        _libs()

    def prepare(self):
        # No sidecar catalog or preloaded coordinate vectors in FS-native.
        return dict(prepare_ns=0,retains_decoded_values=False,retains_coordinates=False,
                    hdf5_library=self.native.library_path)

    def execute(self,case):
        np,_=_libs()
        source=self.source
        start=time.perf_counter_ns()
        require(_identity(source["raw"])==self.identity,"native source identity changed")
        fid=self.native.open_file(source["raw"])
        try:
            descriptor=self.native.describe(fid,source["dataset"])
            require(descriptor["source_shape"]==source["source_shape"] and descriptor["dtype"]==source["dtype"],
                    "native HDF5 dataset metadata differs")
            require(self.text_attribute(fid,source["dataset"],source["attribute"])==source["attribute_value"],
                    "native HDF5 attribute predicate failed")
            origins=[];shape=[];axes={}
            for dimension,(role,path) in enumerate(zip(source["roles"],source["coordinate_datasets"])):
                extent=source["source_shape"][dimension]
                coord=self.native.read_dataset(fid,path,[0],[extent],"<f8")
                require(np.isfinite(coord).all(),"nonfinite native coordinate")
                positions=np.arange(extent)
                if role in case.get("selectors",{}):
                    lo,hi=case["selectors"][role]
                    positions=np.flatnonzero((coord>=lo)&(coord<hi))
                require(len(positions)>0 and np.array_equal(positions,np.arange(positions[0],positions[-1]+1)),
                        "nonrectangular native coordinate selection")
                origins.append(int(positions[0]));shape.append(len(positions));axes[role]=coord
            attrs=self.native.numeric_attributes(fid,source["dataset"],self.NUMERIC_ATTRIBUTES) if source["view"]=="science" else {}
            discovered=time.perf_counter_ns()
            values=self.native.read_dataset(fid,source["dataset"],origins,shape,source["dtype"],
                                            max_result_bytes=512*1024*1024)
            mask=np.ones(values.size,dtype="u1")
            if source["view"]=="science":
                raw=values
                mask[~np.isfinite(raw)]=0
                for name in ("_FillValue","missing_value"):
                    for missing in attrs.get(name,[]):
                        mask[raw==missing]=0
                valid_range=attrs.get("valid_range",[])
                lower=valid_range[0] if len(valid_range)==2 else (attrs.get("valid_min") or [None])[0]
                upper=valid_range[1] if len(valid_range)==2 else (attrs.get("valid_max") or [None])[0]
                if lower is not None: mask[raw<lower]=0
                if upper is not None: mask[raw>upper]=0
                values=raw.astype("<f8")
                values*=float((attrs.get("scale_factor") or [1.])[0])
                values+=float((attrs.get("add_offset") or [0.])[0])
                require(np.isfinite(values[mask.astype(bool)]).all(),"native scientific transform overflow")
                values[mask==0]=0.
            coordinates={}
            for axis,role in enumerate(source["roles"]):
                if role not in case.get("coordinates",[]): continue
                selected=axes[role][origins[axis]:origins[axis]+shape[axis]]
                axis_shape=[1]*len(shape);axis_shape[axis]=len(selected)
                coordinates[role]=np.broadcast_to(selected.reshape(axis_shape),shape).ravel(order="C").copy()
            del axes
        finally:
            self.native.close_file(fid)
        require(_identity(source["raw"])==self.identity,"native source changed during query")
        end=time.perf_counter_ns()
        require(origins==case["origin"] and shape==case["shape"],"native selection differs from declared ROI")
        owned=dict(values=values,mask=mask,coordinates=coordinates)
        return dict(d_ns=discovered-start,q_ns=end-discovered,T_ns=end-start,origin=origins,shape=shape,
            t0_ns=start,t1_ns=discovered,t2_ns=end,
            rows=values.size,dtype=values.dtype.str,owned_bytes=values.nbytes+mask.nbytes+
                sum(c.nbytes for c in coordinates.values()),coordinate_elements_read=sum(source["source_shape"]),
            hdf5_library=self.native.library_path),owned


class _NativeHdf5Text:
    """Scalar HDF5 text attributes, read through the supplied pinned library."""
    def __init__(self,native):
        import ctypes as C
        self.C=C;self.native=native;self.lib=native.lib
        signatures={"H5Aget_type":(C.c_int64,[C.c_int64]),
            "H5Tis_variable_str":(C.c_int,[C.c_int64]),"H5free_memory":(C.c_int,[C.c_void_p])}
        for name,(ret,args) in signatures.items():
            fun=getattr(self.lib,name);fun.restype=ret;fun.argtypes=args

    def read(self,fid,dataset,name):
        C=self.C;lib=self.lib;n=self.native
        did=n._open_dataset(fid,dataset)
        aid=sid=tid=-1
        try:
            require(lib.H5Aexists(did,name.encode())>0,"required HDF5 attribute absent")
            aid=n._check(lib.H5Aopen(did,name.encode(),0),"open text attribute")
            sid=n._check(lib.H5Aget_space(aid),"text attribute space")
            require(lib.H5Sget_simple_extent_npoints(sid)==1,"nonscalar text attribute")
            tid=n._check(lib.H5Aget_type(aid),"text attribute type")
            require(lib.H5Tget_class(tid)==3,"attribute is not text")
            variable=lib.H5Tis_variable_str(tid)
            n._check(variable,"text storage type")
            if variable:
                ptr=C.c_void_p()
                n._check(lib.H5Aread(aid,tid,C.byref(ptr)),"read variable text")
                try:
                    require(ptr.value is not None,"null text attribute")
                    value=C.string_at(ptr.value)
                    require(len(value)<=65536,"text attribute exceeds bound")
                finally:
                    if ptr.value is not None: n._check(lib.H5free_memory(ptr),"free variable text")
            else:
                size=lib.H5Tget_size(tid)
                require(0<size<=65536,"text attribute exceeds bound")
                buffer=C.create_string_buffer(size)
                n._check(lib.H5Aread(aid,tid,buffer),"read fixed text")
                value=buffer.raw.rstrip(b"\x00 ")
            return value.decode("utf-8")
        finally:
            if tid>=0: n._check(lib.H5Tclose(tid),"close text type")
            if sid>=0: n._check(lib.H5Sclose(sid),"close text space")
            if aid>=0: n._check(lib.H5Aclose(aid),"close text attribute")
            n._check(lib.H5Dclose(did),"close text dataset")
