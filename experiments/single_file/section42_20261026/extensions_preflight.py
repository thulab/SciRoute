"""Raw-source / independent oracle validation only; no service queries.

This command cannot mount, publish, adjust resources or modify any source.
Its output deliberately contains no query latency samples.
"""
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow as pa

from extensions_cases import case_registry, source_registry
from extensions_reader import (ArrowLocalReader, ScientificNativeReader, _owned,
                               digest_owned, hdf5_oracle, verify_source)
from read_matrix_native import NativeHdf5


def arrow_oracle(source,case):
    with pa.memory_map(source["raw"],"r") as mapped:
        stream=pa.ipc.open_stream(mapped)
        for _ in range(case["batch"]+1): batch=stream.read_next_batch()
        outer=batch.column("target")[case["row"]].values
        columns=[_owned(outer[c].values.slice(case["begin"],case["length"])) for c in case["children"]]
        index=np.arange(case["begin"],case["begin"]+case["length"],dtype="<u8")
        del outer,batch,stream
    return dict(columns=columns,index=index)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library",required=True)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    if args.output.exists(): raise ValueError("preflight output already exists")
    sources=source_registry();cases=case_registry()
    report=dict(protocol="iotdbfs-section42-extensions-preflight/1",complete=False,
        scope="raw originals, independent oracles and native library correctness; no scientific performance samples, no service query",
        sources={},cases=[],versions=dict(pyarrow=pa.__version__,numpy=np.__version__))
    def write():
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(json.dumps(report,indent=2,allow_nan=False)+"\n")
    write()
    native=NativeHdf5(args.library)
    report["hdf5_library"]=native.library_path
    for key,source in sources.items():
        report["sources"][key]=dict(source,verification=verify_source(source))
        write()
    for case in cases:
        source=sources[case["source"]]
        if case["format"]=="arrow":
            oracle=arrow_oracle(source,case)
            expected=digest_owned(case,oracle)
            del oracle
            reader=ArrowLocalReader(source,"FS-native");reader.prepare()
            _,owned=reader.execute(case)
            actual=digest_owned(case,owned)
            del owned
            metadata={}
        else:
            metadata,oracle=hdf5_oracle(source,case)
            expected=digest_owned(case,oracle)
            del oracle
            reader=ScientificNativeReader(native,source);reader.prepare()
            _,owned=reader.execute(case)
            actual=digest_owned(case,owned)
            del owned
        if actual!=expected:
            report["cases"].append(dict(case=case,metadata=metadata,expected=expected,actual=actual,valid=False))
            write();raise ValueError("native/oracle mismatch: "+case["id"])
        report["cases"].append(dict(case=case,metadata=metadata,oracle=expected,valid=True))
        write()
    for key,source in sources.items():
        # Stat only: source hashes above are guarded against concurrent changes.
        import os
        stat=os.stat(source["raw"])
        identity=[stat.st_dev,stat.st_ino,stat.st_size,stat.st_mtime_ns]
        if identity!=report["sources"][key]["verification"]["stat_identity"]:
            raise ValueError("source changed during preflight")
    report["complete"]=True;write()
    print(json.dumps(dict(complete=True,validated_cases=len(cases),sources=len(sources),output=str(args.output))))


if __name__=="__main__": main()
