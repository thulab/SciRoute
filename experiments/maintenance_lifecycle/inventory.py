"""Read-only software and host evidence; never export environment credentials."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess


def digest(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
    return h.hexdigest()


def libraries(path):
    result=[]
    out=subprocess.check_output(['ldd',str(path)],text=True)
    for line in out.splitlines():
        match=re.search(r'=> (/[^ ]+)',line)
        if match:
            p=Path(match[1]).resolve();result.append(dict(path=str(p),sha256=digest(p),bytes=p.stat().st_size))
    return dict(binary=str(path),sha256=digest(path),libraries=result)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--binary',type=Path,required=True);p.add_argument('--library',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    import h5py,numpy,pyarrow
    result=dict(platform=platform.platform(),kernel=os.uname().release,python=platform.python_version(),
                numpy=numpy.__version__,pyarrow=pyarrow.__version__,h5py=h5py.__version__,hdf5_oracle=h5py.version.hdf5_version,
                native=libraries(a.library),iotdbfs=libraries(a.binary),cpuinfo=Path('/proc/cpuinfo').read_text().split('\n\n')[0],
                mounts=subprocess.check_output(['findmnt','-T',str(a.output.parent),'-J'],text=True),
                resource_scope='software inventory outside timed worker; measured resource scope is in the supervisor and trace receipts')
    a.output.write_text(json.dumps(result,sort_keys=True,indent=2)+'\n')
