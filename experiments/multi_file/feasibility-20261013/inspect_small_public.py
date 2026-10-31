
import json
from pathlib import Path
import h5py
b=Path('/data_01/zzy/iotdb-fs-exp/public-arrow-baseline')
paths=sorted((b/'hdf5-sc-perf-20261006/raw').glob('*.nc'))+sorted((b/'hdf5-h00-20261001/raw').glob('*.hdf5'))
out=[]
for p in paths:
 with h5py.File(p,'r') as f:
  objects=[]
  def visit(name,o):
   if isinstance(o,h5py.Dataset):
    attrs={}
    for k in ('standard_name','units'):
     if k in o.attrs:
      v=o.attrs[k]
      if isinstance(v,bytes):v=v.decode('utf-8','replace')
      if isinstance(v,str):attrs[k]=v
    objects.append({'path':'/'+name,'shape':list(o.shape),'dtype':str(o.dtype),'chunks':list(o.chunks) if o.chunks else None,'attrs':attrs})
  f.visititems(visit)
  out.append({'path':str(p),'bytes':p.stat().st_size,'datasets':objects})
print(json.dumps(out))
