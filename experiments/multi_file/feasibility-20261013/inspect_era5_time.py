
import json,subprocess,sys
from pathlib import Path
from collections import Counter
base=Path('/data_01/zzy/iotdb-fs-exp')
result=[]
for p in sorted((base/'era5-grib-baseline/raw').glob('*/*.grib')):
 cp=subprocess.run(['grib_ls','-x','-j','-p','shortName,typeOfLevel,level:i,step:i,dataDate:i,dataTime:i,Ni:i,Nj:i,gridType,packingType,bitsPerValue:i',str(p)],capture_output=True,text=True,timeout=90,check=True)
 rows=json.loads(cp.stdout)['messages']
 summary={}
 for k in rows[0]:
  c=Counter(str(r.get(k)) for r in rows)
  summary[k]={'distinct':len(c),'counts':dict(sorted(c.items()))}
 result.append({'path':str(p),'bytes':p.stat().st_size,'messages':len(rows),'summary':summary})
sys.path.insert(0,str(base/'IoTDB-FS-C/tests/benchmarks/hdf5_query'))
from datetime import datetime,timedelta,timezone

def native_utc_point(path):
 with h5py.File(path,'r') as f:
  units=f['time'].attrs['units']
  if isinstance(units,bytes):units=units.decode()
  assert units.startswith('minutes since ')
  epoch=datetime.strptime(units[len('minutes since '):],'%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
  minute=int(f['time'][0])
  return (int((epoch+timedelta(minutes=minute)).timestamp()),)
import h5py
gs=[]
for p in sorted((base/'public-arrow-baseline/geos-time-20261007/raw-scale').glob('*.nc4')):
 with h5py.File(p,'r') as f:
  structure={}
  def visit(name,obj):
   if isinstance(obj,h5py.Dataset):structure[name]=[list(obj.shape),str(obj.dtype),list(obj.chunks) if obj.chunks else None,obj.compression]
  f.visititems(visit)
  dims=[]
  for axis in f['T'].dims:
   dims.append([scale.name for scale in axis.values()])
  gs.append({'path':str(p),'bytes':p.stat().st_size,'structure':structure,'T_scales':dims,'utc':native_utc_point(p)[0]})
for g in gs[1:]:
 g['same_structure_as_first']=g['structure']==gs[0]['structure']
 del g['structure']
print(json.dumps({'era5':result,'geos_structure_and_time':gs},sort_keys=True))
