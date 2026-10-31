
import os,json,subprocess,shutil
from pathlib import Path
from collections import Counter
import h5py,numpy as np
base=Path('/data_01/zzy/iotdb-fs-exp')
roots=[base/'public-arrow-baseline',base/'era5-grib-baseline']
exts={'.h5','.hdf5','.he5','.nc4','.nc','.grib','.grib2','.grb','.grb2'}
inventory=[]
for root in roots:
 if not root.exists():continue
 for directory,dirs,files in os.walk(root,followlinks=False):
  if len(Path(directory).relative_to(root).parts)>=5:dirs[:]=[]
  dirs[:]=[n for n in dirs if not n.startswith('.') and n not in ('venv','.venv','build','site-packages')]
  for n in files:
   p=Path(directory)/n
   if p.suffix.lower() in exts:
    st=p.stat()
    inventory.append({'path':str(p),'bytes':st.st_size,'inode':st.st_ino,'device':st.st_dev,'symlink':p.is_symlink()})
geosdir=base/'public-arrow-baseline/geos-time-20261007/raw-scale'
geos=[]
for p in sorted(geosdir.glob('*.nc4')):
 with h5py.File(p,'r') as f:
  datasets=[]
  def visit(name,obj):
   if isinstance(obj,h5py.Dataset):
    attrs={}
    for k in ('standard_name','units','calendar'):
     if k in obj.attrs:
      v=obj.attrs[k]
      if isinstance(v,(bytes,np.bytes_)):v=v.decode('utf-8','replace')
      if isinstance(v,(str,int,float)):attrs[k]=v
    datasets.append({'path':'/'+name,'shape':list(obj.shape),'dtype':str(obj.dtype),'chunks':list(obj.chunks) if obj.chunks else None,'compression':obj.compression,'attrs':attrs})
  f.visititems(visit)
  time=f['time'][...].tolist()
  geos.append({'path':str(p),'bytes':p.stat().st_size,'datasets':datasets,'time_values':time})
grib=[]
for p in sorted((base/'datasets/grib').glob('*.grib2')):
 cp=subprocess.run(['grib_ls','-x','-j','-p','offset:i,totalLength:i,shortName,typeOfLevel,level:i,step:i,dataDate:i,dataTime:i,validityDate:i,validityTime:i,Ni:i,Nj:i,gridType,packingType,bitsPerValue:i',str(p)],capture_output=True,text=True,timeout=90,check=True)
 rows=json.loads(cp.stdout)['messages']
 summary={}
 for k in ('shortName','typeOfLevel','level','step','dataDate','dataTime','validityDate','validityTime','Ni','Nj','gridType','packingType','bitsPerValue'):
  c=Counter(str(r.get(k)) for r in rows)
  summary[k]={'distinct':len(c),'counts':dict(sorted(c.items()))}
 grib.append({'path':str(p),'bytes':p.stat().st_size,'messages':len(rows),'summary':summary,'indexed_message_bytes':sum(int(r['totalLength']) for r in rows),'messages_step0_level500':sum(r['step']==0 and r['level']==500 for r in rows)})
synthetic=[]
raw=base/'IoTDB-FS-C/experiments/single_file/work/prep-20261012/data/raw_files'
for p in sorted(raw.glob('*.h5')):
 with h5py.File(p,'r') as f:
  d=f['T']
  synthetic.append({'path':str(p),'bytes':p.stat().st_size,'root_objects':list(f),'shape':list(d.shape),'chunks':list(d.chunks),'compression':d.compression,'shuffle':d.shuffle,'allocated_chunks':d.id.get_num_chunks(),'attrs':{k:str(v) for k,v in d.attrs.items()}})
out={'protocol':'iotdbfs-multifile-feasibility-audit/1','h5py_version':h5py.__version__,'hdf5_version':h5py.version.hdf5_version,'disk_free_bytes':{p:shutil.disk_usage(p).free for p in ('/data_01','/data_02')},'additional_inventory':inventory,'geos':geos,'grib':grib,'synthetic':synthetic}

out['additional_inventory_count']=len(inventory)
out['additional_inventory']=[x for x in inventory if x['bytes']>=50000000]
if len(out['additional_inventory'])>100:raise RuntimeError('unexpectedly large raw inventory')
first=out['geos'][0] if out['geos'] else None
for row in out['geos'][1:]:
 row['same_datasets_as_first']=row['datasets']==first['datasets']
 del row['datasets']
for row in out['grib']:
 for k in ('step','validityDate','validityTime'):
  counts=row['summary'][k]['counts']
  row['summary'][k]['count_values']=sorted(set(counts.values()))
  row['summary'][k]['min']=min(map(int,counts))
  row['summary'][k]['max']=max(map(int,counts))
  del row['summary'][k]['counts']
print(json.dumps(out,sort_keys=True))
