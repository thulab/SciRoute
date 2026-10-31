#!/usr/bin/env python3
"""Run the unchanged HF model/sampler with an explicitly experimental reader.

Canonical source/sample/normalization identities in the Trainer report refer to
PB00/PB01; the separate derived_artifact field identifies physical representation.
"""
import argparse
import copy
import json
from pathlib import Path
import numpy as np
from derived_window_reader import DerivedWindowReader
from prepare_public_windows import seal
from public_iotdbfs import WindowArrays
from run_electricity_derived import daemon_environment
from train_hf import run, IoTDBFSBackend

class Backend:
    def __init__(self, reader, normalization, windows, channels):
        self.reader=reader;self.channels=channels
        self.length=windows['input_length'];self.horizon=windows['prediction_length']
        self.mean=np.asarray([normalization['statistics'][c]['mean'] for c in channels],dtype=np.float64)
        self.std=np.asarray([normalization['statistics'][c]['stddev'] for c in channels],dtype=np.float64)
        self.observations=[]
        self.initial_metrics = dict(reader.metrics)
    def read_windows(self, anchors, split):
        values,valid=self.reader.read_windows([a-self.length for a in anchors],self.length+self.horizon,self.channels)
        scaled=((values.astype(np.float64)-self.mean)/self.std).astype(np.float32)
        scaled[~valid]=0
        self.observations.append(dict(split=split,anchors=list(anchors),metrics=dict(self.reader.metrics)))
        return WindowArrays(np.ascontiguousarray(scaled[:,:self.length,:]),
            np.ascontiguousarray(valid[:,:self.length,:]),np.ascontiguousarray(scaled[:,self.length:,:]),
            np.ascontiguousarray(valid[:,self.length:,:]))
    @property
    def metrics(self):return dict(setup_seconds=self.reader.setup_seconds,initial_metrics=self.initial_metrics,totals={k:v-self.initial_metrics.get(k,0) for k,v in self.reader.metrics.items()},requests=self.observations)
    def close(self):self.reader.close()

def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('work-dir','manifest','mount','socket','helper','report','output-dir'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--daemon-pid',type=int,required=True)
    p.add_argument('--layout',choices=['channel_major','step_major'],required=True)
    p.add_argument('--block-steps',type=int,required=True)
    p.add_argument('--codec',required=True)
    p.add_argument('--mode',choices=['local','read','fuse','iotdb','typed'],required=True)
    p.add_argument('--model',choices=['patchtst','patchtsmixer'],default='patchtst')
    p.add_argument('--max-steps',type=int,default=50)
    p.add_argument('--warm',action='store_true',help='prefill the exact frozen training trace, outside training time')
    p.add_argument('--trace',type=Path)
    args=p.parse_args()
    found=[v for v in json.loads(args.manifest.read_text()) if (v['layout'],v['block_steps'],v['codec'])==(args.layout,args.block_steps,args.codec)]
    if len(found)!=1:raise ValueError('derived artifact must resolve exactly once')
    item=found[0]
    env=daemon_environment(args.daemon_pid,args.mount)
    args.dataset='UCI_Electricity_370';args.channels=None;args.batch_size=2;args.seed=20260929
    args.expected_gpus=1;args.eval_windows=4;args.train_windows=0;args.cache_bytes=1024**3
    args.registration_mode='prepared_compact';args.conversion_mode='buffers'
    args.backend='local' if args.mode in ('local','read') else 'iotdbfs'
    args.source_version=None if args.backend=='local' else item['version']
    args.output_dir.mkdir(parents=True,exist_ok=True)
    def factory(pb00,normalization,windows,channels):
        if args.mode=='typed':
            declaration=copy.deepcopy(pb00)
            declaration['arrow'].update(path=item['logical'].lstrip('/'),sha256=item['sha256'],bytes=item['bytes'],
                rows=item['rows'],batch_row_counts=[1]*len(item['rows']),compression=item['codec'],value_layout=item['layout'])
            physical_windows=seal(dict(windows,arrow_sha256=item['sha256']))
            backend=IoTDBFSBackend(args.socket,args.mount,declaration,normalization,physical_windows,channels,
                                  item['version'],'prepared_compact','buffers')
        else:
            reader=DerivedWindowReader(item,mode=args.mode,helper=args.helper,config=env['IOTDB_CONFIG_FILE'],env=env,
                log=args.report.with_suffix('.bridge.log'),max_cache_bytes=args.cache_bytes)
            backend=Backend(reader,normalization,windows,channels)
        if args.warm:
            if args.trace is None:raise ValueError('warm needs exact trace')
            trace=json.loads(args.trace.read_text())['backend_metrics']['train']['request_timings']
            for row in trace:backend.read_windows(row['anchors'],'train')
            if isinstance(backend,Backend):
                backend.observations=[]
                backend.initial_metrics=dict(backend.reader.metrics)
        return backend
    report=run(args,backend_factory=factory)
    report['derived_artifact']={k:item[k] for k in ('layout','block_steps','codec','sha256','version','logical','bytes','source')}
    report['experimental_mode']=args.mode;report['temperature']='warm' if args.warm else 'cold'
    report['timer_boundary']='Trainer wall; reader setup excluded; typed first prepare included unless explicit prefill'
    args.report.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ('experimental_mode','temperature','train_wall_seconds','train_loss','eval_loss','fetch_seconds')}),flush=True)

if __name__=='__main__':main()
