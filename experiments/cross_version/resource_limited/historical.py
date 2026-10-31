"""Read accepted historical samples without making them new strict run records.

A ratio always uses both arms from one original retained mount/campaign block.
No endpoint minimum is paired with a different block, no run ID is invented,
and different engines/cache/delivery conventions retain separate protocols.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path

WORK = Path('experiments/cross_version/work')
GRIB_CASE = '241e127c3583b454ba3149968b0f4e1a9643d153ed80d0f63e7123536082fcbc'
HDF_CASE = 'e3bbf388a538a86f11e6b66245172c9ac20df3fc24b9914eb832d1776f88bd98'
SOURCES = (
    ('grib', WORK/'grib-global-index-20261020/public-ab/final/queries.jsonl',
     'da5d8a7e2be6606250ea7ce4d097745cc66859df0b62f1696cae09dbd4d303a3',
     WORK/'grib-global-index-20261020/public-ab/final/report.json'),
    ('hdf', WORK/'hdf5-two-modes-20261019/final/queries.jsonl',
     '920e6c59a6667753d35303458cf32f17002b847b1ae56663c6b756fdfd43f45e',
     WORK/'hdf5-two-modes-20261019/final/report.json'),
    ('hdf', WORK/'hdf5-two-modes-20261019/reverse-final/queries.jsonl',
     '3625fd344ad99b5b14178bdacae2be0e73efb29863faa76372e2bbf1ac650170',
     WORK/'hdf5-two-modes-20261019/reverse-final/report.json'),
    ('large', WORK/'large-file-upper-r3-20261017/verified-evidence/run/queries.jsonl',
     'a0a3a69831ff6b5a690579e04428d058c307ab34924624ead1db84a636bdd1f9',
     WORK/'large-file-upper-r3-20261017/report.json'),
)


def _need(ok, message):
    if not ok: raise ValueError(message)


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _scenes(documents):
    documents = list(documents.values()) if isinstance(documents,dict) else list(documents)
    result = {}
    for item in documents:
        # Support a normalized public scene or large_scene's {IDs,scene} return.
        scene = dict(item.get('scene',{}),**{k:v for k,v in item.items() if k!='scene'})
        case = scene.get('case_id'); corpus = scene.get('corpus_id')
        _need(isinstance(case,str) and len(case)==64 and isinstance(corpus,str) and len(corpus)==64,
              'historical mapping requires actual strict case and corpus IDs')
        if case == GRIB_CASE: key='grib'
        elif case == HDF_CASE: key='hdf'
        elif scene.get('format')=='grib' and scene.get('N')==8 and scene.get('V_stored',scene.get('V'))==8:
            _need(scene.get('selection') in ('all',{'kind':'all'}), 'large history must match full V8 selection')
            key='large'
        else: continue
        _need(key not in result, 'duplicate historical scene identity')
        result[key]=scene
    _need(set(result)=={'grib','hdf','large'}, 'all three explicit scene identities required')
    return result


def _block(scene, artifact, report_artifact, label, rows, fs_arm, io_arm, protocols,
           *, campaign=None, measured_repeats=None):
    samples = {'FS-index':[], 'IoTDB-FS':[]}; originals={a:[] for a in samples}
    seen=set()
    for line,row in rows:
        original_arm=row.get('arm',row.get('system'))
        if original_arm not in (fs_arm,io_arm):continue
        arm='FS-index' if original_arm==fs_arm else 'IoTDB-FS'
        if campaign is not None and row.get('campaign')!=campaign:continue
        repeat=row.get('repeat')
        _need(type(repeat) is int and repeat>=0, 'invalid historical repeat')
        identity=(arm,row.get('campaign',0),repeat)
        _need(identity not in seen, 'duplicate historical arm/campaign/repeat');seen.add(identity)
        _need(row.get('oracle_verified') is True and row.get('ok',True) is True,
              'failed/unverified historical row')
        duration=row.get('total_ns')
        _need(type(duration) is int and duration>0, 'invalid original full clock')
        originals[arm].append({'line':line,'campaign':row.get('campaign',0),'repeat':repeat,
                               'total_ns':duration,'warmup':repeat==0})
        if repeat>0:samples[arm].append(duration)
    _need(len(samples['FS-index'])==len(samples['IoTDB-FS'])>0, 'missing historical measured arm')
    sets={a:{(r['campaign'],r['repeat']) for r in originals[a]} for a in originals}
    _need(sets['FS-index']==sets['IoTDB-FS'], 'unpaired historical repeat set')
    if measured_repeats is not None:
        campaigns={c for c,r in sets['FS-index']}
        _need(sets['FS-index']=={(c,r) for c in campaigns for r in range(measured_repeats+1)},
              'incomplete original historical sample matrix')
    return dict(scene_id=scene.get('scene_id'),case_id=scene['case_id'],corpus_id=scene['corpus_id'],
        block_id=artifact['path']+'#'+label,arm_protocols=protocols,samples_ns=samples,
        source_evidence={'artifact':artifact['path'],'sha256':artifact['sha256'],
                         'publication_report':report_artifact,'original_rows':originals},
        resource_controlled=False,strict_run_ids_available=False,
        scope='original same-mount/campaign descriptive samples; no current strict run or resource equivalence')


def _protocols(kind, label, report, rows):
    if kind=='grib':
        binary={r['binary_sha256'] for _,r in rows if r.get('arm')=='IoTDB-FS'}
        _need(len(binary)==1,'historical GRIB block mixes binary identities')
        fs=dict(engine='SQLite plus ecCodes native',clock='native internal owned C++ output; digest/JSON/pipe excluded',
                cache='retained native worker; first/cell excluded',delivery='owned C++ Float32 vector and validity mask')
        io=dict(engine='IoTDB-FS GRIB '+next(iter(binary)),
                clock='full client IPC plus owned NumPy; profile harvest/oracle excluded',
                cache='retained owner/query caches; first/cell excluded',delivery='owned NumPy Float32 and uint8 mask')
    elif kind=='hdf':
        native=label=='native-hdf5'
        binary=report['baseline_sha256'] if label=='baseline-r3' else report['candidate_sha256']
        fs=dict(engine='SQLite plus native HDF5 '+report['native_helper_sha256'],
                clock='full native selection/read into owned NumPy; oracle excluded',
                cache='bounded retained native HDF5 context; first/campaign excluded',delivery='owned NumPy Float32 and uint8 mask')
        io=dict(engine=('IoTDB-FS native-HDF5 VFD' if native else 'IoTDB-FS indexed HDF5')+' '+binary,
                clock='full client IPC plus owned NumPy; profile harvest/oracle excluded',
                cache='retained owner '+label+'; first/campaign excluded',delivery='owned NumPy Float32 and uint8 mask')
    else:
        fs=dict(engine='SQLite plus ecCodes native '+report['native_sha256'],
                clock='native worker internal full query; digest/JSON/pipe excluded',
                cache='retained native worker; first/campaign excluded',delivery='owned C++ Float32 vector and validity mask')
        io=dict(engine='IoTDB-FS V04 large GRIB frozen R3',
                clock='full client IPC plus owned NumPy; profile harvest/oracle excluded',
                cache='retained per-campaign owner/query caches; first/campaign excluded',delivery='owned NumPy Float32 and uint8 mask')
    return {'FS-index':fs,'IoTDB-FS':io}


def build_history(repo, scene_documents):
    """Return blocks accepted by analysis.historical_minima and source facts.

    Only existing local JSON/JSONL files are read. Published corpus identity is
    supplied explicitly, with exact known case IDs for the first two scenes.
    The large-scene ID must come from its registered full N8/V8 case document.
    """
    repo=Path(repo);scenes=_scenes(scene_documents);blocks=[];facts=[]
    for kind,relative,expected,report_relative in SOURCES:
        path=repo/relative;report_path=repo/report_relative
        _need(path.is_file() and _sha(path)==expected,'historical raw query artifact missing or changed: '+str(relative))
        report=json.loads(report_path.read_text())
        _need(report.get('complete') is True and report.get('original_service_restored') is True and
              report['queries_sha256']==expected,'historical report incomplete or names different queries')
        rows=[(i+1,json.loads(line)) for i,line in enumerate(path.read_text().splitlines())]
        artifact={'path':relative.as_posix(),'sha256':expected}
        report_artifact={'path':report_relative.as_posix(),'sha256':_sha(report_path)}
        if kind=='grib':
            selected=[(i,r) for i,r in rows if r.get('format')=='grib' and r.get('phase')=='discover-values']
            for label in sorted({r['variant'] for _,r in selected}):
                group=[(i,r) for i,r in selected if r['variant']==label]
                blocks.append(_block(scenes[kind],artifact,report_artifact,label,group,'FS-index','IoTDB-FS',
                    _protocols(kind,label,report,group),measured_repeats=5))
        elif kind=='hdf':
            for label in sorted({r['binary'] for _,r in rows}):
                group=[(i,r) for i,r in rows if r['binary']==label]
                _need(all(r.get('N')==128 and r.get('backing_N')==512 and r.get('V')==1 and
                          r.get('selection')=='latest' and r.get('owned_output_bytes')==40960 for _,r in group),
                      'HDF history does not match N512/B4/C128 case')
                blocks.append(_block(scenes[kind],artifact,report_artifact,label,group,'FS-index','typed',
                    _protocols(kind,label,report,group),measured_repeats=2))
        else:
            selected=[(i,r) for i,r in rows if r.get('format')=='grib' and r.get('scenario')=='identical' and
                       r.get('V')==8 and r.get('selection')=='all' and r.get('system') in ('V04','FS-index')]
            _need(len(selected)==40,'large original comparison matrix differs')
            for campaign in range(4):
                blocks.append(_block(scenes[kind],artifact,report_artifact,'campaign-'+str(campaign),selected,
                    'FS-index','V04',_protocols(kind,'V04',report,selected),campaign=campaign,measured_repeats=4))
        facts.append({'kind':kind,'artifact':artifact,'report':report_artifact,'original_query_rows':len(rows)})
    return dict(protocol='iotdbfs-resource-limited-descriptive-history/1',blocks=blocks,source_facts=facts,
        limitations=['original time/resource conditions were uncontrolled; no fake strict run IDs',
            'same-block ratios only; native C++ and client NumPy delivery differ for GRIB',
            'native-HDF5 VFD is a separate engine group; no prototype/FUSE result pooled with indexed',
            'IoTDB-only later large-GRIB observations are excluded because no contemporaneous FS control exists',
            'historical mount samples are descriptive, not independent replicated current A/B runs'])
