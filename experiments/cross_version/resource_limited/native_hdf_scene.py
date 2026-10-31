"""Native-HDF5-only adapters for the two existing HDF scientific cases.

No source publication, index construction, product build, or registry mutation
of an existing corpus. N128's canonical case certifies values only: its absent
historical mask oracle is supplemented by a real independent native read and
kept as separate run evidence. Both outputs remain inside the original clock.
"""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from experiments.identity.model import build_case, need
from experiments.identity.registry import Registry, file_sha, read_json
from . import scenes, hdf_scene
from .resources import atomic_json

N128_CORPUS = '3d7093a94bdb4eed2cfce2b6c22bc758eafe5ab21b320573dfe975781038c0dc'


def prepare(repo, registry):
    """Read existing evidence; physical inspection does not access the DB."""
    repo = Path(repo)
    source = Registry(repo/scenes.IDENTITY)
    corpus = source.get_corpus(N128_CORPUS)
    registry.register_corpus(corpus)
    params = corpus['parameters']
    request = dict(scope=dict(kind='all', buckets=[], selection_basis='complete independently published population',
        logical_files=sorted(r['logical_file'] for r in corpus['inventory'])),
        selection={'kind':'latest'}, predicate=deepcopy(params['generation']['query']),
        operation='discover-values', roi=dict(kind='whole',origin=[0,0,0,0],shape=[1,1,64,64]),
        output=dict(projection=['value'],dtype='float32'),
        expected_K=dict(K_files=2,K_file_versions=2,K_units=2))
    case = registry.register_case(build_case(corpus,request))
    accepted = scenes._accepted(repo,scenes.SMALL_PREPARED,scenes.SMALL_PREPARED_SHA)
    old = accepted['baseline']
    template_path = Path(accepted['hdf5_manifest'])
    need(file_sha(template_path)==accepted['hdf5_manifest_sha256'], 'N128 source manifest changed')
    template = read_json(template_path)
    need(template['format']=='hdf5' and len(template['sources'])==128 and
         template['query']==request['predicate'], 'N128 source parameters differ')
    source_map = {r['file']:r for r in template['sources']}
    versions = accepted['hdf5_versions']
    for record in corpus['inventory']:
        raw = source_map.get(record['logical_file'],{})
        need(raw.get('sha256')==record['sha256'] and raw.get('bytes')==record['bytes'] and
             raw.get('units')==record['units'] and
             versions.get(record['binding']['path'])==record['binding']['version'],
             'N128 source byte/unit/publication binding differs')
        need(len(record['units'])==4 and all(u.get('schema',{}).get('shape')==[1,1,64,64]
             for u in record['units']), 'N128 source dataset shape differs')
    need(corpus['publication_evidence']['head']==old['head'], 'N128 owner head differs')
    data = Path(old['data_work'])
    checks = deepcopy(accepted['hdf5_inputs'])
    expected = {str(data/'hdf5-index.sqlite')}
    for record in corpus['inventory']:
        expected.update(str(data/folder/record['logical_file']) for folder in ('hdf5-data','hdf5-fs'))
    need(len(checks)==257 and {e['path'] for e in checks}==expected, 'N128 complete raw/copy/index inventory absent')
    for entry in checks:
        if Path(entry['path']).is_relative_to(data/'hdf5-fs'): entry['nlink']=1
    n128 = scenes._base_scene('hdf5-n128',corpus,case)
    n128.update(repo=str(repo),raw_root=str(data/'hdf5-data'),native_fs_root=str(data/'hdf5-fs/raw'),
        native_index=str(data/'hdf5-index.sqlite'),native_binary=old['native'],native_binary_sha256=old['native_sha256'],
        byte_checks=checks,original_owner=old['owner_journal'],journal_digest=deepcopy(old['journal_digest']),
        prefix=old['prefix'],logical_prefix=corpus['publication_evidence']['bindings'][0]['path'].split('/hdf5/raw/',1)[0]+'/hdf5/raw',
        head=deepcopy(old['head']),
        native_index_build=deepcopy(accepted['hdf5_index_build_inherited']),
        frozen_runner=old['runner'],frozen_runner_sha256=old['runner_sha256'],
        frozen_inputs={'template':template,'versions':deepcopy(versions)},
        source_manifest=str(template_path),source_manifest_sha256=accepted['hdf5_manifest_sha256'],
        frozen_manifest=accepted['hdf5_frozen_manifest'],
        frozen_manifest_sha256=accepted['hdf5_frozen_manifest_sha256'],
        preparation_evidence={'path':str(repo/scenes.SMALL_PREPARED),'sha256':scenes.SMALL_PREPARED_SHA},
        canonical_case_certifies='values only; historical mask oracle absent',
        supplemental_mask_required=True,origin=[0,0,0,0],shape=[1,1,64,64],
        native_cache_protocol='persistent SQLite worker; H5F/H5D opened and closed per matching unit per query; no persistent HDF chunk cache',
        clock={'IoTDB-FS':'frozen small_runner client discovery through owned NumPy Float32/mask; profile wait and oracle excluded',
               'FS-index':'native C++ owned Float32/mask; digest, JSON and pipe excluded'})
    c128 = hdf_scene.prepare_hdf(repo,registry)
    c128.update(scenes._base_scene('hdf5-c128',registry.get_corpus(c128['corpus_id']),registry.get_case(c128['case_id'])))
    c128.update(native_binary=c128['native_library'],native_binary_sha256=c128['native_sha256'],
        canonical_case_certifies='values and mask',supplemental_mask_required=False,
        native_cache_protocol='persistent route3 H5F/H5D handles (8/8), raw chunk cache 262144 bytes',
        clock={'IoTDB-FS':'frozen route3 client discovery through owned NumPy Float32/mask; profile wait and oracle excluded',
               'FS-index':'route3 Python selection through native C ABI NumPy-owned Float32/mask'})
    for scene in (n128,c128):
        scene['cache']={'IoTDB-FS':'retained daemon and global candidate/authorization state; fresh source-backed native HDF driver per value query, raw chunk cache disabled',
                        'FS-index':scene['native_cache_protocol']}
    return [n128,c128]


def inspect(scene, registry):
    if scene['scene_id']=='hdf5-c128': return hdf_scene.inspect_hdf(scene,registry)
    need(scene['scene_id']=='hdf5-n128', 'unsupported native HDF scene')
    need(file_sha(scene['source_manifest'])==scene['source_manifest_sha256'], 'N128 source manifest changed')
    need(file_sha(scene['frozen_runner'])==scene['frozen_runner_sha256'] and
         file_sha(scene['frozen_manifest'])==scene['frozen_manifest_sha256'], 'N128 frozen query artifact changed')
    evidence=scene['preparation_evidence']
    need(file_sha(evidence['path'])==evidence['sha256'], 'N128 accepted preparation changed')
    # The shared generic inspector verifies all128 raw,128 independent FS copies,
    # SQLite bytes, pinned native binary, original owner journal and case binding.
    return scenes.inspect(scene,registry)


def open_native(scene, paired, env=None):
    if scene['scene_id']=='hdf5-c128':
        return scenes.NativeHandle(hdf_scene.open_hdf_native(scene,paired,env),paired,'hdf5')
    need(scene['scene_id']=='hdf5-n128','unsupported native HDF scene')
    return scenes.NativeHandle(paired.NativeWorker(Path(scene['native_binary']),env),paired,'hdf5-n128')


def _native_request(scene):
    return dict(op='query',format='hdf5',mode='index',values=True,
        root=scene['native_fs_root'],index=scene['native_index'],predicate=scene['predicate'])


def supplement_mask(scene, native, paired, receipt_path):
    """Real, timed-outside native read of the two predefined N128 matches.

    Never fabricates a mask digest or derives expected evidence from IoTDB-FS.
    Does not alter old manifests/corpora/cases; augmentation is a private copy.
    """
    if scene['scene_id']=='hdf5-c128':
        receipt=dict(protocol='iotdbfs-native-hdf5-canonical-mask-evidence/1',
            corpus_id=scene['corpus_id'],case_id=scene['case_id'],
            expected_matches=deepcopy(scene['expected_matches']),
            new_oracle_generated=False,query_timed=False,old_corpus_modified=False,
            canonical_case_certifies='values and mask; existing independent native ROI oracle')
        atomic_json(Path(receipt_path),receipt)
        scene['supplemental_mask_receipt']={'path':str(receipt_path),'sha256':file_sha(receipt_path)}
        return receipt
    need(scene['scene_id']=='hdf5-n128' and not scene.get('supplemental_mask_receipt'),
         'mask supplementation requires a fresh N128 scene')
    template=deepcopy(scene['frozen_inputs']['template'])
    wanted=paired.expected(template)
    row=native.reader.call(_native_request(scene))
    actual={(r['path'],r['dataset']):r for r in row['units']}
    need(row.get('ok') is True and len(actual)==len(row['units'])==2 and set(actual)==set(wanted),
         'independent native mask probe selected different units')
    records=[]
    inventory={r['logical_file']:r for r in scene['expected_matches']}
    for key,unit in wanted.items():
        got=actual[key]; old=unit['oracle']
        # This pinned native worker emits row count and hashes, not shape.
        # Shape remains independently sealed in the source manifest/schema.
        need(got['rows']==old['elements'] and got['value_sha256']==old['value_sha256'],
             'native supplementary values/count differ from frozen oracle')
        mask=got.get('mask_sha256')
        need(isinstance(mask,str) and len(mask)==64 and all(c in '0123456789abcdef' for c in mask),
             'native supplementary mask hash absent')
        unit['oracle']['mask_sha256']=mask
        binding=inventory['raw/'+key[0]]['binding']
        records.append(dict(relative=key[0],dataset=key[1],elements=got['rows'],shape=scene['shape'],
            value_sha256=got['value_sha256'],mask_sha256=mask,binding=binding))
    receipt=dict(protocol='iotdbfs-native-hdf5-supplemental-mask/1',corpus_id=scene['corpus_id'],
        case_id=scene['case_id'],source_manifest_sha256=scene['source_manifest_sha256'],
        native_binary_sha256=scene['native_binary_sha256'],native_index_sha256=file_sha(scene['native_index']),
        source_byte_inventory=deepcopy(scene['byte_checks']),units=records,
        independent_native_read=True,query_timed=False,old_corpus_modified=False,
        canonical_case_certifies=scene['canonical_case_certifies'],
        scope='two predefined matching whole datasets only; no claim to bind historical mask case')
    atomic_json(Path(receipt_path),receipt)
    scene['supplemental_mask_receipt']={'path':str(receipt_path),'sha256':file_sha(receipt_path)}
    scene['frozen_inputs']['template']=template
    return receipt


def assert_native_mode(row):
    """Fail closed if a typed API silently used the indexed execution engine."""
    calls=row.get('value_calls') or []
    need(len(calls)==2,'native HDF query must retain two exact value calls')
    for call in calls:
        profile=call.get('server_profile',{})
        need(profile.get('server_hdf5_execution',{}).get('mode')=='native-hdf5',
             'actual HDF execution mode is not native-hdf5')
        stages=profile.get('stages_ns',{}); counters=profile.get('counters',{})
        need(stages.get('hdf5.native.setup',0)>0 and stages.get('hdf5.native.read',0)>0,
             'native HDF setup/read functional proof absent')
        need(counters.get('hdf5.native.slabs',0)>0 and counters.get('hdf5.native.source.calls',0)>0,
             'native HDF slab/source functional proof absent')
        need(not counters.get('hdf5.selection.decoder_compiles',0) and
             not counters.get('hdf5.layout.pages.calls',0), 'indexed HDF selection used in native-only run')
        delta=profile.get('runtime_delta',{})
        need(all(not delta.get(k,0) for k in ('decode_count','decompressions','redecodes','hits','misses')),
             'shared indexed decoded-chunk runtime used in native-only run')
    row['actual_native_hdf5_verified']=True
    row['native_mode_verified']=True
    return row


def query_native(native, scene):
    if scene['scene_id']=='hdf5-c128':
        row=hdf_scene.query_hdf_native(native.reader,scene)
    else:
        need(scene.get('supplemental_mask_receipt'),'N128 independent mask evidence required before timing')
        row=native.reader.call(_native_request(scene))
        template=scene['frozen_inputs']['template']
        native.paired.check_native(row,template,native.paired.expected(template),True)
        row.update(oracle_verified=True,selected_owned_rows=sum(u['rows'] for u in row['units']),
            owned_output_bytes=scene['owned_output_bytes'],
            discovery_client_ns=row['discovery_ns'],selected_value_client_ns=row['value_read_ns'],
            discovery_clock_is_residual=True)
    row.update(scene_id=scene['scene_id'],corpus_id=scene['corpus_id'],case_id=scene['case_id'],
        actual_matches=deepcopy(scene['expected_matches']),
        supplemental_mask_verified=bool(scene.get('supplemental_mask_receipt')))
    return row


def query_iotdb(paired, context, scene, offset=0):
    if scene['scene_id']=='hdf5-c128':
        row,offset=hdf_scene.query_hdf_iotdb(paired,context,scene,offset)
    else:
        need(scene.get('supplemental_mask_receipt'),'N128 independent mask evidence required before timing')
        frozen=scene['frozen_inputs']
        row,offset=paired.iotdb_query(context,frozen['template'],scene['logical_prefix'],frozen['versions'],offset,True)
        row.update(oracle_verified=True,selected_owned_rows=8192,owned_output_bytes=scene['owned_output_bytes'])
    assert_native_mode(row)
    row.update(scene_id=scene['scene_id'],corpus_id=scene['corpus_id'],case_id=scene['case_id'],
        actual_matches=deepcopy(scene['expected_matches']),
        supplemental_mask_verified=bool(scene.get('supplemental_mask_receipt')))
    return row,offset
